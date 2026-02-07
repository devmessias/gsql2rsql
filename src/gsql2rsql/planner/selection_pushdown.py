"""Selection Pushdown Optimizer for Logical Plan.

Pushes predicates from Selection operators into DataSource operators
to reduce the number of rows processed in joins.

See subquery_flattening.py module docstring for the broader optimization context.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gsql2rsql.parser.ast import QueryExpression, QueryExpressionBinary, QueryExpressionProperty
from gsql2rsql.parser.operators import (
    BinaryOperator,
    BinaryOperatorInfo,
    BinaryOperatorType,
    Function,
)
from gsql2rsql.planner.operators import (
    DataSourceOperator,
    JoinOperator,
    JoinType,
    LogicalOperator,
    RecursiveTraversalOperator,
    SelectionOperator,
)

if TYPE_CHECKING:
    from gsql2rsql.planner.logical_plan import LogicalPlan


@dataclass
class PushdownStats:
    """Statistics about selection pushdown operations performed."""

    predicates_pushed: int = 0
    predicates_remaining: int = 0
    selections_removed: int = 0

    def __str__(self) -> str:
        return (
            f"PushdownStats("
            f"pushed={self.predicates_pushed}, "
            f"remaining={self.predicates_remaining}, "
            f"selections_removed={self.selections_removed})"
        )


# Class constant for volatile functions
VOLATILE_FUNCTIONS: set[Function] = {
    Function.RAND,
    Function.DATE,
    Function.DATETIME,
    Function.LOCALDATETIME,
    Function.TIME,
    Function.LOCALTIME,
}


class SelectionPushdownOptimizer:
    """Pushes predicates from Selection operators into DataSource operators.

    This optimizer analyzes Selection operators and pushes predicates that
    reference only a single entity (node or relationship) down to the
    corresponding DataSourceOperator. This is especially important for
    undirected relationships where filters should be applied before joins.

    The optimization is based on the relational algebra equivalence:

        sigma_{p(A)}(A join B) = sigma_{p(A)}(A) join B

    For AND conjunctions:
        sigma_{p(A) AND q(B)}(A join B) = sigma_{p(A)}(A) join sigma_{q(B)}(B)

    This is NOT valid for OR:
        sigma_{p(A) OR q(B)}(A join B) != sigma_{p(A)}(A) join sigma_{q(B)}(B)
    """

    def __init__(self, enabled: bool = True) -> None:
        """Initialize the optimizer.

        Args:
            enabled: If False, optimize() becomes a no-op.
        """
        self.enabled = enabled
        self.stats = PushdownStats()

    def optimize(self, plan: LogicalPlan) -> None:
        """Apply selection pushdown optimization to the logical plan.

        Modifies the plan IN-PLACE.

        Args:
            plan: The logical plan to optimize.
        """
        if not self.enabled:
            return

        self.stats = PushdownStats()

        # Collect all operators
        all_operators: list[LogicalOperator] = []
        for start_op in plan.starting_operators:
            for op in start_op.get_all_downstream_operators(LogicalOperator):  # type: ignore[type-abstract]
                if op not in all_operators:
                    all_operators.append(op)

        # Find all Selection operators
        selections = [op for op in all_operators if isinstance(op, SelectionOperator)]

        # Process each Selection
        for selection in selections:
            self._try_push_selection(selection, plan)

    def _try_push_selection(
        self, selection: SelectionOperator, plan: LogicalPlan
    ) -> None:
        """Try to push a Selection's predicates into DataSource operators.

        Algorithm:
            1. Split the filter expression into individual AND conjuncts
            2. Group conjuncts by the variable(s) they reference
            3. For each single-variable group:
               - Find the target DataSource
               - Push the combined predicates to that DataSource
            4. Reconstruct the Selection with remaining predicates (if any)
               or remove it entirely

        Args:
            selection: The Selection operator to analyze.
            plan: The full logical plan (for finding DataSources).
        """
        if not selection.filter_expression:
            return

        # Step 1: Split AND conjunctions
        conjuncts = self._split_and_conjunctions(selection.filter_expression)

        # Step 2: Group predicates by variable
        groups = self._group_predicates_by_variable(conjuncts)

        # Step 3: Push each single-variable group to its DataSource
        pushed_any = False
        remaining_predicates: list[QueryExpression] = []

        for var_name, predicates in groups.items():
            if var_name is None:
                # Multi-variable predicates cannot be pushed
                remaining_predicates.extend(predicates)
                self.stats.predicates_remaining += len(predicates)
                continue

            # Find the DataSourceOperator for this variable
            target_ds = self._find_datasource_for_variable(var_name, plan, selection)
            if not target_ds:
                # Cannot find target or target is in recursive path
                remaining_predicates.extend(predicates)
                self.stats.predicates_remaining += len(predicates)
                continue

            # Combine all predicates for this variable with AND
            combined = self._combine_predicates_with_and(predicates)
            if combined is None:
                continue

            # Push to the DataSource
            self._push_predicate_to_datasource(combined, target_ds)
            self.stats.predicates_pushed += len(predicates)
            pushed_any = True

        # Step 4: Handle remaining predicates or remove Selection
        if not remaining_predicates:
            # All predicates were pushed - remove the Selection entirely
            if pushed_any:
                self._remove_selection(selection)
                self.stats.selections_removed += 1
        else:
            # Some predicates couldn't be pushed - update the Selection
            if pushed_any:
                # Reconstruct filter with only the remaining predicates
                new_filter = self._combine_predicates_with_and(remaining_predicates)
                selection.filter_expression = new_filter

    # =========================================================================
    # Conjunction Splitting Helpers
    # =========================================================================

    def _split_and_conjunctions(
        self, expr: QueryExpression
    ) -> list[QueryExpression]:
        """Split an expression into individual AND conjuncts.

        Recursively splits AND expressions into a flat list of predicates.
        Does NOT split OR expressions (they must be kept together).

        Examples:
            "A AND B AND C" -> [A, B, C]
            "A AND (B OR C)" -> [A, (B OR C)]
            "A OR B" -> [A OR B]  (not split)
        """
        if isinstance(expr, QueryExpressionBinary):
            if (
                expr.operator is not None
                and expr.operator.name == BinaryOperator.AND
                and expr.left_expression is not None
                and expr.right_expression is not None
            ):
                left_conjuncts = self._split_and_conjunctions(expr.left_expression)
                right_conjuncts = self._split_and_conjunctions(expr.right_expression)
                return left_conjuncts + right_conjuncts

        return [expr]

    def _group_predicates_by_variable(
        self, predicates: list[QueryExpression]
    ) -> dict[str | None, list[QueryExpression]]:
        """Group predicates by the variable they reference.

        Predicates referencing a single variable are grouped under that variable.
        Predicates referencing multiple variables or containing unsafe functions
        are grouped under None.
        """
        groups: dict[str | None, list[QueryExpression]] = {}

        for pred in predicates:
            # Safety Check 1: Volatile functions
            if self._contains_volatile_function(pred):
                groups.setdefault(None, []).append(pred)
                continue

            # Safety Check 2: Aggregation functions
            if self._contains_aggregation_function(pred):
                groups.setdefault(None, []).append(pred)
                continue

            # Safety Check 3: Correlated subqueries
            if self._contains_correlated_subquery(pred):
                groups.setdefault(None, []).append(pred)
                continue

            # Safety Check 4: Variable count
            properties = self._collect_property_references(pred)
            var_names = {p.variable_name for p in properties}

            if len(var_names) == 1:
                var_name = next(iter(var_names))
                groups.setdefault(var_name, []).append(pred)
            elif len(var_names) == 0:
                groups.setdefault(None, []).append(pred)
            else:
                groups.setdefault(None, []).append(pred)

        return groups

    def _combine_predicates_with_and(
        self, predicates: list[QueryExpression]
    ) -> QueryExpression | None:
        """Combine a list of predicates using AND."""
        if not predicates:
            return None

        if len(predicates) == 1:
            return predicates[0]

        and_op = BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL)

        result = predicates[0]
        for pred in predicates[1:]:
            result = QueryExpressionBinary(
                left_expression=result,
                operator=and_op,
                right_expression=pred,
            )

        return result

    def _push_predicate_to_datasource(
        self, predicate: QueryExpression, target_ds: DataSourceOperator
    ) -> None:
        """Push a predicate to a DataSource's filter_expression."""
        if target_ds.filter_expression is None:
            target_ds.filter_expression = predicate
        else:
            and_op = BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL)
            target_ds.filter_expression = QueryExpressionBinary(
                left_expression=target_ds.filter_expression,
                operator=and_op,
                right_expression=predicate,
            )

    # =========================================================================
    # Safety Check Helpers
    # =========================================================================

    def _collect_property_references(
        self,
        expr: QueryExpression,
    ) -> list[QueryExpressionProperty]:
        """Collect all property references from an expression tree."""
        from gsql2rsql.parser.ast import (
            QueryExpressionAggregationFunction,
            QueryExpressionBinary,
            QueryExpressionCaseExpression,
            QueryExpressionFunction,
            QueryExpressionList,
            QueryExpressionListPredicate,
            QueryExpressionProperty,
            QueryExpressionWithAlias,
        )

        properties: list[QueryExpressionProperty] = []

        if isinstance(expr, QueryExpressionProperty):
            properties.append(expr)
        elif isinstance(expr, QueryExpressionBinary):
            if expr.left_expression:
                properties.extend(self._collect_property_references(expr.left_expression))
            if expr.right_expression:
                properties.extend(self._collect_property_references(expr.right_expression))
        elif isinstance(expr, QueryExpressionFunction):
            for arg in expr.parameters or []:
                properties.extend(self._collect_property_references(arg))
        elif isinstance(expr, QueryExpressionAggregationFunction):
            if expr.inner_expression:
                properties.extend(self._collect_property_references(expr.inner_expression))
        elif isinstance(expr, QueryExpressionCaseExpression):
            if expr.test_expression:
                properties.extend(self._collect_property_references(expr.test_expression))
            for when_expr, then_expr in expr.alternatives or []:
                properties.extend(self._collect_property_references(when_expr))
                properties.extend(self._collect_property_references(then_expr))
            if expr.else_expression:
                properties.extend(self._collect_property_references(expr.else_expression))
        elif isinstance(expr, QueryExpressionList):
            for item in expr.items or []:
                properties.extend(self._collect_property_references(item))
        elif isinstance(expr, QueryExpressionListPredicate):
            if expr.list_expression:
                properties.extend(self._collect_property_references(expr.list_expression))
            if expr.filter_expression:
                properties.extend(self._collect_property_references(expr.filter_expression))
        elif isinstance(expr, QueryExpressionWithAlias):
            if expr.inner_expression:
                properties.extend(self._collect_property_references(expr.inner_expression))
        elif hasattr(expr, 'children'):
            for child in expr.children:
                if isinstance(child, QueryExpression):
                    properties.extend(self._collect_property_references(child))

        return properties

    def _contains_volatile_function(self, expr: QueryExpression) -> bool:
        """Check if expression contains volatile functions (rand, datetime, etc.)."""
        from gsql2rsql.parser.ast import (
            QueryExpressionBinary,
            QueryExpressionCaseExpression,
            QueryExpressionFunction,
            QueryExpressionList,
            QueryExpressionListPredicate,
            QueryExpressionWithAlias,
        )

        if isinstance(expr, QueryExpressionFunction):
            if expr.function in VOLATILE_FUNCTIONS:
                return True
            for arg in expr.parameters or []:
                if self._contains_volatile_function(arg):
                    return True

        elif isinstance(expr, QueryExpressionBinary):
            if expr.left_expression and self._contains_volatile_function(expr.left_expression):
                return True
            if expr.right_expression and self._contains_volatile_function(expr.right_expression):
                return True

        elif isinstance(expr, QueryExpressionCaseExpression):
            if expr.test_expression and self._contains_volatile_function(expr.test_expression):
                return True
            for when_expr, then_expr in expr.alternatives or []:
                if self._contains_volatile_function(when_expr):
                    return True
                if self._contains_volatile_function(then_expr):
                    return True
            if expr.else_expression and self._contains_volatile_function(expr.else_expression):
                return True

        elif isinstance(expr, QueryExpressionList):
            for item in expr.items or []:
                if self._contains_volatile_function(item):
                    return True

        elif isinstance(expr, QueryExpressionListPredicate):
            if expr.list_expression and self._contains_volatile_function(expr.list_expression):
                return True
            if expr.filter_expression and self._contains_volatile_function(expr.filter_expression):
                return True

        elif isinstance(expr, QueryExpressionWithAlias):
            if expr.inner_expression and self._contains_volatile_function(expr.inner_expression):
                return True

        elif hasattr(expr, 'children'):
            for child in expr.children:
                if isinstance(child, QueryExpression) and self._contains_volatile_function(child):
                    return True

        return False

    def _contains_aggregation_function(self, expr: QueryExpression) -> bool:
        """Check if expression contains aggregation functions (COUNT, SUM, etc.)."""
        from gsql2rsql.parser.ast import (
            QueryExpressionAggregationFunction,
            QueryExpressionBinary,
            QueryExpressionCaseExpression,
            QueryExpressionFunction,
            QueryExpressionList,
            QueryExpressionListPredicate,
            QueryExpressionWithAlias,
        )

        if isinstance(expr, QueryExpressionAggregationFunction):
            return True

        elif isinstance(expr, QueryExpressionFunction):
            for arg in expr.parameters or []:
                if self._contains_aggregation_function(arg):
                    return True

        elif isinstance(expr, QueryExpressionBinary):
            if expr.left_expression and self._contains_aggregation_function(expr.left_expression):
                return True
            if expr.right_expression and self._contains_aggregation_function(expr.right_expression):
                return True

        elif isinstance(expr, QueryExpressionCaseExpression):
            if expr.test_expression and self._contains_aggregation_function(expr.test_expression):
                return True
            for when_expr, then_expr in expr.alternatives or []:
                if self._contains_aggregation_function(when_expr):
                    return True
                if self._contains_aggregation_function(then_expr):
                    return True
            if expr.else_expression and self._contains_aggregation_function(expr.else_expression):
                return True

        elif isinstance(expr, QueryExpressionList):
            for item in expr.items or []:
                if self._contains_aggregation_function(item):
                    return True

        elif isinstance(expr, QueryExpressionListPredicate):
            if expr.list_expression and self._contains_aggregation_function(expr.list_expression):
                return True
            if expr.filter_expression and self._contains_aggregation_function(expr.filter_expression):
                return True

        elif isinstance(expr, QueryExpressionWithAlias):
            if expr.inner_expression and self._contains_aggregation_function(expr.inner_expression):
                return True

        elif hasattr(expr, 'children'):
            for child in expr.children:
                if isinstance(child, QueryExpression) and self._contains_aggregation_function(child):
                    return True

        return False

    def _contains_correlated_subquery(self, expr: QueryExpression) -> bool:
        """Check if expression contains correlated subqueries (EXISTS, etc.)."""
        from gsql2rsql.parser.ast import (
            QueryExpressionBinary,
            QueryExpressionCaseExpression,
            QueryExpressionExists,
            QueryExpressionFunction,
            QueryExpressionList,
            QueryExpressionListPredicate,
            QueryExpressionWithAlias,
        )

        # Direct EXISTS expression
        if isinstance(expr, QueryExpressionExists):
            return True

        elif isinstance(expr, QueryExpressionBinary):
            if expr.left_expression and self._contains_correlated_subquery(expr.left_expression):
                return True
            if expr.right_expression and self._contains_correlated_subquery(expr.right_expression):
                return True

        elif isinstance(expr, QueryExpressionFunction):
            for arg in expr.parameters or []:
                if self._contains_correlated_subquery(arg):
                    return True

        elif isinstance(expr, QueryExpressionCaseExpression):
            if expr.test_expression and self._contains_correlated_subquery(expr.test_expression):
                return True
            for when_expr, then_expr in expr.alternatives or []:
                if self._contains_correlated_subquery(when_expr):
                    return True
                if self._contains_correlated_subquery(then_expr):
                    return True
            if expr.else_expression and self._contains_correlated_subquery(expr.else_expression):
                return True

        elif isinstance(expr, QueryExpressionList):
            for item in expr.items or []:
                if self._contains_correlated_subquery(item):
                    return True

        elif isinstance(expr, QueryExpressionListPredicate):
            if expr.list_expression and self._contains_correlated_subquery(expr.list_expression):
                return True
            if expr.filter_expression and self._contains_correlated_subquery(expr.filter_expression):
                return True

        elif isinstance(expr, QueryExpressionWithAlias):
            if expr.inner_expression and self._contains_correlated_subquery(expr.inner_expression):
                return True

        elif hasattr(expr, 'children'):
            for child in expr.children:
                if isinstance(child, QueryExpression) and self._contains_correlated_subquery(child):
                    return True

        return False

    # =========================================================================
    # DataSource Finding & Join Path Checking
    # =========================================================================

    def _find_datasource_for_variable(
        self, variable: str, plan: LogicalPlan, selection: SelectionOperator | None = None
    ) -> DataSourceOperator | None:
        """Find the DataSourceOperator that provides a given variable.

        Only returns DataSources that can have filters pushed to them.
        Excludes DataSources involved in recursive path patterns or LEFT JOIN paths.
        """
        for start_op in plan.starting_operators:
            if isinstance(start_op, DataSourceOperator):
                if start_op.entity and start_op.entity.alias == variable:
                    if self._is_in_recursive_path(start_op):
                        return None
                    if selection and self._has_non_inner_join_in_path(start_op, selection):
                        return None
                    return start_op
        return None

    def _has_non_inner_join_in_path(
        self, ds: DataSourceOperator, selection: SelectionOperator
    ) -> bool:
        """Check if DataSource is on the RIGHT (optional) side of a LEFT JOIN."""
        visited: set[int] = set()
        return self._is_on_optional_side_of_left_join(ds, selection, visited)

    def _is_on_optional_side_of_left_join(
        self,
        current: LogicalOperator,
        target: SelectionOperator,
        visited: set[int],
    ) -> bool:
        """Check if current operator is on the optional side of a LEFT JOIN."""
        op_id = id(current)
        if op_id in visited:
            return False
        visited.add(op_id)

        if current is target:
            return False

        for out_op in current.graph_out_operators:
            if isinstance(out_op, JoinOperator):
                if out_op.join_type == JoinType.LEFT:
                    is_right_input = self._is_input_of_join(current, out_op, is_right=True)
                    if is_right_input:
                        if self._can_reach_operator(out_op, target, set()):
                            return True

                if out_op.join_type == JoinType.CROSS:
                    if self._can_reach_operator(out_op, target, set()):
                        return True

            if self._is_on_optional_side_of_left_join(out_op, target, visited):
                return True

        return False

    def _is_input_of_join(
        self, op: LogicalOperator, join_op: JoinOperator, is_right: bool
    ) -> bool:
        """Check if operator is a specific input (left or right) of a join."""
        if is_right:
            target_input = join_op.in_operator_right
        else:
            target_input = join_op.in_operator_left

        if target_input is None:
            return False

        return op is target_input or self._is_ancestor_of(op, target_input)

    def _is_ancestor_of(self, ancestor: LogicalOperator, descendant: LogicalOperator) -> bool:
        """Check if ancestor is an upstream operator of descendant."""
        for in_op in descendant.graph_in_operators:
            if in_op is ancestor:
                return True
            if self._is_ancestor_of(ancestor, in_op):
                return True
        return False

    def _can_reach_operator(
        self,
        start: LogicalOperator,
        target: LogicalOperator,
        visited: set[int],
    ) -> bool:
        """Check if target is reachable from start."""
        op_id = id(start)
        if op_id in visited:
            return False
        visited.add(op_id)

        if start is target:
            return True

        for out_op in start.graph_out_operators:
            if self._can_reach_operator(out_op, target, visited):
                return True

        return False

    def _is_in_recursive_path(self, ds: DataSourceOperator) -> bool:
        """Check if a DataSource is involved in a recursive path pattern."""
        for out_op in ds.graph_out_operators:
            if isinstance(out_op, RecursiveTraversalOperator):
                return True
            if isinstance(out_op, JoinOperator):
                for in_op in out_op.graph_in_operators:
                    if isinstance(in_op, RecursiveTraversalOperator):
                        return True
                    if self._has_recursive_ancestor(in_op):
                        return True
        return False

    def _has_recursive_ancestor(self, op: LogicalOperator) -> bool:
        """Check if an operator has a RecursiveTraversalOperator as ancestor."""
        if isinstance(op, RecursiveTraversalOperator):
            return True
        for in_op in op.graph_in_operators:
            if self._has_recursive_ancestor(in_op):
                return True
        return False

    def _remove_selection(self, selection: SelectionOperator) -> None:
        """Remove a Selection operator from the plan by bypassing it."""
        in_op = selection.in_operator
        if not in_op:
            return

        for out_op in selection.graph_out_operators:
            if selection in out_op.graph_in_operators:
                idx = out_op.graph_in_operators.index(selection)
                out_op.graph_in_operators[idx] = in_op

            if out_op not in in_op.graph_out_operators:
                in_op.graph_out_operators.append(out_op)

        if selection in in_op.graph_out_operators:
            in_op.graph_out_operators.remove(selection)

        selection.graph_in_operators = []
        selection.graph_out_operators = []
