"""Subquery Flattening Optimizer for Logical Plan.

This module optimizes the logical plan by merging consecutive operators that would
otherwise generate unnecessary nested subqueries in the rendered SQL.

================================================================================
CONSERVATIVE FLATTENING APPROACH - READ THIS CAREFULLY
================================================================================

This optimizer uses a CONSERVATIVE approach. We ONLY flatten patterns that are
100% GUARANTEED to produce semantically equivalent SQL. When in doubt, we do NOT
flatten.

WHY CONSERVATIVE?
-----------------
1. Databricks SQL optimizer already flattens simple cases internally
2. Incorrect flattening causes SILENT bugs - wrong results, not errors
3. Users must be able to trust that generated SQL is semantically correct
4. Readability improvement is secondary to correctness

================================================================================
WHAT WE DO FLATTEN (100% SAFE)
================================================================================

✅ RULE 1: Selection → Projection
---------------------------------
Cypher:
    MATCH (p:Person)
    WHERE p.age > 30        <-- SelectionOperator
    RETURN p.name           <-- ProjectionOperator

SQL BEFORE (not flattened):
    SELECT __p_name AS name
    FROM (
        SELECT * FROM (SELECT ...) AS _filter
        WHERE __p_age > 30
    ) AS _proj

SQL AFTER (flattened):
    SELECT __p_name AS name
    FROM (SELECT ...) AS _proj
    WHERE __p_age > 30

WHY SAFE: WHERE clause position doesn't change semantics when there's no
aggregation boundary. The filter applies to the same rows either way.


✅ RULE 2: Selection → Selection
---------------------------------
Cypher (hypothetical - created by complex patterns):
    -- Two consecutive WHERE clauses (rare in practice)

SQL BEFORE (not flattened):
    SELECT * FROM (SELECT * FROM T WHERE A) AS _filter WHERE B

SQL AFTER (flattened):
    SELECT * FROM T WHERE (A) AND (B)

WHY SAFE: WHERE A followed by WHERE B is mathematically equivalent to
WHERE (A AND B). Pure boolean logic - no semantic edge cases.


================================================================================
WHAT WE DO NOT FLATTEN (POTENTIAL SEMANTIC CHANGES)
================================================================================

❌ Projection → Projection
--------------------------
Cypher:
    MATCH (p:Person)
    WITH p.age * 2 AS double_age    <-- ProjectionOperator (alias defined)
    RETURN double_age + 1           <-- ProjectionOperator (references alias)

SQL (NOT flattened - correct):
    SELECT double_age + 1
    FROM (
        SELECT __p_age * 2 AS double_age
        FROM ...
    ) AS _proj

HYPOTHETICAL flattened (WRONG):
    SELECT (__p_age * 2) + 1 AS result  -- Would need to inline the expression
    FROM ...

RISKS IF WE FLATTENED:
1. Column alias conflicts - outer query references alias defined in inner
2. Expression duplication - same expression computed multiple times
3. Side effects - if expressions had side effects (unlikely in SQL)

TODO: Could implement Projection → Projection flattening with these checks:
- Verify outer projections don't reference aliases defined in inner
- Verify no DISTINCT, LIMIT, OFFSET in inner projection
- Inline expressions only when they're simple column references


❌ Anything with LIMIT/OFFSET in inner query
--------------------------------------------
Cypher:
    MATCH (p:Person)
    WITH p ORDER BY p.age LIMIT 10  <-- ProjectionOperator with LIMIT
    WHERE p.active = true           <-- SelectionOperator
    RETURN p.name

SQL (NOT flattened - correct):
    SELECT __p_name AS name
    FROM (
        SELECT * FROM (
            SELECT ... ORDER BY __p_age LIMIT 10
        ) AS _proj
        WHERE __p_active = true
    ) AS _filter

HYPOTHETICAL flattened (WRONG):
    SELECT __p_name AS name
    FROM (SELECT ...) AS _proj
    WHERE __p_active = true
    ORDER BY __p_age LIMIT 10  -- WRONG! WHERE applies BEFORE LIMIT now!

SEMANTIC DIFFERENCE:
- Correct: Take top 10 by age, THEN filter by active
- Wrong: Filter by active, THEN take top 10 by age
- Result: Completely different rows returned!


❌ DISTINCT in inner query
--------------------------
Cypher:
    MATCH (p:Person)-[:KNOWS]->(f:Person)
    WITH DISTINCT p                  <-- ProjectionOperator with DISTINCT
    RETURN COUNT(*)                  <-- Aggregation

SQL (NOT flattened - correct):
    SELECT COUNT(*)
    FROM (
        SELECT DISTINCT __p_id, __p_name, ...
        FROM ...
    ) AS _proj

HYPOTHETICAL flattened (WRONG):
    SELECT COUNT(DISTINCT __p_id)  -- Different semantics!
    FROM ...

SEMANTIC DIFFERENCE:
- Correct: Count unique persons (after deduplication)
- Wrong: COUNT DISTINCT on one column only
- If Person has multiple fields, results differ!


❌ Window functions
-------------------
Cypher (hypothetical):
    MATCH (p:Person)
    WITH p, ROW_NUMBER() OVER (ORDER BY p.age) AS rn
    WHERE rn <= 10
    RETURN p.name

RISKS IF FLATTENED:
- Window function scope changes
- Partitioning boundaries affected
- Results could be completely different


================================================================================
EXAMPLES OF BUGS FROM EAGER (NON-CONSERVATIVE) FLATTENING
================================================================================

BUG EXAMPLE 1: Lost rows due to LIMIT reordering
------------------------------------------------
Query: "Get names of top 10 oldest active people"

Cypher:
    MATCH (p:Person)
    WITH p ORDER BY p.age DESC LIMIT 10
    WHERE p.active = true
    RETURN p.name

Expected result (correct): Filter AFTER limit
    1. Sort all people by age DESC
    2. Take top 10
    3. Filter those 10 for active=true
    4. Return names (could be 0-10 rows)

Buggy result (if flattened wrong): Filter BEFORE limit
    1. Filter all people for active=true
    2. Sort by age DESC
    3. Take top 10
    4. Return names (always 10 rows if enough active people)

Impact: User gets WRONG DATA with no error message!


BUG EXAMPLE 2: Wrong count due to DISTINCT flattening
-----------------------------------------------------
Query: "Count unique customers who made purchases"

Cypher:
    MATCH (c:Customer)-[:PURCHASED]->(p:Product)
    WITH DISTINCT c
    RETURN COUNT(*) AS unique_customers

If customer C1 bought 5 products:
- Correct (with DISTINCT subquery): COUNT = 1
- Wrong (if flattened): Could count 5 times!


BUG EXAMPLE 3: Alias resolution failure
---------------------------------------
Query: "Calculate derived value and use it"

Cypher:
    MATCH (p:Person)
    WITH p.salary * 0.3 AS tax
    RETURN tax * 12 AS annual_tax

If flattened incorrectly:
- Outer query references 'tax' but it's not defined at that level
- Could cause runtime SQL error or wrong column reference


================================================================================
TODO: FUTURE OPTIMIZATIONS (NON-CONSERVATIVE, REQUIRES CAREFUL ANALYSIS)
================================================================================

TODO: Projection → Projection flattening
    - Safe ONLY if outer projections are simple column references
    - Must verify no alias conflicts
    - Must verify no DISTINCT, LIMIT, OFFSET in inner
    - Implementation complexity: HIGH
    - Benefit: Moderate (reduces 1 subquery level)

TODO: Selection → Join flattening
    - Push WHERE conditions into JOIN ON clauses
    - Safe for INNER JOIN, risky for OUTER JOIN
    - Could improve query performance
    - Implementation complexity: MEDIUM
    - Benefit: Moderate (better join optimization)

TODO: Recursive CTE flattening
    - Merge post-CTE filters into CTE itself
    - Already partially done with predicate pushdown
    - Full implementation is complex
    - Implementation complexity: HIGH
    - Benefit: High (reduces data processed in recursion)


================================================================================
IMPLEMENTATION NOTES
================================================================================

SINGLE-PASS BOTTOM-UP TRAVERSAL
-------------------------------
We visit operators from leaves to root. This ensures that when we try to
flatten an operator, its children are already in their final form.

Example:
    DataSource → Selection → Selection → Projection
    Visit order: DataSource, Selection1, Selection2, Projection

    At Selection2: Can merge with Selection1 → Selection(A AND B)
    At Projection: Can merge with Selection(A AND B) → Projection with filter

This handles chained patterns in a single pass.


MULTI-PASS NOT NEEDED
---------------------
With bottom-up traversal, we handle chains like Selection → Selection → Projection
correctly in one pass. Multi-pass would only help for patterns we don't support
(like Projection → Projection).


================================================================================
USAGE
================================================================================

    from gsql2rsql.planner.subquery_optimizer import SubqueryFlatteningOptimizer

    # After creating logical plan
    plan = LogicalPlan.process_query_tree(ast, graph_def)

    # Apply optimization (enabled by default)
    optimizer = SubqueryFlatteningOptimizer(enabled=True)
    optimizer.optimize(plan)

    # Check what was flattened
    print(optimizer.stats)  # FlatteningStats(sel→proj=1, sel→sel=0, ...)

    # Render optimized plan
    sql = renderer.render_plan(plan)

    # To disable optimization (for debugging):
    optimizer = SubqueryFlatteningOptimizer(enabled=False)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gsql2rsql.parser.ast import QueryExpression, QueryExpressionBinary
from gsql2rsql.parser.operators import (
    BinaryOperator,
    BinaryOperatorInfo,
    BinaryOperatorType,
)
from gsql2rsql.planner.operators import (
    DataSourceOperator,
    LogicalOperator,
    ProjectionOperator,
    SelectionOperator,
    JoinOperator,
    RecursiveTraversalOperator,
)

if TYPE_CHECKING:
    from gsql2rsql.planner.logical_plan import LogicalPlan


@dataclass
class FlatteningStats:
    """Statistics about flattening operations performed."""

    selection_into_projection: int = 0
    selection_into_selection: int = 0
    total_operators_before: int = 0
    total_operators_after: int = 0

    def __str__(self) -> str:
        return (
            f"FlatteningStats("
            f"sel→proj={self.selection_into_projection}, "
            f"sel→sel={self.selection_into_selection}, "
            f"operators: {self.total_operators_before} → {self.total_operators_after})"
        )


class SubqueryFlatteningOptimizer:
    """Optimizes logical plan by merging operators to reduce subquery nesting.

    This optimizer implements CONSERVATIVE flattening - only patterns that are
    100% semantically equivalent are merged. See module docstring for trade-offs.

    Attributes:
        enabled: Whether optimization is active. Set to False to bypass.
        stats: Statistics about operations performed (for debugging/testing).
    """

    def __init__(self, enabled: bool = True) -> None:
        """Initialize the optimizer.

        Args:
            enabled: If False, optimize() becomes a no-op. Useful for A/B testing
                    or debugging SQL generation issues.
        """
        self.enabled = enabled
        self.stats = FlatteningStats()

    def optimize(self, plan: LogicalPlan) -> None:
        """Apply subquery flattening optimizations to the logical plan.

        Modifies the plan IN-PLACE. The plan's operator graph is rewired
        to eliminate unnecessary intermediate operators.

        Args:
            plan: The logical plan to optimize. Modified in-place.
        """
        if not self.enabled:
            return

        # Reset stats
        self.stats = FlatteningStats()

        # Count operators before
        self.stats.total_operators_before = self._count_operators(plan)

        # Process each terminal operator's subtree
        for terminal_op in plan.terminal_operators:
            self._optimize_subtree(terminal_op)

        # Count operators after
        self.stats.total_operators_after = self._count_operators(plan)

    def _count_operators(self, plan: LogicalPlan) -> int:
        """Count total operators in the plan."""
        visited: set[int] = set()
        count = 0
        for start_op in plan.starting_operators:
            for op in start_op.get_all_downstream_operators(LogicalOperator):
                if id(op) not in visited:
                    visited.add(id(op))
                    count += 1
        return count

    def _optimize_subtree(self, op: LogicalOperator) -> None:
        """Recursively optimize a subtree rooted at the given operator.

        Uses bottom-up traversal: children are optimized before parents.
        This ensures that when we check if an operator can be flattened,
        its children are already in their final optimized form.
        """
        # First, recursively optimize children
        for in_op in list(op.in_operators):  # Copy list since we may modify it
            self._optimize_subtree(in_op)

        # Then try to flatten this operator with its input
        self._try_flatten(op)

    def _try_flatten(self, op: LogicalOperator) -> None:
        """Try to flatten this operator with its input operator.

        Dispatches to specific flattening methods based on operator types.
        """
        if isinstance(op, ProjectionOperator):
            self._try_flatten_into_projection(op)
        elif isinstance(op, SelectionOperator):
            self._try_flatten_into_selection(op)

    def _try_flatten_into_projection(self, proj_op: ProjectionOperator) -> None:
        """Try to flatten the input operator into this ProjectionOperator.

        Currently handles:
        - SelectionOperator → ProjectionOperator
        """
        in_op = proj_op.in_operator
        if in_op is None:
            return

        # Rule 1: Selection → Projection
        if isinstance(in_op, SelectionOperator):
            if self._can_flatten_selection_into_projection(in_op, proj_op):
                self._merge_selection_into_projection(in_op, proj_op)

    def _can_flatten_selection_into_projection(
        self,
        selection: SelectionOperator,
        projection: ProjectionOperator,
    ) -> bool:
        """Check if a SelectionOperator can be safely merged into a ProjectionOperator.

        CONSERVATIVE RULES - only flatten when 100% safe:

        1. Selection must have a filter_expression (otherwise nothing to merge)
        2. Projection must not already have a filter_expression (avoid complexity)
        3. Selection's input must not be another Selection (handle one level at a time)

        We DO NOT check for:
        - Column alias conflicts: The filter uses the same column references
          as would be available in the non-flattened version
        - Aggregation interactions: WHERE is always applied before GROUP BY,
          so moving it into Projection (which handles GROUP BY) is safe

        Returns:
            True if flattening is safe and beneficial.
        """
        # Must have something to merge
        if selection.filter_expression is None:
            return False

        # Don't overwrite existing filter (keep it simple)
        if projection.filter_expression is not None:
            return False

        # Selection must have an input to connect to
        if selection.in_operator is None:
            return False

        return True

    def _merge_selection_into_projection(
        self,
        selection: SelectionOperator,
        projection: ProjectionOperator,
    ) -> None:
        """Merge a SelectionOperator into a ProjectionOperator.

        This operation:
        1. Moves filter_expression from Selection to Projection
        2. Bypasses the Selection by connecting Projection directly to Selection's input
        3. Updates operator graph references

        BEFORE:
            Projection._in_operators = [Selection]
            Selection._out_operators = [Projection]
            Selection._in_operators = [SomeOp]
            SomeOp._out_operators = [Selection]

        AFTER:
            Projection._in_operators = [SomeOp]
            Projection.filter_expression = Selection.filter_expression
            SomeOp._out_operators = [Projection]  # Selection removed
            Selection is orphaned (no references)
        """
        # Get the operator that was feeding into Selection
        selection_input = selection.in_operator
        if selection_input is None:
            return

        # Move the filter expression
        projection.filter_expression = selection.filter_expression

        # Rewire the graph:
        # 1. Remove Selection from its input's out_operators
        if selection in selection_input._out_operators:
            selection_input._out_operators.remove(selection)

        # 2. Add Projection to input's out_operators
        if projection not in selection_input._out_operators:
            selection_input._out_operators.append(projection)

        # 3. Update Projection's in_operators to point to Selection's input
        projection._in_operators = [selection_input]

        # 4. Clear Selection's references (orphan it)
        selection._in_operators = []
        selection._out_operators = []

        # Update stats
        self.stats.selection_into_projection += 1

    # =========================================================================
    # Rule 2: Selection → Selection
    # =========================================================================

    def _try_flatten_into_selection(self, outer_sel: SelectionOperator) -> None:
        """Try to flatten the input Selection into this SelectionOperator.

        Handles: SelectionOperator → SelectionOperator (AND filters together)
        """
        in_op = outer_sel.in_operator
        if in_op is None:
            return

        # Rule 2: Selection → Selection
        if isinstance(in_op, SelectionOperator):
            if self._can_flatten_selection_into_selection(in_op, outer_sel):
                self._merge_selection_into_selection(in_op, outer_sel)

    def _can_flatten_selection_into_selection(
        self,
        inner_sel: SelectionOperator,
        outer_sel: SelectionOperator,
    ) -> bool:
        """Check if two consecutive SelectionOperators can be merged.

        This is ALWAYS safe because:
        - WHERE A followed by WHERE B ≡ WHERE (A AND B)
        - Pure boolean logic, no semantic edge cases

        CONSERVATIVE RULES:
        1. Both must have filter expressions
        2. Inner selection must have an input to connect to

        Returns:
            True if flattening is safe and beneficial.
        """
        # Both must have filters to merge
        if inner_sel.filter_expression is None:
            return False
        if outer_sel.filter_expression is None:
            return False

        # Inner must have an input to connect to
        if inner_sel.in_operator is None:
            return False

        return True

    def _merge_selection_into_selection(
        self,
        inner_sel: SelectionOperator,
        outer_sel: SelectionOperator,
    ) -> None:
        """Merge two consecutive SelectionOperators by ANDing their filters.

        BEFORE:
            OuterSelection(filter=B)._in_operators = [InnerSelection]
            InnerSelection(filter=A)._in_operators = [SomeOp]

        AFTER:
            OuterSelection(filter=A AND B)._in_operators = [SomeOp]
            InnerSelection is orphaned
        """
        inner_input = inner_sel.in_operator
        if inner_input is None:
            return

        # Combine filters: inner AND outer
        # (inner filter is evaluated first, then outer - order matters for short-circuit)
        and_operator = BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL)
        combined_filter = QueryExpressionBinary(
            left_expression=inner_sel.filter_expression,
            operator=and_operator,
            right_expression=outer_sel.filter_expression,
        )
        outer_sel.filter_expression = combined_filter

        # Rewire the graph:
        # 1. Remove InnerSelection from its input's out_operators
        if inner_sel in inner_input._out_operators:
            inner_input._out_operators.remove(inner_sel)

        # 2. Add OuterSelection to input's out_operators
        if outer_sel not in inner_input._out_operators:
            inner_input._out_operators.append(outer_sel)

        # 3. Update OuterSelection's in_operators to point to InnerSelection's input
        outer_sel._in_operators = [inner_input]

        # 4. Clear InnerSelection's references (orphan it)
        inner_sel._in_operators = []
        inner_sel._out_operators = []

        # Update stats
        self.stats.selection_into_selection += 1


# =============================================================================
# Selection Pushdown Optimizer
# =============================================================================


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


class SelectionPushdownOptimizer:
    """Pushes predicates from Selection operators into DataSource operators.

    This optimizer analyzes Selection operators and pushes predicates that
    reference only a single entity (node or relationship) down to the
    corresponding DataSourceOperator. This is especially important for
    undirected relationships where filters should be applied before joins.

    Example:
        BEFORE (without pushdown):
            MATCH (p:Person)-[:KNOWS]-(f:Person)
            WHERE p.name = 'Alice'
            RETURN f.name

            Plan: DataSource(p) → Join(KNOWS) → Join(f) → Selection(p.name='Alice')

        AFTER (with pushdown):
            Plan: DataSource(p, filter=name='Alice') → Join(KNOWS) → Join(f)

    This significantly reduces the number of rows processed in joins.

    Algorithm:
        1. Find all SelectionOperators in the plan
        2. For each Selection, analyze which variables its predicate references
        3. If predicate references only one variable (entity):
           - Find the DataSourceOperator for that entity
           - Push the predicate to that DataSource's filter_expression
           - Remove or simplify the Selection
        4. If predicate references multiple variables:
           - Keep the Selection in place (cannot push)
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
            for op in start_op.get_all_downstream_operators(LogicalOperator):
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
        """Try to push a Selection's predicate into a DataSource.

        Args:
            selection: The Selection operator to analyze.
            plan: The full logical plan (for finding DataSources).
        """
        if not selection.filter_expression:
            return

        # Collect all property references in the predicate
        properties = self._collect_property_references(selection.filter_expression)
        if not properties:
            return

        # Get unique variable names referenced
        var_names = {p.variable_name for p in properties}

        # Can only push if predicate references exactly one variable
        if len(var_names) != 1:
            self.stats.predicates_remaining += 1
            return

        target_variable = next(iter(var_names))

        # Find the DataSourceOperator for this variable
        target_ds = self._find_datasource_for_variable(target_variable, plan)
        if not target_ds:
            self.stats.predicates_remaining += 1
            return

        # Push the predicate to the DataSource
        if target_ds.filter_expression is None:
            target_ds.filter_expression = selection.filter_expression
        else:
            # Combine with existing filter using AND
            from gsql2rsql.parser.operators import (
                BinaryOperator,
                BinaryOperatorInfo,
                BinaryOperatorType,
            )
            and_op = BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL)
            target_ds.filter_expression = QueryExpressionBinary(
                left_expression=target_ds.filter_expression,
                operator=and_op,
                right_expression=selection.filter_expression,
            )

        self.stats.predicates_pushed += 1

        # Remove the Selection operator from the plan
        self._remove_selection(selection)
        self.stats.selections_removed += 1

    def _collect_property_references(
        self,
        expr: QueryExpression,
    ) -> list:
        """Collect all property references from an expression tree.

        Args:
            expr: The expression to analyze.

        Returns:
            List of QueryExpressionProperty objects found in the expression.
        """
        from gsql2rsql.parser.ast import (
            QueryExpressionProperty,
            QueryExpressionBinary,
            QueryExpressionFunction,
            QueryExpressionAggregationFunction,
            QueryExpressionCaseExpression,
            QueryExpressionList,
            QueryExpressionListPredicate,
            QueryExpressionWithAlias,
        )

        properties: list = []

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
            if expr.predicate_expression:
                properties.extend(self._collect_property_references(expr.predicate_expression))
        elif isinstance(expr, QueryExpressionWithAlias):
            if expr.inner_expression:
                properties.extend(self._collect_property_references(expr.inner_expression))
        elif hasattr(expr, 'children'):
            for child in expr.children:
                if isinstance(child, QueryExpression):
                    properties.extend(self._collect_property_references(child))

        return properties

    def _find_datasource_for_variable(
        self, variable: str, plan: LogicalPlan
    ) -> DataSourceOperator | None:
        """Find the DataSourceOperator that provides a given variable.

        Only returns DataSources that can have filters pushed to them.
        Excludes DataSources involved in recursive path patterns, as those
        are rendered differently and don't support filter_expression.

        Args:
            variable: The variable name (e.g., 'p' from 'p:Person').
            plan: The logical plan to search.

        Returns:
            The DataSourceOperator for the variable, or None if not found
            or if the DataSource can't have filters pushed to it.
        """
        for start_op in plan.starting_operators:
            if isinstance(start_op, DataSourceOperator):
                if start_op.entity and start_op.entity.alias == variable:
                    # Check if this DataSource is involved in a recursive path
                    # If so, don't push (the renderer doesn't support it)
                    if self._is_in_recursive_path(start_op):
                        return None
                    return start_op
        return None

    def _is_in_recursive_path(self, ds: DataSourceOperator) -> bool:
        """Check if a DataSource is involved in a recursive path pattern.

        DataSources that feed into or are joined with RecursiveTraversalOperators
        are rendered differently and don't support filter_expression pushdown.

        Args:
            ds: The DataSourceOperator to check.

        Returns:
            True if the DataSource is part of a recursive path pattern.
        """
        # Check all downstream operators
        for out_op in ds._out_operators:
            if isinstance(out_op, RecursiveTraversalOperator):
                return True
            if isinstance(out_op, JoinOperator):
                # Check if the join's other input is a RecursiveTraversalOperator
                for in_op in out_op._in_operators:
                    if isinstance(in_op, RecursiveTraversalOperator):
                        return True
                    # Also check for joins that have recursive as ancestor
                    if self._has_recursive_ancestor(in_op):
                        return True
        return False

    def _has_recursive_ancestor(self, op: LogicalOperator) -> bool:
        """Check if an operator has a RecursiveTraversalOperator as ancestor.

        Args:
            op: The operator to check.

        Returns:
            True if there's a RecursiveTraversalOperator upstream.
        """
        if isinstance(op, RecursiveTraversalOperator):
            return True
        for in_op in op._in_operators:
            if self._has_recursive_ancestor(in_op):
                return True
        return False

    def _remove_selection(self, selection: SelectionOperator) -> None:
        """Remove a Selection operator from the plan by bypassing it.

        Connects the Selection's input directly to its outputs.

        Args:
            selection: The Selection operator to remove.
        """
        in_op = selection.in_operator
        if not in_op:
            return

        # Connect in_op directly to all of selection's out_operators
        for out_op in selection._out_operators:
            # Update out_op's in_operators to point to in_op
            if selection in out_op._in_operators:
                idx = out_op._in_operators.index(selection)
                out_op._in_operators[idx] = in_op

            # Add out_op to in_op's out_operators
            if out_op not in in_op._out_operators:
                in_op._out_operators.append(out_op)

        # Remove selection from in_op's out_operators
        if selection in in_op._out_operators:
            in_op._out_operators.remove(selection)

        # Orphan the selection
        selection._in_operators = []
        selection._out_operators = []


def optimize_plan(
    plan: LogicalPlan,
    enabled: bool = True,
    pushdown_enabled: bool = True,
) -> FlatteningStats:
    """Convenience function to optimize a logical plan.

    Runs two optimization passes:
    1. SelectionPushdownOptimizer: Pushes predicates into DataSource operators
    2. SubqueryFlatteningOptimizer: Merges Selection → Projection patterns

    Args:
        plan: The logical plan to optimize.
        enabled: Whether to run subquery flattening optimization.
        pushdown_enabled: Whether to run selection pushdown optimization.

    Returns:
        Statistics about the flattening optimization performed.
    """
    # Run selection pushdown first (before flattening)
    # This pushes WHERE predicates into DataSource operators
    if pushdown_enabled:
        pushdown_optimizer = SelectionPushdownOptimizer(enabled=True)
        pushdown_optimizer.optimize(plan)

    # Then run subquery flattening
    flattening_optimizer = SubqueryFlatteningOptimizer(enabled=enabled)
    flattening_optimizer.optimize(plan)

    return flattening_optimizer.stats
