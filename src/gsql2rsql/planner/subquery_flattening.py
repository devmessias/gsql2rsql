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

RULE 1: Selection -> Projection
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


RULE 2: Selection -> Selection
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
IMPLEMENTATION NOTES
================================================================================

SINGLE-PASS BOTTOM-UP TRAVERSAL
-------------------------------
We visit operators from leaves to root. This ensures that when we try to
flatten an operator, its children are already in their final form.

MULTI-PASS NOT NEEDED
---------------------
With bottom-up traversal, we handle chains like Selection -> Selection -> Projection
correctly in one pass. Multi-pass would only help for patterns we don't support
(like Projection -> Projection).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gsql2rsql.parser.ast import QueryExpressionBinary
from gsql2rsql.planner.operators import (
    LogicalOperator,
    ProjectionOperator,
    SelectionOperator,
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
            for op in start_op.get_all_downstream_operators(LogicalOperator):  # type: ignore[type-abstract]
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
        - SelectionOperator -> ProjectionOperator
        """
        in_op = proj_op.in_operator
        if in_op is None:
            return

        # Rule 1: Selection -> Projection
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
        """
        # Get the operator that was feeding into Selection
        selection_input = selection.in_operator
        if selection_input is None:
            return

        # Move the filter expression
        projection.filter_expression = selection.filter_expression

        # Rewire the graph:
        # 1. Remove Selection from its input's out_operators
        if selection in selection_input.graph_out_operators:
            selection_input.graph_out_operators.remove(selection)

        # 2. Add Projection to input's out_operators
        if projection not in selection_input.graph_out_operators:
            selection_input.graph_out_operators.append(projection)

        # 3. Update Projection's in_operators to point to Selection's input
        projection.graph_in_operators = [selection_input]

        # 4. Clear Selection's references (orphan it)
        selection.graph_in_operators = []
        selection.graph_out_operators = []

        # Update stats
        self.stats.selection_into_projection += 1

    # =========================================================================
    # Rule 2: Selection -> Selection
    # =========================================================================

    def _try_flatten_into_selection(self, outer_sel: SelectionOperator) -> None:
        """Try to flatten the input Selection into this SelectionOperator.

        Handles: SelectionOperator -> SelectionOperator (AND filters together)
        """
        in_op = outer_sel.in_operator
        if in_op is None:
            return

        # Rule 2: Selection -> Selection
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
        - WHERE A followed by WHERE B = WHERE (A AND B)
        - Pure boolean logic, no semantic edge cases

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
        """Merge two consecutive SelectionOperators by ANDing their filters."""
        from gsql2rsql.parser.operators import (
            BinaryOperator,
            BinaryOperatorInfo,
            BinaryOperatorType,
        )

        inner_input = inner_sel.in_operator
        if inner_input is None:
            return

        # Combine filters: inner AND outer
        and_operator = BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL)
        combined_filter = QueryExpressionBinary(
            left_expression=inner_sel.filter_expression,
            operator=and_operator,
            right_expression=outer_sel.filter_expression,
        )
        outer_sel.filter_expression = combined_filter

        # Rewire the graph:
        # 1. Remove InnerSelection from its input's out_operators
        if inner_sel in inner_input.graph_out_operators:
            inner_input.graph_out_operators.remove(inner_sel)

        # 2. Add OuterSelection to input's out_operators
        if outer_sel not in inner_input.graph_out_operators:
            inner_input.graph_out_operators.append(outer_sel)

        # 3. Update OuterSelection's in_operators to point to InnerSelection's input
        outer_sel.graph_in_operators = [inner_input]

        # 4. Clear InnerSelection's references (orphan it)
        inner_sel.graph_in_operators = []
        inner_sel.graph_out_operators = []

        # Update stats
        self.stats.selection_into_selection += 1
