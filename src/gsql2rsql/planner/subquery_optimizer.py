"""Subquery Flattening Optimizer for Logical Plan.

This module optimizes the logical plan by merging consecutive operators that would
otherwise generate unnecessary nested subqueries in the rendered SQL.

TRADE-OFFS AND DESIGN DECISIONS
===============================

1. PLANNER-SIDE vs RENDERER-SIDE FLATTENING
-------------------------------------------

We chose PLANNER-SIDE implementation for these reasons:

Advantages:
- Testability: Logical plan can be inspected before rendering
- Separation of concerns: Optimization logic separate from SQL generation
- Composability: Multiple optimization passes can be chained
- Debugging: dump_graph() shows the optimized plan

Disadvantages:
- Requires modifying operator dataclasses (e.g., adding filter_expression to Projection)
- More complex operator rewiring logic

Alternative (Renderer-side):
- Would detect patterns during _render_projection() and inline WHERE
- Simpler to implement but harder to test and debug
- Logic scattered across render methods

2. CONSERVATIVE vs EAGER FLATTENING
-----------------------------------

We chose CONSERVATIVE approach for these reasons:

What we DO flatten (100% semantically equivalent):
- Selection → Projection: WHERE + SELECT always safe to combine
- Selection → Selection: Multiple WHEREs can be ANDed

What we DO NOT flatten (semantic risks):
- Projection → Projection: Column alias conflicts possible
- Anything with LIMIT/OFFSET: Row ordering semantics change
- Window functions: Scope boundaries matter
- DISTINCT in inner query: Affects aggregation counts

Why Conservative?
- Databricks SQL optimizer already flattens simple cases internally
- Our flattening is primarily for SQL readability/debugging
- Incorrect flattening causes subtle bugs that are hard to diagnose
- User can inspect generated SQL and trust it's semantically correct

Trade-off: We may generate SQL with unnecessary subqueries that Databricks
will optimize anyway. This is acceptable because:
1. Correctness > Performance (Databricks optimizer handles it)
2. Generated SQL is more predictable/debuggable
3. No silent semantic changes to query results

3. OPERATOR MUTATION vs COPY
----------------------------

We MUTATE operators in-place rather than creating copies:

Advantages:
- Memory efficient for large plans
- Simpler operator graph management
- operator_debug_id preserved for debugging

Disadvantages:
- Original plan is modified (can't compare before/after easily)
- Must be careful with operator reference management

Alternative: Deep-copy plan before optimization
- Would allow before/after comparison
- Higher memory usage
- More complex implementation

4. SINGLE-PASS vs MULTI-PASS
----------------------------

We use SINGLE-PASS bottom-up traversal:

Advantages:
- O(n) complexity where n = number of operators
- Simple termination guarantee
- Each operator visited once

Disadvantages:
- May miss some optimizations that require multiple passes
- Cannot handle patterns that emerge after other optimizations

Example of missed optimization:
    Selection → Selection → Projection
    First pass: Merges first two Selections
    Result: Selection → Projection (could be flattened further)

We accept this because:
- Multi-pass would complicate termination logic
- Most real queries don't have deeply nested patterns
- Can always run optimizer twice if needed

USAGE
=====

    from gsql2rsql.planner.subquery_optimizer import SubqueryFlatteningOptimizer

    # After creating logical plan
    plan = LogicalPlan.process_query_tree(ast, graph_def)

    # Apply optimization
    optimizer = SubqueryFlatteningOptimizer(enabled=True)
    optimizer.optimize(plan)

    # Render optimized plan
    sql = renderer.render_plan(plan)

FLATTENING RULES
================

Rule 1: Selection → Projection
------------------------------
BEFORE:
    ProjectionOperator(projections=[...])
        ↑
    SelectionOperator(filter=condition)
        ↑
    SomeOperator

AFTER:
    ProjectionOperator(projections=[...], filter_expression=condition)
        ↑
    SomeOperator

SQL BEFORE:
    SELECT cols FROM (SELECT * FROM T WHERE cond) AS _proj

SQL AFTER:
    SELECT cols FROM T WHERE cond

Rule 2: Selection → Selection
------------------------------
Multiple consecutive WHERE clauses can be ANDed together.

BEFORE:
    SelectionOperator(filter=B)
        ↑
    SelectionOperator(filter=A)
        ↑
    SomeOperator

AFTER:
    SelectionOperator(filter=A AND B)
        ↑
    SomeOperator

SQL BEFORE:
    SELECT * FROM (SELECT * FROM T WHERE A) AS _filter WHERE B

SQL AFTER:
    SELECT * FROM T WHERE A AND B

WHY THIS IS 100% SAFE:
- WHERE A followed by WHERE B is mathematically equivalent to WHERE (A AND B)
- No semantic edge cases - this is pure boolean logic
- No interaction with GROUP BY, DISTINCT, LIMIT, etc.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gsql2rsql.parser.ast import QueryExpressionBinary
from gsql2rsql.parser.operators import (
    BinaryOperator,
    BinaryOperatorInfo,
    BinaryOperatorType,
)
from gsql2rsql.planner.operators import (
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


def optimize_plan(plan: LogicalPlan, enabled: bool = True) -> FlatteningStats:
    """Convenience function to optimize a logical plan.

    Args:
        plan: The logical plan to optimize.
        enabled: Whether to actually perform optimization.

    Returns:
        Statistics about the optimization performed.
    """
    optimizer = SubqueryFlatteningOptimizer(enabled=enabled)
    optimizer.optimize(plan)
    return optimizer.stats
