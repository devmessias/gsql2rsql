"""Optimization pass manager for the logical plan.

Coordinates multiple optimization passes in the correct order.
Each optimizer implements the same interface (Protocol-based duck typing).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from gsql2rsql.planner.logical_plan import LogicalPlan
    from gsql2rsql.planner.subquery_flattening import FlatteningStats


class OptimizationPass(Protocol):
    """Protocol for optimization passes.

    Any class with an `optimize(plan)` method satisfies this protocol.
    """

    def optimize(self, plan: LogicalPlan) -> None: ...


def optimize_plan(
    plan: LogicalPlan,
    enabled: bool = True,
    pushdown_enabled: bool = True,
    dead_table_elimination_enabled: bool = True,
) -> "FlatteningStats":  # noqa: F821
    """Convenience function to optimize a logical plan.

    Runs optimization passes in order:
    1. DeadTableEliminationOptimizer: Removes unnecessary JOINs with unused tables
    2. SelectionPushdownOptimizer: Pushes predicates into DataSource operators
    3. SubqueryFlatteningOptimizer: Merges Selection -> Projection patterns

    Dead Table Elimination runs FIRST because it can remove entire JoinOperators,
    making the subsequent optimizations simpler and faster.

    Args:
        plan: The logical plan to optimize.
        enabled: Whether to run subquery flattening optimization.
        pushdown_enabled: Whether to run selection pushdown optimization.
        dead_table_elimination_enabled: Whether to run dead table elimination.
            Disabling this preserves INNER JOINs that filter orphan edges.

    Returns:
        Statistics about the flattening optimization performed.

    See Also:
        new_bugs/002_dead_table_elimination.md: Documentation on dead table
        elimination, including trade-offs (orphan edges, performance, etc.)
    """
    # Import here to avoid circular dependency
    from gsql2rsql.planner.dead_table_eliminator import DeadTableEliminationOptimizer
    from gsql2rsql.planner.selection_pushdown import SelectionPushdownOptimizer
    from gsql2rsql.planner.subquery_flattening import SubqueryFlatteningOptimizer

    # Run dead table elimination FIRST
    if dead_table_elimination_enabled:
        dead_table_optimizer = DeadTableEliminationOptimizer(enabled=True)
        dead_table_optimizer.optimize(plan)

    # Run selection pushdown (pushes WHERE predicates into DataSource operators)
    if pushdown_enabled:
        pushdown_optimizer = SelectionPushdownOptimizer(enabled=True)
        pushdown_optimizer.optimize(plan)

    # Then run subquery flattening
    flattening_optimizer = SubqueryFlatteningOptimizer(enabled=enabled)
    flattening_optimizer.optimize(plan)

    return flattening_optimizer.stats
