"""Backward-compatible re-exports for the optimizer modules.

The original monolith (1,737 lines) has been split into:
- subquery_flattening.py: FlatteningStats, SubqueryFlatteningOptimizer
- selection_pushdown.py: PushdownStats, SelectionPushdownOptimizer
- pass_manager.py: OptimizationPass protocol, optimize_plan()

All existing imports from this module continue to work.
"""

from gsql2rsql.planner.pass_manager import optimize_plan
from gsql2rsql.planner.selection_pushdown import PushdownStats, SelectionPushdownOptimizer
from gsql2rsql.planner.subquery_flattening import FlatteningStats, SubqueryFlatteningOptimizer

__all__ = [
    "FlatteningStats",
    "SubqueryFlatteningOptimizer",
    "PushdownStats",
    "SelectionPushdownOptimizer",
    "optimize_plan",
]
