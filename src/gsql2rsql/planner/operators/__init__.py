"""Logical operators for the query plan.

This package was originally a single module (operators.py, 1857 lines).
It has been split into one file per operator for maintainability.
All public names are re-exported here for backward compatibility.
"""

from gsql2rsql.planner.operators.aggregation import AggregationBoundaryOperator
from gsql2rsql.planner.operators.base import (
    BinaryLogicalOperator,
    IBindable,
    LogicalOperator,
    StartLogicalOperator,
    UnaryLogicalOperator,
)
from gsql2rsql.planner.operators.data_source import DataSourceOperator
from gsql2rsql.planner.operators.join import (
    JoinKeyPair,
    JoinKeyPairType,
    JoinOperator,
    JoinType,
)
from gsql2rsql.planner.operators.projection import ProjectionOperator
from gsql2rsql.planner.operators.recursive import RecursiveTraversalOperator
from gsql2rsql.planner.operators.selection import SelectionOperator
from gsql2rsql.planner.operators.set_op import SetOperationType, SetOperator
from gsql2rsql.planner.operators.unwind import UnwindOperator

__all__ = [
    "AggregationBoundaryOperator",
    "BinaryLogicalOperator",
    "DataSourceOperator",
    "IBindable",
    "JoinKeyPair",
    "JoinKeyPairType",
    "JoinOperator",
    "JoinType",
    "LogicalOperator",
    "ProjectionOperator",
    "RecursiveTraversalOperator",
    "SelectionOperator",
    "SetOperationType",
    "SetOperator",
    "StartLogicalOperator",
    "UnaryLogicalOperator",
    "UnwindOperator",
]
