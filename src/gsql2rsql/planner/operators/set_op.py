"""SetOperator for UNION and other set operations."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from gsql2rsql.planner.operators.base import BinaryLogicalOperator
from gsql2rsql.planner.schema import Schema


class SetOperationType(Enum):
    """Type of set operation."""

    UNION = auto()
    UNION_ALL = auto()
    INTERSECT = auto()
    EXCEPT = auto()


@dataclass
class SetOperator(BinaryLogicalOperator):
    """Operator for set operations (UNION, etc.)."""

    set_operation: SetOperationType = SetOperationType.UNION

    @property
    def depth(self) -> int:
        left_depth = self.in_operator_left.depth if self.in_operator_left else 0
        right_depth = self.in_operator_right.depth if self.in_operator_right else 0
        return max(left_depth, right_depth) + 1

    def __str__(self) -> str:
        base = super().__str__()
        return f"{base}\n  SetOp: {self.set_operation.name}"

    def propagate_data_types_for_in_schema(self) -> None:
        """Propagate data types from upstream operators to input schema.

        SetOperator merges both branches into input schema.
        """
        if self.in_operator_left and self.in_operator_right:
            self.input_schema = Schema.merge(
                self.in_operator_left.output_schema,
                self.in_operator_right.output_schema,
            )

    def propagate_data_types_for_out_schema(self) -> None:
        """Propagate data types to output schema.

        For UNION, the output schema follows the left branch's schema.
        (SQL UNION semantics: column names from first SELECT).
        """
        if self.in_operator_left and self.in_operator_left.output_schema:
            self.output_schema = Schema(self.in_operator_left.output_schema.fields)
