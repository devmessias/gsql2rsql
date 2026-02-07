"""SelectionOperator for filtering (WHERE clause)."""

from __future__ import annotations

from dataclasses import dataclass

from gsql2rsql.parser.ast import QueryExpression
from gsql2rsql.planner.operators.base import UnaryLogicalOperator
from gsql2rsql.planner.schema import Schema


@dataclass
class SelectionOperator(UnaryLogicalOperator):
    """Operator for filtering (WHERE clause)."""

    filter_expression: QueryExpression | None = None

    @property
    def depth(self) -> int:
        return (self.in_operator.depth if self.in_operator else 0) + 1

    def propagate_data_types_for_in_schema(self) -> None:
        """Propagate data types from upstream operators to input schema.

        Selection (WHERE) doesn't transform the schema, so input schema
        is the same as the in_operator's output schema.
        """
        if self.in_operator and self.in_operator.output_schema:
            self.input_schema = Schema(self.in_operator.output_schema.fields)

    def propagate_data_types_for_out_schema(self) -> None:
        """Propagate data types from input schema to output schema.

        Selection (WHERE) doesn't transform the schema, so output schema
        is the same as input schema.
        """
        if self.input_schema:
            self.output_schema = Schema(self.input_schema.fields)

    def required_input_symbols(self) -> set[str]:
        """Return symbols required from input for the filter expression."""
        from gsql2rsql.parser.ast import QueryExpressionProperty

        required: set[str] = set()
        if self.filter_expression:
            # Direct property reference
            if isinstance(self.filter_expression, QueryExpressionProperty):
                required.add(self.filter_expression.variable_name)

            # Recursively find all property references
            for prop in self.filter_expression.get_children_query_expression_type(
                QueryExpressionProperty
            ):
                required.add(prop.variable_name)

        return required

    def __str__(self) -> str:
        base = super().__str__()
        return f"{base}\n  Filter: {self.filter_expression}"
