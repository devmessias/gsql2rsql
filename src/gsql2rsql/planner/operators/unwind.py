"""UnwindOperator for UNWIND clause (list expansion)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from gsql2rsql.parser.ast import (
    QueryExpression,
    QueryExpressionFunction,
    QueryExpressionProperty,
)
from gsql2rsql.parser.operators import Function
from gsql2rsql.planner.operators.base import UnaryLogicalOperator
from gsql2rsql.planner.schema import (
    Field,
    Schema,
    ValueField,
)


@dataclass
class UnwindOperator(UnaryLogicalOperator):
    """Operator for UNWIND clause that expands a list into rows.

    UNWIND expression AS variable

    In Databricks SQL, this becomes LATERAL EXPLODE:
    FROM ..., LATERAL EXPLODE(expression) AS t(variable)
    """

    list_expression: QueryExpression | None = None
    variable_name: str = ""

    @property
    def depth(self) -> int:
        return (self.in_operator.depth if self.in_operator else 0) + 1

    def __str__(self) -> str:
        base = super().__str__()
        return f"{base}\n  Unwind: {self.list_expression} AS {self.variable_name}"

    def propagate_data_types_for_in_schema(self) -> None:
        """Propagate data types from upstream operator to input schema."""
        if self.in_operator and self.in_operator.output_schema:
            self.input_schema = Schema(self.in_operator.output_schema.fields)

    def propagate_data_types_for_out_schema(self) -> None:
        """Propagate data types to output schema.

        UNWIND adds the variable_name as a new ValueField while preserving
        all upstream fields.

        For VLP relationship variables (arrays of edge structs), the element
        type is propagated to the unwound variable so that downstream property
        access (e.g., r.src, r.dst) can be resolved correctly.
        """
        fields: list[Field] = []

        # Preserve upstream fields
        if self.input_schema:
            fields.extend(self.input_schema.fields)

        # Add the unwound variable with inferred element type
        if self.variable_name:
            element_type = self._infer_element_type()

            fields.append(ValueField(
                field_alias=self.variable_name,
                # Use the variable name directly for struct field access
                # (EXPLODE alias matches this name in rendered SQL)
                field_name=self.variable_name,
                data_type=None,
                structured_type=element_type,  # Propagate struct type for property resolution
            ))

        self.output_schema = Schema(fields)

    def _infer_element_type(self) -> Any:
        """Infer the element type from the list expression being unwound.

        For VLP relationship variables, the list is an ArrayType(StructType(...))
        and we extract the StructType to enable property access on the unwound
        variable.

        Returns:
            The element type (typically StructType) if the list expression is
            a VLP relationship variable with known array type, None otherwise.
        """
        from gsql2rsql.parser.ast import QueryExpressionListComprehension
        from gsql2rsql.planner.data_types import ArrayType

        if not self.input_schema or not self.list_expression:
            return None

        # Handle case where list_expression is a simple variable reference
        # e.g., UNWIND e AS r where e is the VLP relationship variable
        if isinstance(self.list_expression, QueryExpressionProperty):
            var_name = self.list_expression.variable_name
            # property_name is None for bare variable reference like 'e'
            if self.list_expression.property_name is None:
                # Look up the variable in input schema
                for fld in self.input_schema.fields:
                    if isinstance(fld, ValueField) and fld.field_alias == var_name:
                        if fld.structured_type and isinstance(fld.structured_type, ArrayType):
                            # Return the element type (StructType for VLP edges)
                            return fld.structured_type.element_type

        # Handle case where list_expression is relationships(path) function
        # e.g., UNWIND relationships(p) AS r
        elif isinstance(self.list_expression, QueryExpressionFunction):
            if self.list_expression.function == Function.RELATIONSHIPS:
                # relationships(path) returns an array of edge structs
                # Extract the path variable from the parameter
                params = self.list_expression.parameters or []
                if params and isinstance(params[0], QueryExpressionProperty):
                    path_var = params[0].variable_name
                    # Look for the path_edges ValueField in input schema
                    # Priority 1: Check for exact path_edges field match
                    for fld in self.input_schema.fields:
                        if isinstance(fld, ValueField):
                            # Check for path_edges field with matching path variable
                            # Naming convention: _path_edges_{path_var}
                            if (
                                fld.field_alias == f"_path_edges_{path_var}"
                                or fld.field_name == f"_gsql2rsql_{path_var}_edges"
                            ):
                                if fld.structured_type and isinstance(
                                    fld.structured_type, ArrayType
                                ):
                                    return fld.structured_type.element_type
                    # Priority 2: Check for relationship variable with edges
                    # (e.g., 'e' in [e*1..3] which maps to _gsql2rsql_e_edges)
                    for fld in self.input_schema.fields:
                        if isinstance(fld, ValueField):
                            if (
                                fld.field_name
                                and fld.field_name.endswith("_edges")
                                and fld.structured_type
                                and isinstance(fld.structured_type, ArrayType)
                            ):
                                return fld.structured_type.element_type

        # Handle case where list_expression is a list comprehension
        # e.g., UNWIND [r IN relationships(p) WHERE r.prop <> 'x'] AS r
        # A filter-only comprehension preserves the element type of the source list.
        # A comprehension with a map expression changes the type (return None).
        elif isinstance(self.list_expression, QueryExpressionListComprehension):
            if self.list_expression.map_expression is None:
                # Filter-only: element type is same as source list's element type.
                # Recursively infer from the inner list_expression.
                inner = UnwindOperator.__new__(UnwindOperator)
                inner.variable_name = self.variable_name
                inner.list_expression = self.list_expression.list_expression
                inner.input_schema = self.input_schema
                return inner._infer_element_type()

        return None

    def introduced_symbols(self) -> set[str]:
        """Return symbols introduced by UNWIND."""
        if self.variable_name:
            return {self.variable_name}
        return set()
