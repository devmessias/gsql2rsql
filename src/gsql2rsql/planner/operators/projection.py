"""ProjectionOperator for projection (SELECT/RETURN clause)."""

from __future__ import annotations

from dataclasses import dataclass, field

from gsql2rsql.parser.ast import QueryExpression
from gsql2rsql.planner.operators.base import UnaryLogicalOperator
from gsql2rsql.planner.schema import (
    EntityField,
    Field,
    Schema,
    ValueField,
)


@dataclass
class ProjectionOperator(UnaryLogicalOperator):
    """Operator for projection (SELECT/RETURN clause).

    Attributes:
        projections: List of (alias, expression) tuples for SELECT columns.
        is_distinct: Whether to use SELECT DISTINCT.
        order_by: List of (expression, is_descending) for ORDER BY.
        limit: LIMIT value.
        skip: OFFSET value.
        filter_expression: WHERE clause filter (from flattened SelectionOperator).
        having_expression: HAVING clause filter (for aggregated columns).

    Note on filter_expression vs having_expression:
        - filter_expression: Applied BEFORE aggregation (SQL WHERE clause)
        - having_expression: Applied AFTER aggregation (SQL HAVING clause)

        This distinction is critical for correct SQL generation:
        - WHERE filters rows before GROUP BY
        - HAVING filters groups after GROUP BY

        The filter_expression is populated by SubqueryFlatteningOptimizer when
        merging a SelectionOperator into this ProjectionOperator.
    """

    projections: list[tuple[str, QueryExpression]] = field(default_factory=list)
    is_distinct: bool = False
    order_by: list[tuple[QueryExpression, bool]] = field(
        default_factory=list
    )  # (expr, is_descending)
    limit: int | None = None
    skip: int | None = None
    # WHERE expression for filtering rows BEFORE aggregation (from flattened Selection)
    filter_expression: QueryExpression | None = None
    # HAVING expression for filtering aggregated results AFTER aggregation
    having_expression: QueryExpression | None = None

    @property
    def depth(self) -> int:
        return (self.in_operator.depth if self.in_operator else 0) + 1

    def __str__(self) -> str:
        base = super().__str__()
        projs = ", ".join(f"{alias}={expr}" for alias, expr in self.projections)
        filter_str = f"\n  Filter: {self.filter_expression}" if self.filter_expression else ""
        having = f"\n  Having: {self.having_expression}" if self.having_expression else ""
        return f"{base}\n  Projections: {projs}{filter_str}{having}"

    def propagate_data_types_for_in_schema(self) -> None:
        """Propagate data types from upstream operator to input schema.

        Projection's input schema is the output schema of its input operator.
        """
        if self.in_operator and self.in_operator.output_schema:
            self.input_schema = Schema(self.in_operator.output_schema.fields)

    def introduced_symbols(self) -> set[str]:
        """Return symbols introduced by this projection.

        A symbol is introduced if the projection alias is not an existing
        symbol in the input scope. For example:
        - RETURN p.name AS name  -> 'name' is introduced
        - RETURN p               -> no new symbol if 'p' exists in input
        - RETURN p AS q          -> 'q' is introduced (aliasing)
        """
        introduced: set[str] = set()

        # Get input symbol names
        input_names: set[str] = set()
        if self.in_operator and self.in_operator.output_schema:
            for f in self.in_operator.output_schema:
                input_names.add(f.field_alias)

        for alias, _ in self.projections:
            if alias not in input_names:
                introduced.add(alias)

        return introduced

    def required_input_symbols(self) -> set[str]:
        """Return symbols required from input to compute output.

        Extracts all variable names referenced in projection expressions,
        filter_expression, having_expression, and order_by expressions.
        """
        required: set[str] = set()

        # Collect variable references from all projections
        for _, expr in self.projections:
            self._collect_variable_refs(expr, required)

        # Also collect from filter_expression if present
        if self.filter_expression:
            self._collect_variable_refs(self.filter_expression, required)

        # And from having_expression if present
        if self.having_expression:
            self._collect_variable_refs(self.having_expression, required)

        # And from order_by if present
        for order_expr, _ in self.order_by:
            self._collect_variable_refs(order_expr, required)

        return required

    def _collect_variable_refs(self, expr: QueryExpression, out: set[str]) -> None:
        """Recursively collect variable references from an expression."""
        from gsql2rsql.parser.ast import QueryExpressionProperty

        if isinstance(expr, QueryExpressionProperty):
            out.add(expr.variable_name)

        # Recurse into child expressions
        for child in expr.get_children_query_expression_type(QueryExpressionProperty):
            out.add(child.variable_name)

    def propagate_data_types_for_out_schema(self) -> None:
        """Propagate data types from input schema to output schema.

        The output schema contains:
        - For entity projections (bare variable like 'p'): EntityField or ValueField with ID
        - For property projections (p.name): ValueField with inferred type
        - For computed expressions: ValueField with inferred type

        This method ensures that downstream operators have correct schema information
        for column resolution and SQL generation.
        """
        from gsql2rsql.parser.ast import (
            QueryExpressionAggregationFunction,
            QueryExpressionMapLiteral,
            QueryExpressionProperty,
        )
        from gsql2rsql.planner.data_types import (
            ArrayType,
            PrimitiveType,
            StructField,
            StructType,
        )

        fields: list[Field] = []

        for alias, expr in self.projections:
            # Case 1: Bare entity reference (e.g., 'p' in RETURN p)
            if isinstance(expr, QueryExpressionProperty) and expr.property_name is None:
                var_name = expr.variable_name
                # Try to find entity in input schema
                entity_field = None
                for fld in self.input_schema:
                    if isinstance(fld, EntityField) and fld.field_alias == var_name:
                        entity_field = fld
                        break

                if entity_field:
                    # Create a new EntityField with the alias
                    if alias == var_name:
                        # Same name - keep the entity field
                        fields.append(EntityField(
                            field_alias=alias,
                            entity_name=entity_field.entity_name,
                            entity_type=entity_field.entity_type,
                            bound_entity_name=entity_field.bound_entity_name,
                            bound_source_entity_name=entity_field.bound_source_entity_name,
                            bound_sink_entity_name=entity_field.bound_sink_entity_name,
                            node_join_field=entity_field.node_join_field,
                            rel_source_join_field=entity_field.rel_source_join_field,
                            rel_sink_join_field=entity_field.rel_sink_join_field,
                            encapsulated_fields=entity_field.encapsulated_fields,
                        ))
                    else:
                        # Different alias - project the ID as a value
                        # Use the actual field_name from node_join_field to match
                        # what the renderer generates (e.g., "_gsql2rsql_other_node_id")
                        if entity_field.node_join_field:
                            id_field_name = entity_field.node_join_field.field_name
                            id_data_type = entity_field.node_join_field.data_type
                        else:
                            id_field_name = f"_gsql2rsql_{var_name}_id"
                            id_data_type = None
                        fields.append(ValueField(
                            field_alias=alias,
                            field_name=id_field_name,
                            data_type=id_data_type,
                        ))
                else:
                    # No entity found - might be a value reference
                    for fld in self.input_schema:
                        if isinstance(fld, ValueField) and fld.field_alias == var_name:
                            fields.append(ValueField(
                                field_alias=alias,
                                field_name=fld.field_name,
                                data_type=fld.data_type,
                                # Preserve structured_type for VLP arrays/structs
                                # Without this, UNWIND loses the struct field info
                                structured_type=fld.structured_type,
                            ))
                            # BUG FIX: When projecting a path variable, also carry through
                            # the associated _path_edges field. Without this, UNWIND
                            # relationships(path) loses the struct type info for edges.
                            # Detection: path variable has field_name like _gsql2rsql_X_id
                            # and has a structured_type (ArrayType for path nodes).
                            if (
                                fld.structured_type is not None
                                and fld.field_name
                                and fld.field_name.endswith("_id")
                            ):
                                # Look for associated _path_edges_{var_name} field
                                path_edges_alias = f"_path_edges_{var_name}"
                                for edges_fld in self.input_schema:
                                    if (
                                        isinstance(edges_fld, ValueField)
                                        and edges_fld.field_alias == path_edges_alias
                                    ):
                                        # Carry through the edges field unchanged
                                        fields.append(ValueField(
                                            field_alias=edges_fld.field_alias,
                                            field_name=edges_fld.field_name,
                                            data_type=edges_fld.data_type,
                                            structured_type=edges_fld.structured_type,
                                        ))
                                        break
                            break
                    else:
                        # Fallback: create a generic value field
                        fields.append(ValueField(
                            field_alias=alias,
                            field_name=f"_gsql2rsql_{alias}",
                            data_type=None,
                        ))

            # Case 2: Property access (e.g., 'p.name' in RETURN p.name AS name)
            elif isinstance(expr, QueryExpressionProperty) and expr.property_name is not None:
                var_name = expr.variable_name
                prop_name = expr.property_name

                # Try to find entity and get property type
                data_type = None
                for fld in self.input_schema:
                    if isinstance(fld, EntityField) and fld.field_alias == var_name:
                        for prop_field in fld.encapsulated_fields:
                            if prop_field.field_name == prop_name:
                                data_type = prop_field.data_type
                                break
                        break

                fields.append(ValueField(
                    field_alias=alias,
                    field_name=f"_gsql2rsql_{var_name}_{prop_name}",
                    data_type=data_type,
                ))

            # Case 3: Aggregation expression
            elif isinstance(expr, QueryExpressionAggregationFunction):
                # Infer type from aggregation function
                agg_name = expr.aggregation_function.name if expr.aggregation_function else ""
                data_type = None
                structured_type = None

                if agg_name in ("COUNT",):
                    data_type = int
                elif agg_name in ("SUM", "AVG"):
                    data_type = float
                elif agg_name in ("COLLECT",):
                    # COLLECT returns an array. If the inner expression is a map literal,
                    # we create a structured_type so that UNWIND can access struct fields.
                    # e.g., COLLECT({name: a.name, dept: a.dept}) -> ArrayType(StructType)
                    data_type = list
                    if (
                        expr.inner_expression
                        and isinstance(expr.inner_expression, QueryExpressionMapLiteral)
                    ):
                        map_literal = expr.inner_expression
                        # Extract field names from the map literal entries
                        struct_fields: list[StructField] = []
                        for key, _ in map_literal.entries:
                            struct_fields.append(
                                StructField(
                                    name=key,
                                    data_type=PrimitiveType.STRING,
                                    sql_name=key,
                                )
                            )
                        if struct_fields:
                            element_struct = StructType(
                                name=f"CollectedStruct_{alias}",
                                fields=tuple(struct_fields),
                            )
                            structured_type = ArrayType(element_type=element_struct)

                fields.append(ValueField(
                    field_alias=alias,
                    field_name=f"_gsql2rsql_{alias}",
                    data_type=data_type,
                    structured_type=structured_type,
                ))

            # Case 4: Other expressions (computed values)
            else:
                fields.append(ValueField(
                    field_alias=alias,
                    field_name=f"_gsql2rsql_{alias}",
                    data_type=None,  # Type inference could be added
                ))

        self.output_schema = Schema(fields)
