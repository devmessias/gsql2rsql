"""Logical operators for the query plan."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Iterator

from gsql2rsql.common.exceptions import TranspilerInternalErrorException
from gsql2rsql.common.schema import IGraphSchemaProvider
from gsql2rsql.common.utils import change_indentation, fnv_hash
from gsql2rsql.parser.ast import (
    Entity,
    NodeEntity,
    QueryExpression,
    RelationshipEntity,
)
from gsql2rsql.planner.schema import (
    EntityField,
    EntityType,
    Field,
    Schema,
    ValueField,
)

if TYPE_CHECKING:
    pass


class IBindable(ABC):
    """Interface for operators that can be bound to a graph schema."""

    @abstractmethod
    def bind(self, graph_definition: IGraphSchemaProvider) -> None:
        """Bind this operator to a graph schema definition."""
        ...


@dataclass
class LogicalOperator(ABC):
    """
    Base class for logical operators in the query plan.

    The logical plan is a DAG (directed acyclic graph) of logical operators.
    """

    input_schema: Schema = field(default_factory=Schema)
    output_schema: Schema = field(default_factory=Schema)
    operator_debug_id: int = 0
    _in_operators: list[LogicalOperator] = field(default_factory=list)
    _out_operators: list[LogicalOperator] = field(default_factory=list)

    @property
    @abstractmethod
    def depth(self) -> int:
        """The level of this operator in the plan."""
        ...

    @property
    def in_operators(self) -> list[LogicalOperator]:
        """Upstream operators."""
        return self._in_operators

    @property
    def out_operators(self) -> list[LogicalOperator]:
        """Downstream operators."""
        return self._out_operators

    def add_in_operator(self, op: LogicalOperator) -> None:
        """Add an upstream operator."""
        if op in self._in_operators:
            raise TranspilerInternalErrorException(f"Operator {op} already added")
        self._in_operators.append(op)

    def add_out_operator(self, op: LogicalOperator) -> None:
        """Add a downstream operator."""
        if op in self._out_operators:
            raise TranspilerInternalErrorException(f"Operator {op} already added")
        self._out_operators.append(op)

    def get_all_downstream_operators[T: LogicalOperator](
        self, op_type: type[T]
    ) -> Iterator[T]:
        """Get all downstream operators of a specific type."""
        if isinstance(self, op_type):
            yield self
        for out_op in self._out_operators:
            yield from out_op.get_all_downstream_operators(op_type)

    def get_all_upstream_operators[T: LogicalOperator](
        self, op_type: type[T]
    ) -> Iterator[T]:
        """Get all upstream operators of a specific type."""
        if isinstance(self, op_type):
            yield self
        for in_op in self._in_operators:
            yield from in_op.get_all_upstream_operators(op_type)

    def propagate_data_types_for_in_schema(self) -> None:
        """Propagate data types from upstream operators to input schema."""
        pass

    def propagate_data_types_for_out_schema(self) -> None:
        """Propagate data types from input schema to output schema."""
        pass

    # =========================================================================
    # Schema Propagation API (new methods for complete schema propagation)
    # See docs/development/schema-propagation.md for design details.
    # =========================================================================

    def get_output_scope(self) -> Schema:
        """Return the authoritative output scope for this operator.

        This is the single source of truth for what columns are available
        downstream. The renderer MUST use this instead of guessing.

        Returns:
            Schema containing all fields available to downstream operators.
        """
        return self.output_schema

    def required_input_symbols(self) -> set[str]:
        """Return symbols required from input to compute output.

        If a required symbol is not in input_schema, propagation should fail.
        Override in subclasses that consume symbols (e.g., Selection, Projection).

        Returns:
            Set of symbol names (field aliases) required from input.
        """
        # Default: no requirements
        return set()

    def introduced_symbols(self) -> set[str]:
        """Return symbols newly created by this operator.

        These are symbols that don't exist in the input but are created
        by this operator (e.g., DataSource introduces entity alias).
        Override in subclasses that create new symbols.

        Returns:
            Set of symbol names (field aliases) introduced by this operator.
        """
        # Default: no new symbols
        return set()

    def dump_scope(self) -> str:
        """Return human-readable dump of the output scope.

        Useful for debugging schema propagation issues.

        Returns:
            Multi-line string describing the output scope.
        """
        lines = [f"=== {self.__class__.__name__} (id={self.operator_debug_id}) ==="]
        lines.append(f"Output Scope ({len(self.output_schema)} fields):")
        for field in self.output_schema:
            if isinstance(field, EntityField):
                props = [f.field_alias for f in field.encapsulated_fields]
                lines.append(
                    f"  {field.field_alias}: {field.entity_name} "
                    f"({field.entity_type.name}) props={props}"
                )
            elif isinstance(field, ValueField):
                # Show authoritative structured_type when available
                if field.structured_type is not None:
                    type_info = f"authoritative={field.structured_type.sql_type_name()}"
                else:
                    type_info = f"legacy_type={field.data_type}"
                lines.append(
                    f"  {field.field_alias}: {field.field_name} ({type_info})"
                )
            else:
                lines.append(f"  {field.field_alias}: {type(field).__name__}")
        return "\n".join(lines)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(id={self.operator_debug_id})"


@dataclass
class UnaryLogicalOperator(LogicalOperator):
    """Operator with a single input."""

    @property
    def in_operator(self) -> LogicalOperator | None:
        """Get the single input operator."""
        return self._in_operators[0] if self._in_operators else None

    def set_in_operator(self, op: LogicalOperator) -> None:
        """Set the input operator."""
        self._in_operators = [op]
        op.add_out_operator(self)


@dataclass
class BinaryLogicalOperator(LogicalOperator):
    """Operator with two inputs."""

    @property
    def in_operator_left(self) -> LogicalOperator | None:
        """Get the left input operator."""
        return self._in_operators[0] if len(self._in_operators) > 0 else None

    @property
    def in_operator_right(self) -> LogicalOperator | None:
        """Get the right input operator."""
        return self._in_operators[1] if len(self._in_operators) > 1 else None

    def set_in_operators(self, left: LogicalOperator, right: LogicalOperator) -> None:
        """Set both input operators."""
        self._in_operators = [left, right]
        left.add_out_operator(self)
        right.add_out_operator(self)


@dataclass
class StartLogicalOperator(LogicalOperator):
    """Starting operator with no inputs (data source)."""

    @property
    def depth(self) -> int:
        return 0


@dataclass
class DataSourceOperator(StartLogicalOperator, IBindable):
    """Operator representing a data source (node or edge table).

    Attributes:
        entity: The graph entity (NodeEntity or RelationshipEntity) this source represents.
        filter_expression: Optional filter expression to apply to this data source.
            When set, the renderer should generate a WHERE clause for this source.
            This is populated by SelectionPushdownOptimizer when a predicate
            references only this entity's variable.
    """

    entity: Entity | None = None
    filter_expression: QueryExpression | None = None

    def __post_init__(self) -> None:
        if self.entity:
            # Initialize output schema with the entity
            entity_type = (
                EntityType.NODE
                if isinstance(self.entity, NodeEntity)
                else EntityType.RELATIONSHIP
            )
            self.output_schema = Schema([
                EntityField(
                    field_alias=self.entity.alias,
                    entity_name=self.entity.entity_name,
                    entity_type=entity_type,
                )
            ])

    def bind(self, graph_definition: IGraphSchemaProvider) -> None:
        """Bind this data source to a graph schema."""
        if not self.entity:
            return

        properties: list[ValueField] = []
        entity_unique_name = ""
        source_entity_name = ""
        sink_entity_name = ""
        node_id_field: ValueField | None = None
        edge_src_id_field: ValueField | None = None
        edge_sink_id_field: ValueField | None = None

        if isinstance(self.entity, NodeEntity):
            entity_name = self.entity.entity_name

            if not entity_name:
                # No label provided - try wildcard support
                node_def = graph_definition.get_wildcard_node_definition()
                if not node_def:
                    from gsql2rsql.common.exceptions import (
                        TranspilerBindingException,
                    )
                    raise TranspilerBindingException(
                        f"No-label node '{self.entity.alias}' not supported. "
                        f"Specify a label or enable no_label_support."
                    )
            else:
                node_def = graph_definition.get_node_definition(entity_name)
                if not node_def:
                    from gsql2rsql.common.exceptions import (
                        TranspilerBindingException,
                    )
                    raise TranspilerBindingException(
                        f"Failed to bind entity '{self.entity.alias}' "
                        f"of type '{entity_name}'"
                    )

            entity_unique_name = node_def.id
            if node_def.node_id_property:
                node_id_field = ValueField(
                    field_alias=node_def.node_id_property.property_name,
                    field_name=node_def.node_id_property.property_name,
                    data_type=node_def.node_id_property.data_type,
                )

            for prop in node_def.properties:
                properties.append(ValueField(
                    field_alias=prop.property_name,
                    field_name=prop.property_name,
                    data_type=prop.data_type,
                ))
            if node_id_field:
                properties.append(node_id_field)

        elif isinstance(self.entity, RelationshipEntity):
            rel_entity = self.entity
            edge_def = None
            resolved_edge_types: list[str] = []

            from gsql2rsql.parser.ast import RelationshipDirection

            # Parse edge types (handle OR syntax like "KNOWS|WORKS_AT")
            raw_edge_types = [
                t.strip() for t in rel_entity.entity_name.split("|") if t.strip()
            ]

            # Determine source/sink based on direction
            if rel_entity.direction == RelationshipDirection.FORWARD:
                source_type = rel_entity.left_entity_name or None
                sink_type = rel_entity.right_entity_name or None
            elif rel_entity.direction == RelationshipDirection.BACKWARD:
                source_type = rel_entity.right_entity_name or None
                sink_type = rel_entity.left_entity_name or None
            else:
                source_type = rel_entity.left_entity_name or None
                sink_type = rel_entity.right_entity_name or None

            # Try to bind each edge type
            for edge_type in raw_edge_types:
                found_edge = None

                # Check if either endpoint is unknown (no label)
                if source_type is None or sink_type is None:
                    # Partial lookup
                    edges = graph_definition.find_edges_by_verb(
                        edge_type,
                        from_node_name=source_type,
                        to_node_name=sink_type,
                    )
                    if edges:
                        found_edge = edges[0]
                    # If direction is BOTH, also try reverse
                    if not found_edge and rel_entity.direction == RelationshipDirection.BOTH:
                        edges = graph_definition.find_edges_by_verb(
                            edge_type,
                            from_node_name=sink_type,
                            to_node_name=source_type,
                        )
                        if edges:
                            found_edge = edges[0]
                else:
                    # Exact lookup
                    if rel_entity.direction == RelationshipDirection.FORWARD:
                        found_edge = graph_definition.get_edge_definition(
                            edge_type,
                            rel_entity.left_entity_name,
                            rel_entity.right_entity_name,
                        )
                    elif rel_entity.direction == RelationshipDirection.BACKWARD:
                        found_edge = graph_definition.get_edge_definition(
                            edge_type,
                            rel_entity.right_entity_name,
                            rel_entity.left_entity_name,
                        )
                    else:
                        # Try both directions
                        found_edge = graph_definition.get_edge_definition(
                            edge_type,
                            rel_entity.left_entity_name,
                            rel_entity.right_entity_name,
                        )
                        if not found_edge:
                            found_edge = graph_definition.get_edge_definition(
                                edge_type,
                                rel_entity.right_entity_name,
                                rel_entity.left_entity_name,
                            )

                if found_edge:
                    resolved_edge_types.append(edge_type)
                    if edge_def is None:
                        edge_def = found_edge  # Use first for schema

            if not edge_def:
                # No edge found - check if this is an untyped edge
                if not raw_edge_types:
                    # Untyped edge (e.g., -[]- or --), use wildcard edge
                    edge_def = graph_definition.get_wildcard_edge_definition()

                if not edge_def:
                    from gsql2rsql.common.exceptions import (
                        TranspilerBindingException,
                    )
                    raise TranspilerBindingException(
                        f"Failed to bind relationship '{rel_entity.alias}' "
                        f"of type '{rel_entity.entity_name}'"
                    )

            entity_unique_name = edge_def.id
            source_entity_name = edge_def.source_node_id
            sink_entity_name = edge_def.sink_node_id

            if edge_def.source_id_property:
                edge_src_id_field = ValueField(
                    field_alias=edge_def.source_id_property.property_name,
                    field_name=edge_def.source_id_property.property_name,
                    data_type=edge_def.source_id_property.data_type,
                )
            if edge_def.sink_id_property:
                edge_sink_id_field = ValueField(
                    field_alias=edge_def.sink_id_property.property_name,
                    field_name=edge_def.sink_id_property.property_name,
                    data_type=edge_def.sink_id_property.data_type,
                )

            for prop in edge_def.properties:
                properties.append(ValueField(
                    field_alias=prop.property_name,
                    field_name=prop.property_name,
                    data_type=prop.data_type,
                ))
            if edge_src_id_field:
                properties.append(edge_src_id_field)
            if edge_sink_id_field:
                properties.append(edge_sink_id_field)

        # Update the output schema entity field
        if self.output_schema:
            entity_field = self.output_schema[0]
            if isinstance(entity_field, EntityField):
                entity_field.bound_entity_name = entity_unique_name
                entity_field.bound_source_entity_name = source_entity_name
                entity_field.bound_sink_entity_name = sink_entity_name
                entity_field.encapsulated_fields = properties
                entity_field.node_join_field = node_id_field
                entity_field.rel_source_join_field = edge_src_id_field
                entity_field.rel_sink_join_field = edge_sink_id_field
                # Store resolved edge types for OR syntax ([:KNOWS|WORKS_AT])
                if isinstance(self.entity, RelationshipEntity):
                    entity_field.bound_edge_types = resolved_edge_types

    def introduced_symbols(self) -> set[str]:
        """Return symbols introduced by this data source.

        DataSource introduces exactly one symbol: the entity alias.
        """
        if self.entity:
            return {self.entity.alias}
        return set()

    def __str__(self) -> str:
        base = super().__str__()
        filter_str = (
            f"\n  Filter: {self.filter_expression}"
            if self.filter_expression
            else ""
        )
        return f"{base}\n  DataSource: {self.entity}{filter_str}"


class JoinType(Enum):
    """Type of join operation."""

    CROSS = 0
    LEFT = 1
    INNER = 2


class JoinKeyPairType(Enum):
    """Type of join key pairing."""

    NONE = auto()
    SOURCE = auto()  # Node join to Relationship's SourceId
    SINK = auto()  # Node join to Relationship's SinkId
    EITHER = auto()  # Node can join either source or sink (legacy, for VLP)
    BOTH = auto()  # Node joins both source and sink
    NODE_ID = auto()  # Node to node join
    # For undirected single-hop with UNION ALL expansion:
    # - EITHER_AS_SOURCE: source-side node joins on source_key after UNION
    # - EITHER_AS_SINK: sink-side node joins on sink_key after UNION
    EITHER_AS_SOURCE = auto()
    EITHER_AS_SINK = auto()


@dataclass
class JoinKeyPair:
    """Structure designating how two entities should be joined."""

    node_alias: str
    relationship_or_node_alias: str
    pair_type: JoinKeyPairType = JoinKeyPairType.NONE

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JoinKeyPair):
            return False
        return (
            self.pair_type == other.pair_type
            and self.node_alias == other.node_alias
            and self.relationship_or_node_alias == other.relationship_or_node_alias
        )

    def __hash__(self) -> int:
        return fnv_hash(self.pair_type, self.node_alias, self.relationship_or_node_alias)

    def __str__(self) -> str:
        return (
            f"JoinPair: Node={self.node_alias} "
            f"RelOrNode={self.relationship_or_node_alias} Type={self.pair_type.name}"
        )


@dataclass
class JoinOperator(BinaryLogicalOperator):
    """Operator to perform joins between data sources."""

    join_type: JoinType = JoinType.INNER
    join_pairs: list[JoinKeyPair] = field(default_factory=list)

    def propagate_data_types_for_in_schema(self) -> None:
        """Propagate data types from upstream operators to input schema."""
        if self.in_operator_left and self.in_operator_right:
            self.input_schema = Schema.merge(
                self.in_operator_left.output_schema,
                self.in_operator_right.output_schema,
            )

    def propagate_data_types_for_out_schema(self) -> None:
        """Propagate data types from input schema to output schema."""
        self.output_schema = Schema(self.input_schema.fields)

    @property
    def depth(self) -> int:
        left_depth = self.in_operator_left.depth if self.in_operator_left else 0
        right_depth = self.in_operator_right.depth if self.in_operator_right else 0
        return max(left_depth, right_depth) + 1

    def add_join_pair(self, pair: JoinKeyPair) -> None:
        """Add a join key pair."""
        if pair not in self.join_pairs:
            self.join_pairs.append(pair)

    def __str__(self) -> str:
        base = super().__str__()
        joins = ", ".join(str(p) for p in self.join_pairs)
        return f"{base}\n  JoinType: {self.join_type.name}\n  Joins: {joins}"


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
            for field in self.in_operator.output_schema:
                input_names.add(field.field_alias)

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
            QueryExpressionProperty,
            QueryExpressionAggregationFunction,
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
                        fields.append(ValueField(
                            field_alias=alias,
                            field_name=f"_gsql2rsql_{var_name}_id",
                            data_type=entity_field.node_join_field.data_type if entity_field.node_join_field else None,
                        ))
                else:
                    # No entity found - might be a value reference
                    for fld in self.input_schema:
                        if isinstance(fld, ValueField) and fld.field_alias == var_name:
                            fields.append(ValueField(
                                field_alias=alias,
                                field_name=fld.field_name,
                                data_type=fld.data_type,
                            ))
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
                if agg_name in ("COUNT",):
                    data_type = int
                elif agg_name in ("SUM", "AVG"):
                    data_type = float
                else:
                    data_type = None

                fields.append(ValueField(
                    field_alias=alias,
                    field_name=f"_gsql2rsql_{alias}",
                    data_type=data_type,
                ))

            # Case 4: Other expressions (computed values)
            else:
                fields.append(ValueField(
                    field_alias=alias,
                    field_name=f"_gsql2rsql_{alias}",
                    data_type=None,  # Type inference could be added
                ))

        self.output_schema = Schema(fields)


@dataclass
class AggregationBoundaryOperator(UnaryLogicalOperator):
    """Operator representing a materialization boundary after aggregation.

    This operator represents a WITH clause that contains aggregation functions.
    It creates a "checkpoint" in the query plan where:
    - The input relation is aggregated according to group_keys and aggregates
    - Only projected columns are visible after this point
    - Subsequent MATCH clauses must join with the aggregated result

    This is semantically equivalent to a SQL CTE/subquery and enforces Cypher's
    variable scoping rules where aggregating WITH creates a new scope.

    Example:
        MATCH (a)-[:R1]->(b)
        WITH a, COUNT(b) AS cnt    -- Creates AggregationBoundaryOperator
        WHERE cnt > 5              -- Having filter (applied after aggregation)
        MATCH (a)-[:R2]->(c)       -- Joins with aggregated result
        RETURN a, cnt, COUNT(c)

    Attributes:
        group_keys: List of (alias, expression) tuples for GROUP BY columns.
            These are the non-aggregated columns from the WITH clause.
        aggregates: List of (alias, aggregation_expression) tuples for aggregations.
            These are the columns containing aggregate functions (COUNT, SUM, etc.)
        having_filter: Optional filter expression applied AFTER aggregation (HAVING).
            Comes from WITH ... WHERE when the filter references aggregated columns.
        order_by: Optional list of (expression, is_descending) for ORDER BY.
        limit: Optional LIMIT value.
        skip: Optional SKIP/OFFSET value.
        cte_name: Name for the CTE when rendered to SQL (auto-generated if not set).
        projected_variables: Set of variable names that are projected through this
            boundary. Used to validate that subsequent MATCHes can reference them.
    """

    group_keys: list[tuple[str, QueryExpression]] = field(default_factory=list)
    aggregates: list[tuple[str, QueryExpression]] = field(default_factory=list)
    having_filter: QueryExpression | None = None
    order_by: list[tuple[QueryExpression, bool]] = field(default_factory=list)
    limit: int | None = None
    skip: int | None = None
    cte_name: str = ""
    projected_variables: set[str] = field(default_factory=set)

    @property
    def depth(self) -> int:
        return (self.in_operator.depth if self.in_operator else 0) + 1

    @property
    def all_projections(self) -> list[tuple[str, QueryExpression]]:
        """Return all projections (group keys + aggregates) in order."""
        return self.group_keys + self.aggregates

    def propagate_data_types_for_in_schema(self) -> None:
        """Propagate data types from upstream operators to input schema."""
        if self.in_operator and self.in_operator.output_schema:
            self.input_schema = Schema(self.in_operator.output_schema.fields)

    def propagate_data_types_for_out_schema(self) -> None:
        """Propagate data types from input schema to output schema.

        The output schema contains only the projected columns (group keys + aggregates).
        Entity fields from input are converted to ValueFields with their ID columns.
        """
        fields: list[Field] = []
        for alias, _ in self.all_projections:
            # Create a ValueField for each projected column
            # The actual data type would be inferred from the expression
            fields.append(ValueField(
                field_alias=alias,
                field_name=alias,
                data_type=None,  # Would need type inference
            ))
        self.output_schema = Schema(fields)

    def __str__(self) -> str:
        base = super().__str__()
        group_str = ", ".join(alias for alias, _ in self.group_keys)
        agg_str = ", ".join(alias for alias, _ in self.aggregates)
        having_str = f"\n  Having: {self.having_filter}" if self.having_filter else ""
        return f"{base}\n  GroupBy: [{group_str}]\n  Aggregates: [{agg_str}]{having_str}"


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


class RecursiveTraversalOperator(LogicalOperator):
    r"""Operator for recursive traversal (BFS/DFS with variable-length paths).

    Supports path accumulation for nodes(path) and relationships(path) functions.
    When path_variable is set, the CTE accumulates:
    - path_nodes: ARRAY of node IDs in traversal order
    - path_edges: ARRAY of STRUCT with edge properties

    This enables HoF predicates like:
    - ALL(rel IN relationships(path) WHERE rel.amount > 1000)
    - [n IN nodes(path) | n.id]

    PREDICATE PUSHDOWN OPTIMIZATION
    ================================

    The `edge_filter` field enables a critical optimization called "Predicate Pushdown"
    that can dramatically reduce memory usage and execution time for path queries.

    Problem: Exponential Path Growth
    --------------------------------

    Without pushdown, recursive CTEs explore ALL possible paths first, then filter:

                                        A
                                       /|\
                   depth=1 →  $100    $50    $2000
                               /|\     |       |
                  depth=2 →  $20 $30  $15    $5000
                              ...     ...     ...
                               ↓       ↓       ↓
                        ═══════════════════════════
                        AFTER CTE: 10,000+ paths
                        ═══════════════════════════
                               ↓
                        FORALL(edges, e -> e.amount > 1000)
                               ↓
                        ═══════════════════════════
                        FINAL: Only 2 paths survive!
                        ═══════════════════════════

    This is wasteful: we explored 10,000 paths but kept only 2.

    Solution: Push Filter INTO the CTE
    -----------------------------------

    With predicate pushdown, we filter DURING recursion:

                                        A
                                        |
                   depth=1 →         $2000  ← Only edges with amount > 1000
                                        |
                  depth=2 →          $5000
                                        |
                        ═══════════════════════════
                        AFTER CTE: Only 2 paths (already filtered!)
                        ═══════════════════════════

    SQL Comparison:

    BEFORE (no pushdown):
        WITH RECURSIVE paths AS (
          SELECT ... FROM Transfer e          -- ALL edges
          UNION ALL
          SELECT ... FROM paths p JOIN Transfer e ...  -- ALL paths
        )
        SELECT ... WHERE FORALL(path_edges, r -> r.amount > 1000)

    AFTER (with pushdown):
        WITH RECURSIVE paths AS (
          SELECT ... FROM Transfer e
            WHERE e.amount > 1000             ← PREDICATE IN BASE CASE
          UNION ALL
          SELECT ... FROM paths p JOIN Transfer e ...
            WHERE e.amount > 1000             ← PREDICATE IN RECURSIVE CASE
        )
        SELECT ...  -- No FORALL needed!

    When is Pushdown Safe?
    ----------------------

    Only ALL() predicates can be pushed down:
    - ALL(r IN relationships(path) WHERE r.amount > 1000)
      → "Every edge must satisfy" = filter each edge individually ✓

    ANY() predicates CANNOT be pushed:
    - ANY(r IN relationships(path) WHERE r.flagged)
      → "At least one edge must satisfy" = need complete path first ✗
    """

    def __init__(
        self,
        edge_types: list[str],
        source_node_type: str,
        target_node_type: str,
        min_hops: int,
        max_hops: int | None = None,
        source_id_column: str = "id",
        target_id_column: str = "id",
        start_node_filter: QueryExpression | None = None,
        sink_node_filter: QueryExpression | None = None,
        cte_name: str = "",
        source_alias: str = "",
        target_alias: str = "",
        path_variable: str = "",
        collect_nodes: bool = False,
        collect_edges: bool = False,
        edge_properties: list[str] | None = None,
        edge_filter: QueryExpression | None = None,
        edge_filter_lambda_var: str = "",
    ) -> None:
        super().__init__()
        self.edge_types = edge_types
        self.source_node_type = source_node_type
        self.target_node_type = target_node_type
        self.min_hops = min_hops
        self.max_hops = max_hops
        self.source_id_column = source_id_column
        self.target_id_column = target_id_column
        self.start_node_filter = start_node_filter
        self.sink_node_filter = sink_node_filter
        self.cte_name = cte_name
        self.source_alias = source_alias
        self.target_alias = target_alias
        # Path accumulation support
        self.path_variable = path_variable
        self.collect_nodes = collect_nodes or bool(path_variable)
        self.collect_edges = collect_edges or bool(path_variable)
        self.edge_properties = edge_properties or []

        # Predicate pushdown for early path filtering
        # See class docstring for detailed explanation of this optimization
        self.edge_filter = edge_filter
        self.edge_filter_lambda_var = edge_filter_lambda_var

    @property
    def depth(self) -> int:
        if not self._in_operators:
            return 1
        return max(op.depth for op in self._in_operators) + 1

    @property
    def is_circular(self) -> bool:
        """Check if this is a circular path (source and target are the same variable)."""
        return bool(self.source_alias and self.source_alias == self.target_alias)

    def __str__(self) -> str:
        edge_str = "|".join(self.edge_types)
        hops_str = f"*{self.min_hops}..{self.max_hops}" if self.max_hops else f"*{self.min_hops}.."
        path_str = f", path={self.path_variable}" if self.path_variable else ""
        circular_str = ", circular=True" if self.is_circular else ""
        return f"RecursiveTraversal({edge_str}{hops_str}{path_str}{circular_str})"

    def propagate_data_types_for_in_schema(self) -> None:
        """Propagate data types from upstream operators to input schema.

        RecursiveTraversal's input schema is the merged output of all input operators
        (typically the source node's DataSourceOperator).
        """
        if self._in_operators:
            merged_fields: list[Field] = []
            for op in self._in_operators:
                if op.output_schema:
                    merged_fields.extend(op.output_schema.fields)
            self.input_schema = Schema(merged_fields)

    def propagate_data_types_for_out_schema(self) -> None:
        """Propagate data types to output schema.

        RecursiveTraversal output includes:
        1. All fields from input (source node)
        2. Target node as EntityField
        3. Path variable if specified (AUTHORITATIVE ArrayType with structured element)

        AUTHORITATIVE SCHEMA DECLARATION
        ---------------------------------
        This method is the source of truth for the path variable's type.
        The path is declared as ARRAY<STRUCT<id: INT, ...>> where the struct
        contains at minimum the node ID field. This enables downstream components
        (ColumnResolver, Renderer) to correctly resolve expressions like:
            [n IN nodes(path) | n.id]

        The resolver MUST trust this declaration and NOT infer the type.
        The renderer MUST use this type information and NOT guess.
        """
        fields: list[Field] = []

        # Copy input fields (source node)
        if self.input_schema:
            fields.extend(self.input_schema.fields)

        # Add target node as EntityField
        if self.target_alias:
            target_field = EntityField(
                field_alias=self.target_alias,
                entity_name=self.target_alias,
                entity_type=EntityType.NODE,
                bound_entity_name=self.target_node_type,
                node_join_field=ValueField(
                    field_alias=f"{self.target_alias}_id",
                    field_name=f"_gsql2rsql_{self.target_alias}_id",
                    data_type=int,
                ),
                encapsulated_fields=[],
            )
            fields.append(target_field)

        # Add path variable if specified (with AUTHORITATIVE structured type)
        if self.path_variable:
            path_field = self._create_authoritative_path_field()
            fields.append(path_field)

        self.output_schema = Schema(fields)

    def _create_authoritative_path_field(self) -> ValueField:
        """Create an authoritative path field with structured type.

        This method creates a ValueField for the path variable with a fully
        specified ArrayType(StructType(...)) that enables proper resolution
        of expressions like [n IN nodes(path) | n.id].

        DESIGN NOTE:
        ------------
        The path contains node IDs (not full node objects), so when we iterate
        over nodes(path), we're iterating over integers. However, since Cypher
        semantics allow n.id on path elements, we model the element as a struct
        with an 'id' field.

        For now, we use a minimal struct with just the ID field. If we need
        additional node properties in the future, we can extend this.

        TODO: If multi-label nodes are traversed, the struct should include
              only fields guaranteed to exist on all possible node types.

        Returns:
            ValueField with authoritative ArrayType(StructType) type
        """
        from gsql2rsql.planner.data_types import (
            ArrayType,
            StructType,
            StructField,
            PrimitiveType,
        )
        from gsql2rsql.planner.column_ref import compute_sql_column_name

        # Build the struct fields for path elements
        # At minimum, we guarantee the 'id' field exists
        struct_fields: list[StructField] = [
            StructField(
                name="id",
                data_type=PrimitiveType.INT,
                sql_name=compute_sql_column_name("node", "id"),
            ),
        ]

        # Create the element struct type
        # TODO: Add 'label' field if needed for multi-label traversals
        element_struct = StructType(
            name=f"PathElement_{self.path_variable}",
            fields=tuple(struct_fields),
        )

        # Create the array type
        path_type = ArrayType(element_type=element_struct)

        # Create the ValueField with authoritative type
        return ValueField(
            field_alias=self.path_variable,
            field_name=f"_gsql2rsql_{self.path_variable}",
            data_type=list,  # Legacy type for backward compatibility
            structured_type=path_type,  # AUTHORITATIVE type declaration
        )

    def introduced_symbols(self) -> set[str]:
        """Return symbols introduced by this traversal.

        RecursiveTraversal introduces:
        - target_alias (if specified)
        - path_variable (if specified)
        """
        introduced: set[str] = set()
        if self.target_alias:
            introduced.add(self.target_alias)
        if self.path_variable:
            introduced.add(self.path_variable)
        return introduced


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
        """
        fields: list[Field] = []

        # Preserve upstream fields
        if self.input_schema:
            fields.extend(self.input_schema.fields)

        # Add the unwound variable
        if self.variable_name:
            fields.append(ValueField(
                field_alias=self.variable_name,
                field_name=f"_gsql2rsql_{self.variable_name}",
                data_type=None,
            ))

        self.output_schema = Schema(fields)

    def introduced_symbols(self) -> set[str]:
        """Return symbols introduced by UNWIND."""
        if self.variable_name:
            return {self.variable_name}
        return set()
