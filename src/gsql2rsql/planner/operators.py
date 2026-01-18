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
            node_def = graph_definition.get_node_definition(self.entity.entity_name)
            if not node_def:
                from gsql2rsql.common.exceptions import (
                    TranspilerBindingException,
                )
                raise TranspilerBindingException(
                    f"Failed to bind entity '{self.entity.alias}' "
                    f"of type '{self.entity.entity_name}'"
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

            from gsql2rsql.parser.ast import RelationshipDirection

            if rel_entity.direction == RelationshipDirection.FORWARD:
                edge_def = graph_definition.get_edge_definition(
                    rel_entity.entity_name,
                    rel_entity.left_entity_name,
                    rel_entity.right_entity_name,
                )
            elif rel_entity.direction == RelationshipDirection.BACKWARD:
                edge_def = graph_definition.get_edge_definition(
                    rel_entity.entity_name,
                    rel_entity.right_entity_name,
                    rel_entity.left_entity_name,
                )
            else:
                # Try both directions
                edge_def = graph_definition.get_edge_definition(
                    rel_entity.entity_name,
                    rel_entity.left_entity_name,
                    rel_entity.right_entity_name,
                )
                if not edge_def:
                    edge_def = graph_definition.get_edge_definition(
                        rel_entity.entity_name,
                        rel_entity.right_entity_name,
                        rel_entity.left_entity_name,
                    )

            if not edge_def:
                from gsql2rsql.common.exceptions import (
                    TranspilerBindingException,
                )
                raise TranspilerBindingException(
                    f"Failed to bind entity '{self.entity.alias}' "
                    f"of type '{self.entity.entity_name}'"
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

    def __str__(self) -> str:
        base = super().__str__()
        filter_str = f"\n  Filter: {self.filter_expression}" if self.filter_expression else ""
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
    EITHER = auto()  # Node can join either source or sink
    BOTH = auto()  # Node joins both source and sink
    NODE_ID = auto()  # Node to node join


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
