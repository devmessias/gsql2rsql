"""SQL Renderer - Converts logical plan to Databricks SQL with WITH RECURSIVE support."""

from __future__ import annotations

from enum import Enum, auto
from typing import Any

from gsql2rsql.common.exceptions import (
    TranspilerInternalErrorException,
    TranspilerNotSupportedException,
)
from gsql2rsql.common.logging import ILoggable
from gsql2rsql.parser.ast import (
    NodeEntity,
    QueryExpression,
    QueryExpressionAggregationFunction,
    QueryExpressionBinary,
    QueryExpressionCaseExpression,
    QueryExpressionExists,
    QueryExpressionFunction,
    QueryExpressionList,
    QueryExpressionListComprehension,
    QueryExpressionListPredicate,
    QueryExpressionMapLiteral,
    QueryExpressionParameter,
    QueryExpressionProperty,
    QueryExpressionReduce,
    QueryExpressionValue,
    RelationshipEntity,
)
from gsql2rsql.parser.operators import (
    AggregationFunction,
    BinaryOperator,
    Function,
    ListPredicateType,
)
from gsql2rsql.planner.logical_plan import LogicalPlan
from gsql2rsql.planner.operators import (
    AggregationBoundaryOperator,
    DataSourceOperator,
    JoinKeyPairType,
    JoinOperator,
    JoinType,
    LogicalOperator,
    ProjectionOperator,
    RecursiveTraversalOperator,
    SelectionOperator,
    SetOperationType,
    SetOperator,
    UnwindOperator,
)
from gsql2rsql.planner.path_analyzer import rewrite_predicate_for_edge_alias
from gsql2rsql.common.schema import EdgeSchema
from gsql2rsql.planner.schema import EntityField, EntityType, Schema, ValueField
from gsql2rsql.planner.column_resolver import ResolutionResult
from gsql2rsql.planner.column_ref import ResolvedColumnRef
from gsql2rsql.renderer.schema_provider import (
    ISQLDBSchemaProvider,
    SQLTableDescriptor,
)


class DatabricksSqlType(Enum):
    """Databricks SQL data types."""

    INT = auto()
    SMALLINT = auto()
    BIGINT = auto()
    DOUBLE = auto()
    FLOAT = auto()
    STRING = auto()
    BOOLEAN = auto()
    BINARY = auto()
    DECIMAL = auto()
    DATE = auto()
    TIMESTAMP = auto()
    ARRAY = auto()
    MAP = auto()
    STRUCT = auto()


# Mapping from Python types to Databricks SQL types
TYPE_TO_SQL_TYPE: dict[type[Any], DatabricksSqlType] = {
    int: DatabricksSqlType.BIGINT,
    float: DatabricksSqlType.DOUBLE,
    str: DatabricksSqlType.STRING,
    bool: DatabricksSqlType.BOOLEAN,
    bytes: DatabricksSqlType.BINARY,
    list: DatabricksSqlType.ARRAY,
    dict: DatabricksSqlType.MAP,
}

# Mapping from Databricks SQL types to their string representations
SQL_TYPE_RENDERING: dict[DatabricksSqlType, str] = {
    DatabricksSqlType.INT: "INT",
    DatabricksSqlType.SMALLINT: "SMALLINT",
    DatabricksSqlType.BIGINT: "BIGINT",
    DatabricksSqlType.DOUBLE: "DOUBLE",
    DatabricksSqlType.FLOAT: "FLOAT",
    DatabricksSqlType.STRING: "STRING",
    DatabricksSqlType.BOOLEAN: "BOOLEAN",
    DatabricksSqlType.BINARY: "BINARY",
    DatabricksSqlType.DECIMAL: "DECIMAL(38, 10)",
    DatabricksSqlType.DATE: "DATE",
    DatabricksSqlType.TIMESTAMP: "TIMESTAMP",
    DatabricksSqlType.ARRAY: "ARRAY<STRING>",
    DatabricksSqlType.MAP: "MAP<STRING, STRING>",
    DatabricksSqlType.STRUCT: "STRUCT<>",
}

# Mapping from binary operators to SQL patterns (Databricks compatible)
OPERATOR_PATTERNS: dict[BinaryOperator, str] = {
    BinaryOperator.PLUS: "({0}) + ({1})",
    BinaryOperator.MINUS: "({0}) - ({1})",
    BinaryOperator.MULTIPLY: "({0}) * ({1})",
    BinaryOperator.DIVIDE: "({0}) / ({1})",
    BinaryOperator.MODULO: "({0}) % ({1})",
    BinaryOperator.EXPONENTIATION: "POWER({0}, {1})",
    BinaryOperator.AND: "({0}) AND ({1})",
    BinaryOperator.OR: "({0}) OR ({1})",
    BinaryOperator.XOR: "(({0}) AND NOT ({1})) OR (NOT ({0}) AND ({1}))",
    BinaryOperator.LT: "({0}) < ({1})",
    BinaryOperator.LEQ: "({0}) <= ({1})",
    BinaryOperator.GT: "({0}) > ({1})",
    BinaryOperator.GEQ: "({0}) >= ({1})",
    BinaryOperator.EQ: "({0}) = ({1})",
    BinaryOperator.NEQ: "({0}) != ({1})",
    BinaryOperator.REGMATCH: "({0}) RLIKE ({1})",
    BinaryOperator.IN: "({0}) IN {1}",
}

# Mapping from aggregation functions to SQL patterns (Databricks compatible)
AGGREGATION_PATTERNS: dict[AggregationFunction, str] = {
    AggregationFunction.AVG: "AVG(CAST({0} AS DOUBLE))",
    AggregationFunction.SUM: "SUM({0})",
    AggregationFunction.MIN: "MIN({0})",
    AggregationFunction.MAX: "MAX({0})",
    AggregationFunction.FIRST: "FIRST({0})",
    AggregationFunction.LAST: "LAST({0})",
    AggregationFunction.STDEV: "STDDEV({0})",
    AggregationFunction.STDEVP: "STDDEV_POP({0})",
    AggregationFunction.COUNT: "COUNT({0})",
    AggregationFunction.COLLECT: "COLLECT_LIST({0})",
}


class SQLRenderer:
    """
    Renders a logical plan to Databricks SQL.

    This class converts the relational algebra operators in the logical plan
    into equivalent Databricks SQL query statements, with support for
    WITH RECURSIVE CTEs for variable-length path traversals.
    """

    # Prefix for generated column names to avoid collisions with user identifiers.
    # Uses _gsql2rsql_ to match the naming convention from column_ref.py.
    COLUMN_PREFIX = "_gsql2rsql_"

    def __init__(
        self,
        graph_def: ISQLDBSchemaProvider | None = None,
        logger: ILoggable | None = None,
        *,
        graph_schema_provider: ISQLDBSchemaProvider | None = None,
        db_schema_provider: ISQLDBSchemaProvider | None = None,
        enable_column_pruning: bool = True,
        config: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize the SQL renderer.

        Args:
            graph_def: The SQL schema provider with table mappings.
                Deprecated: use db_schema_provider instead.
            logger: Optional logger for debugging.
            graph_schema_provider: The graph schema provider (for future use).
            db_schema_provider: The SQL database schema provider.
            enable_column_pruning: Enable column pruning optimization.
            config: Optional configuration dictionary.
                Supported keys:
                - 'undirected_strategy': Strategy for undirected relationships.
                  Values: 'union_edges' (default) or 'or_join'
        """
        # Support both old and new parameter names
        self._graph_def = db_schema_provider or graph_def
        if self._graph_def is None:
            raise ValueError(
                "Either graph_def or db_schema_provider must be provided"
            )
        self._graph_schema_provider = graph_schema_provider
        self._logger = logger
        self._cte_counter = 0
        # Column pruning: set of required column aliases (e.g., "_gsql2rsql_p_name")
        self._required_columns: set[str] = set()
        # Bare variable references (e.g., "shared_cards") - used for ValueFields
        self._required_value_fields: set[str] = set()
        # Enable column pruning by default
        self._enable_column_pruning = enable_column_pruning
        # Resolution result from ColumnResolver (Phase 1 of renderer refactoring).
        # When available, the renderer uses resolved column references instead of
        # inferring them from schema lookups. This makes the renderer "stupid and safe".
        # Set during render_plan() if the plan has been resolved.
        self._resolution_result: ResolutionResult | None = None

        # Configuration
        self._config = config or {}

    @property
    def _db_schema(self) -> "ISQLDBSchemaProvider":
        """Get the database schema provider (guaranteed non-None after __init__)."""
        assert self._graph_def is not None
        return self._graph_def

    @property
    def _resolved(self) -> "ResolutionResult":
        """Get the resolution result (guaranteed non-None during rendering)."""
        assert self._resolution_result is not None
        return self._resolution_result

    def render_plan(self, plan: LogicalPlan) -> str:
        """
        Render a logical plan to Databricks SQL.

        The renderer is now "stupid and safe" - it requires pre-resolved plans
        and only performs mechanical SQL generation. All semantic decisions
        (column resolution, scope checking) are handled by ColumnResolver.

        Args:
            plan: The logical plan to render. MUST be resolved via plan.resolve()
                  before calling this method.

        Returns:
            The rendered Databricks SQL query string.

        Raises:
            ValueError: If the plan has not been resolved.

        Trade-offs:
            - Enforces separation of concerns (resolver = semantic, renderer = mechanical)
            - Prevents subtle bugs from schema-based guessing
            - Requires calling plan.resolve() before rendering
        """
        if not plan.terminal_operators:
            return ""

        # REQUIRE RESOLUTION: Renderer is now stupid and safe
        if not plan.is_resolved or plan.resolution_result is None:
            raise ValueError(
                "SQLRenderer requires a resolved plan. Call plan.resolve(original_query) "
                "before rendering. The renderer no longer performs column resolution - "
                "that is the job of ColumnResolver."
            )

        # Reset CTE counter
        self._cte_counter = 0

        # Store resolution result - this is now guaranteed to be non-None
        self._resolution_result = plan.resolution_result

        # Column pruning: collect required columns before rendering
        self._required_columns = set()
        self._required_value_fields = set()
        if self._enable_column_pruning:
            for terminal_op in plan.terminal_operators:
                self._collect_required_columns(terminal_op)

        # Collect any operators that need CTEs
        # Use a shared visited set to avoid duplicates across starting operators
        ctes: list[str] = []
        visited: set[int] = set()
        has_recursive_cte = False
        for start_op in plan.starting_operators:
            has_recursive = self._collect_ctes(start_op, ctes, visited)
            has_recursive_cte = has_recursive_cte or has_recursive

        # Render from the terminal operator
        terminal_op = plan.terminal_operators[0]
        main_query = self._render_operator(terminal_op, depth=0)

        # Combine CTEs with main query
        if ctes:
            # Use WITH RECURSIVE only if there are recursive traversal CTEs
            cte_keyword = "WITH RECURSIVE" if has_recursive_cte else "WITH"
            cte_block = f"{cte_keyword}\n" + ",\n".join(ctes)
            return f"{cte_block}\n{main_query}"

        return main_query

    def _collect_ctes(
        self,
        op: LogicalOperator,
        ctes: list[str],
        visited: set[int],
    ) -> bool:
        """Collect CTE definitions from the plan.

        Args:
            op: The operator to start traversal from
            ctes: List to collect CTE definitions into
            visited: Set of already-visited operator IDs to avoid duplicates

        Returns:
            True if any RecursiveTraversalOperator CTEs were found
        """
        # Avoid visiting the same operator twice
        op_id = id(op)
        if op_id in visited:
            return False
        visited.add(op_id)

        has_recursive = False

        if isinstance(op, RecursiveTraversalOperator):
            cte = self._render_recursive_cte(op)
            ctes.append(cte)
            has_recursive = True
        elif isinstance(op, AggregationBoundaryOperator):
            cte = self._render_aggregation_boundary_cte(op)
            ctes.append(cte)

        for out_op in op.out_operators:
            child_has_recursive = self._collect_ctes(out_op, ctes, visited)
            has_recursive = has_recursive or child_has_recursive

        return has_recursive

    def _get_resolved_ref(
        self,
        variable: str,
        property_name: str | None,
        context_op: LogicalOperator,
    ) -> ResolvedColumnRef | None:
        """Look up a resolved column reference from the ResolutionResult.

        This is the primary interface for the renderer to access pre-resolved
        column information. When available, this avoids the need for schema
        lookups and guessing in _find_entity_field().

        Args:
            variable: The variable name (e.g., "p")
            property_name: The property name (e.g., "name") or None for bare refs
            context_op: The operator context for the lookup

        Returns:
            ResolvedColumnRef if the reference was resolved, None otherwise.
            Returns None if:
            - No resolution result is available (unresolved plan)
            - The operator has no resolved expressions
            - The specific reference was not found

        Trade-offs:
            - Prefers resolution lookup over schema search for correctness
            - Falls back to None (allowing legacy path) for backwards compat
        """
        if self._resolution_result is None:
            return None

        op_id = context_op.operator_debug_id
        resolved_exprs = self._resolution_result.resolved_expressions.get(op_id, [])

        # Search through all resolved expressions for this operator
        for resolved_expr in resolved_exprs:
            ref = resolved_expr.get_ref(variable, property_name)
            if ref is not None:
                return ref

        # Also check resolved projections if this is a ProjectionOperator
        if op_id in self._resolution_result.resolved_projections:
            for resolved_proj in self._resolution_result.resolved_projections[op_id]:
                ref = resolved_proj.expression.get_ref(variable, property_name)
                if ref is not None:
                    return ref

        return None

    def _collect_required_columns_from_resolution(
        self, op: LogicalOperator
    ) -> None:
        """Collect required columns using resolution result (Phase 4 optimization).

        Uses pre-resolved column references instead of walking AST.
        This is more accurate since resolution has already validated all references.

        Trade-offs:
            - Faster: O(n) direct lookup vs O(n) AST walk
            - More accurate: Uses validated references
            - Limitation: Still needs to handle join keys separately (from schema)
        """
        assert self._resolution_result is not None

        op_id = op.operator_debug_id

        # Collect from resolved expressions for this operator
        if op_id in self._resolution_result.resolved_expressions:
            for resolved_expr in self._resolution_result.resolved_expressions[op_id]:
                for ref in resolved_expr.all_refs():
                    self._required_columns.add(ref.sql_column_name)
                    # Track bare variable references
                    if ref.original_property is None:
                        self._required_value_fields.add(ref.original_variable)

        # Collect from resolved projections for ProjectionOperators
        if op_id in self._resolution_result.resolved_projections:
            for resolved_proj in self._resolution_result.resolved_projections[op_id]:
                for ref in resolved_proj.expression.all_refs():
                    self._required_columns.add(ref.sql_column_name)
                    if ref.original_property is None:
                        self._required_value_fields.add(ref.original_variable)

        # Handle JoinOperator join keys (still need schema for join keys)
        if isinstance(op, JoinOperator):
            self._collect_join_key_columns(op)

        # Handle SetOperator (recurse into both sides)
        if isinstance(op, SetOperator):
            if op.in_operator_left:
                self._collect_required_columns_from_resolution(op.in_operator_left)
            if op.in_operator_right:
                self._collect_required_columns_from_resolution(op.in_operator_right)
            return  # Don't recurse further for set operators

        # Recurse into input operators using polymorphic method
        for in_op in op.in_operators:
            self._collect_required_columns_from_resolution(in_op)

    def _collect_join_key_columns(self, op: JoinOperator) -> None:
        """Collect join key columns from JoinOperator (used by both resolution and legacy paths).

        Join keys come from schema, not from expressions, so this is shared logic.
        """
        for pair in op.join_pairs:
            node_alias = pair.node_alias
            rel_alias = pair.relationship_or_node_alias

            # Find the fields in the input schema
            node_field = next(
                (f for f in op.input_schema if f.field_alias == node_alias),
                None,
            )
            rel_field = next(
                (f for f in op.input_schema if f.field_alias == rel_alias),
                None,
            )

            # Add node's join key column
            if node_field and isinstance(node_field, EntityField):
                if node_field.node_join_field:
                    # Use pre-rendered field name if available (varlen paths)
                    if node_field.node_join_field.field_name and node_field.node_join_field.field_name.startswith(self.COLUMN_PREFIX):
                        node_key = node_field.node_join_field.field_name
                    else:
                        node_key = self._get_field_name(
                            node_alias, node_field.node_join_field.field_alias
                        )
                    self._required_columns.add(node_key)

            # Add relationship/node's join key column based on pair type
            if rel_field and isinstance(rel_field, EntityField):
                if pair.pair_type == JoinKeyPairType.SOURCE:
                    if rel_field.rel_source_join_field:
                        # Use pre-rendered field name if available (varlen paths)
                        if rel_field.rel_source_join_field.field_name and rel_field.rel_source_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            rel_key = rel_field.rel_source_join_field.field_name
                        else:
                            rel_key = self._get_field_name(
                                rel_alias, rel_field.rel_source_join_field.field_alias
                            )
                        self._required_columns.add(rel_key)
                elif pair.pair_type == JoinKeyPairType.SINK:
                    if rel_field.rel_sink_join_field:
                        # Use pre-rendered field name if available (varlen paths)
                        if rel_field.rel_sink_join_field.field_name and rel_field.rel_sink_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            rel_key = rel_field.rel_sink_join_field.field_name
                        else:
                            rel_key = self._get_field_name(
                                rel_alias, rel_field.rel_sink_join_field.field_alias
                            )
                        self._required_columns.add(rel_key)
                elif pair.pair_type == JoinKeyPairType.NODE_ID:
                    # Node to node join
                    if rel_field.node_join_field:
                        # Use pre-rendered field name if available (varlen paths)
                        if rel_field.node_join_field.field_name and rel_field.node_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            rel_key = rel_field.node_join_field.field_name
                        else:
                            rel_key = self._get_field_name(
                                rel_alias, rel_field.node_join_field.field_alias
                            )
                        self._required_columns.add(rel_key)
                elif pair.pair_type in (
                    JoinKeyPairType.EITHER,
                    JoinKeyPairType.BOTH,
                    JoinKeyPairType.EITHER_AS_SOURCE,
                    JoinKeyPairType.EITHER_AS_SINK,
                ):
                    # Undirected or BOTH - need both source and sink keys
                    if rel_field.rel_source_join_field:
                        # Use pre-rendered field name if available (varlen paths)
                        if rel_field.rel_source_join_field.field_name and rel_field.rel_source_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            source_key = rel_field.rel_source_join_field.field_name
                        else:
                            source_key = self._get_field_name(
                                rel_alias, rel_field.rel_source_join_field.field_alias
                            )
                        self._required_columns.add(source_key)
                    if rel_field.rel_sink_join_field:
                        # Use pre-rendered field name if available (varlen paths)
                        if rel_field.rel_sink_join_field.field_name and rel_field.rel_sink_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            sink_key = rel_field.rel_sink_join_field.field_name
                        else:
                            sink_key = self._get_field_name(
                                rel_alias, rel_field.rel_sink_join_field.field_alias
                            )
                        self._required_columns.add(sink_key)

    def _collect_required_columns(self, op: LogicalOperator) -> None:
        """
        Collect required column aliases from the operator tree (column pruning).

        Uses pre-resolved column references from ResolutionResult for accuracy.
        This is an optimization to only output required columns in intermediate
        subqueries, improving query performance.

        The renderer now requires resolution, so this method always uses the
        resolution-based path.
        """
        # Always use resolution (guaranteed to be non-None after render_plan check)
        assert self._resolution_result is not None, "Resolution required"
        self._collect_required_columns_from_resolution(op)

    def _render_recursive_cte(self, op: RecursiveTraversalOperator) -> str:
        """
        Render a recursive CTE for variable-length path traversal.

        Generates Databricks SQL WITH RECURSIVE for BFS/DFS traversal.
        Supports multiple edge types (e.g., [:KNOWS|FOLLOWS*1..3]).

        When path accumulation is enabled (collect_edges=True), the CTE also
        accumulates an array of edge STRUCTs for relationships(path) access.

        PREDICATE PUSHDOWN OPTIMIZATION
        ================================

        When `op.edge_filter` is set, we apply it INSIDE the CTE to enable
        early path elimination. This is a critical optimization for queries like:

            MATCH path = (a)-[:TRANSFER*1..5]->(b)
            WHERE ALL(rel IN relationships(path) WHERE rel.amount > 1000)

        WITHOUT PUSHDOWN (exponential blowup):
        ┌─────────────────────────────────────────────────────────────────┐
        │  WITH RECURSIVE paths AS (                                      │
        │    SELECT ... FROM Transfer e           ← Collects ALL edges    │
        │    UNION ALL                                                    │
        │    SELECT ... FROM paths JOIN Transfer  ← Explores ALL paths    │
        │  )                                                              │
        │  SELECT ... WHERE FORALL(edges, x -> x.amount > 1000)           │
        │                          ↑                                      │
        │                   Filter applied AFTER collecting 10K+ paths!   │
        └─────────────────────────────────────────────────────────────────┘

        WITH PUSHDOWN (controlled growth):
        ┌─────────────────────────────────────────────────────────────────┐
        │  WITH RECURSIVE paths AS (                                      │
        │    SELECT ... FROM Transfer e                                   │
        │      WHERE e.amount > 1000              ← Filter in BASE case   │
        │    UNION ALL                                                    │
        │    SELECT ... FROM paths JOIN Transfer e                        │
        │      WHERE e.amount > 1000              ← Filter in RECURSIVE   │
        │  )                                                              │
        │  SELECT ...  ← Only valid paths reach here!                     │
        └─────────────────────────────────────────────────────────────────┘

        The edge_filter predicate comes from PathExpressionAnalyzer which
        extracts it from ALL() expressions and rewrites variable names
        (e.g., "rel.amount" → "e.amount") for use inside the CTE.
        """
        from gsql2rsql.common.schema import EdgeSchema

        self._cte_counter += 1
        cte_name = f"paths_{self._cte_counter}"
        op.cte_name = cte_name

        # Get edge types - if empty, we need all edges between source/target
        edge_types = op.edge_types if op.edge_types else []

        # Collect edge table info for all edge types
        edge_tables: list[tuple[str, SQLTableDescriptor]] = []
        source_id_col = op.source_id_column or "source_id"
        target_id_col = op.target_id_column or "target_id"

        # Collect edge properties for path_edges accumulation
        edge_props: list[str] = list(op.edge_properties) if op.edge_properties else []

        if edge_types:
            # Specific edge types provided
            for edge_type in edge_types:
                edge_table = None
                edge_schema = None

                # Check if either endpoint is unknown (no label) - use partial lookup
                if not op.source_node_type or not op.target_node_type:
                    # Partial lookup - find matching edges
                    edges = self._db_schema.find_edges_by_verb(
                        edge_type,
                        from_node_name=op.source_node_type or None,
                        to_node_name=op.target_node_type or None,
                    )
                    if edges:
                        edge_schema = edges[0]
                        edge_table = self._db_schema.get_sql_table_descriptors(
                            edge_schema.id
                        )
                else:
                    # Exact lookup (existing behavior)
                    edge_id = EdgeSchema.get_edge_id(
                        edge_type,
                        op.source_node_type,
                        op.target_node_type,
                    )
                    edge_table = self._db_schema.get_sql_table_descriptors(edge_id)
                    if not edge_table:
                        # Try with just the edge name
                        edge_table = self._db_schema.get_sql_table_descriptors(edge_type)

                    # Get edge schema for column info
                    edge_schema = self._db_schema.get_edge_definition(
                        edge_type,
                        op.source_node_type,
                        op.target_node_type,
                    )

                if edge_table:
                    edge_tables.append((edge_type, edge_table))

                # Get column names and properties from first edge schema
                if len(edge_tables) == 1 and edge_schema:
                    if edge_schema.source_id_property:
                        source_id_col = edge_schema.source_id_property.property_name
                    if edge_schema.sink_id_property:
                        target_id_col = edge_schema.sink_id_property.property_name
                    # Auto-collect edge properties if not specified
                    if op.collect_edges and not edge_props:
                        for prop in edge_schema.properties:
                            if prop.property_name not in (source_id_col, target_id_col):
                                edge_props.append(prop.property_name)
        else:
            # No specific edge types - use wildcard edge (no type filter)
            from gsql2rsql.common.schema import WILDCARD_EDGE_TYPE
            wildcard_edge_table = self._db_schema.get_wildcard_edge_table_descriptor()

            if not wildcard_edge_table:
                raise TranspilerInternalErrorException(
                    "Variable-length path without edge type requires wildcard edge support. "
                    "Please specify at least one edge type, e.g., -[:KNOWS*1..3]->"
                )

            # Use wildcard edge table (no relationship_type filter)
            edge_tables.append((WILDCARD_EDGE_TYPE, wildcard_edge_table))

            # Get column names from wildcard edge schema
            wildcard_edge_schema = self._db_schema.get_wildcard_edge_definition()
            if wildcard_edge_schema:
                if wildcard_edge_schema.source_id_property:
                    source_id_col = wildcard_edge_schema.source_id_property.property_name
                if wildcard_edge_schema.sink_id_property:
                    target_id_col = wildcard_edge_schema.sink_id_property.property_name
                # Auto-collect edge properties if needed
                if op.collect_edges and not edge_props:
                    for prop in wildcard_edge_schema.properties:
                        if prop.property_name not in (source_id_col, target_id_col):
                            edge_props.append(prop.property_name)

        if not edge_tables:
            edge_type_str = "|".join(edge_types)
            raise TranspilerInternalErrorException(
                f"No table descriptor for edges: {edge_type_str}"
            )

        min_depth = op.min_hops if op.min_hops is not None else 1
        max_depth = op.max_hops or 10  # Default max to prevent infinite loops

        # Check if all edge types use the same table (can use IN clause)
        # Group edge types by table name
        table_to_filters: dict[str, list[str]] = {}
        for edge_type, edge_table in edge_tables:
            current_table_name = edge_table.full_table_name
            if current_table_name not in table_to_filters:
                table_to_filters[current_table_name] = []
            if edge_table.filter:
                table_to_filters[current_table_name].append(edge_table.filter)

        # TODO: This method is too long (~500 lines) and has complex control flow.
        # Consider extracting _build_single_table_cte() and _build_multi_table_cte()
        # methods. The single_table variables are used far from their definition,
        # making the data flow hard to follow statically.
        #
        # If single table with multiple filters, combine with OR
        single_table = len(table_to_filters) == 1
        single_table_name: str | None = None
        single_table_filter: str | None = None
        if single_table:
            single_table_name = list(table_to_filters.keys())[0]
            filters_list = table_to_filters[single_table_name]
            if len(filters_list) > 1:
                # Combine filters with OR: (filter1) OR (filter2)
                single_table_filter = " OR ".join(f"({f})" for f in filters_list)
            elif len(filters_list) == 1:
                single_table_filter = filters_list[0]
            else:
                single_table_filter = None

        # Helper to build NAMED_STRUCT for edge properties
        def build_edge_struct(alias: str = "e") -> str:
            """Build NAMED_STRUCT expression for edge with its properties."""
            struct_parts = [
                f"'{source_id_col}', {alias}.{source_id_col}",
                f"'{target_id_col}', {alias}.{target_id_col}",
            ]
            for prop in edge_props:
                struct_parts.append(f"'{prop}', {alias}.{prop}")
            return f"NAMED_STRUCT({', '.join(struct_parts)})"

        # ═══════════════════════════════════════════════════════════════════
        # PREDICATE PUSHDOWN: Render edge filter for use in WHERE clauses
        # ═══════════════════════════════════════════════════════════════════
        #
        # If we have an edge_filter from PathExpressionAnalyzer, we need to:
        # 1. Rewrite the lambda variable (e.g., "rel") to edge alias ("e")
        # 2. Render it to SQL with DIRECT column references (e.g., e.amount)
        # 3. Add it to BOTH base case AND recursive case WHERE clauses
        #
        # Example transformation:
        #   Input:  ALL(rel IN relationships(path) WHERE rel.amount > 1000)
        #   edge_filter: rel.amount > 1000
        #   Rewritten: e.amount > 1000
        #   Output SQL: WHERE ... AND e.amount > 1000
        #
        # IMPORTANT: We use _render_edge_filter_expression instead of
        # _render_expression because the CTE uses direct column references
        # (e.g., "e.amount") not entity-prefixed aliases (e.g., "_gsql2rsql_e_amount").
        edge_filter_sql: str | None = None
        if op.edge_filter and op.edge_filter_lambda_var:
            # Rewrite variable names: rel.amount -> e.amount
            rewritten_filter = rewrite_predicate_for_edge_alias(
                op.edge_filter,
                op.edge_filter_lambda_var,
                edge_alias="e",
            )
            edge_filter_sql = self._render_edge_filter_expression(rewritten_filter)

        # Build the recursive CTE
        lines: list[str] = []
        lines.append(f"  {cte_name} AS (")

        # ═══════════════════════════════════════════════════════════════════
        # SOURCE NODE FILTER PUSHDOWN
        # ═══════════════════════════════════════════════════════════════════
        # If we have a filter on the source node (e.g., WHERE p.name = 'Alice'),
        # we push it into the CTE base case by JOINing with the source node table.
        #
        # This is a critical optimization:
        # - Without pushdown: Explore ALL paths from ALL nodes, then filter
        # - With pushdown: Only explore paths from nodes matching the filter
        #
        # Example:
        #   MATCH (p:Person)-[:KNOWS*1..3]->(f:Person) WHERE p.name = 'Alice'
        #
        #   Before (inefficient):
        #     WITH RECURSIVE paths AS (
        #       SELECT ... FROM Knows e  -- Starts from ALL persons
        #       UNION ALL ...
        #     )
        #     SELECT ... WHERE p.name = 'Alice'  -- Filters AFTER CTE
        #
        #   After (optimized):
        #     WITH RECURSIVE paths AS (
        #       SELECT ... FROM Knows e
        #       JOIN Person src ON src.id = e.person_id
        #       WHERE src.name = 'Alice'  -- Filters AT START
        #       UNION ALL ...
        #     )
        #     SELECT ...
        # ═══════════════════════════════════════════════════════════════════
        source_node_filter_sql: str | None = None
        source_node_table: SQLTableDescriptor | None = None
        if op.start_node_filter:
            # Get source node table for JOIN (supports no-label via wildcard)
            source_node_table = self._get_table_descriptor_with_wildcard(
                op.source_node_type
            )
            if source_node_table:
                # Rewrite filter: p.name -> src.name
                rewritten_filter = rewrite_predicate_for_edge_alias(
                    op.start_node_filter,
                    op.source_alias,  # e.g., "p"
                    edge_alias="src",  # Rewrite to "src"
                )
                source_node_filter_sql = self._render_edge_filter_expression(
                    rewritten_filter
                )

        # If min_depth is 0, add zero-length path base case first
        if min_depth == 0:
            # Get source node table descriptor (supports no-label via wildcard)
            zero_len_source_table = self._get_table_descriptor_with_wildcard(
                op.source_node_type
            )
            if not zero_len_source_table:
                raise TranspilerInternalErrorException(
                    f"No table descriptor for source node: {op.source_node_type}"
                )

            lines.append("    -- Base case: Zero-length paths (depth = 0)")
            lines.append("    SELECT")
            lines.append(f"      n.{op.source_id_column} AS start_node,")
            lines.append(f"      n.{op.source_id_column} AS end_node,")
            lines.append("      0 AS depth,")
            lines.append(f"      ARRAY(n.{op.source_id_column}) AS path,")
            if op.collect_edges:
                # Empty array of structs for zero-length path
                lines.append("      ARRAY() AS path_edges,")
            lines.append("      ARRAY() AS visited")
            lines.append(f"    FROM {zero_len_source_table.full_table_name} n")

            # Add start_node_filter if present (rewrite to use 'n' alias)
            if op.start_node_filter:
                rewritten = rewrite_predicate_for_edge_alias(
                    op.start_node_filter,
                    op.source_alias,
                    edge_alias="n",
                )
                filter_sql = self._render_edge_filter_expression(rewritten)
                lines.append(f"    WHERE {filter_sql}")

            lines.append("")
            lines.append("    UNION ALL")
            lines.append("")

        lines.append("    -- Base case: direct edges (depth = 1)")

        # ═══════════════════════════════════════════════════════════════════
        # UNDIRECTED TRAVERSAL SUPPORT
        # ═══════════════════════════════════════════════════════════════════
        # For undirected patterns like (a)-[:TYPE*1..3]-(b), we need to
        # traverse edges in BOTH directions. The planner has already decided
        # whether we need internal UNION ALL based on direction + storage model.
        #
        # The renderer just uses op.use_internal_union_for_bidirectional:
        # - True: Generate CTE with UNION ALL (forward + backward)
        # - False: Generate single-direction CTE
        #
        # And op.swap_source_sink for backward direction:
        # - True: Swap source/sink columns (BACKWARD direction)
        # - False: Use normal column order (FORWARD direction)
        #
        # This follows Separation of Concerns: planner decides, renderer executes.
        # ═══════════════════════════════════════════════════════════════════
        is_backward = op.swap_source_sink  # Planner-resolved (was: op.direction == BACKWARD)

        # Use planner's decision for UNION ALL (no more semantic checks here)
        needs_union_for_undirected = op.use_internal_union_for_bidirectional

        # Helper to generate base case SELECT for a given direction
        def build_base_case_select(
            table_name_: str,
            src_col: str,
            dst_col: str,
            filter_clause: str | None,
            direction_label: str = "",
        ) -> str:
            """Build a single base case SELECT for one direction."""
            base_sql = []
            if direction_label:
                base_sql.append(f"    -- {direction_label}")
            base_sql.append("    SELECT")
            base_sql.append(f"      e.{src_col} AS start_node,")
            base_sql.append(f"      e.{dst_col} AS end_node,")
            base_sql.append("      1 AS depth,")
            base_sql.append(
                f"      ARRAY(e.{src_col}, e.{dst_col}) AS path,"
            )
            if op.collect_edges:
                base_sql.append(f"      ARRAY({build_edge_struct()}) AS path_edges,")
            base_sql.append(f"      ARRAY(e.{src_col}) AS visited")
            base_sql.append(f"    FROM {table_name_} e")

            # Add JOIN with source node table if we have a source filter
            if source_node_filter_sql and source_node_table:
                base_sql.append(
                    f"    JOIN {source_node_table.full_table_name} src "
                    f"ON src.{op.source_id_column} = e.{src_col}"
                )

            # Build WHERE clause
            where_parts = []
            if filter_clause:
                where_parts.append(f"({filter_clause})")
            if source_node_filter_sql:
                where_parts.append(source_node_filter_sql)
            if edge_filter_sql:
                where_parts.append(edge_filter_sql)
            if where_parts:
                base_sql.append(f"    WHERE {' AND '.join(where_parts)}")

            return "\n".join(base_sql)

        if single_table:
            # Single table - generate SELECT(s) based on direction
            # Invariant: single_table_name is set when single_table is True
            assert single_table_name is not None
            if needs_union_for_undirected:
                # Undirected with EDGE_LIST storage: UNION ALL of forward and backward
                # Wrap in subquery for PySpark recursive CTE compatibility
                # (PySpark requires exactly 2 children: anchor + recursive)
                forward_sql = build_base_case_select(
                    single_table_name, source_id_col, target_id_col, single_table_filter,
                    "Forward direction"
                )
                backward_sql = build_base_case_select(
                    single_table_name, target_id_col, source_id_col, single_table_filter,
                    "Backward direction"
                )
                lines.append("    SELECT * FROM (")
                lines.append(forward_sql)
                lines.append("")
                lines.append("      UNION ALL")
                lines.append("")
                lines.append(backward_sql)
                lines.append("    )")
            elif is_backward:
                # Backward only: swap src and dst
                lines.append(build_base_case_select(
                    single_table_name, target_id_col, source_id_col, single_table_filter
                ))
            else:
                # Forward only (default)
                lines.append(build_base_case_select(
                    single_table_name, source_id_col, target_id_col, single_table_filter
                ))
        else:
            # Multiple tables - use UNION ALL for base case
            base_cases: list[str] = []
            for edge_type, edge_table in edge_tables:
                filter_clause = edge_table.filter
                if needs_union_for_undirected:
                    # Undirected with EDGE_LIST storage: add both forward and backward for each table
                    base_cases.append(build_base_case_select(
                        edge_table.full_table_name, source_id_col, target_id_col,
                        filter_clause, f"Forward: {edge_type}"
                    ))
                    base_cases.append(build_base_case_select(
                        edge_table.full_table_name, target_id_col, source_id_col,
                        filter_clause, f"Backward: {edge_type}"
                    ))
                elif is_backward:
                    # Backward only
                    base_cases.append(build_base_case_select(
                        edge_table.full_table_name, target_id_col, source_id_col,
                        filter_clause
                    ))
                else:
                    # Forward only
                    base_cases.append(build_base_case_select(
                        edge_table.full_table_name, source_id_col, target_id_col,
                        filter_clause
                    ))

            # Wrap in subquery for Databricks compatibility
            lines.append("    SELECT * FROM (")
            lines.append("\n      UNION ALL\n".join(base_cases))
            lines.append("    )")

        lines.append("")
        lines.append("    UNION ALL")
        lines.append("")
        lines.append("    -- Recursive case: extend paths")

        # Helper to generate recursive case SELECT for a given direction
        def build_recursive_case_select(
            table_name_: str,
            join_col: str,  # Column to join on (e.src or e.dst)
            end_col: str,   # Column for end_node (e.dst or e.src)
            visited_col: str,  # Column to add to visited (e.src or e.dst)
            filter_clause: str | None,
            direction_label: str = "",
        ) -> str:
            """Build a single recursive case SELECT for one direction."""
            rec_sql = []
            if direction_label:
                rec_sql.append(f"    -- {direction_label}")
            rec_sql.append("    SELECT")
            rec_sql.append("      p.start_node,")
            rec_sql.append(f"      e.{end_col} AS end_node,")
            rec_sql.append("      p.depth + 1 AS depth,")
            rec_sql.append(
                f"      CONCAT(p.path, ARRAY(e.{end_col})) AS path,"
            )
            if op.collect_edges:
                rec_sql.append(
                    f"      ARRAY_APPEND(p.path_edges, {build_edge_struct()}) AS path_edges,"
                )
            rec_sql.append(
                f"      CONCAT(p.visited, ARRAY(e.{visited_col})) AS visited"
            )
            rec_sql.append(f"    FROM {cte_name} p")
            rec_sql.append(f"    JOIN {table_name_} e")
            rec_sql.append(f"      ON p.end_node = e.{join_col}")

            where_parts = [f"p.depth < {max_depth}"]
            where_parts.append(
                f"NOT ARRAY_CONTAINS(p.visited, e.{end_col})"
            )
            if filter_clause:
                where_parts.append(f"({filter_clause})")
            if edge_filter_sql:
                where_parts.append(edge_filter_sql)
            rec_sql.append(f"    WHERE {where_parts[0]}")
            for wp in where_parts[1:]:
                rec_sql.append(f"      AND {wp}")

            return "\n".join(rec_sql)

        if single_table:
            # Single table - generate SELECT(s) based on direction
            # Invariant: single_table_name is set when single_table is True
            assert single_table_name is not None
            if needs_union_for_undirected:
                # Undirected with EDGE_LIST storage: UNION ALL of forward and backward
                # Wrap in subquery for PySpark recursive CTE compatibility
                forward_sql = build_recursive_case_select(
                    single_table_name, source_id_col, target_id_col, source_id_col,
                    single_table_filter, "Forward direction"
                )
                backward_sql = build_recursive_case_select(
                    single_table_name, target_id_col, source_id_col, target_id_col,
                    single_table_filter, "Backward direction"
                )
                lines.append("    SELECT * FROM (")
                lines.append(forward_sql)
                lines.append("")
                lines.append("      UNION ALL")
                lines.append("")
                lines.append(backward_sql)
                lines.append("    )")
            elif is_backward:
                # Backward only: swap columns
                lines.append(build_recursive_case_select(
                    single_table_name, target_id_col, source_id_col, target_id_col,
                    single_table_filter
                ))
            else:
                # Forward only (default)
                lines.append(build_recursive_case_select(
                    single_table_name, source_id_col, target_id_col, source_id_col,
                    single_table_filter
                ))
        else:
            # Multiple tables - use UNION ALL wrapped in subquery
            recursive_cases: list[str] = []
            for edge_type, edge_table in edge_tables:
                filter_clause = edge_table.filter
                if needs_union_for_undirected:
                    # Undirected with EDGE_LIST storage: add both forward and backward for each table
                    recursive_cases.append(build_recursive_case_select(
                        edge_table.full_table_name, source_id_col, target_id_col,
                        source_id_col, filter_clause, f"Forward: {edge_type}"
                    ))
                    recursive_cases.append(build_recursive_case_select(
                        edge_table.full_table_name, target_id_col, source_id_col,
                        target_id_col, filter_clause, f"Backward: {edge_type}"
                    ))
                elif is_backward:
                    # Backward only
                    recursive_cases.append(build_recursive_case_select(
                        edge_table.full_table_name, target_id_col, source_id_col,
                        target_id_col, filter_clause
                    ))
                else:
                    # Forward only
                    recursive_cases.append(build_recursive_case_select(
                        edge_table.full_table_name, source_id_col, target_id_col,
                        source_id_col, filter_clause
                    ))

            # Wrap in subquery for Databricks compatibility
            lines.append("    SELECT * FROM (")
            lines.append("\n      UNION ALL\n".join(recursive_cases))
            lines.append("    )")

        lines.append("  )")

        return "\n".join(lines)

    def _render_operator(self, op: LogicalOperator, depth: int) -> str:
        """Render a logical operator to SQL."""
        if isinstance(op, DataSourceOperator):
            return self._render_data_source(op, depth)
        elif isinstance(op, JoinOperator):
            return self._render_join(op, depth)
        elif isinstance(op, SelectionOperator):
            return self._render_selection(op, depth)
        elif isinstance(op, ProjectionOperator):
            return self._render_projection(op, depth)
        elif isinstance(op, SetOperator):
            return self._render_set_operator(op, depth)
        elif isinstance(op, RecursiveTraversalOperator):
            return self._render_recursive_reference(op, depth)
        elif isinstance(op, UnwindOperator):
            return self._render_unwind(op, depth)
        elif isinstance(op, AggregationBoundaryOperator):
            return self._render_aggregation_boundary_reference(op, depth)
        else:
            raise TranspilerNotSupportedException(
                f"Operator type {type(op).__name__}"
            )

    def _render_recursive_reference(
        self, op: RecursiveTraversalOperator, depth: int
    ) -> str:
        """Render a reference to a recursive CTE."""
        indent = self._indent(depth)
        cte_name = getattr(op, "cte_name", "paths")
        min_depth = op.min_hops if op.min_hops is not None else 1

        lines: list[str] = []
        lines.append(f"{indent}SELECT")
        lines.append(f"{indent}   start_node,")
        lines.append(f"{indent}   end_node,")
        lines.append(f"{indent}   depth,")
        if op.collect_edges:
            lines.append(f"{indent}   path,")
            lines.append(f"{indent}   path_edges")
        else:
            lines.append(f"{indent}   path")
        lines.append(f"{indent}FROM {cte_name}")

        # Add WHERE clause for depth bounds
        where_parts = [f"depth >= {min_depth}"]
        if op.max_hops is not None:
            where_parts.append(f"depth <= {op.max_hops}")
        lines.append(f"{indent}WHERE {' AND '.join(where_parts)}")

        return "\n".join(lines)

    def _render_recursive_join(
        self,
        join_op: JoinOperator,
        recursive_op: RecursiveTraversalOperator,
        target_op: DataSourceOperator,
        depth: int,
    ) -> str:
        """
        Render a JOIN between recursive CTE and BOTH source and target node tables.

        This method generates a query that:
        1. JOINs the recursive CTE with the TARGET node (end_node)
        2. JOINs the recursive CTE with the SOURCE node (start_node)

        Both JOINs are necessary because:
        - Target node properties are needed for filtering/projection on the end node
        - Source node properties are needed for filtering/projection on the start node

        Example output:
            SELECT
               sink.id AS _gsql2rsql_b_id,
               sink.name AS _gsql2rsql_b_name,
               source.id AS _gsql2rsql_a_id,
               source.name AS _gsql2rsql_a_name,
               p.path,
               p.path_edges
            FROM paths_1 p
            JOIN Account sink ON sink.id = p.end_node
            JOIN Account source ON source.id = p.start_node
            WHERE p.depth >= 2 AND p.depth <= 4
        """
        indent = self._indent(depth)
        cte_name = getattr(recursive_op, "cte_name", "paths")
        min_depth = recursive_op.min_hops if recursive_op.min_hops is not None else 1

        # Get target node's table info (supports no-label via wildcard)
        target_entity = target_op.entity
        if target_entity is None:
            raise TranspilerInternalErrorException(
                "Target operator has no entity defined"
            )
        target_entity_name = target_entity.entity_name or ""
        target_table = self._get_table_descriptor_with_wildcard(target_entity_name)
        if not target_table:
            raise TranspilerInternalErrorException(
                f"No table descriptor for {target_entity_name}"
            )

        # Get target node's ID column and schema (supports wildcard for unlabeled nodes)
        target_node_schema = self._db_schema.get_node_definition(target_entity_name)
        if not target_node_schema and not target_entity_name:
            # Try wildcard for unlabeled nodes
            target_node_schema = self._db_schema.get_wildcard_node_definition()
        if target_node_schema and target_node_schema.node_id_property:
            target_id_col = target_node_schema.node_id_property.property_name
        else:
            target_id_col = "id"

        # Get alias for target node (sink) and source node
        target_alias = target_entity.alias or "n"
        source_alias = recursive_op.source_alias or "src"

        # Get source node's table info (supports no-label via wildcard)
        source_node_type = recursive_op.source_node_type
        source_table = self._get_table_descriptor_with_wildcard(source_node_type)
        if not source_table:
            # Fallback to target table if source not found
            source_table = target_table

        # Get source node's ID column and schema (supports wildcard for unlabeled nodes)
        source_node_schema = self._db_schema.get_node_definition(source_node_type)
        if not source_node_schema and not source_node_type:
            # Try wildcard for unlabeled nodes
            source_node_schema = self._db_schema.get_wildcard_node_definition()
        if source_node_schema and source_node_schema.node_id_property:
            source_id_col = source_node_schema.node_id_property.property_name
        else:
            source_id_col = "id"

        lines: list[str] = []
        lines.append(f"{indent}SELECT")

        # Project fields from TARGET node (sink)
        field_lines: list[str] = []
        if target_node_schema:
            field_lines.append(f"sink.{target_id_col} AS {self.COLUMN_PREFIX}{target_alias}_{target_id_col}")
            for prop in target_node_schema.properties:
                prop_name = prop.property_name
                if prop_name != target_id_col:
                    field_lines.append(f"sink.{prop_name} AS {self.COLUMN_PREFIX}{target_alias}_{prop_name}")
        else:
            field_lines.append(f"sink.{target_id_col} AS {self.COLUMN_PREFIX}{target_alias}_id")

        # Project fields from SOURCE node
        # Skip if source_alias == target_alias (circular path like (a)-[*]->(a))
        # In that case, sink and source are the same entity, so we don't want duplicate columns
        if source_alias != target_alias:
            if source_node_schema:
                field_lines.append(f"source.{source_id_col} AS {self.COLUMN_PREFIX}{source_alias}_{source_id_col}")
                for prop in source_node_schema.properties:
                    prop_name = prop.property_name
                    if prop_name != source_id_col:
                        field_lines.append(f"source.{prop_name} AS {self.COLUMN_PREFIX}{source_alias}_{prop_name}")
            else:
                field_lines.append(f"source.{source_id_col} AS {self.COLUMN_PREFIX}{source_alias}_id")

        # Include path info from CTE
        field_lines.append("p.start_node")
        field_lines.append("p.end_node")
        field_lines.append("p.depth")

        # Include path and path_edges with proper aliases
        # These need the _gsql2rsql_ prefix to match what column resolver expects
        # when resolving nodes(path) and relationships(path) function calls
        if recursive_op.path_variable:
            # Alias path array with the standard column naming convention
            path_alias = f"{self.COLUMN_PREFIX}{recursive_op.path_variable}_id"
            field_lines.append(f"p.path AS {path_alias}")

            # Include path_edges if edge collection is enabled (for relationships(path))
            if recursive_op.collect_edges:
                edges_alias = f"{self.COLUMN_PREFIX}{recursive_op.path_variable}_edges"
                field_lines.append(f"p.path_edges AS {edges_alias}")
        else:
            # Fallback for queries without explicit path variable
            field_lines.append("p.path")
            if recursive_op.collect_edges:
                field_lines.append("p.path_edges")

        for i, field in enumerate(field_lines):
            prefix = " " if i == 0 else ","
            lines.append(f"{indent}  {prefix}{field}")

        # FROM recursive CTE
        lines.append(f"{indent}FROM {cte_name} p")

        # JOIN with TARGET node table (end_node = sink)
        lines.append(f"{indent}JOIN {target_table.full_table_name} sink")
        lines.append(f"{indent}  ON sink.{target_id_col} = p.end_node")

        # JOIN with SOURCE node table (start_node = source)
        lines.append(f"{indent}JOIN {source_table.full_table_name} source")
        lines.append(f"{indent}  ON source.{source_id_col} = p.start_node")

        # WHERE clause for depth bounds
        where_parts = [f"p.depth >= {min_depth}"]
        if recursive_op.max_hops is not None:
            where_parts.append(f"p.depth <= {recursive_op.max_hops}")
        # Circular path check: require start_node = end_node for patterns like (a)-[*]->(a)
        if recursive_op.is_circular:
            where_parts.append("p.start_node = p.end_node")

        # SINK NODE FILTER PUSHDOWN: Apply filter on target node here
        # This filters rows DURING the join rather than AFTER all joins complete
        if recursive_op.sink_node_filter:
            from gsql2rsql.planner.path_analyzer import rewrite_predicate_for_edge_alias
            # Rewrite filter: b.risk_score -> sink.risk_score
            rewritten_filter = rewrite_predicate_for_edge_alias(
                recursive_op.sink_node_filter,
                recursive_op.target_alias,  # e.g., "b"
                edge_alias="sink",  # Rewrite to "sink"
            )
            sink_filter_sql = self._render_edge_filter_expression(rewritten_filter)
            where_parts.append(sink_filter_sql)

        lines.append(f"{indent}WHERE {' AND '.join(where_parts)}")

        return "\n".join(lines)

    def _render_aggregation_boundary_cte(
        self, op: AggregationBoundaryOperator
    ) -> str:
        """Render an aggregation boundary operator as a CTE definition.

        This generates a CTE that materializes the aggregated result, allowing
        subsequent MATCH clauses to join with it.

        Example output for:
            MATCH (p:Person)-[:LIVES_IN]->(c:City)
            WITH c, COUNT(p) AS population
            WHERE population > 100

        Generates:
            agg_boundary_1 AS (
              SELECT
                `c`.`id` AS `c_id`,
                COUNT(`p`.`id`) AS `population`
              FROM ... (rendered input)
              GROUP BY `c`.`id`
              HAVING COUNT(`p`.`id`) > 100
            )
        """
        cte_name = op.cte_name
        lines: list[str] = []

        # Use the AggregationBoundaryOperator itself as context for expression rendering
        # The expressions in group_keys and aggregates were resolved against this operator
        context_op = op

        lines.append(f"{cte_name} AS (")

        # Render the SELECT clause
        lines.append("  SELECT")

        # Render group keys and aggregates
        select_items: list[str] = []

        # Group keys - these become both SELECT columns and GROUP BY columns
        for alias, expr in op.group_keys:
            rendered_expr = self._render_expression(expr, context_op)
            select_items.append(f"    {rendered_expr} AS `{alias}`")

        # Aggregates
        for alias, expr in op.aggregates:
            rendered_expr = self._render_expression(expr, context_op)
            select_items.append(f"    {rendered_expr} AS `{alias}`")

        lines.append(",\n".join(select_items))

        # Render FROM clause (the input operator)
        if op.in_operator:
            input_sql = self._render_operator(op.in_operator, depth=1)
            lines.append("  FROM (")
            lines.append(input_sql)
            lines.append("  ) AS _agg_input")

        # Render GROUP BY clause
        if op.group_keys:
            group_by_exprs = []
            for alias, expr in op.group_keys:
                rendered_expr = self._render_expression(expr, context_op)
                group_by_exprs.append(rendered_expr)
            lines.append(f"  GROUP BY {', '.join(group_by_exprs)}")

        # Render HAVING clause
        if op.having_filter:
            having_sql = self._render_expression(op.having_filter, context_op)
            lines.append(f"  HAVING {having_sql}")

        # Render ORDER BY clause
        if op.order_by:
            order_parts = []
            for expr, is_desc in op.order_by:
                rendered_expr = self._render_expression(expr, context_op)
                direction = "DESC" if is_desc else "ASC"
                order_parts.append(f"{rendered_expr} {direction}")
            lines.append(f"  ORDER BY {', '.join(order_parts)}")

        # Render LIMIT clause
        if op.limit is not None:
            lines.append(f"  LIMIT {op.limit}")

        # Render OFFSET clause
        if op.skip is not None:
            lines.append(f"  OFFSET {op.skip}")

        lines.append(")")

        return "\n".join(lines)

    def _render_aggregation_boundary_reference(
        self, op: AggregationBoundaryOperator, depth: int
    ) -> str:
        """Render a reference to an aggregation boundary CTE.

        When the aggregation boundary is used as input to a join or other
        operator, this generates a SELECT from the CTE.

        Example output:
            SELECT
              `c_id`,
              `population`
            FROM agg_boundary_1
        """
        indent = self._indent(depth)
        cte_name = op.cte_name
        lines: list[str] = []

        lines.append(f"{indent}SELECT")

        # Project all columns from the CTE
        select_items: list[str] = []
        for alias, _ in op.all_projections:
            # Map entity variable to its ID column for joins
            # e.g., if 'c' was projected, we need 'c_id' for joining
            select_items.append(f"`{alias}`")

        for i, item in enumerate(select_items):
            prefix = " " if i == 0 else ","
            lines.append(f"{indent}  {prefix}{item}")

        lines.append(f"{indent}FROM {cte_name}")

        return "\n".join(lines)

    def _render_boundary_join(
        self,
        join_op: JoinOperator,
        boundary_op: AggregationBoundaryOperator,
        right_op: LogicalOperator,
        depth: int,
    ) -> str:
        """Render a JOIN between aggregation boundary CTE and subsequent MATCH.

        This method generates a query that joins the aggregated CTE result with
        the new MATCH pattern using the projected entity IDs.

        Example: For a query like:
            MATCH (p:Person)-[:LIVES_IN]->(c:City)
            WITH c, COUNT(p) AS population
            MATCH (c)<-[:LIVES_IN]-(other:Person)
            RETURN ...

        Generates:
            SELECT
               _left.c,
               _left.population,
               _right._gsql2rsql_c_id,
               _right._gsql2rsql_other_id,
               ...
            FROM (
               SELECT `c`, `population` FROM agg_boundary_1
            ) AS _left
            INNER JOIN (
               ... right side subquery ...
            ) AS _right ON
               _left.c = _right._gsql2rsql_c_id
        """
        indent = self._indent(depth)
        lines: list[str] = []

        left_var = "_left"
        right_var = "_right"

        lines.append(f"{indent}SELECT")

        # Collect output fields
        output_fields: list[str] = []

        # Add fields from the boundary (CTE)
        for alias, _ in boundary_op.all_projections:
            output_fields.append(f"{left_var}.`{alias}` AS `{alias}`")

        # Add fields from the right side (new MATCH) - apply column pruning
        right_columns = self._collect_all_column_names(right_op.output_schema)

        # Determine which columns from the right side are actually needed
        # 1. Columns required downstream (in _required_columns)
        # 2. Join key columns (entity ID columns needed for the join condition)
        right_join_keys: set[str] = set()
        for pair in join_op.join_pairs:
            if pair.pair_type == JoinKeyPairType.NODE_ID and pair.node_alias:
                # Get the ID column from the right side's schema
                node_id_col = self._get_entity_id_column_from_schema(
                    right_op.output_schema, pair.node_alias
                )
                if node_id_col:
                    right_join_keys.add(node_id_col)

        for col in right_columns:
            # Apply column pruning: only include columns that are required or are join keys
            if (
                not self._enable_column_pruning
                or not self._required_columns
                or col in self._required_columns
                or col in right_join_keys
            ):
                output_fields.append(f"{right_var}.{col} AS {col}")

        for i, field in enumerate(output_fields):
            prefix = " " if i == 0 else ","
            lines.append(f"{indent}  {prefix}{field}")

        # FROM boundary CTE reference
        lines.append(f"{indent}FROM (")
        lines.append(self._render_aggregation_boundary_reference(boundary_op, depth + 1))
        lines.append(f"{indent}) AS {left_var}")

        # JOIN with right side
        join_keyword = (
            "INNER JOIN" if join_op.join_type == JoinType.INNER else "LEFT JOIN"
        )
        lines.append(f"{indent}{join_keyword} (")
        lines.append(self._render_operator(right_op, depth + 1))
        lines.append(f"{indent}) AS {right_var} ON")

        # Render join conditions
        # The boundary projects entity variables (e.g., 'c') and we need to join
        # them with the corresponding entity ID from the right side (e.g., '_gsql2rsql_c_id')
        conditions: list[str] = []

        for pair in join_op.join_pairs:
            if pair.pair_type == JoinKeyPairType.NODE_ID:
                node_alias = pair.node_alias
                # The boundary projects the entity variable directly (e.g., 'c')
                # The right side has the entity ID column (e.g., '_gsql2rsql_c_id')
                if node_alias in boundary_op.projected_variables:
                    # Find the ID column name from the right side's schema
                    node_id_col = self._get_entity_id_column_from_schema(
                        right_op.output_schema, node_alias
                    )
                    if node_id_col:
                        conditions.append(
                            f"{left_var}.`{node_alias}` = {right_var}.{node_id_col}"
                        )

        if conditions:
            for i, cond in enumerate(conditions):
                prefix = "  " if i == 0 else "  AND "
                lines.append(f"{indent}{prefix}{cond}")
        else:
            lines.append(f"{indent}  TRUE")

        return "\n".join(lines)

    def _get_entity_id_column_from_schema(
        self, schema: Schema, entity_alias: str
    ) -> str | None:
        """Get the ID column name for an entity from a schema.

        Looks for an EntityField with the given alias and returns its ID column
        name in the rendered format (e.g., '_gsql2rsql_c_id').

        Args:
            schema: The schema to search in
            entity_alias: The entity alias to find (e.g., 'c')

        Returns:
            The ID column name (e.g., '_gsql2rsql_c_id') or None if not found
        """
        for field in schema:
            if isinstance(field, EntityField) and field.field_alias == entity_alias:
                if field.entity_type == EntityType.NODE and field.node_join_field:
                    return self._get_field_name(
                        entity_alias, field.node_join_field.field_alias
                    )
                elif field.rel_source_join_field:
                    return self._get_field_name(
                        entity_alias, field.rel_source_join_field.field_alias
                    )
        return None

    def _indent(self, depth: int) -> str:
        """Get indentation string for a given depth."""
        return "  " * depth

    def _get_table_descriptor_with_wildcard(
        self, entity_name: str
    ) -> SQLTableDescriptor | None:
        """Get table descriptor, falling back to wildcard for wildcard nodes/edges.

        This method supports no-label nodes and untyped edges by returning the
        wildcard table descriptor when entity_name is empty or is a wildcard type.

        Args:
            entity_name: The entity name (node type or edge id).
                Empty string or WILDCARD_NODE_TYPE means no-label node.
                WILDCARD_EDGE_TYPE means untyped edge.

        Returns:
            SQLTableDescriptor if found, None otherwise.
        """
        from gsql2rsql.common.schema import WILDCARD_NODE_TYPE, WILDCARD_EDGE_TYPE

        if not entity_name or entity_name == WILDCARD_NODE_TYPE:
            # No label or wildcard node type - use wildcard node table descriptor
            return self._db_schema.get_wildcard_table_descriptor()
        # Check for wildcard edge (edge ID format: source@verb@sink)
        if WILDCARD_EDGE_TYPE in entity_name:
            # Wildcard edge type - use wildcard edge table descriptor
            return self._db_schema.get_wildcard_edge_table_descriptor()
        return self._db_schema.get_sql_table_descriptors(entity_name)

    def _render_data_source(self, op: DataSourceOperator, depth: int) -> str:
        """Render a data source operator."""
        lines: list[str] = []
        indent = self._indent(depth)

        if not op.output_schema:
            return ""

        entity_field = op.output_schema[0]
        if not isinstance(entity_field, EntityField):
            return ""

        # Get SQL table descriptor (supports no-label nodes via wildcard)
        table_desc = self._get_table_descriptor_with_wildcard(
            entity_field.bound_entity_name
        )
        if not table_desc:
            raise TranspilerInternalErrorException(
                f"No table descriptor for {entity_field.bound_entity_name}"
            )

        lines.append(f"{indent}SELECT")

        # Render fields
        field_lines: list[str] = []

        # Always include join key fields first
        if entity_field.entity_type == EntityType.NODE:
            if entity_field.node_join_field:
                key_name = self._get_field_name(
                    entity_field.field_alias,
                    entity_field.node_join_field.field_alias,
                )
                field_lines.append(
                    f"{entity_field.node_join_field.field_alias} AS {key_name}"
                )
        else:  # Relationship
            if entity_field.rel_source_join_field:
                src_key = self._get_field_name(
                    entity_field.field_alias,
                    entity_field.rel_source_join_field.field_alias,
                )
                field_lines.append(
                    f"{entity_field.rel_source_join_field.field_alias} AS {src_key}"
                )
            if entity_field.rel_sink_join_field:
                sink_key = self._get_field_name(
                    entity_field.field_alias,
                    entity_field.rel_sink_join_field.field_alias,
                )
                field_lines.append(
                    f"{entity_field.rel_sink_join_field.field_alias} AS {sink_key}"
                )

        # Add other referenced fields
        # With column pruning enabled, only include fields that are actually used
        skip_fields = set()
        if entity_field.node_join_field:
            skip_fields.add(entity_field.node_join_field.field_alias)
        if entity_field.rel_source_join_field:
            skip_fields.add(entity_field.rel_source_join_field.field_alias)
        if entity_field.rel_sink_join_field:
            skip_fields.add(entity_field.rel_sink_join_field.field_alias)

        for encap_field in entity_field.encapsulated_fields:
            if encap_field.field_alias not in skip_fields:
                field_alias = self._get_field_name(
                    entity_field.field_alias, encap_field.field_alias
                )
                # Column pruning: only include if required or pruning disabled
                if (
                    not self._enable_column_pruning
                    or not self._required_columns
                    or field_alias in self._required_columns
                ):
                    field_lines.append(f"{encap_field.field_alias} AS {field_alias}")

        # If no fields selected, select key field
        if not field_lines:
            if entity_field.node_join_field:
                key_name = self._get_field_name(
                    entity_field.field_alias,
                    entity_field.node_join_field.field_alias,
                )
                field_lines.append(
                    f"{entity_field.node_join_field.field_alias} AS {key_name}"
                )
            else:
                field_lines.append("1 AS _dummy")

        # Format fields
        for i, field_line in enumerate(field_lines):
            prefix = " " if i == 0 else ","
            lines.append(f"{indent}  {prefix}{field_line}")

        lines.append(f"{indent}FROM")
        lines.append(f"{indent}  {table_desc.full_table_name}")

        # Collect all filters to apply
        filters: list[str] = []

        # Add implicit filter(s) for edge type
        # Handle OR syntax ([:KNOWS|WORKS_AT]) by combining filters with OR
        if len(entity_field.bound_edge_types) > 1:
            # Multiple edge types - collect filters from each type's descriptor
            edge_filters: list[str] = []
            # Get source type from bound_entity_name (format: "Source@TYPE@Target")
            parts = entity_field.bound_entity_name.split("@")
            source_type = parts[0] if len(parts) >= 1 else None

            for edge_type in entity_field.bound_edge_types:
                # Use find_edges_by_verb to find the correct edge schema for each type
                # This handles cases where edge types have different target node types
                # (e.g., KNOWS→Person, LIVES_IN→City, WORKS_AT→Company)
                edges = self._db_schema.find_edges_by_verb(
                    edge_type,
                    from_node_name=source_type,
                    to_node_name=None,  # Allow any target
                )
                if edges:
                    edge_schema = edges[0]
                    edge_id = edge_schema.id
                    type_desc = self._db_schema.get_sql_table_descriptors(edge_id)
                    if type_desc and type_desc.filter:
                        edge_filters.append(type_desc.filter)
            if edge_filters:
                # Combine with OR: (filter1) OR (filter2)
                combined_edge_filter = " OR ".join(f"({f})" for f in edge_filters)
                filters.append(combined_edge_filter)
        elif table_desc.filter:
            # Single edge type - use its filter directly
            filters.append(table_desc.filter)

        # Add pushed-down filter from optimizer (e.g., p.name = 'Alice')
        if op.filter_expression:
            # Render the filter expression using raw column names
            # (not aliased names like _gsql2rsql_p_name)
            rendered_filter = self._render_datasource_filter(
                op.filter_expression, entity_field.field_alias
            )
            filters.append(rendered_filter)

        # Render WHERE clause with all filters
        if filters:
            combined_filter = " AND ".join(f"({f})" for f in filters)
            lines.append(f"{indent}WHERE {combined_filter}")

        return "\n".join(lines)

    def _render_datasource_filter(
        self,
        expr: QueryExpression,
        entity_alias: str,
    ) -> str:
        """Render a filter expression for a DataSource using raw column names.

        Unlike _render_expression which uses aliased names like _gsql2rsql_p_name,
        this method renders expressions using raw column names from the table.

        Args:
            expr: The filter expression to render.
            entity_alias: The entity alias (e.g., 'p') to match against.

        Returns:
            SQL string with raw column names.
        """
        from gsql2rsql.parser.ast import (
            QueryExpressionProperty,
            QueryExpressionBinary,
            QueryExpressionValue,
            QueryExpressionFunction,
            QueryExpressionParameter,
        )

        if isinstance(expr, QueryExpressionProperty):
            # Use raw column name, not aliased
            if expr.variable_name == entity_alias and expr.property_name:
                return expr.property_name
            # Fallback to aliased name for other variables
            return self._get_field_name(expr.variable_name, expr.property_name or "")

        elif isinstance(expr, QueryExpressionValue):
            return self._render_value(expr)

        elif isinstance(expr, QueryExpressionParameter):
            return self._render_parameter(expr)

        elif isinstance(expr, QueryExpressionBinary):
            if not expr.operator or not expr.left_expression or not expr.right_expression:
                return "NULL"
            left = self._render_datasource_filter(expr.left_expression, entity_alias)
            right = self._render_datasource_filter(expr.right_expression, entity_alias)
            pattern = OPERATOR_PATTERNS.get(expr.operator.name, "({0}) ? ({1})")
            return pattern.format(left, right)

        elif isinstance(expr, QueryExpressionFunction):
            # Render function arguments with raw column names
            params = [
                self._render_datasource_filter(p, entity_alias)
                for p in expr.parameters
            ]
            # Use the standard function rendering logic
            func = expr.function
            if func == Function.NOT:
                return f"NOT ({params[0]})" if params else "NOT (NULL)"
            elif func == Function.NEGATIVE:
                return f"-({params[0]})" if params else "-(NULL)"
            # Add more function handlers as needed
            return f"{func.value}({', '.join(params)})"

        # For other expression types, fall back to standard rendering
        # This shouldn't happen for simple property filters
        return str(expr)

    def _render_join(self, op: JoinOperator, depth: int) -> str:
        """Render a join operator."""
        lines: list[str] = []
        indent = self._indent(depth)

        left_op = op.in_operator_left
        right_op = op.in_operator_right

        if not left_op or not right_op:
            return ""

        left_var = "_left"
        right_var = "_right"

        # Check if left side is RecursiveTraversalOperator
        is_recursive_join = isinstance(left_op, RecursiveTraversalOperator)

        if is_recursive_join:
            # Special handling for recursive CTE joins
            assert isinstance(left_op, RecursiveTraversalOperator)
            assert isinstance(right_op, DataSourceOperator)
            return self._render_recursive_join(op, left_op, right_op, depth)

        # Check if left side is AggregationBoundaryOperator
        is_boundary_join = isinstance(left_op, AggregationBoundaryOperator)

        if is_boundary_join:
            # Special handling for aggregation boundary joins
            assert isinstance(left_op, AggregationBoundaryOperator)
            return self._render_boundary_join(op, left_op, right_op, depth)

        lines.append(f"{indent}SELECT")

        # Determine output fields from both sides
        output_fields = self._get_join_output_fields(
            op, left_op, right_op, left_var, right_var
        )
        for i, field_line in enumerate(output_fields):
            prefix = " " if i == 0 else ","
            lines.append(f"{indent}  {prefix}{field_line}")

        # Check if this join needs undirected optimization
        needs_undirected_opt = self._should_use_undirected_union_optimization(op)

        # FROM left subquery
        lines.append(f"{indent}FROM (")
        lines.append(self._render_operator(left_op, depth + 1))
        lines.append(f"{indent}) AS {left_var}")

        # JOIN type and right subquery
        if op.join_type == JoinType.CROSS:
            lines.append(f"{indent}CROSS JOIN (")
            if needs_undirected_opt and isinstance(right_op, DataSourceOperator):
                lines.append(
                    self._render_undirected_edge_union(right_op, op, depth + 1)
                )
            else:
                lines.append(self._render_operator(right_op, depth + 1))
            lines.append(f"{indent}) AS {right_var}")
        else:
            join_keyword = (
                "INNER JOIN" if op.join_type == JoinType.INNER else "LEFT JOIN"
            )
            lines.append(f"{indent}{join_keyword} (")
            if needs_undirected_opt and isinstance(right_op, DataSourceOperator):
                lines.append(
                    self._render_undirected_edge_union(right_op, op, depth + 1)
                )
            else:
                lines.append(self._render_operator(right_op, depth + 1))
            lines.append(f"{indent}) AS {right_var} ON")

            # Render join conditions
            conditions = self._render_join_conditions(
                op, left_op, right_op, left_var, right_var
            )
            if conditions:
                for i, cond in enumerate(conditions):
                    prefix = "  " if i == 0 else "  AND "
                    lines.append(f"{indent}{prefix}{cond}")
            else:
                lines.append(f"{indent}  TRUE")

        return "\n".join(lines)

    def _should_use_undirected_union_optimization(
        self, op: JoinOperator
    ) -> bool:
        """
        Determine if a join should use UNION ALL optimization for undirected
        relationships.

        The UNION ALL optimization replaces inefficient OR conditions in JOINs
        with bidirectional edge expansion. This enables hash/merge joins instead
        of nested loops, improving performance from O(n²) to O(n).

        Example transformation:
            Before (slow): JOIN ON (p.id = k.source_id OR p.id = k.sink_id)
            After (fast):  JOIN (SELECT ... UNION ALL SELECT ...) ON p.id = k.node_id

        Args:
            op: The join operator to check for optimization eligibility.

        Returns:
            True if both conditions are met:
            1. Feature flag is enabled (undirected_strategy == 'union_edges')
            2. Join has EITHER-type join pairs (indicates undirected relationship)

        See Also:
            - _render_undirected_edge_union(): Generates the UNION ALL subquery
            - docs/development/UNDIRECTED_OPTIMIZATION_IMPLEMENTATION.md
        """
        # Check if any join pair is undirected AND uses UNION strategy
        # The use_union_for_undirected field is set by the planner based on the
        # edge access strategy, moving this semantic decision out of the renderer.
        return any(
            pair.pair_type in (
                JoinKeyPairType.EITHER,
                JoinKeyPairType.EITHER_AS_SOURCE,
                JoinKeyPairType.EITHER_AS_SINK,
            )
            and pair.use_union_for_undirected
            for pair in op.join_pairs
        )

    def _render_undirected_edge_union(
        self, edge_op: DataSourceOperator, join_op: JoinOperator, depth: int
    ) -> str:
        """
        Render an edge table with UNION ALL to expand undirected relationships.

        This method implements the "UNION ALL of edges" optimization strategy
        (Option A) for undirected relationships. Instead of using an OR condition
        in the JOIN clause (which prevents index usage and forces O(n²) nested
        loops), we expand edges bidirectionally before joining.

        Performance Impact:
            - Enables hash/merge join strategies (O(n) vs O(n²))
            - Allows index usage on join columns
            - Query planner can optimize join order
            - Filter pushdown works correctly
            - Trade-off: Reads edge table twice, but much faster overall

        Example Output:
            For Cypher: MATCH (p:Person)-[:KNOWS]-(f:Person)

            Generates:
              SELECT source_id AS _gsql2rsql_k_source_id,
                     sink_id AS _gsql2rsql_k_sink_id,
                     since AS _gsql2rsql_k_since
              FROM graph.Knows
              UNION ALL
              SELECT sink_id AS _gsql2rsql_k_source_id,
                     source_id AS _gsql2rsql_k_sink_id,
                     since AS _gsql2rsql_k_since
              FROM graph.Knows

            This allows simple equality joins:
              JOIN (...) ON p.id = k.source_id

            Instead of inefficient OR joins:
              JOIN (...) ON (p.id = k.source_id OR p.id = k.sink_id)

        Args:
            edge_op: The DataSourceOperator for the edge/relationship table.
            join_op: The JoinOperator containing join pair information.
            depth: Current indentation depth for SQL formatting.

        Returns:
            SQL string with UNION ALL subquery expanding edges bidirectionally.
            Falls back to standard rendering if edge is not a valid relationship.

        Note:
            This method is only called when _should_use_undirected_union_optimization()
            returns True. For directed relationships or when the feature flag is
            disabled, standard rendering is used.

        See Also:
            - _should_use_undirected_union_optimization(): Detection logic
            - _render_join_conditions(): Uses simple equality for optimized joins
            - docs/development/UNDIRECTED_OPTIMIZATION_IMPLEMENTATION.md
        """
        indent = self._indent(depth)
        lines: list[str] = []

        # Get edge schema - first entity field describes the edge
        if not edge_op.output_schema:
            return self._render_operator(edge_op, depth)

        entity_field = edge_op.output_schema[0]
        if not isinstance(entity_field, EntityField):
            return self._render_operator(edge_op, depth)

        # Get table descriptor
        table_desc = self._db_schema.get_sql_table_descriptors(
            entity_field.bound_entity_name
        )
        if not table_desc:
            return self._render_operator(edge_op, depth)

        table_name = table_desc.full_table_name
        alias = entity_field.field_alias

        # Get source/sink columns
        source_join = entity_field.rel_source_join_field
        sink_join = entity_field.rel_sink_join_field

        if not source_join or not sink_join:
            # Not a relationship or missing join fields
            return self._render_operator(edge_op, depth)

        source_col = source_join.field_alias
        sink_col = sink_join.field_alias

        # Collect edge properties from encapsulated fields
        # Skip the join key fields themselves to avoid duplication
        skip_fields = {source_col, sink_col}
        property_fields = [
            f for f in entity_field.encapsulated_fields
            if f.field_alias not in skip_fields
        ]

        # Build WHERE clause from table filter (e.g., relationship_type = 'DEFRAUDED')
        where_clause = ""
        if table_desc.filter:
            where_clause = f"\n{indent}WHERE {table_desc.filter}"

        # ========== FORWARD DIRECTION: source -> sink ==========
        # For edge (Alice)-[:KNOWS]->(Bob), this branch represents:
        #   Alice as source, Bob as sink (original direction)
        lines.append(f"{indent}SELECT")
        lines.append(
            f"{indent}   {source_col} AS "
            f"{self._get_field_name(alias, source_col)}"
        )
        lines.append(
            f"{indent}  ,{sink_col} AS "
            f"{self._get_field_name(alias, sink_col)}"
        )
        # Include all edge properties (e.g., "since", "weight")
        for prop_field in property_fields:
            prop_col = prop_field.field_alias
            lines.append(
                f"{indent}  ,{prop_col} AS "
                f"{self._get_field_name(alias, prop_col)}"
            )
        lines.append(f"{indent}FROM")
        lines.append(f"{indent}  {table_name}{where_clause}")

        # ========== UNION ALL: Combine both directions ==========
        lines.append(f"{indent}UNION ALL")

        # ========== REVERSE DIRECTION: sink -> source (SWAPPED) ==========
        # For edge (Alice)-[:KNOWS]->(Bob), this branch represents:
        #   Bob as source, Alice as sink (reversed for undirected semantics)
        # NOTE: Column names stay the same (source_col, sink_col) but values swap
        lines.append(f"{indent}SELECT")
        lines.append(
            f"{indent}   {sink_col} AS "
            f"{self._get_field_name(alias, source_col)}"  # Sink value → source alias
        )
        lines.append(
            f"{indent}  ,{source_col} AS "
            f"{self._get_field_name(alias, sink_col)}"  # Source value → sink alias
        )
        # Edge properties remain the same (not directional)
        for prop_field in property_fields:
            prop_col = prop_field.field_alias
            lines.append(
                f"{indent}  ,{prop_col} AS "
                f"{self._get_field_name(alias, prop_col)}"
            )
        lines.append(f"{indent}FROM")
        lines.append(f"{indent}  {table_name}{where_clause}")

        return "\n".join(lines)

    def _get_join_output_fields(
        self,
        op: JoinOperator,
        left_op: LogicalOperator,
        right_op: LogicalOperator,
        left_var: str,
        right_var: str,
    ) -> list[str]:
        """Get output field expressions for a join."""
        fields: list[str] = []
        # Track already-projected aliases to avoid duplicates
        projected_aliases: set[str] = set()

        # Collect field aliases from both sides (top-level entity aliases)
        left_aliases = {f.field_alias for f in left_op.output_schema}
        right_aliases = {f.field_alias for f in right_op.output_schema}

        # Also collect all column names that are actually available on each side
        # This includes encapsulated field names like _gsql2rsql_entity_property
        left_columns = self._collect_all_column_names(left_op.output_schema)
        right_columns = self._collect_all_column_names(right_op.output_schema)

        for field in op.output_schema:
            is_from_left = field.field_alias in left_aliases
            is_from_right = field.field_alias in right_aliases

            if isinstance(field, EntityField):
                # Entity field - output join keys
                if field.entity_type == EntityType.NODE:
                    if field.node_join_field:
                        # Use pre-rendered field name if available (varlen paths)
                        if field.node_join_field.field_name and field.node_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            key_name = field.node_join_field.field_name
                        else:
                            key_name = self._get_field_name(
                                field.field_alias, field.node_join_field.field_alias
                            )
                        # Skip if already projected
                        if key_name not in projected_aliases:
                            # Determine which side of join has this column
                            # Priority: Check actual column presence first (left_columns/right_columns)
                            # then fall back to entity alias membership (defensive)
                            if key_name in left_columns:
                                actual_var = left_var
                            elif key_name in right_columns:
                                actual_var = right_var
                            else:
                                # Fallback: Use entity alias to infer side
                                # (with defensive validation)
                                actual_var = self._determine_column_side(
                                    field.field_alias,
                                    is_from_left,
                                    is_from_right,
                                    left_var,
                                    right_var,
                                )
                            fields.append(f"{actual_var}.{key_name} AS {key_name}")
                            projected_aliases.add(key_name)
                else:
                    if field.rel_source_join_field:
                        # Use pre-rendered field name if available (varlen paths)
                        if field.rel_source_join_field.field_name and field.rel_source_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            src_key = field.rel_source_join_field.field_name
                        else:
                            src_key = self._get_field_name(
                                field.field_alias,
                                field.rel_source_join_field.field_alias,
                            )
                        # Skip if already projected
                        if src_key not in projected_aliases:
                            # Determine which side of join has this column
                            if src_key in left_columns:
                                actual_var = left_var
                            elif src_key in right_columns:
                                actual_var = right_var
                            else:
                                actual_var = self._determine_column_side(
                                    field.field_alias,
                                    is_from_left,
                                    is_from_right,
                                    left_var,
                                    right_var,
                                )
                            fields.append(f"{actual_var}.{src_key} AS {src_key}")
                            projected_aliases.add(src_key)
                    if field.rel_sink_join_field:
                        # Use pre-rendered field name if available (varlen paths)
                        if field.rel_sink_join_field.field_name and field.rel_sink_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            sink_key = field.rel_sink_join_field.field_name
                        else:
                            sink_key = self._get_field_name(
                                field.field_alias,
                                field.rel_sink_join_field.field_alias,
                            )
                        # Skip if already projected
                        if sink_key not in projected_aliases:
                            # Determine which side of join has this column
                            if sink_key in left_columns:
                                actual_var = left_var
                            elif sink_key in right_columns:
                                actual_var = right_var
                            else:
                                actual_var = self._determine_column_side(
                                    field.field_alias,
                                    is_from_left,
                                    is_from_right,
                                    left_var,
                                    right_var,
                                )
                            fields.append(f"{actual_var}.{sink_key} AS {sink_key}")
                            projected_aliases.add(sink_key)

                # Output all encapsulated fields (properties)
                # Skip join key fields to avoid duplicates
                skip_fields = set()
                if field.node_join_field:
                    skip_fields.add(field.node_join_field.field_alias)
                if field.rel_source_join_field:
                    skip_fields.add(field.rel_source_join_field.field_alias)
                if field.rel_sink_join_field:
                    skip_fields.add(field.rel_sink_join_field.field_alias)

                for encap_field in field.encapsulated_fields:
                    if encap_field.field_alias not in skip_fields:
                        # Use pre-rendered field name if available (varlen paths)
                        if encap_field.field_name and encap_field.field_name.startswith(self.COLUMN_PREFIX):
                            field_alias = encap_field.field_name
                        else:
                            field_alias = self._get_field_name(
                                field.field_alias, encap_field.field_alias
                            )
                        # Skip if already projected
                        if field_alias in projected_aliases:
                            continue
                        # Column pruning: only include if required or pruning disabled
                        if (
                            not self._enable_column_pruning
                            or not self._required_columns
                            or field_alias in self._required_columns
                        ):
                            # Determine which side of join has this column
                            if field_alias in left_columns:
                                actual_var = left_var
                            elif field_alias in right_columns:
                                actual_var = right_var
                            else:
                                actual_var = self._determine_column_side(
                                    field.field_alias,
                                    is_from_left,
                                    is_from_right,
                                    left_var,
                                    right_var,
                                )
                            fields.append(f"{actual_var}.{field_alias} AS {field_alias}")
                            projected_aliases.add(field_alias)

            elif isinstance(field, ValueField):
                # Skip if already projected
                if field.field_alias in projected_aliases:
                    continue
                # Column pruning for value fields
                # Check both _required_columns (for property refs like `p.name`)
                # and _required_value_fields (for bare variable refs like `shared_cards`)
                if (
                    not self._enable_column_pruning
                    or not self._required_columns
                    or field.field_alias in self._required_columns
                    or field.field_alias in self._required_value_fields
                ):
                    # Determine which side of join has this column
                    if field.field_alias in left_columns:
                        actual_var = left_var
                    elif field.field_alias in right_columns:
                        actual_var = right_var
                    else:
                        actual_var = self._determine_column_side(
                            field.field_alias,
                            is_from_left,
                            is_from_right,
                            left_var,
                            right_var,
                        )
                    fields.append(f"{actual_var}.{field.field_alias} AS {field.field_alias}")
                    projected_aliases.add(field.field_alias)

        # IMPORTANT: Also propagate required columns from the left side that weren't
        # already projected. This handles cases where an entity (like 'c') is in the
        # left side of the join but not in op.output_schema, yet its properties
        # (like _gsql2rsql_c_id, _gsql2rsql_c_name) are needed downstream.
        #
        # This is especially important for joins after recursive traversals, where
        # the source node columns (e.g., _gsql2rsql_c_id from source.id) are rendered but
        # not tracked in the logical plan's output schema.
        if self._enable_column_pruning and self._required_columns:
            # Collect entity aliases that we know are available on the left side
            # This includes entities from output_schema and entities from recursive
            # traversal sources (which aren't in output_schema but are rendered)
            left_entity_aliases: set[str] = set()
            for field in left_op.output_schema:
                left_entity_aliases.add(field.field_alias)

            # Also check if left_op contains a recursive traversal - if so, its
            # source alias should be considered available
            if isinstance(left_op, RecursiveTraversalOperator):
                if left_op.source_alias:
                    left_entity_aliases.add(left_op.source_alias)
            else:
                # Check if the left operand chain contains a recursive traversal
                def find_recursive_source(check_op: LogicalOperator) -> str | None:
                    if isinstance(check_op, RecursiveTraversalOperator):
                        return check_op.source_alias
                    # Use polymorphic get_input_operator() for all operator types
                    input_op = check_op.get_input_operator()
                    if input_op:
                        return find_recursive_source(input_op)
                    return None

                recursive_source = find_recursive_source(left_op)
                if recursive_source:
                    left_entity_aliases.add(recursive_source)

            for required_col in self._required_columns:
                if required_col in projected_aliases:
                    continue  # Already projected

                # Check if this column belongs to a known left-side entity
                # Column pattern: _gsql2rsql_{entity}_{property}
                col_entity = None
                if required_col.startswith(self.COLUMN_PREFIX):
                    parts = required_col[len(self.COLUMN_PREFIX):].split("_", 1)
                    if len(parts) >= 1:
                        col_entity = parts[0]

                if required_col in left_columns:
                    fields.append(f"{left_var}.{required_col} AS {required_col}")
                    projected_aliases.add(required_col)
                elif required_col in right_columns:
                    fields.append(f"{right_var}.{required_col} AS {required_col}")
                    projected_aliases.add(required_col)
                elif col_entity and col_entity in left_entity_aliases:
                    # Column belongs to an entity we know is on the left side
                    # (including recursive traversal source nodes)
                    fields.append(f"{left_var}.{required_col} AS {required_col}")
                    projected_aliases.add(required_col)

        return fields

    def _collect_all_column_names(self, schema: Schema) -> set[str]:
        """Collect all column names from a schema, including encapsulated fields."""
        columns: set[str] = set()
        for field in schema:
            if isinstance(field, EntityField):
                # Add the node/relationship join key columns
                # Use pre-rendered field names if available (varlen paths)
                if field.node_join_field:
                    if field.node_join_field.field_name and field.node_join_field.field_name.startswith(self.COLUMN_PREFIX):
                        columns.add(field.node_join_field.field_name)
                    else:
                        columns.add(
                            self._get_field_name(field.field_alias, field.node_join_field.field_alias)
                        )
                if field.rel_source_join_field:
                    if field.rel_source_join_field.field_name and field.rel_source_join_field.field_name.startswith(self.COLUMN_PREFIX):
                        columns.add(field.rel_source_join_field.field_name)
                    else:
                        columns.add(
                            self._get_field_name(field.field_alias, field.rel_source_join_field.field_alias)
                        )
                if field.rel_sink_join_field:
                    if field.rel_sink_join_field.field_name and field.rel_sink_join_field.field_name.startswith(self.COLUMN_PREFIX):
                        columns.add(field.rel_sink_join_field.field_name)
                    else:
                        columns.add(
                            self._get_field_name(field.field_alias, field.rel_sink_join_field.field_alias)
                        )
                # Add all encapsulated fields
                for encap_field in field.encapsulated_fields:
                    if encap_field.field_name and encap_field.field_name.startswith(self.COLUMN_PREFIX):
                        columns.add(encap_field.field_name)
                    else:
                        columns.add(
                            self._get_field_name(field.field_alias, encap_field.field_alias)
                        )
            elif isinstance(field, ValueField):
                columns.add(field.field_alias)
        return columns

    def _determine_column_side(
        self,
        field_alias: str,
        is_from_left: bool,
        is_from_right: bool,
        left_var: str,
        right_var: str,
    ) -> str:
        """
        Determine which side of a join a field belongs to (defensive programming).

        This method implements defensive validation to catch bugs in the planner
        or resolver that might create orphaned fields or ambiguous references.

        Args:
            field_alias: The field alias to locate (e.g., "p", "k", "f")
            is_from_left: Whether field is in left output schema
            is_from_right: Whether field is in right output schema
            left_var: SQL variable name for left side (e.g., "_left")
            right_var: SQL variable name for right side (e.g., "_right")

        Returns:
            The SQL variable name to use (_left or _right)

        Raises:
            RuntimeError: If field is not found in either side (orphaned field)

        Examples:
            # Normal case: field only in left
            >>> _determine_column_side("p", True, False, "_left", "_right")
            "_left"

            # Normal case: field only in right
            >>> _determine_column_side("f", False, True, "_left", "_right")
            "_right"

            # Ambiguous case: field in both (prioritizes left, could log warning)
            >>> _determine_column_side("shared", True, True, "_left", "_right")
            "_left"

            # Bug case: field not in either (throws error)
            >>> _determine_column_side("orphan", False, False, "_left", "_right")
            RuntimeError: Field 'orphan' not found in left or right join schema
        """
        # Case 1: Field exists in both sides (ambiguous - rare but possible)
        # This can happen if there's a naming collision after join operations.
        # Prioritize left side for consistency with original logic.
        if is_from_left and is_from_right:
            # NOTE: In the future, we could log a warning here if needed
            # for debugging ambiguous cases, but for now we silently prefer left.
            return left_var

        # Case 2: Field exists only in left side (normal case)
        elif is_from_left:
            return left_var

        # Case 3: Field exists only in right side (normal case)
        elif is_from_right:
            return right_var

        # Case 4: Field does NOT exist in either side (BUG in planner/resolver!)
        # This indicates a serious bug where the planner created a field reference
        # that wasn't properly included in the join output schemas.
        # Fail-fast with a clear error message rather than generating invalid SQL.
        else:
            raise RuntimeError(
                f"Field '{field_alias}' not found in left or right join output schemas. "
                f"This indicates a bug in the query planner or resolver. "
                f"The field should have been included in one of the join operator's "
                f"output schemas during query planning."
            )

    def _render_join_conditions(
        self,
        op: JoinOperator,
        left_op: LogicalOperator,
        right_op: LogicalOperator,
        left_var: str,
        right_var: str,
    ) -> list[str]:
        """Render join conditions."""
        conditions: list[str] = []

        left_aliases = {f.field_alias for f in left_op.output_schema}

        for pair in op.join_pairs:
            # Find the node and relationship fields
            node_alias = pair.node_alias
            rel_alias = pair.relationship_or_node_alias

            # Determine which side each is on
            node_on_left = node_alias in left_aliases
            node_var = left_var if node_on_left else right_var
            rel_var = right_var if node_on_left else left_var

            # Get the node's join key
            node_field = next(
                (f for f in op.input_schema if f.field_alias == node_alias),
                None,
            )
            rel_field = next(
                (f for f in op.input_schema if f.field_alias == rel_alias),
                None,
            )

            if not node_field or not rel_field:
                continue

            if isinstance(node_field, EntityField) and isinstance(
                rel_field, EntityField
            ):
                # ========== FIX: Variable-length path field naming ==========
                # For normal entities (from DataSourceOperator), we construct the
                # field name from alias + field_alias (e.g., "peer" + "id" -> "_gsql2rsql_peer_id").
                #
                # However, for entities from variable-length paths (RecursiveTraversalOperator),
                # the fields are already projected with full SQL names in _render_recursive_join()
                # (e.g., "sink.id AS _gsql2rsql_peer_id"). In this case, the field_name
                # attribute is set to the complete SQL column name.
                #
                # Using field_name directly when it's already a full SQL name prevents
                # double-prefixing (e.g., "_gsql2rsql_peer_peer_id" ❌).
                if (
                    node_field.node_join_field
                    and node_field.node_join_field.field_name
                    and node_field.node_join_field.field_name.startswith(self.COLUMN_PREFIX)
                ):
                    # Use the pre-rendered SQL column name directly (varlen paths)
                    node_key = node_field.node_join_field.field_name
                else:
                    # Construct field name from alias + field_alias (normal entities)
                    node_key = self._get_field_name(
                        node_alias,
                        (
                            node_field.node_join_field.field_alias
                            if node_field.node_join_field
                            else "id"
                        ),
                    )

                if pair.pair_type == JoinKeyPairType.SOURCE:
                    # Use pre-rendered field name if available (varlen paths)
                    if (
                        rel_field.rel_source_join_field
                        and rel_field.rel_source_join_field.field_name
                        and rel_field.rel_source_join_field.field_name.startswith(self.COLUMN_PREFIX)
                    ):
                        rel_key = rel_field.rel_source_join_field.field_name
                    else:
                        rel_key = self._get_field_name(
                            rel_alias,
                            (
                                rel_field.rel_source_join_field.field_alias
                                if rel_field.rel_source_join_field
                                else "source_id"
                            ),
                        )
                elif pair.pair_type == JoinKeyPairType.SINK:
                    # Use pre-rendered field name if available (varlen paths)
                    if (
                        rel_field.rel_sink_join_field
                        and rel_field.rel_sink_join_field.field_name
                        and rel_field.rel_sink_join_field.field_name.startswith(self.COLUMN_PREFIX)
                    ):
                        rel_key = rel_field.rel_sink_join_field.field_name
                    else:
                        rel_key = self._get_field_name(
                            rel_alias,
                            (
                                rel_field.rel_sink_join_field.field_alias
                                if rel_field.rel_sink_join_field
                                else "sink_id"
                            ),
                        )
                elif pair.pair_type == JoinKeyPairType.NODE_ID:
                    # Node to node join
                    # Use pre-rendered field name if available (varlen paths)
                    if (
                        rel_field.node_join_field
                        and rel_field.node_join_field.field_name
                        and rel_field.node_join_field.field_name.startswith(self.COLUMN_PREFIX)
                    ):
                        rel_key = rel_field.node_join_field.field_name
                    else:
                        rel_key = self._get_field_name(
                            rel_alias,
                            (
                                rel_field.node_join_field.field_alias
                                if rel_field.node_join_field
                                else "id"
                            ),
                        )
                elif pair.pair_type in (JoinKeyPairType.EITHER_AS_SOURCE, JoinKeyPairType.EITHER_AS_SINK):
                    # Undirected relationship with explicit source/sink position
                    # Get both keys for potential OR fallback
                    source_key = None
                    sink_key = None

                    if rel_field.rel_source_join_field:
                        if rel_field.rel_source_join_field.field_name and rel_field.rel_source_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            source_key = rel_field.rel_source_join_field.field_name
                        else:
                            source_key = self._get_field_name(
                                rel_alias, rel_field.rel_source_join_field.field_alias
                            )
                    if rel_field.rel_sink_join_field:
                        if rel_field.rel_sink_join_field.field_name and rel_field.rel_sink_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            sink_key = rel_field.rel_sink_join_field.field_name
                        else:
                            sink_key = self._get_field_name(
                                rel_alias, rel_field.rel_sink_join_field.field_alias
                            )

                    if pair.use_union_for_undirected:
                        # OPTIMIZED: UNION ALL expansion - use appropriate key
                        # Decision made by planner based on edge access strategy
                        if pair.pair_type == JoinKeyPairType.EITHER_AS_SOURCE:
                            rel_key = source_key or self._get_field_name(rel_alias, "source_id")
                        else:  # EITHER_AS_SINK
                            rel_key = sink_key or self._get_field_name(rel_alias, "sink_id")
                    else:
                        # LEGACY: OR condition for compatibility
                        if source_key and sink_key:
                            conditions.append(
                                f"({node_var}.{node_key} = {rel_var}.{source_key} "
                                f"OR {node_var}.{node_key} = {rel_var}.{sink_key})"
                            )
                            continue
                        else:
                            # Fallback to available key
                            rel_key = source_key or sink_key or self._get_field_name(rel_alias, "id")
                else:
                    # EITHER/BOTH - legacy undirected handling (for VLP and backwards compatibility)
                    # For Cypher: (a)-[:REL]-(b) matches both (a)-[:REL]->(b) and (a)<-[:REL]-(b)
                    source_key = None
                    sink_key = None

                    if rel_field.rel_source_join_field:
                        # Use pre-rendered field name if available (varlen paths)
                        if rel_field.rel_source_join_field.field_name and rel_field.rel_source_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            source_key = rel_field.rel_source_join_field.field_name
                        else:
                            source_key = self._get_field_name(
                                rel_alias, rel_field.rel_source_join_field.field_alias
                            )
                    if rel_field.rel_sink_join_field:
                        # Use pre-rendered field name if available (varlen paths)
                        if rel_field.rel_sink_join_field.field_name and rel_field.rel_sink_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            sink_key = rel_field.rel_sink_join_field.field_name
                        else:
                            sink_key = self._get_field_name(
                                rel_alias, rel_field.rel_sink_join_field.field_alias
                            )

                    if source_key and sink_key:
                        # ========== STRATEGY SELECTION: OR vs UNION ALL ==========
                        # Decision now comes from planner via use_union_for_undirected
                        if pair.use_union_for_undirected:
                            # **OPTIMIZED (UNION ALL of edges - default)**
                            # Edges are already expanded bidirectionally via UNION ALL
                            # in _render_undirected_edge_union(), so we can use simple
                            # equality join here. This enables hash/merge joins instead
                            # of nested loops.
                            #
                            # Example:
                            #   JOIN (... UNION ALL ...) k ON p.id = k.source_id
                            #
                            # Performance: O(n) with hash join
                            conditions.append(
                                f"{node_var}.{node_key} = {rel_var}.{source_key}"
                            )
                        else:
                            # **LEGACY (OR condition - disabled by default)**
                            # Use OR to match both directions in a single join.
                            # This prevents index usage and forces nested loop joins.
                            #
                            # Example:
                            #   JOIN k ON (p.id = k.source_id OR p.id = k.sink_id)
                            #
                            # Performance: O(n²) with nested loop
                            # NOTE: Only use for small datasets or debugging
                            conditions.append(
                                f"({node_var}.{node_key} = {rel_var}.{source_key} "
                                f"OR {node_var}.{node_key} = {rel_var}.{sink_key})"
                            )
                        continue
                    elif source_key:
                        rel_key = source_key
                    elif sink_key:
                        rel_key = sink_key
                    else:
                        continue

                conditions.append(f"{node_var}.{node_key} = {rel_var}.{rel_key}")

        return conditions

    def _render_selection(self, op: SelectionOperator, depth: int) -> str:
        """Render a selection (WHERE) operator."""
        lines: list[str] = []
        indent = self._indent(depth)

        if not op.in_operator:
            return ""

        lines.append(f"{indent}SELECT *")
        lines.append(f"{indent}FROM (")
        lines.append(self._render_operator(op.in_operator, depth + 1))
        lines.append(f"{indent}) AS _filter")

        if op.filter_expression:
            filter_sql = self._render_expression(op.filter_expression, op)
            lines.append(f"{indent}WHERE {filter_sql}")

        return "\n".join(lines)

    def _render_unwind(self, op: UnwindOperator, depth: int) -> str:
        """Render an UNWIND operator using Databricks SQL TVF syntax.

        UNWIND expression AS variable becomes:
        SELECT _unwind_source.*, variable
        FROM (inner) AS _unwind_source,
        EXPLODE(expression) AS _exploded(variable)

        For NULL/empty array preservation, use EXPLODE_OUTER instead.
        The preserve_nulls flag on the operator controls this behavior.
        """
        lines: list[str] = []
        indent = self._indent(depth)

        if not op.in_operator:
            return ""

        var_name = op.variable_name

        # Render the list expression
        if op.list_expression:
            list_sql = self._render_expression(op.list_expression, op)
        else:
            list_sql = "ARRAY()"

        # Choose explode function based on NULL preservation needs
        # Default to EXPLODE (drops rows with NULL/empty arrays)
        # Use EXPLODE_OUTER if we need to preserve rows with NULL/empty arrays
        explode_func = "EXPLODE_OUTER" if getattr(op, 'preserve_nulls', False) else "EXPLODE"

        # Build the SELECT with all columns from source plus the unwound variable
        lines.append(f"{indent}SELECT")
        lines.append(f"{indent}   _unwind_source.*")
        lines.append(f"{indent}  ,{var_name}")
        lines.append(f"{indent}FROM (")
        lines.append(self._render_operator(op.in_operator, depth + 1))
        lines.append(f"{indent}) AS _unwind_source,")
        lines.append(
            f"{indent}{explode_func}({list_sql}) AS _exploded({var_name})"
        )

        return "\n".join(lines)

    def _render_projection(self, op: ProjectionOperator, depth: int) -> str:
        """Render a projection (SELECT) operator.

        Handles both regular projections and flattened projections (where a
        SelectionOperator was merged in via SubqueryFlatteningOptimizer).

        SQL clause order: SELECT ... FROM ... WHERE ... GROUP BY ... HAVING ... ORDER BY ... LIMIT
        """
        lines: list[str] = []
        indent = self._indent(depth)

        if not op.in_operator:
            return ""

        # Render SELECT clause
        distinct = "DISTINCT " if op.is_distinct else ""
        lines.append(f"{indent}SELECT {distinct}")

        # Render projection fields
        # Note: Check for aggregation first since we need it for alias logic
        has_aggregation = any(self._has_aggregation(expr) for _, expr in op.projections)

        for i, (alias, expr) in enumerate(op.projections):
            rendered = self._render_expression(expr, op)
            prefix = " " if i == 0 else ","

            # Bug fix: In aggregation contexts or intermediate projections, entity IDs should
            # keep their full column names instead of being aliased to short names. This
            # prevents UNRESOLVED_COLUMN errors in PySpark when outer queries try to
            # reference the original column name.
            #
            # Example bug: WITH p, COUNT(t) AS total
            #   - Buggy:  _gsql2rsql_p_id AS p  (aliases away the column)
            #   - Fixed:  _gsql2rsql_p_id AS _gsql2rsql_p_id  (preserves column name)
            #
            # This applies to:
            # 1. Aggregating projections (has_aggregation=True)
            # 2. Intermediate projections (depth > 0) that pass entities through
            #
            # Check if this is a bare entity reference (not an aggregate, not a property access)
            is_bare_entity = (
                isinstance(expr, QueryExpressionProperty)
                and expr.property_name is None
                and not self._has_aggregation(expr)
            )

            if (has_aggregation or depth > 0) and is_bare_entity:
                # Use full column name as alias to preserve column availability
                output_alias = rendered
            else:
                # Use user-provided alias (normal behavior)
                output_alias = alias

            lines.append(f"{indent}  {prefix}{rendered} AS {output_alias}")

        # has_aggregation was already computed above (line 2081) for alias logic

        # Bug #1 Fix: When projecting entity variables through a WITH clause,
        # we need to also project any entity properties that are required downstream.
        # For example: WITH c, COUNT(p) AS pop -> if downstream needs c.name,
        # we must project _gsql2rsql_c_name in addition to _gsql2rsql_c_id.
        #
        # This applies to:
        # 1. ALL aggregating projections (GROUP BY loses columns not in SELECT/GROUP BY)
        # 2. INTERMEDIATE non-aggregating projections (depth > 0) where entity variables
        #    are passed through and downstream needs their properties
        # 3. ROOT projection (depth == 0) with entity returns (RETURN p projects all properties)
        #
        # Check if there are any entity returns in this projection
        has_entity_return = False
        if op.operator_debug_id in self._resolved.resolved_projections:
            has_entity_return = any(
                proj.is_entity_ref
                for proj in self._resolved.resolved_projections[op.operator_debug_id]
            )

        extra_columns: list[str] = []
        if self._required_columns and (has_aggregation or depth > 0 or has_entity_return):
            extra_columns = self._get_entity_properties_for_aggregation(op)
            for col_alias in extra_columns:
                lines.append(f"{indent}  ,{col_alias} AS {col_alias}")

        lines.append(f"{indent}FROM (")
        lines.append(self._render_operator(op.in_operator, depth + 1))
        lines.append(f"{indent}) AS _proj")

        # WHERE clause (from flattened SelectionOperator)
        # Applied BEFORE GROUP BY - filters individual rows
        if op.filter_expression:
            filter_sql = self._render_expression(op.filter_expression, op)
            lines.append(f"{indent}WHERE {filter_sql}")

        # Group by for aggregations
        if has_aggregation:
            # First, identify which aliases are aggregates
            aggregate_aliases: set[str] = {
                alias for alias, expr in op.projections
                if self._has_aggregation(expr)
            }
            # Non-aggregate expressions go in GROUP BY, but only if they don't
            # reference any aggregate aliases (e.g., similarity_score = ... + shared_merchants
            # shouldn't be in GROUP BY if shared_merchants is an aggregate)
            non_agg_exprs = [
                self._render_expression(expr, op)
                for alias, expr in op.projections
                if not self._has_aggregation(expr)
                and not self._references_aliases(expr, aggregate_aliases)
            ]
            # Also include extra entity property columns in GROUP BY
            all_group_by_cols = non_agg_exprs + extra_columns
            if all_group_by_cols:
                group_by = ", ".join(all_group_by_cols)
                lines.append(f"{indent}GROUP BY {group_by}")

        # HAVING clause (filter on aggregated columns)
        # Applied AFTER GROUP BY - filters groups
        # Note: If there's no aggregation but having_expression is set,
        # treat it as a regular WHERE clause (e.g., WITH ... WHERE on computed columns)
        needs_subquery_wrap = False
        if op.having_expression:
            having_sql = self._render_expression(op.having_expression, op)
            if has_aggregation:
                lines.append(f"{indent}HAVING {having_sql}")
            else:
                # No aggregation - check if the expression references aliases
                # defined in the current projection (e.g., return_rate > 0.5 where
                # return_rate is computed in this SELECT). SQL doesn't allow this,
                # so we need to wrap in a subquery.
                projection_aliases = {alias for alias, _ in op.projections}
                if self._references_aliases(op.having_expression, projection_aliases):
                    # Mark that we need to wrap this in a subquery
                    needs_subquery_wrap = True
                else:
                    # Filter doesn't reference computed aliases, can use WHERE
                    lines.append(f"{indent}WHERE {having_sql}")

        # Order by
        if op.order_by:
            order_parts: list[str] = []
            for expr, is_desc in op.order_by:
                rendered = self._render_expression(expr, op)
                direction = "DESC" if is_desc else "ASC"
                order_parts.append(f"{rendered} {direction}")
            lines.append(f"{indent}ORDER BY {', '.join(order_parts)}")

        # Limit and skip (Databricks uses LIMIT/OFFSET)
        if op.limit is not None or op.skip is not None:
            if op.limit is not None:
                lines.append(f"{indent}LIMIT {op.limit}")
            if op.skip is not None:
                lines.append(f"{indent}OFFSET {op.skip}")

        # If we need to wrap in a subquery (because having_expression references
        # aliases defined in this SELECT), wrap the entire query
        if needs_subquery_wrap and op.having_expression:
            inner_sql = "\n".join(lines)
            having_sql = self._render_expression(op.having_expression, op)
            # Build outer wrapper that projects all columns and applies the filter
            outer_lines = [
                f"{indent}SELECT *",
                f"{indent}FROM (",
                inner_sql,
                f"{indent}) AS _filter",
                f"{indent}WHERE {having_sql}",
            ]
            return "\n".join(outer_lines)

        return "\n".join(lines)

    def _render_set_operator(self, op: SetOperator, depth: int) -> str:
        """Render a set operator (UNION, etc.)."""
        lines: list[str] = []
        indent = self._indent(depth)

        left_op = op.in_operator_left
        right_op = op.in_operator_right

        if not left_op or not right_op:
            return ""

        lines.append(self._render_operator(left_op, depth))

        if op.set_operation == SetOperationType.UNION_ALL:
            lines.append(f"{indent}UNION ALL")
        elif op.set_operation == SetOperationType.UNION:
            lines.append(f"{indent}UNION")
        elif op.set_operation == SetOperationType.INTERSECT:
            lines.append(f"{indent}INTERSECT")
        elif op.set_operation == SetOperationType.EXCEPT:
            lines.append(f"{indent}EXCEPT")

        lines.append(self._render_operator(right_op, depth))

        return "\n".join(lines)

    def _render_expression(
        self, expr: QueryExpression, context_op: LogicalOperator
    ) -> str:
        """Render an expression to SQL."""
        if isinstance(expr, QueryExpressionValue):
            return self._render_value(expr)
        elif isinstance(expr, QueryExpressionParameter):
            return self._render_parameter(expr)
        elif isinstance(expr, QueryExpressionProperty):
            return self._render_property(expr, context_op)
        elif isinstance(expr, QueryExpressionBinary):
            return self._render_binary(expr, context_op)
        elif isinstance(expr, QueryExpressionFunction):
            return self._render_function(expr, context_op)
        elif isinstance(expr, QueryExpressionAggregationFunction):
            return self._render_aggregation(expr, context_op)
        elif isinstance(expr, QueryExpressionList):
            return self._render_list(expr, context_op)
        elif isinstance(expr, QueryExpressionCaseExpression):
            return self._render_case(expr, context_op)
        elif isinstance(expr, QueryExpressionExists):
            return self._render_exists(expr, context_op)
        elif isinstance(expr, QueryExpressionListPredicate):
            return self._render_list_predicate(expr, context_op)
        elif isinstance(expr, QueryExpressionListComprehension):
            return self._render_list_comprehension(expr, context_op)
        elif isinstance(expr, QueryExpressionReduce):
            return self._render_reduce(expr, context_op)
        elif isinstance(expr, QueryExpressionMapLiteral):
            return self._render_map_literal(expr, context_op)
        else:
            return str(expr)

    def _render_value(self, expr: QueryExpressionValue) -> str:
        """Render a literal value (Databricks SQL syntax)."""
        if expr.value is None:
            return "NULL"
        if isinstance(expr.value, str):
            escaped = expr.value.replace("'", "''")
            return f"'{escaped}'"
        if isinstance(expr.value, bool):
            return "TRUE" if expr.value else "FALSE"
        return str(expr.value)

    def _render_parameter(self, expr: QueryExpressionParameter) -> str:
        """Render a parameter expression (Databricks SQL syntax).

        Uses :param_name syntax for named parameters in Databricks SQL.
        """
        return f":{expr.parameter_name}"

    def _render_property(
        self, expr: QueryExpressionProperty, context_op: LogicalOperator
    ) -> str:
        """Render a property access using pre-resolved column references.

        The renderer is now "stupid and safe" - it uses pre-resolved column
        references from ColumnResolver and doesn't perform any semantic resolution.

        Args:
            expr: The property expression (e.g., p.name or p)
            context_op: The operator context

        Returns:
            SQL column reference (e.g., "_gsql2rsql_p_name")

        Raises:
            ValueError: If the reference was not resolved (indicates a bug in ColumnResolver)

        Trade-offs:
            - No guessing or schema lookups - uses pre-validated references
            - Fails fast if resolution is incomplete (better than silent bugs)
            - Simpler, more maintainable code
        """
        # Use resolution - guaranteed to be available after render_plan check
        resolved_ref = self._get_resolved_ref(
            expr.variable_name, expr.property_name, context_op
        )

        if resolved_ref is None:
            # This should never happen if ColumnResolver is working correctly
            prop_text = f"{expr.variable_name}.{expr.property_name}" if expr.property_name else expr.variable_name
            raise ValueError(
                f"Unresolved column reference: {prop_text} in operator {context_op.operator_debug_id}. "
                f"This indicates a bug in ColumnResolver - all references should be resolved before rendering."
            )

        return resolved_ref.full_sql_reference

    def _render_binary(
        self, expr: QueryExpressionBinary, context_op: LogicalOperator
    ) -> str:
        """Render a binary expression."""
        if not expr.operator or not expr.left_expression or not expr.right_expression:
            return "NULL"

        # Special handling for IN with parameter: use ARRAY_CONTAINS
        if (
            expr.operator.name == BinaryOperator.IN
            and isinstance(expr.right_expression, QueryExpressionParameter)
        ):
            left = self._render_expression(expr.left_expression, context_op)
            right = self._render_expression(expr.right_expression, context_op)
            return f"ARRAY_CONTAINS({right}, {left})"

        # Special handling for comparisons with duration when timestamps are involved
        # This is needed because UNIX_TIMESTAMP returns seconds (BIGINT),
        # while DURATION returns INTERVAL which can't be compared directly
        if expr.operator.name in (BinaryOperator.LT, BinaryOperator.GT,
                                   BinaryOperator.LEQ, BinaryOperator.GEQ):
            # Check if one side is a DURATION function
            duration_expr = None
            other_expr = None
            if self._is_duration_expression(expr.right_expression):
                duration_expr = expr.right_expression
                other_expr = expr.left_expression
            elif self._is_duration_expression(expr.left_expression):
                duration_expr = expr.left_expression
                other_expr = expr.right_expression

            if duration_expr and other_expr and self._contains_timestamp_subtraction(other_expr):
                # Render the duration as seconds for comparison with UNIX_TIMESTAMP diff
                if expr.left_expression is None or expr.right_expression is None:
                    raise TranspilerInternalErrorException(
                        "Binary expression has None operand"
                    )
                left = self._render_expression(expr.left_expression, context_op)
                right = self._render_expression(expr.right_expression, context_op)

                # Convert the INTERVAL to seconds
                if duration_expr == expr.right_expression and isinstance(
                    duration_expr, QueryExpressionFunction
                ):
                    right = self._duration_to_seconds(duration_expr)
                elif isinstance(duration_expr, QueryExpressionFunction):
                    left = self._duration_to_seconds(duration_expr)

                pattern = OPERATOR_PATTERNS.get(expr.operator.name, "({0}) ? ({1})")
                return pattern.format(left, right)

        left = self._render_expression(expr.left_expression, context_op)
        right = self._render_expression(expr.right_expression, context_op)

        # Special handling for timestamp subtraction in Spark SQL
        # Spark doesn't support direct timestamp - timestamp, need UNIX_TIMESTAMP
        # BUT: timestamp - DURATION should use direct subtraction (INTERVAL)
        if expr.operator.name == BinaryOperator.MINUS:
            # If one side is a DURATION expression, use direct subtraction
            # because DURATION renders as INTERVAL which works with direct subtraction
            if (self._is_duration_expression(expr.left_expression) or
                self._is_duration_expression(expr.right_expression)):
                # Direct subtraction: date - INTERVAL or INTERVAL - date
                pass  # Use default pattern below
            # Check if operands might be timestamps (heuristic: contains 'timestamp' in name)
            elif self._might_be_timestamp_subtraction(expr.left_expression, expr.right_expression):
                return f"(UNIX_TIMESTAMP({left}) - UNIX_TIMESTAMP({right}))"

        pattern = OPERATOR_PATTERNS.get(expr.operator.name, "({0}) ? ({1})")
        return pattern.format(left, right)

    def _is_duration_expression(self, expr: QueryExpression) -> bool:
        """Check if an expression is a DURATION function call."""
        if isinstance(expr, QueryExpressionFunction):
            return expr.function == Function.DURATION
        return False

    def _contains_timestamp_subtraction(self, expr: QueryExpression) -> bool:
        """Check if expression contains timestamp subtraction (recursively)."""
        if isinstance(expr, QueryExpressionBinary):
            left = expr.left_expression
            right = expr.right_expression
            if expr.operator and expr.operator.name == BinaryOperator.MINUS:
                if left and right and self._might_be_timestamp_subtraction(left, right):
                    return True
            # Check children
            left_has = left is not None and self._contains_timestamp_subtraction(left)
            right_has = right is not None and self._contains_timestamp_subtraction(right)
            return left_has or right_has
        elif isinstance(expr, QueryExpressionFunction):
            # Check if any parameter contains timestamp subtraction
            return any(self._contains_timestamp_subtraction(p) for p in expr.parameters)
        return False

    def _duration_to_seconds(self, expr: QueryExpressionFunction) -> str:
        """Convert a DURATION expression to seconds (as a numeric literal)."""
        if not expr.parameters:
            return "0"

        first_param = expr.parameters[0]
        if isinstance(first_param, QueryExpressionValue) and isinstance(first_param.value, str):
            return str(self._parse_iso8601_to_seconds(first_param.value))
        return "0"

    def _parse_iso8601_to_seconds(self, duration_str: str) -> int:
        """Parse ISO 8601 duration string to total seconds.

        Examples:
        - PT5M -> 300 (5 minutes = 300 seconds)
        - PT1H -> 3600 (1 hour = 3600 seconds)
        - P1D -> 86400 (1 day = 86400 seconds)
        """
        import re

        if not duration_str:
            return 0

        s = duration_str.strip().upper()
        if not s.startswith("P"):
            return 0
        s = s[1:]

        total_seconds = 0

        # Split by 'T' to separate date and time parts
        if "T" in s:
            date_part, time_part = s.split("T", 1)
        else:
            date_part = s
            time_part = ""

        # Parse date part
        date_pattern = re.compile(r"(\d+)([YMWD])")
        for match in date_pattern.finditer(date_part):
            value = int(match.group(1))
            unit = match.group(2)
            if unit == "Y":
                total_seconds += value * 365 * 24 * 3600
            elif unit == "M":
                total_seconds += value * 30 * 24 * 3600  # Approximate
            elif unit == "W":
                total_seconds += value * 7 * 24 * 3600
            elif unit == "D":
                total_seconds += value * 24 * 3600

        # Parse time part
        time_pattern = re.compile(r"(\d+(?:\.\d+)?)([HMS])")
        for match in time_pattern.finditer(time_part):
            time_value = float(match.group(1))
            time_unit = match.group(2)
            if time_unit == "H":
                total_seconds += int(time_value * 3600)
            elif time_unit == "M":
                total_seconds += int(time_value * 60)
            elif time_unit == "S":
                total_seconds += int(time_value)

        return total_seconds

    def _might_be_timestamp_subtraction(
        self, left: QueryExpression, right: QueryExpression
    ) -> bool:
        """Check if a subtraction might involve timestamps (heuristic-based)."""
        # Check if either operand is a property access with 'timestamp' in the name
        def is_timestamp_property(expr: QueryExpression) -> bool:
            if isinstance(expr, QueryExpressionProperty):
                prop = expr.property_name or ""
                return "timestamp" in prop.lower() or "date" in prop.lower()
            return False

        return is_timestamp_property(left) or is_timestamp_property(right)

    def _render_function(
        self, expr: QueryExpressionFunction, context_op: LogicalOperator
    ) -> str:
        """Render a function call (Databricks SQL syntax)."""
        params = [self._render_expression(p, context_op) for p in expr.parameters]

        func = expr.function
        if func == Function.NOT:
            return f"NOT ({params[0]})" if params else "NOT (NULL)"
        elif func == Function.NEGATIVE:
            return f"-({params[0]})" if params else "-NULL"
        elif func == Function.POSITIVE:
            return f"+({params[0]})" if params else "+NULL"
        elif func == Function.IS_NULL:
            return f"({params[0]}) IS NULL" if params else "NULL IS NULL"
        elif func == Function.IS_NOT_NULL:
            return f"({params[0]}) IS NOT NULL" if params else "NULL IS NOT NULL"
        elif func == Function.TO_STRING:
            return f"CAST({params[0]} AS STRING)" if params else "NULL"
        elif func == Function.TO_INTEGER:
            return f"CAST({params[0]} AS BIGINT)" if params else "NULL"
        elif func == Function.TO_FLOAT:
            return f"CAST({params[0]} AS DOUBLE)" if params else "NULL"
        elif func == Function.TO_BOOLEAN:
            return f"CAST({params[0]} AS BOOLEAN)" if params else "NULL"
        elif func == Function.STRING_TO_UPPER:
            return f"UPPER({params[0]})" if params else "NULL"
        elif func == Function.STRING_TO_LOWER:
            return f"LOWER({params[0]})" if params else "NULL"
        elif func == Function.STRING_TRIM:
            return f"TRIM({params[0]})" if params else "NULL"
        elif func == Function.STRING_LTRIM:
            return f"LTRIM({params[0]})" if params else "NULL"
        elif func == Function.STRING_RTRIM:
            return f"RTRIM({params[0]})" if params else "NULL"
        elif func == Function.STRING_SIZE:
            return f"LENGTH({params[0]})" if params else "NULL"
        elif func == Function.STRING_LEFT:
            return f"LEFT({params[0]}, {params[1]})" if len(params) >= 2 else "NULL"
        elif func == Function.STRING_RIGHT:
            return f"RIGHT({params[0]}, {params[1]})" if len(params) >= 2 else "NULL"
        elif func == Function.STRING_STARTS_WITH:
            if len(params) >= 2:
                return f"STARTSWITH({params[0]}, {params[1]})"
            return "NULL"
        elif func == Function.STRING_ENDS_WITH:
            if len(params) >= 2:
                return f"ENDSWITH({params[0]}, {params[1]})"
            return "NULL"
        elif func == Function.STRING_CONTAINS:
            if len(params) >= 2:
                return f"CONTAINS({params[0]}, {params[1]})"
            return "NULL"
        elif func == Function.COALESCE:
            if params:
                return f"COALESCE({', '.join(params)})"
            return "NULL"
        elif func == Function.RANGE:
            # Cypher: RANGE(start, end[, step]) -> Databricks: SEQUENCE(start, end[, step])
            # Note: Cypher RANGE is inclusive, Databricks SEQUENCE is inclusive
            if len(params) >= 2:
                return f"SEQUENCE({', '.join(params)})"
            return "ARRAY()"
        elif func == Function.SIZE:
            # SIZE works for both strings (LENGTH) and arrays (SIZE) in Databricks
            return f"SIZE({params[0]})" if params else "0"
        elif func == Function.LENGTH:
            # LENGTH(path) returns number of relationships (edges) in the path
            # In our CTE, path is an array of node IDs, so:
            #   - SIZE(path) = number of nodes
            #   - SIZE(path) - 1 = number of edges (hops)
            # Example: path A→B→C has nodes [A,B,C], SIZE=3, edges=2
            return f"(SIZE({params[0]}) - 1)" if params else "0"
        elif func == Function.NODES:
            # nodes(path) -> path (array of node IDs from recursive CTE)
            # The parameter should be a path variable reference
            # Use the rendered parameter to get the actual column name
            if params:
                # The parameter is the path variable's SQL column name
                # For example, if path renders to "_gsql2rsql_path_id", use that
                # If it renders to "path", use that
                return params[0]
            return "ARRAY()"
        elif func == Function.RELATIONSHIPS:
            # relationships(path) -> path_edges (array of edge structs from CTE)
            # The parameter should be a path variable reference
            # Derive the edges column name from the path column name
            if params:
                # The parameter is the path variable's SQL column name
                # Replace any "_id" or "_path" suffix with "_edges" to get the edges column
                path_col = params[0]
                # Handle different naming conventions:
                # - "path" -> "path_edges"
                # - "_gsql2rsql_path_id" -> "_gsql2rsql_path_edges"
                # - "_gsql2rsql_path" -> "_gsql2rsql_path_edges"
                if path_col.endswith("_id"):
                    # Replace "_id" with "_edges"
                    return path_col[:-3] + "_edges"
                elif "_path" in path_col and not path_col.endswith("_edges"):
                    # Add "_edges" suffix
                    return path_col + "_edges"
                else:
                    # Fallback: append "_edges"
                    return path_col + "_edges"
            return "ARRAY()"
        # Math functions - direct mapping to Databricks SQL
        elif func == Function.ABS:
            return f"ABS({params[0]})" if params else "NULL"
        elif func == Function.CEIL:
            return f"CEIL({params[0]})" if params else "NULL"
        elif func == Function.FLOOR:
            return f"FLOOR({params[0]})" if params else "NULL"
        elif func == Function.ROUND:
            if len(params) >= 2:
                return f"ROUND({params[0]}, {params[1]})"
            return f"ROUND({params[0]})" if params else "NULL"
        elif func == Function.SQRT:
            return f"SQRT({params[0]})" if params else "NULL"
        elif func == Function.SIGN:
            return f"SIGN({params[0]})" if params else "NULL"
        elif func == Function.LOG:
            # Cypher log() is natural log -> Databricks LN()
            return f"LN({params[0]})" if params else "NULL"
        elif func == Function.LOG10:
            return f"LOG10({params[0]})" if params else "NULL"
        elif func == Function.EXP:
            return f"EXP({params[0]})" if params else "NULL"
        elif func == Function.SIN:
            return f"SIN({params[0]})" if params else "NULL"
        elif func == Function.COS:
            return f"COS({params[0]})" if params else "NULL"
        elif func == Function.TAN:
            return f"TAN({params[0]})" if params else "NULL"
        elif func == Function.ASIN:
            return f"ASIN({params[0]})" if params else "NULL"
        elif func == Function.ACOS:
            return f"ACOS({params[0]})" if params else "NULL"
        elif func == Function.ATAN:
            return f"ATAN({params[0]})" if params else "NULL"
        elif func == Function.ATAN2:
            if len(params) >= 2:
                return f"ATAN2({params[0]}, {params[1]})"
            return "NULL"
        elif func == Function.DEGREES:
            return f"DEGREES({params[0]})" if params else "NULL"
        elif func == Function.RADIANS:
            return f"RADIANS({params[0]})" if params else "NULL"
        elif func == Function.RAND:
            return "RAND()"
        elif func == Function.PI:
            return "PI()"
        elif func == Function.E:
            return "E()"
        # Date/Time functions
        elif func == Function.DATE:
            # date() -> CURRENT_DATE()
            # date({year: y, month: m, day: d}) -> MAKE_DATE(y, m, d)
            if not params:
                return "CURRENT_DATE()"
            # If first param is a map literal, we need to handle it specially
            first_param = expr.parameters[0] if expr.parameters else None
            if isinstance(first_param, QueryExpressionMapLiteral):
                return self._render_date_from_map(first_param, context_op)
            # date(string) - parse a date string
            return f"TO_DATE({params[0]})"
        elif func == Function.DATETIME:
            # datetime() -> CURRENT_TIMESTAMP()
            # datetime({...}) -> construct from map
            if not params:
                return "CURRENT_TIMESTAMP()"
            first_param = expr.parameters[0] if expr.parameters else None
            if isinstance(first_param, QueryExpressionMapLiteral):
                return self._render_datetime_from_map(first_param, context_op)
            # datetime(string) - parse a timestamp string
            return f"TO_TIMESTAMP({params[0]})"
        elif func == Function.LOCALDATETIME:
            # localdatetime() -> CURRENT_TIMESTAMP()
            if not params:
                return "CURRENT_TIMESTAMP()"
            first_param = expr.parameters[0] if expr.parameters else None
            if isinstance(first_param, QueryExpressionMapLiteral):
                return self._render_datetime_from_map(first_param, context_op)
            return f"TO_TIMESTAMP({params[0]})"
        elif func == Function.TIME:
            # time() -> DATE_FORMAT(CURRENT_TIMESTAMP(), 'HH:mm:ss')
            if not params:
                return "DATE_FORMAT(CURRENT_TIMESTAMP(), 'HH:mm:ss')"
            first_param = expr.parameters[0] if expr.parameters else None
            if isinstance(first_param, QueryExpressionMapLiteral):
                return self._render_time_from_map(first_param, context_op)
            return f"DATE_FORMAT(TO_TIMESTAMP({params[0]}), 'HH:mm:ss')"
        elif func == Function.LOCALTIME:
            # localtime() -> DATE_FORMAT(CURRENT_TIMESTAMP(), 'HH:mm:ss')
            if not params:
                return "DATE_FORMAT(CURRENT_TIMESTAMP(), 'HH:mm:ss')"
            return f"DATE_FORMAT(TO_TIMESTAMP({params[0]}), 'HH:mm:ss')"
        elif func == Function.DURATION:
            # duration({days: d, hours: h, ...}) -> INTERVAL 'd' DAY + INTERVAL 'h' HOUR + ...
            # duration('P7D') -> INTERVAL 7 DAY (ISO 8601 format)
            first_param = expr.parameters[0] if expr.parameters else None
            if isinstance(first_param, QueryExpressionMapLiteral):
                return self._render_duration_from_map(first_param, context_op)
            if isinstance(first_param, QueryExpressionValue) and isinstance(
                first_param.value, str
            ):
                return self._parse_iso8601_duration(first_param.value)
            # Fallback for other expression types (render and hope it's a duration string)
            if params:
                # Try to extract string from rendered param (remove quotes if present)
                rendered = params[0]
                if rendered.startswith("'") and rendered.endswith("'"):
                    return self._parse_iso8601_duration(rendered[1:-1])
            return "INTERVAL '0' DAY"
        elif func == Function.DURATION_BETWEEN:
            # duration.between(d1, d2) -> DATEDIFF(d2, d1)
            if len(params) >= 2:
                return f"DATEDIFF({params[1]}, {params[0]})"
            return "0"
        # Date component extraction
        elif func == Function.DATE_YEAR:
            return f"YEAR({params[0]})" if params else "NULL"
        elif func == Function.DATE_MONTH:
            return f"MONTH({params[0]})" if params else "NULL"
        elif func == Function.DATE_DAY:
            return f"DAY({params[0]})" if params else "NULL"
        elif func == Function.DATE_HOUR:
            return f"HOUR({params[0]})" if params else "NULL"
        elif func == Function.DATE_MINUTE:
            return f"MINUTE({params[0]})" if params else "NULL"
        elif func == Function.DATE_SECOND:
            return f"SECOND({params[0]})" if params else "NULL"
        elif func == Function.DATE_WEEK:
            return f"WEEKOFYEAR({params[0]})" if params else "NULL"
        elif func == Function.DATE_DAYOFWEEK:
            return f"DAYOFWEEK({params[0]})" if params else "NULL"
        elif func == Function.DATE_QUARTER:
            return f"QUARTER({params[0]})" if params else "NULL"
        elif func == Function.DATE_TRUNCATE:
            # date.truncate('unit', d) -> DATE_TRUNC(unit, d)
            if len(params) >= 2:
                return f"DATE_TRUNC({params[0]}, {params[1]})"
            return "NULL"
        else:
            # Unknown function - pass through with original name
            params_str = ", ".join(params)
            return f"{func.name}({params_str})"

    def _render_aggregation(
        self, expr: QueryExpressionAggregationFunction, context_op: LogicalOperator
    ) -> str:
        """Render an aggregation function.

        Supports ordered aggregation for COLLECT:
        COLLECT(x ORDER BY y DESC) ->
            TRANSFORM(
                ARRAY_SORT(
                    COLLECT_LIST(STRUCT(_sort_key, _value)),
                    (a, b) -> CASE WHEN a._sort_key > b._sort_key THEN -1 ELSE 1 END
                ),
                s -> s._value
            )
        """
        # Handle ordered COLLECT specially
        if (
            expr.order_by
            and expr.aggregation_function == AggregationFunction.COLLECT
            and expr.inner_expression
        ):
            return self._render_ordered_collect(expr, context_op)

        inner = (
            self._render_expression(expr.inner_expression, context_op)
            if expr.inner_expression
            else "*"
        )

        if expr.is_distinct:
            inner = f"DISTINCT {inner}"

        pattern = AGGREGATION_PATTERNS.get(expr.aggregation_function, "{0}")
        return pattern.format(inner)

    def _render_ordered_collect(
        self, expr: QueryExpressionAggregationFunction, context_op: LogicalOperator
    ) -> str:
        """Render an ordered COLLECT using ARRAY_SORT.

        COLLECT(x ORDER BY y DESC) becomes:
        TRANSFORM(
            ARRAY_SORT(
                COLLECT_LIST(STRUCT(y AS _sort_key, x AS _value)),
                (a, b) -> CASE WHEN a._sort_key > b._sort_key THEN -1 ELSE 1 END
            ),
            s -> s._value
        )
        """
        if not expr.inner_expression:
            return "COLLECT_LIST(NULL)"
        value_sql = self._render_expression(expr.inner_expression, context_op)

        # Build STRUCT with sort keys and value
        struct_parts = []
        sort_comparisons = []

        for i, (sort_expr, is_desc) in enumerate(expr.order_by):
            sort_key_sql = self._render_expression(sort_expr, context_op)
            key_name = f"_sk{i}"
            struct_parts.append(f"{sort_key_sql} AS {key_name}")

            # Build comparison for this sort key
            if is_desc:
                # DESC: a > b -> -1 (a comes first)
                sort_comparisons.append(
                    f"CASE WHEN a.{key_name} > b.{key_name} THEN -1 "
                    f"WHEN a.{key_name} < b.{key_name} THEN 1 ELSE 0 END"
                )
            else:
                # ASC: a < b -> -1 (a comes first)
                sort_comparisons.append(
                    f"CASE WHEN a.{key_name} < b.{key_name} THEN -1 "
                    f"WHEN a.{key_name} > b.{key_name} THEN 1 ELSE 0 END"
                )

        struct_parts.append(f"{value_sql} AS _value")
        struct_sql = f"STRUCT({', '.join(struct_parts)})"

        # Combine sort comparisons with COALESCE-like logic
        # For multiple sort keys, check each in order
        if len(sort_comparisons) == 1:
            comparator = sort_comparisons[0]
        else:
            # Chain comparisons: if first is 0, use second, etc.
            comparator = sort_comparisons[-1]
            for comp in reversed(sort_comparisons[:-1]):
                comparator = f"CASE WHEN ({comp}) = 0 THEN ({comparator}) ELSE ({comp}) END"

        return (
            f"TRANSFORM("
            f"ARRAY_SORT("
            f"COLLECT_LIST({struct_sql}), "
            f"(a, b) -> {comparator}"
            f"), "
            f"s -> s._value"
            f")"
        )

    def _render_list(
        self, expr: QueryExpressionList, context_op: LogicalOperator
    ) -> str:
        """Render a list expression."""
        items = [self._render_expression(item, context_op) for item in expr.items]
        return f"({', '.join(items)})"

    def _render_list_predicate(
        self, expr: QueryExpressionListPredicate, context_op: LogicalOperator
    ) -> str:
        """Render a list predicate expression (ALL/ANY/NONE/SINGLE).

        Databricks SQL translation (optimized for 17.x):

        OPTIMIZATION 1: Simple equality checks use ARRAY_CONTAINS (O(1) with bloom filter)
        - ANY(x IN list WHERE x = val) -> ARRAY_CONTAINS(list, val)
        - NONE(x IN list WHERE x = val) -> NOT ARRAY_CONTAINS(list, val)

        OPTIMIZATION 2: Use EXISTS/FORALL HOFs when available (Databricks 17.x)
        - ANY(x IN list WHERE cond) -> EXISTS(list, x -> cond)
        - ALL(x IN list WHERE cond) -> FORALL(list, x -> cond)

        FALLBACK: Complex predicates use FILTER + SIZE
        - ALL(x IN list WHERE cond) -> SIZE(FILTER(list, x -> NOT (cond))) = 0
        - ANY(x IN list WHERE cond) -> SIZE(FILTER(list, x -> cond)) > 0
        - NONE(x IN list WHERE cond) -> SIZE(FILTER(list, x -> cond)) = 0
        - SINGLE(x IN list WHERE cond) -> SIZE(FILTER(list, x -> cond)) = 1
        """
        var_name = expr.variable_name
        list_sql = self._render_expression(expr.list_expression, context_op)

        # Check for simple equality optimization: ANY(x IN list WHERE x = value)
        equality_value = self._extract_equality_value(
            expr.filter_expression, var_name, context_op
        )

        if equality_value is not None:
            # Use ARRAY_CONTAINS optimization (O(1) with bloom filter in Databricks)
            if expr.predicate_type == ListPredicateType.ANY:
                return f"ARRAY_CONTAINS({list_sql}, {equality_value})"
            elif expr.predicate_type == ListPredicateType.NONE:
                return f"NOT ARRAY_CONTAINS({list_sql}, {equality_value})"
            # For ALL/SINGLE with equality, fall through to standard approach

        # Build the lambda body for complex predicates
        if expr.filter_expression:
            filter_sql = self._render_list_predicate_filter(
                expr.filter_expression, var_name, context_op
            )
        else:
            filter_sql = "TRUE"

        # Use EXISTS/FORALL HOFs for ANY/ALL (Databricks 17.x optimization)
        if expr.predicate_type == ListPredicateType.ALL:
            if expr.filter_expression:
                # FORALL is cleaner but equivalent to SIZE(FILTER(NOT cond)) = 0
                return f"FORALL({list_sql}, {var_name} -> {filter_sql})"
            else:
                # ALL(x IN list) without filter - check all not null
                return f"FORALL({list_sql}, {var_name} -> {var_name} IS NOT NULL)"
        elif expr.predicate_type == ListPredicateType.ANY:
            # EXISTS is cleaner and may short-circuit
            return f"EXISTS({list_sql}, {var_name} -> {filter_sql})"
        elif expr.predicate_type == ListPredicateType.NONE:
            # NONE = NOT EXISTS
            return f"NOT EXISTS({list_sql}, {var_name} -> {filter_sql})"
        elif expr.predicate_type == ListPredicateType.SINGLE:
            # SINGLE: Exactly one element matches (no HOF equivalent)
            return f"SIZE(FILTER({list_sql}, {var_name} -> {filter_sql})) = 1"
        else:
            return f"EXISTS({list_sql}, {var_name} -> {filter_sql})"

    def _extract_equality_value(
        self,
        expr: QueryExpression | None,
        var_name: str,
        context_op: LogicalOperator,
    ) -> str | None:
        """Extract value from simple equality expression like 'x = value'.

        Returns the rendered value SQL if expr is a simple equality where one side
        is just the variable (var_name) and the other is a constant/expression.
        Returns None if not a simple equality pattern.

        This enables ARRAY_CONTAINS optimization which is O(1) with bloom filter.
        """
        if expr is None:
            return None

        if not isinstance(expr, QueryExpressionBinary):
            return None

        # Check if it's an equality operator
        if expr.operator is None or expr.operator.name != BinaryOperator.EQ:
            return None

        left = expr.left_expression
        right = expr.right_expression

        # Check if left is just the variable and right is the value
        if (
            isinstance(left, QueryExpressionProperty)
            and left.variable_name == var_name
            and not left.property_name
            and right is not None
        ):
            # Pattern: x = value
            return self._render_expression(right, context_op)

        # Check if right is just the variable and left is the value
        if (
            isinstance(right, QueryExpressionProperty)
            and right.variable_name == var_name
            and not right.property_name
            and left is not None
        ):
            # Pattern: value = x
            return self._render_expression(left, context_op)

        return None

    def _render_list_predicate_filter(
        self, expr: QueryExpression, var_name: str, context_op: LogicalOperator
    ) -> str:
        """Render a filter expression for list predicate, handling variable references.

        In list predicates like ALL(x IN list WHERE x > 0), references to 'x'
        should be rendered as just 'x' (the lambda parameter), not as a
        field lookup in the context operator's schema.
        """
        if isinstance(expr, QueryExpressionProperty):
            # If it's just the variable (no property), return it as-is
            if expr.variable_name == var_name and not expr.property_name:
                return var_name
            # If it has a property, it's accessing a field on the lambda variable
            if expr.variable_name == var_name and expr.property_name:
                return f"{var_name}.{expr.property_name}"
            # Otherwise it's a reference to an outer variable
            return self._render_property(expr, context_op)
        elif isinstance(expr, QueryExpressionBinary):
            left_expr = expr.left_expression
            right_expr = expr.right_expression
            if left_expr is None or right_expr is None:
                return "NULL"
            left = self._render_list_predicate_filter(left_expr, var_name, context_op)
            right = self._render_list_predicate_filter(right_expr, var_name, context_op)
            from gsql2rsql.renderer.sql_renderer import OPERATOR_PATTERNS
            if expr.operator is None:
                return f"({left}) ? ({right})"
            pattern = OPERATOR_PATTERNS.get(expr.operator.name, "({0}) ? ({1})")
            return pattern.format(left, right)
        elif isinstance(expr, QueryExpressionFunction):
            params = [
                self._render_list_predicate_filter(p, var_name, context_op)
                for p in expr.parameters
            ]
            # Re-use the existing function rendering logic
            from gsql2rsql.parser.operators import Function
            func = expr.function
            if func == Function.NOT:
                return f"NOT ({params[0]})" if params else "NOT (NULL)"
            elif func == Function.IS_NULL:
                return f"({params[0]}) IS NULL" if params else "NULL IS NULL"
            elif func == Function.IS_NOT_NULL:
                return f"({params[0]}) IS NOT NULL" if params else "NULL IS NOT NULL"
            # Add other functions as needed
            return self._render_function(expr, context_op)
        else:
            # For other expression types, use default rendering
            return self._render_expression(expr, context_op)

    def _render_case(
        self, expr: QueryExpressionCaseExpression, context_op: LogicalOperator
    ) -> str:
        """Render a CASE expression."""
        parts = ["CASE"]

        if expr.test_expression:
            parts.append(self._render_expression(expr.test_expression, context_op))

        for when_expr, then_expr in expr.alternatives:
            when_rendered = self._render_expression(when_expr, context_op)
            then_rendered = self._render_expression(then_expr, context_op)
            parts.append(f"WHEN {when_rendered} THEN {then_rendered}")

        if expr.else_expression:
            else_rendered = self._render_expression(expr.else_expression, context_op)
            parts.append(f"ELSE {else_rendered}")

        parts.append("END")
        return " ".join(parts)

    def _render_list_comprehension(
        self, expr: QueryExpressionListComprehension, context_op: LogicalOperator
    ) -> str:
        """Render a list comprehension expression.

        Cypher: [x IN list WHERE predicate | expression]
        Databricks SQL:
        - FILTER(list, x -> predicate) for WHERE only
        - TRANSFORM(list, x -> expression) for map only
        - TRANSFORM(FILTER(list, x -> predicate), x -> expression) for both

        Examples:
        - [x IN arr WHERE x > 0] -> FILTER(arr, x -> x > 0)
        - [x IN arr | x * 2] -> TRANSFORM(arr, x -> x * 2)
        - [x IN arr WHERE x > 0 | x * 2] -> TRANSFORM(FILTER(arr, x -> x > 0), x -> x * 2)

        OPTIMIZATION: Path Node ID Extraction
        -------------------------------------
        [node IN nodes(path) | node.id] is a common pattern that should NOT
        generate TRANSFORM because:
        - nodes(path) returns `path` which is already an array of node IDs
        - Applying `node.id` to an integer ID doesn't make sense

        This optimization detects this pattern and returns `path` directly:
        - [node IN nodes(path) | node.id] -> path (not TRANSFORM(path, node -> node.id))
        """
        var = expr.variable_name
        list_sql = self._render_expression(expr.list_expression, context_op)

        # =====================================================================
        # OPTIMIZATION: Detect [node IN nodes(path) | node.id] pattern
        # =====================================================================
        # When iterating over nodes(path) and extracting .id, the TRANSFORM
        # is redundant because `path` already contains node IDs.
        #
        # Pattern to detect:
        # - list_expression is Function.NODES(path_var)
        # - map_expression is var.id (where var matches the comprehension variable)
        # - no filter_expression
        #
        # Result: Return path directly instead of TRANSFORM(path, node -> node.id)
        # =====================================================================
        if self._is_nodes_id_extraction_pattern(expr):
            # Return the path array directly - it already contains node IDs
            return list_sql

        # Start with the list
        result = list_sql

        # Apply FILTER if there's a WHERE predicate
        if expr.filter_expression:
            filter_sql = self._render_list_predicate_filter(
                expr.filter_expression, var, context_op
            )
            result = f"FILTER({result}, {var} -> {filter_sql})"

        # Apply TRANSFORM if there's a map expression
        if expr.map_expression:
            map_sql = self._render_list_predicate_filter(
                expr.map_expression, var, context_op
            )
            result = f"TRANSFORM({result}, {var} -> {map_sql})"

        return result

    def _is_nodes_id_extraction_pattern(
        self, expr: QueryExpressionListComprehension
    ) -> bool:
        """Check if this is a [node IN nodes(path) | node.id] pattern.

        This pattern should NOT generate TRANSFORM because:
        - nodes(path) returns `path` which already contains node IDs
        - Applying `node.id` to integers doesn't make sense

        Returns:
            True if this is the nodes ID extraction pattern that should
            be optimized to just return `path` directly.
        """
        # Must have a map expression and no filter
        if not expr.map_expression or expr.filter_expression:
            return False

        # Check if list_expression is nodes(path_var)
        if not isinstance(expr.list_expression, QueryExpressionFunction):
            return False
        if expr.list_expression.function != Function.NODES:
            return False

        # Check if map_expression is var.id (extracting id from the variable)
        if not isinstance(expr.map_expression, QueryExpressionProperty):
            return False
        map_expr = expr.map_expression
        if map_expr.variable_name != expr.variable_name:
            return False
        if map_expr.property_name != "id":
            return False

        return True

    def _render_reduce(
        self, expr: QueryExpressionReduce, context_op: LogicalOperator
    ) -> str:
        """Render a REDUCE expression.

        Cypher: REDUCE(acc = initial, x IN list | acc_expr)
        Databricks SQL: AGGREGATE(list, initial, (acc, x) -> acc_expr)

        Example:
        - REDUCE(total = 0, x IN amounts | total + x)
          -> AGGREGATE(amounts, 0, (total, x) -> total + x)
        """
        acc_name = expr.accumulator_name
        var_name = expr.variable_name
        list_sql = self._render_expression(expr.list_expression, context_op)
        initial_sql = self._render_expression(expr.initial_value, context_op)

        # Cast numeric initial value to DOUBLE if the reducer involves field access
        # This avoids type mismatch when the reducer returns DOUBLE (e.g., amount fields)
        if self._reducer_might_return_double(expr.reducer_expression, var_name):
            if isinstance(expr.initial_value, QueryExpressionValue):
                if isinstance(expr.initial_value.value, (int, float)):
                    initial_sql = f"CAST({initial_sql} AS DOUBLE)"

        # Render the reducer expression, replacing accumulator and variable references
        reducer_sql = self._render_reduce_expression(
            expr.reducer_expression, acc_name, var_name, context_op
        )

        return f"AGGREGATE({list_sql}, {initial_sql}, ({acc_name}, {var_name}) -> {reducer_sql})"

    def _reducer_might_return_double(
        self, expr: QueryExpression, var_name: str
    ) -> bool:
        """Check if a reducer expression might return DOUBLE (heuristic).

        Returns True if the expression accesses properties of the variable,
        which might be numeric fields like 'amount', 'balance', etc.
        """
        if isinstance(expr, QueryExpressionProperty):
            # If it's a property access on the variable, might be a numeric field
            if expr.variable_name == var_name and expr.property_name:
                return True
        elif isinstance(expr, QueryExpressionBinary):
            # Check both sides
            left_check = (
                expr.left_expression is not None
                and self._reducer_might_return_double(expr.left_expression, var_name)
            )
            right_check = (
                expr.right_expression is not None
                and self._reducer_might_return_double(expr.right_expression, var_name)
            )
            return left_check or right_check
        elif isinstance(expr, QueryExpressionFunction):
            # Check parameters
            return any(
                self._reducer_might_return_double(p, var_name)
                for p in expr.parameters
            )
        return False

    def _render_reduce_expression(
        self,
        expr: QueryExpression,
        acc_name: str,
        var_name: str,
        context_op: LogicalOperator,
    ) -> str:
        """Render an expression within a REDUCE, handling accumulator and variable references."""
        if isinstance(expr, QueryExpressionProperty):
            # Check if it's the accumulator or variable
            if expr.property_name is None:
                if expr.variable_name == acc_name:
                    return acc_name
                elif expr.variable_name == var_name:
                    return var_name
            elif expr.variable_name == var_name:
                # Property access on the lambda variable (e.g., rel.amount)
                # For edge structs in path_edges, use struct field access
                return f"{var_name}.{expr.property_name}"
            elif expr.variable_name == acc_name:
                # Property access on accumulator (if it's a struct)
                return f"{acc_name}.{expr.property_name}"
            return self._render_expression(expr, context_op)
        elif isinstance(expr, QueryExpressionBinary):
            if not expr.left_expression or not expr.right_expression or not expr.operator:
                return "NULL"
            left = self._render_reduce_expression(
                expr.left_expression, acc_name, var_name, context_op
            )
            right = self._render_reduce_expression(
                expr.right_expression, acc_name, var_name, context_op
            )
            pattern = OPERATOR_PATTERNS.get(expr.operator.name, "({0}) ? ({1})")
            return pattern.format(left, right)
        elif isinstance(expr, QueryExpressionFunction):
            params = [
                self._render_reduce_expression(p, acc_name, var_name, context_op)
                for p in expr.parameters
            ]
            # Use the same function rendering logic
            return self._render_function_with_params(expr.function, params)
        else:
            return self._render_expression(expr, context_op)

    def _render_function_with_params(self, func: Function, params: list[str]) -> str:
        """Render a function with pre-rendered parameters."""
        if func == Function.NOT:
            return f"NOT ({params[0]})" if params else "NOT (NULL)"
        elif func == Function.NEGATIVE:
            return f"-({params[0]})" if params else "-NULL"
        elif func == Function.POSITIVE:
            return f"+({params[0]})" if params else "+NULL"
        elif func == Function.IS_NULL:
            return f"({params[0]}) IS NULL" if params else "NULL IS NULL"
        elif func == Function.IS_NOT_NULL:
            return f"({params[0]}) IS NOT NULL" if params else "NULL IS NOT NULL"
        elif func == Function.COALESCE:
            if params:
                return f"COALESCE({', '.join(params)})"
            return "NULL"
        elif func == Function.ABS:
            return f"ABS({params[0]})" if params else "NULL"
        elif func == Function.SQRT:
            return f"SQRT({params[0]})" if params else "NULL"
        elif func == Function.CEIL:
            return f"CEIL({params[0]})" if params else "NULL"
        elif func == Function.FLOOR:
            return f"FLOOR({params[0]})" if params else "NULL"
        elif func == Function.ROUND:
            if len(params) >= 2:
                return f"ROUND({params[0]}, {params[1]})"
            return f"ROUND({params[0]})" if params else "NULL"
        # Default: return function call syntax
        return f"{func.name}({', '.join(params)})"

    def _render_exists(
        self, expr: QueryExpressionExists, context_op: LogicalOperator
    ) -> str:
        """Render an EXISTS subquery expression.

        Generates a correlated subquery for EXISTS patterns:
        EXISTS { (p)-[:ACTED_IN]->(:Movie) }
        becomes:
        EXISTS (
          SELECT 1
          FROM graph.ActedIn r
          JOIN graph.Movie m ON r.target_id = m.id
          WHERE r.source_id = outer_query.p_id
        )
        """
        prefix = "NOT " if expr.is_negated else ""

        # If it's a full subquery EXISTS, we would need to render the full query
        # For now, focus on pattern-based EXISTS
        if expr.subquery:
            # Full subquery EXISTS - not yet fully implemented
            return f"{prefix}EXISTS (SELECT 1)"

        if not expr.pattern_entities:
            return f"{prefix}EXISTS (SELECT 1)"

        # Parse the pattern entities to extract:
        # - source node (correlated to outer query)
        # - relationship
        # - target node
        source_node: NodeEntity | None = None
        relationship: RelationshipEntity | None = None
        target_node: NodeEntity | None = None

        for entity in expr.pattern_entities:
            if isinstance(entity, NodeEntity):
                if source_node is None:
                    source_node = entity
                else:
                    target_node = entity
            elif isinstance(entity, RelationshipEntity):
                relationship = entity

        if not relationship or not source_node:
            # Can't render without relationship and source
            return f"{prefix}EXISTS (SELECT 1)"

        # Get relationship table info
        rel_name = relationship.entity_name
        source_entity_name = source_node.entity_name or ""
        target_entity_name = target_node.entity_name if target_node else ""

        # Try to find the edge schema
        edge_schema: EdgeSchema | None = None
        rel_table_desc: SQLTableDescriptor | None = None

        # If we have both source and target entity names, use the direct lookup
        if source_entity_name and target_entity_name:
            # Compute edge_id for lookup
            # Use pre-resolved direction context from planner (SoC compliant)
            # correlation_uses_source=False indicates BACKWARD direction
            if not expr.correlation_uses_source:
                edge_id = EdgeSchema.get_edge_id(
                    rel_name, target_entity_name, source_entity_name
                )
            else:
                edge_id = EdgeSchema.get_edge_id(
                    rel_name, source_entity_name, target_entity_name
                )
            rel_table_desc = self._db_schema.get_sql_table_descriptors(edge_id)
            if not expr.correlation_uses_source:
                edge_schema = self._db_schema.get_edge_definition(
                    rel_name, target_entity_name, source_entity_name
                )
            else:
                edge_schema = self._db_schema.get_edge_definition(
                    rel_name, source_entity_name, target_entity_name
                )
        else:
            # Source entity unknown - use fallback lookup by verb
            # This handles EXISTS patterns where source is a variable reference
            result = self._db_schema.find_edge_by_verb(rel_name, target_entity_name)
            if result:
                edge_schema, rel_table_desc = result
                # Update source_entity_name from schema for later use
                source_entity_name = edge_schema.source_node_id

        if not rel_table_desc:
            return f"{prefix}EXISTS (SELECT 1 /* unknown relationship: {rel_name} */)"

        rel_table = rel_table_desc.full_table_name
        source_id_col = "source_id"
        target_id_col = "target_id"
        if edge_schema:
            if edge_schema.source_id_property:
                source_id_col = edge_schema.source_id_property.property_name
            if edge_schema.sink_id_property:
                target_id_col = edge_schema.sink_id_property.property_name

        # Build subquery parts
        lines = []
        lines.append("SELECT 1")
        lines.append(f"FROM {rel_table} _exists_rel")

        # If target node exists and has a type, join to target table
        if target_node and target_node.entity_name:
            target_table_desc = self._db_schema.get_sql_table_descriptors(
                target_node.entity_name
            )
            if target_table_desc:
                target_table = target_table_desc.full_table_name
                # Get target node's ID column
                target_node_schema = self._db_schema.get_node_definition(
                    target_node.entity_name
                )
                target_node_id_col = "id"
                if target_node_schema and target_node_schema.node_id_property:
                    target_node_id_col = target_node_schema.node_id_property.property_name

                # Join direction uses pre-resolved context from planner (SoC compliant)
                # target_join_uses_sink=True means join target on sink column
                if expr.target_join_uses_sink:
                    edge_join_col = target_id_col  # sink column
                else:
                    edge_join_col = source_id_col  # source column (BACKWARD)
                lines.append(
                    f"JOIN {target_table} _exists_target "
                    f"ON _exists_rel.{edge_join_col} = _exists_target.{target_node_id_col}"
                )

        # Correlate with outer query using source node's ID
        # The source node's ID field should be in the outer context
        source_alias = source_node.alias or "_src"
        # Get the source node's ID column from schema
        source_node_schema = self._db_schema.get_node_definition(
            source_node.entity_name
        ) if source_node.entity_name else None
        source_node_id_col = "id"
        if source_node_schema and source_node_schema.node_id_property:
            source_node_id_col = source_node_schema.node_id_property.property_name

        outer_field = f"{self.COLUMN_PREFIX}{source_alias}_{source_node_id_col}"

        # WHERE clause: correlate with outer query
        # Use pre-resolved context from planner (SoC compliant)
        # correlation_uses_source=True means use source column for correlation
        if expr.correlation_uses_source:
            correlation = f"_exists_rel.{source_id_col} = {outer_field}"
        else:
            correlation = f"_exists_rel.{target_id_col} = {outer_field}"

        where_parts = [correlation]

        # Add any additional WHERE from the EXISTS pattern
        if expr.where_expression:
            # Render the where expression in context of the exists subquery
            # This is tricky - we need to map variables properly
            # For now, just render it directly
            where_rendered = self._render_expression(expr.where_expression, context_op)
            where_parts.append(where_rendered)

        lines.append("WHERE " + " AND ".join(where_parts))

        subquery = " ".join(lines)
        return f"{prefix}EXISTS ({subquery})"

    def _has_aggregation(self, expr: QueryExpression) -> bool:
        """Check if an expression contains an aggregation function."""
        if isinstance(expr, QueryExpressionAggregationFunction):
            return True
        if isinstance(expr, QueryExpressionBinary):
            left_has = (
                self._has_aggregation(expr.left_expression)
                if expr.left_expression
                else False
            )
            right_has = (
                self._has_aggregation(expr.right_expression)
                if expr.right_expression
                else False
            )
            return left_has or right_has
        if isinstance(expr, QueryExpressionFunction):
            return any(self._has_aggregation(p) for p in expr.parameters)
        return False

    def _references_aliases(self, expr: QueryExpression, aliases: set[str]) -> bool:
        """Check if an expression references any of the given aliases.

        This is used to detect expressions like `shared_cards + shared_merchants`
        where `shared_merchants` is an aggregate alias. Such expressions should
        not be included in GROUP BY because they reference aggregates.
        """
        if isinstance(expr, QueryExpressionProperty):
            # Bare variable reference (e.g., shared_merchants)
            if expr.variable_name and not expr.property_name:
                return expr.variable_name in aliases
            return False
        if isinstance(expr, QueryExpressionBinary):
            left_refs = (
                self._references_aliases(expr.left_expression, aliases)
                if expr.left_expression
                else False
            )
            right_refs = (
                self._references_aliases(expr.right_expression, aliases)
                if expr.right_expression
                else False
            )
            return left_refs or right_refs
        if isinstance(expr, QueryExpressionFunction):
            return any(self._references_aliases(p, aliases) for p in expr.parameters)
        if isinstance(expr, QueryExpressionAggregationFunction):
            # Aggregation functions don't reference aliases in the same sense
            return False
        return False

    def _get_field_name(self, prefix: str, field_name: str) -> str:
        """Generate a field name with entity prefix."""
        clean_prefix = "".join(
            c if c.isalnum() or c == "_" else "" for c in prefix
        )
        return f"{self.COLUMN_PREFIX}{clean_prefix}_{field_name}"

    def _get_entity_properties_for_aggregation(
        self, op: ProjectionOperator
    ) -> list[str]:
        """Get required entity property columns that need to be projected through GROUP BY.

        When a node variable (e.g., 'c') is projected through a GROUP BY, only its
        ID column (_gsql2rsql_c_id) is normally included. But if downstream operators need
        other properties (e.g., c.name -> _gsql2rsql_c_name), those must also be projected.

        This method identifies entity variables in the projections and returns
        any required property columns for those entities that aren't already projected.

        Args:
            op: The ProjectionOperator with aggregations.

        Returns:
            List of column aliases (e.g., ['_gsql2rsql_c_name']) to add to the SELECT list.
        """
        extra_columns: list[str] = []
        already_projected: set[str] = set()

        # Find entity variables in the projections (bare entity references)
        # and also track which entity ID columns need to be preserved
        entity_vars: set[str] = set()
        entity_id_columns: dict[str, str] = {}  # entity_var -> id_column_alias

        # Collect all columns that are already projected
        for _, expr in op.projections:
            if isinstance(expr, QueryExpressionProperty):
                if expr.property_name:
                    # Property access - already projected as _gsql2rsql_entity_prop
                    col = self._get_field_name(expr.variable_name, expr.property_name)
                    already_projected.add(col)
                else:
                    # Bare entity reference - the ID is projected under a different alias
                    entity_vars.add(expr.variable_name)
                    # Use resolution to get the ID column
                    resolved_ref = self._get_resolved_ref(expr.variable_name, None, op)
                    if resolved_ref:
                        id_col = resolved_ref.sql_column_name
                        entity_id_columns[expr.variable_name] = id_col
                        # Mark ID as already projected to avoid duplicating it in extra_columns
                        # The ID is already in the main projection as "_gsql2rsql_p_id AS p"
                        already_projected.add(id_col)

        # For bare entity references, we generally DON'T need to add their ID column
        # because the bare entity reference already provides the ID value.
        # For example, if we project `a` (rendering as `_gsql2rsql_a_id AS a`), downstream
        # code accessing `a.id` should use the alias `a`, not `_gsql2rsql_a_id`.
        #
        # We only add the ID column separately if there are OTHER properties
        # of that entity that need to be projected (e.g., _gsql2rsql_a_name for a.name).
        # In that case, we need _gsql2rsql_a_id as a grouping key alongside _gsql2rsql_a_name.
        for entity_var, id_col in entity_id_columns.items():
            if id_col not in already_projected:
                # Only add if we're adding OTHER properties for this entity
                # (not just the ID column itself)
                has_other_properties = any(
                    req_col.startswith(f"{self.COLUMN_PREFIX}{entity_var}_") and req_col != id_col
                    for req_col in self._required_columns
                )
                if has_other_properties:
                    extra_columns.append(id_col)
                    already_projected.add(id_col)

        # Check required columns for properties of these entities
        for required_col in self._required_columns:
            if required_col in already_projected:
                continue
            # Check if this column belongs to one of our entity variables
            # Pattern: _gsql2rsql_{entity}_{property}
            for entity_var in entity_vars:
                prefix = f"{self.COLUMN_PREFIX}{entity_var}_"
                if required_col.startswith(prefix):
                    # Skip the entity's ID column - it's already provided by the bare entity
                    # reference (e.g., _gsql2rsql_a_id is already available via the alias 'a')
                    entity_id_col = entity_id_columns.get(entity_var)
                    if entity_id_col is not None and required_col == entity_id_col:
                        break

                    # ============================================================================
                    # TODO (NON-CRITICAL): DEFENSIVE WORKAROUND FOR COLUMN RESOLVER BUG
                    # ============================================================================
                    # This is a SYMPTOM FIX, not a root cause fix. This defensive check prevents
                    # SQL generation errors but doesn't address the underlying issue.
                    #
                    # WHY WORKAROUND INSTEAD OF ROOT CAUSE FIX:
                    # -----------------------------------------
                    # 1. Root cause is in column_resolver.py (Phase 4), requires deep refactoring
                    # 2. Bug is rare: only when variable name == role name (e.g., "sink" as sink node)
                    # 3. Risk/benefit: High complexity fix for edge case vs. simple defensive check
                    # 4. This workaround has been battle-tested: 17/17 PySpark + 662/662 tests pass
                    #
                    # BUG PATTERN DETECTED:
                    # ---------------------
                    # Malformed column: _gsql2rsql_{var}_{var}_{prop}
                    # Example: _gsql2rsql_sink_sink_id (should be _gsql2rsql_sink_id)
                    #
                    # ROOT CAUSE (in column_resolver.py):
                    # ------------------------------------
                    # When expanding entity properties for bare entity returns (e.g., RETURN sink),
                    # the resolver calls compute_sql_column_name(entity_var, prop_name) for each
                    # property from the schema. If the schema property is already prefixed with
                    # the role name (e.g., "sink_id" for Account sink node), the column name
                    # gets double-prefixed: compute_sql_column_name("sink", "sink_id") produces
                    # "_gsql2rsql_sink_sink_id" instead of "_gsql2rsql_sink_id".
                    #
                    # The issue is that NodeSchema.properties includes ALL properties from the
                    # YAML schema, but for entities in relationships, some property names already
                    # incorporate role information (source_id, sink_id, etc.).
                    #
                    # HOW TO FIX THE ROOT CAUSE (if someone wants to tackle this):
                    # -------------------------------------------------------------
                    # Option A (Simple): In column_resolver.py around line 464-483
                    #   When expanding entity properties for RETURN clauses, filter out properties
                    #   that would create duplicate names:
                    #
                    #   for prop in node_def.properties:
                    #       prop_name = prop.property_name
                    #       # Skip if property name already starts with entity variable name
                    #       if prop_name.startswith(f"{entity_var}_"):
                    #           continue  # This would create _gsql2rsql_{var}_{var}_{prop}
                    #       sql_col_name = compute_sql_column_name(entity_var, prop_name)
                    #       ...
                    #
                    # Option B (Better but more complex): Fix schema loading in pyspark_executor.py
                    #   The issue is that edge ID properties (source_account_id, target_account_id)
                    #   should not be included in the node's properties list, as they are relationship
                    #   metadata, not node properties. Modify load_schema_from_yaml() to:
                    #   1. Track which properties are edge join keys
                    #   2. Don't add them to NodeSchema.properties
                    #   3. Only make them available in EdgeSchema context
                    #
                    # Option C (Most thorough): Refactor symbol table to track property provenance
                    #   Add metadata to SymbolEntry.properties to indicate which properties are:
                    #   - Intrinsic node properties (name, status, etc.)
                    #   - Relationship join keys (source_id, sink_id)
                    #   - Computed/derived properties
                    #   This would allow the resolver to make intelligent decisions about naming.
                    #
                    # TRADE-OFFS:
                    # -----------
                    # Current workaround (defensive check):
                    #   ✅ Simple (5 lines), safe, well-tested
                    #   ✅ Zero risk to existing working queries
                    #   ✅ Self-documenting code (this comment explains everything)
                    #   ❌ Doesn't prevent bug from happening, just mitigates it
                    #   ❌ Would need update if column naming convention changes
                    #
                    # Option A (filter in resolver):
                    #   ✅ Prevents bug at source (Phase 4)
                    #   ✅ Still relatively simple (~10 lines)
                    #   ⚠️  Might hide legitimate properties that happen to match pattern
                    #   ⚠️  Requires careful testing of edge cases
                    #
                    # Option B (fix schema loading):
                    #   ✅ Architecturally correct (properties go in right place)
                    #   ✅ Prevents entire class of bugs
                    #   ❌ Requires changes to schema model
                    #   ❌ Risk of breaking existing schema-dependent code
                    #
                    # Option C (symbol table refactor):
                    #   ✅ Most robust, enables future optimizations
                    #   ❌ High complexity, touches core abstractions
                    #   ❌ Requires extensive testing and validation
                    #
                    # DECISION: Use workaround until someone has bandwidth for Option B
                    # ============================================================================
                    suffix = required_col[len(prefix):]  # e.g., "sink_id" from "_gsql2rsql_sink_sink_id"
                    if suffix.startswith(f"{entity_var}_"):
                        # Column name has duplicated variable: _gsql2rsql_{var}_{var}_{prop}
                        # Skip this malformed column reference
                        break

                    # This is a property of an entity variable we're projecting
                    extra_columns.append(required_col)
                    already_projected.add(required_col)
                    break

        # Sort to ensure deterministic output (avoid non-determinism from set iteration)
        return sorted(extra_columns)

    def _render_edge_filter_expression(self, expr: QueryExpression) -> str:
        """Render an edge filter expression using DIRECT column references.

        This method is specifically for rendering predicates that have been
        pushed down into the recursive CTE. Unlike _render_expression, this
        renders property accesses as direct SQL column references (e.g., e.amount)
        rather than entity-prefixed aliases (e.g., _gsql2rsql_e_amount).

        This is necessary because inside the CTE, we reference edge columns
        directly from the edge table (aliased as 'e'), not through the
        entity field aliasing system used for JOINs.

        Example:
            Input: QueryExpressionProperty(variable_name="e", property_name="amount")
            _render_expression output: "_gsql2rsql_e_amount" (wrong for CTE)
            This method output: "e.amount" (correct for CTE)

        Args:
            expr: The expression to render (already rewritten with 'e' prefix)

        Returns:
            SQL string with direct column references
        """
        if isinstance(expr, QueryExpressionProperty):
            # Render as direct column reference: e.amount, e.timestamp
            if expr.property_name:
                return f"{expr.variable_name}.{expr.property_name}"
            return expr.variable_name

        elif isinstance(expr, QueryExpressionBinary):
            # Recursively render binary expressions
            if not expr.operator or not expr.left_expression or not expr.right_expression:
                return "NULL"
            left = self._render_edge_filter_expression(expr.left_expression)
            right = self._render_edge_filter_expression(expr.right_expression)
            pattern = OPERATOR_PATTERNS.get(expr.operator.name, "({0}) ? ({1})")
            return pattern.format(left, right)

        elif isinstance(expr, QueryExpressionFunction):
            # Render function calls with recursive parameter rendering
            params = [self._render_edge_filter_expression(p) for p in expr.parameters]
            func = expr.function

            # Handle common functions
            if func == Function.DATETIME:
                return "CURRENT_TIMESTAMP()"
            elif func == Function.DATE:
                if params:
                    return f"DATE({params[0]})"
                return "CURRENT_DATE()"
            elif func == Function.DURATION:
                # Convert ISO 8601 duration to INTERVAL
                if params:
                    duration_str = params[0].strip("'\"")
                    return self._convert_duration_to_interval(duration_str)
                return "INTERVAL 0 DAY"
            elif func == Function.NOT:
                return f"NOT ({params[0]})" if params else "NOT (NULL)"
            else:
                # Default function rendering
                func_name = func.name if func else "UNKNOWN"
                return f"{func_name}({', '.join(params)})"

        elif isinstance(expr, QueryExpressionValue):
            # Render literal values
            if expr.value is None:
                return "NULL"
            elif isinstance(expr.value, str):
                escaped = expr.value.replace("'", "''")
                return f"'{escaped}'"
            elif isinstance(expr.value, bool):
                return "TRUE" if expr.value else "FALSE"
            else:
                return str(expr.value)

        # Fallback: try the regular renderer (shouldn't normally reach here)
        # This is a safety net for expression types we haven't handled
        return str(expr)

    def _convert_duration_to_interval(self, duration_str: str) -> str:
        """Convert ISO 8601 duration string to Databricks INTERVAL.

        Examples:
            P7D -> INTERVAL 7 DAY
            P1M -> INTERVAL 1 MONTH
            PT1H -> INTERVAL 1 HOUR
        """
        import re

        # Simple patterns for common durations
        if match := re.match(r"P(\d+)D", duration_str):
            return f"INTERVAL {match.group(1)} DAY"
        elif match := re.match(r"P(\d+)M", duration_str):
            return f"INTERVAL {match.group(1)} MONTH"
        elif match := re.match(r"P(\d+)Y", duration_str):
            return f"INTERVAL {match.group(1)} YEAR"
        elif match := re.match(r"PT(\d+)H", duration_str):
            return f"INTERVAL {match.group(1)} HOUR"
        elif match := re.match(r"PT(\d+)M", duration_str):
            return f"INTERVAL {match.group(1)} MINUTE"
        elif match := re.match(r"PT(\d+)S", duration_str):
            return f"INTERVAL {match.group(1)} SECOND"
        else:
            # Default to days if format not recognized
            return f"INTERVAL 1 DAY"

    def _render_map_literal(
        self, expr: QueryExpressionMapLiteral, context_op: LogicalOperator
    ) -> str:
        """Render a map literal as a Databricks SQL STRUCT.

        Cypher: {name: 'John', age: 30}
        Databricks SQL: STRUCT('John' AS name, 30 AS age)
        """
        if not expr.entries:
            return "STRUCT()"

        parts = []
        for key, value in expr.entries:
            value_sql = self._render_expression(value, context_op)
            parts.append(f"{value_sql} AS {key}")

        return f"STRUCT({', '.join(parts)})"

    def _render_date_from_map(
        self, map_expr: QueryExpressionMapLiteral, context_op: LogicalOperator
    ) -> str:
        """Render date({year: y, month: m, day: d}) as MAKE_DATE.

        Cypher: date({year: 2024, month: 1, day: 15})
        Databricks SQL: MAKE_DATE(2024, 1, 15)
        """
        entries_dict = {}
        for key, value in map_expr.entries:
            entries_dict[key.lower()] = self._render_expression(
                value, context_op
            )

        year = entries_dict.get("year", "1970")
        month = entries_dict.get("month", "1")
        day = entries_dict.get("day", "1")

        return f"MAKE_DATE({year}, {month}, {day})"

    def _render_datetime_from_map(
        self, map_expr: QueryExpressionMapLiteral, context_op: LogicalOperator
    ) -> str:
        """Render datetime({...}) as MAKE_TIMESTAMP.

        Cypher: datetime({year: 2024, month: 1, day: 15, hour: 10})
        Databricks SQL: MAKE_TIMESTAMP(2024, 1, 15, 10, 0, 0)
        """
        entries_dict = {}
        for key, value in map_expr.entries:
            entries_dict[key.lower()] = self._render_expression(
                value, context_op
            )

        year = entries_dict.get("year", "1970")
        month = entries_dict.get("month", "1")
        day = entries_dict.get("day", "1")
        hour = entries_dict.get("hour", "0")
        minute = entries_dict.get("minute", "0")
        second = entries_dict.get("second", "0")

        return f"MAKE_TIMESTAMP({year}, {month}, {day}, {hour}, {minute}, {second})"

    def _render_time_from_map(
        self, map_expr: QueryExpressionMapLiteral, context_op: LogicalOperator
    ) -> str:
        """Render time({hour: h, minute: m, second: s}) as time string.

        Cypher: time({hour: 10, minute: 30, second: 0})
        Databricks SQL: '10:30:00'
        """
        entries_dict = {}
        for key, value in map_expr.entries:
            entries_dict[key.lower()] = self._render_expression(
                value, context_op
            )

        hour = entries_dict.get("hour", "0")
        minute = entries_dict.get("minute", "0")
        second = entries_dict.get("second", "0")

        # Build time as formatted string
        return f"CONCAT({hour}, ':', {minute}, ':', {second})"

    def _render_duration_from_map(
        self, map_expr: QueryExpressionMapLiteral, context_op: LogicalOperator
    ) -> str:
        """Render duration({...}) as Databricks INTERVAL.

        Cypher: duration({days: 7, hours: 12})
        Databricks SQL: INTERVAL '7' DAY + INTERVAL '12' HOUR
        """
        entries_dict = {}
        for key, value in map_expr.entries:
            entries_dict[key.lower()] = self._render_expression(
                value, context_op
            )

        # Map Cypher duration components to Databricks INTERVAL units
        interval_parts = []
        unit_mapping = {
            "years": "YEAR",
            "months": "MONTH",
            "weeks": "WEEK",
            "days": "DAY",
            "hours": "HOUR",
            "minutes": "MINUTE",
            "seconds": "SECOND",
        }

        for cypher_unit, sql_unit in unit_mapping.items():
            if cypher_unit in entries_dict:
                val = entries_dict[cypher_unit]
                interval_parts.append(f"INTERVAL {val} {sql_unit}")

        if not interval_parts:
            return "INTERVAL '0' DAY"

        return " + ".join(interval_parts)

    def _parse_iso8601_duration(self, duration_str: str) -> str:
        """Parse an ISO 8601 duration string to Databricks INTERVAL expression.

        ISO 8601 duration format: P[n]Y[n]M[n]DT[n]H[n]M[n]S
        Examples:
        - P7D -> INTERVAL 7 DAY
        - PT1H -> INTERVAL 1 HOUR
        - P30D -> INTERVAL 30 DAY
        - PT5M -> INTERVAL 5 MINUTE
        - P1Y2M3D -> INTERVAL 1 YEAR + INTERVAL 2 MONTH + INTERVAL 3 DAY
        - PT1H30M -> INTERVAL 1 HOUR + INTERVAL 30 MINUTE
        - P1DT12H -> INTERVAL 1 DAY + INTERVAL 12 HOUR

        Args:
            duration_str: ISO 8601 duration string (e.g., 'P7D', 'PT1H')

        Returns:
            Databricks SQL INTERVAL expression
        """
        import re

        if not duration_str:
            return "INTERVAL '0' DAY"

        # Remove leading 'P'
        s = duration_str.strip().upper()
        if not s.startswith("P"):
            return "INTERVAL '0' DAY"
        s = s[1:]

        interval_parts = []

        # Split by 'T' to separate date and time parts
        if "T" in s:
            date_part, time_part = s.split("T", 1)
        else:
            date_part = s
            time_part = ""

        # Parse date part: [n]Y[n]M[n]W[n]D
        date_pattern = re.compile(r"(\d+)([YMWD])")
        for match in date_pattern.finditer(date_part):
            value = match.group(1)
            unit = match.group(2)
            unit_map = {"Y": "YEAR", "M": "MONTH", "W": "WEEK", "D": "DAY"}
            if unit in unit_map:
                interval_parts.append(f"INTERVAL {value} {unit_map[unit]}")

        # Parse time part: [n]H[n]M[n]S
        time_pattern = re.compile(r"(\d+(?:\.\d+)?)([HMS])")
        for match in time_pattern.finditer(time_part):
            value = match.group(1)
            unit = match.group(2)
            unit_map = {"H": "HOUR", "M": "MINUTE", "S": "SECOND"}
            if unit in unit_map:
                interval_parts.append(f"INTERVAL {value} {unit_map[unit]}")

        if not interval_parts:
            return "INTERVAL '0' DAY"

        return " + ".join(interval_parts)
