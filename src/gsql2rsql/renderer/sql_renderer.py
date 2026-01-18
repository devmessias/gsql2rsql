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
    Entity,
    ListPredicateType,
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
    RelationshipDirection,
    RelationshipEntity,
)
from gsql2rsql.parser.operators import (
    AggregationFunction,
    BinaryOperator,
    Function,
)
from gsql2rsql.planner.logical_plan import LogicalPlan
from gsql2rsql.planner.operators import (
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
from gsql2rsql.planner.schema import EntityField, EntityType, ValueField
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

    def __init__(
        self,
        graph_def: ISQLDBSchemaProvider | None = None,
        logger: ILoggable | None = None,
        *,
        graph_schema_provider: ISQLDBSchemaProvider | None = None,
        db_schema_provider: ISQLDBSchemaProvider | None = None,
    ) -> None:
        """
        Initialize the SQL renderer.

        Args:
            graph_def: The SQL schema provider with table mappings.
                Deprecated: use db_schema_provider instead.
            logger: Optional logger for debugging.
            graph_schema_provider: The graph schema provider (for future use).
            db_schema_provider: The SQL database schema provider.
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
        # Column pruning: set of required column aliases (e.g., "__p_name")
        self._required_columns: set[str] = set()
        # Enable column pruning by default
        self._enable_column_pruning = True

    def render_plan(self, plan: LogicalPlan) -> str:
        """
        Render a logical plan to Databricks SQL.

        Args:
            plan: The logical plan to render.

        Returns:
            The rendered Databricks SQL query string.
        """
        if not plan.terminal_operators:
            return ""

        # Reset CTE counter
        self._cte_counter = 0

        # Column pruning: collect required columns before rendering
        self._required_columns = set()
        if self._enable_column_pruning:
            for terminal_op in plan.terminal_operators:
                self._collect_required_columns(terminal_op)

        # Collect any recursive operators that need CTEs
        ctes: list[str] = []
        for start_op in plan.starting_operators:
            self._collect_recursive_ctes(start_op, ctes)

        # Render from the terminal operator
        terminal_op = plan.terminal_operators[0]
        main_query = self._render_operator(terminal_op, depth=0)

        # Combine CTEs with main query
        if ctes:
            cte_block = "WITH RECURSIVE\n" + ",\n".join(ctes)
            return f"{cte_block}\n{main_query}"

        return main_query

    def _collect_recursive_ctes(self, op: LogicalOperator, ctes: list[str]) -> None:
        """Collect recursive CTE definitions from the plan."""
        if isinstance(op, RecursiveTraversalOperator):
            cte = self._render_recursive_cte(op)
            ctes.append(cte)

        for out_op in op.out_operators:
            self._collect_recursive_ctes(out_op, ctes)

    def _collect_required_columns(self, op: LogicalOperator) -> None:
        """
        Collect required column aliases from the operator tree (column pruning).

        Traverses the operator tree and collects all column aliases that are
        actually referenced in projections, filters, joins, aggregations, etc.
        This allows the renderer to only output required columns in intermediate
        subqueries, improving query performance.
        """
        if isinstance(op, ProjectionOperator):
            # Collect columns from projection expressions
            for _, expr in op.projections:
                self._collect_columns_from_expression(expr)
            # Collect from order by
            if op.order_by:
                for expr, _ in op.order_by:
                    self._collect_columns_from_expression(expr)

        elif isinstance(op, SelectionOperator):
            # Collect columns from filter expression
            if op.filter_expression:
                self._collect_columns_from_expression(op.filter_expression)

        elif isinstance(op, JoinOperator):
            # Join key columns are always required for the join condition
            # These are handled separately in _render_data_source via join fields
            pass

        elif isinstance(op, SetOperator):
            # Recurse into both sides of set operation
            if op.in_operator_left:
                self._collect_required_columns(op.in_operator_left)
            if op.in_operator_right:
                self._collect_required_columns(op.in_operator_right)
            return  # Don't recurse further for set operators

        # Recurse into input operators
        if hasattr(op, "in_operator") and op.in_operator:
            self._collect_required_columns(op.in_operator)
        if hasattr(op, "in_operator_left") and op.in_operator_left:
            self._collect_required_columns(op.in_operator_left)
        if hasattr(op, "in_operator_right") and op.in_operator_right:
            self._collect_required_columns(op.in_operator_right)

    def _collect_columns_from_expression(self, expr: QueryExpression) -> None:
        """Extract column aliases from an expression and add to required set."""
        if isinstance(expr, QueryExpressionProperty):
            # Property access like p.name -> __p_name
            if expr.variable_name and expr.property_name:
                col_alias = self._get_field_name(expr.variable_name, expr.property_name)
                self._required_columns.add(col_alias)
            elif expr.variable_name:
                # Just a variable reference (might reference all fields)
                # We'll handle this conservatively by not pruning
                pass
        elif isinstance(expr, QueryExpressionBinary):
            if expr.left_expression:
                self._collect_columns_from_expression(expr.left_expression)
            if expr.right_expression:
                self._collect_columns_from_expression(expr.right_expression)
        elif isinstance(expr, QueryExpressionFunction):
            for param in expr.parameters:
                self._collect_columns_from_expression(param)
        elif isinstance(expr, QueryExpressionAggregationFunction):
            if expr.inner_expression:
                self._collect_columns_from_expression(expr.inner_expression)
        elif isinstance(expr, QueryExpressionList):
            for item in expr.items:
                self._collect_columns_from_expression(item)
        elif isinstance(expr, QueryExpressionCaseExpression):
            if expr.test_expression:
                self._collect_columns_from_expression(expr.test_expression)
            for when_expr, then_expr in expr.alternatives:
                self._collect_columns_from_expression(when_expr)
                self._collect_columns_from_expression(then_expr)
            if expr.else_expression:
                self._collect_columns_from_expression(expr.else_expression)
        elif isinstance(expr, QueryExpressionExists):
            # EXISTS subquery - collect from pattern entities if they reference outer columns
            pass  # EXISTS patterns are rendered separately

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
                edge_id = EdgeSchema.get_edge_id(
                    edge_type,
                    op.source_node_type,
                    op.target_node_type,
                )
                edge_table = self._graph_def.get_sql_table_descriptors(edge_id)
                if not edge_table:
                    # Try with just the edge name
                    edge_table = self._graph_def.get_sql_table_descriptors(edge_type)
                if edge_table:
                    edge_tables.append((edge_type, edge_table))

                # Get column names and properties from first edge schema
                if len(edge_tables) == 1:
                    edge_schema = self._graph_def.get_edge_definition(
                        edge_type,
                        op.source_node_type,
                        op.target_node_type,
                    )
                    if edge_schema:
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
            # No specific types - would need to get all edges between nodes
            # For now, raise an error - this could be improved later
            raise TranspilerInternalErrorException(
                "Variable-length path without edge type not yet supported. "
                "Please specify at least one edge type, e.g., -[:KNOWS*1..3]->"
            )

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
            table_name = edge_table.full_table_name
            if table_name not in table_to_filters:
                table_to_filters[table_name] = []
            if edge_table.filter:
                table_to_filters[table_name].append(edge_table.filter)

        # If single table with multiple filters, combine with OR
        single_table = len(table_to_filters) == 1
        if single_table:
            table_name = list(table_to_filters.keys())[0]
            filters_list = table_to_filters[table_name]
            if len(filters_list) > 1:
                # Combine filters with OR: (filter1) OR (filter2)
                combined_filter = " OR ".join(f"({f})" for f in filters_list)
            elif len(filters_list) == 1:
                combined_filter = filters_list[0]
            else:
                combined_filter = None

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
        # (e.g., "e.amount") not entity-prefixed aliases (e.g., "__e_amount").
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
            # Get source node table for JOIN
            source_node_table = self._graph_def.get_sql_table_descriptors(
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
            # Get source node table descriptor
            zero_len_source_table = self._graph_def.get_sql_table_descriptors(
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

        if single_table:
            # Single table - generate one SELECT with combined filter
            base_sql = []
            base_sql.append("    SELECT")
            base_sql.append(f"      e.{source_id_col} AS start_node,")
            base_sql.append(f"      e.{target_id_col} AS end_node,")
            base_sql.append("      1 AS depth,")
            base_sql.append(
                f"      ARRAY(e.{source_id_col}, e.{target_id_col}) AS path,"
            )
            if op.collect_edges:
                # Array with single edge struct
                base_sql.append(f"      ARRAY({build_edge_struct()}) AS path_edges,")
            base_sql.append(f"      ARRAY(e.{source_id_col}) AS visited")
            base_sql.append(f"    FROM {table_name} e")

            # Add JOIN with source node table if we have a source filter
            if source_node_filter_sql and source_node_table:
                base_sql.append(
                    f"    JOIN {source_node_table.full_table_name} src "
                    f"ON src.{op.source_id_column} = e.{source_id_col}"
                )

            # Build WHERE clause
            where_parts = []
            if combined_filter:
                where_parts.append(f"({combined_filter})")
            # ═══════════════════════════════════════════════════════════════
            # SOURCE NODE FILTER PUSHDOWN: Apply source filter in base case
            # ═══════════════════════════════════════════════════════════════
            if source_node_filter_sql:
                where_parts.append(source_node_filter_sql)
            # ═══════════════════════════════════════════════════════════════
            # PREDICATE PUSHDOWN: Apply edge filter in base case
            # ═══════════════════════════════════════════════════════════════
            # This filters edges BEFORE they enter the CTE, preventing
            # invalid paths from being explored in the first place.
            if edge_filter_sql:
                where_parts.append(edge_filter_sql)
            if where_parts:
                base_sql.append(f"    WHERE {' AND '.join(where_parts)}")

            lines.append("\n".join(base_sql))
        else:
            # Multiple tables - use UNION ALL for base case only
            base_cases: list[str] = []
            for edge_type, edge_table in edge_tables:
                base_sql = []
                base_sql.append("    SELECT")
                base_sql.append(f"      e.{source_id_col} AS start_node,")
                base_sql.append(f"      e.{target_id_col} AS end_node,")
                base_sql.append("      1 AS depth,")
                base_sql.append(
                    f"      ARRAY(e.{source_id_col}, e.{target_id_col}) AS path,"
                )
                if op.collect_edges:
                    base_sql.append(f"      ARRAY({build_edge_struct()}) AS path_edges,")
                base_sql.append(f"      ARRAY(e.{source_id_col}) AS visited")
                base_sql.append(f"    FROM {edge_table.full_table_name} e")

                # Add JOIN with source node table if we have a source filter
                if source_node_filter_sql and source_node_table:
                    base_sql.append(
                        f"    JOIN {source_node_table.full_table_name} src "
                        f"ON src.{op.source_id_column} = e.{source_id_col}"
                    )

                filters = []
                if edge_table.filter:
                    filters.append(edge_table.filter)
                # SOURCE NODE FILTER PUSHDOWN
                if source_node_filter_sql:
                    filters.append(source_node_filter_sql)
                # PREDICATE PUSHDOWN: Apply edge filter in multi-table base case
                if edge_filter_sql:
                    filters.append(edge_filter_sql)
                if filters:
                    base_sql.append(f"    WHERE {' AND '.join(filters)}")

                base_cases.append("\n".join(base_sql))

            # Wrap in subquery for Databricks compatibility
            lines.append("    SELECT * FROM (")
            lines.append("\n      UNION ALL\n".join(base_cases))
            lines.append("    )")

        lines.append("")
        lines.append("    UNION ALL")
        lines.append("")
        lines.append("    -- Recursive case: extend paths")

        if single_table:
            # Single table - generate one SELECT with combined filter
            rec_sql = []
            rec_sql.append("    SELECT")
            rec_sql.append("      p.start_node,")
            rec_sql.append(f"      e.{target_id_col} AS end_node,")
            rec_sql.append("      p.depth + 1 AS depth,")
            rec_sql.append(
                f"      CONCAT(p.path, ARRAY(e.{target_id_col})) AS path,"
            )
            if op.collect_edges:
                # Append new edge struct to path_edges array
                rec_sql.append(
                    f"      ARRAY_APPEND(p.path_edges, {build_edge_struct()}) AS path_edges,"
                )
            rec_sql.append(
                f"      CONCAT(p.visited, ARRAY(e.{source_id_col})) AS visited"
            )
            rec_sql.append(f"    FROM {cte_name} p")
            rec_sql.append(f"    JOIN {table_name} e")
            rec_sql.append(f"      ON p.end_node = e.{source_id_col}")

            where_parts = [f"p.depth < {max_depth}"]
            where_parts.append(
                f"NOT ARRAY_CONTAINS(p.visited, e.{target_id_col})"
            )
            if combined_filter:
                where_parts.append(f"({combined_filter})")
            # ═══════════════════════════════════════════════════════════════
            # PREDICATE PUSHDOWN: Apply edge filter in recursive case
            # ═══════════════════════════════════════════════════════════════
            # This is the key optimization: by filtering edges DURING
            # recursion, we prune entire subtrees of invalid paths.
            #
            # Example: For rel.amount > 1000
            # - Without pushdown: explore 10,000 paths, filter to 2
            # - With pushdown: only explore the 2 valid paths from start
            if edge_filter_sql:
                where_parts.append(edge_filter_sql)
            rec_sql.append(f"    WHERE {where_parts[0]}")
            for wp in where_parts[1:]:
                rec_sql.append(f"      AND {wp}")

            lines.append("\n".join(rec_sql))
        else:
            # Multiple tables - use UNION ALL wrapped in subquery
            recursive_cases: list[str] = []
            for edge_type, edge_table in edge_tables:
                rec_sql = []
                rec_sql.append("    SELECT")
                rec_sql.append("      p.start_node,")
                rec_sql.append(f"      e.{target_id_col} AS end_node,")
                rec_sql.append("      p.depth + 1 AS depth,")
                rec_sql.append(
                    f"      CONCAT(p.path, ARRAY(e.{target_id_col})) AS path,"
                )
                if op.collect_edges:
                    rec_sql.append(
                        f"      ARRAY_APPEND(p.path_edges, {build_edge_struct()}) AS path_edges,"
                    )
                rec_sql.append(
                    f"      CONCAT(p.visited, ARRAY(e.{source_id_col})) AS visited"
                )
                rec_sql.append(f"    FROM {cte_name} p")
                rec_sql.append(f"    JOIN {edge_table.full_table_name} e")
                rec_sql.append(f"      ON p.end_node = e.{source_id_col}")

                where_parts = [f"p.depth < {max_depth}"]
                where_parts.append(
                    f"NOT ARRAY_CONTAINS(p.visited, e.{target_id_col})"
                )
                if edge_table.filter:
                    where_parts.append(edge_table.filter)
                # PREDICATE PUSHDOWN: Apply edge filter in multi-table recursive case
                if edge_filter_sql:
                    where_parts.append(edge_filter_sql)
                rec_sql.append(f"    WHERE {where_parts[0]}")
                for wp in where_parts[1:]:
                    rec_sql.append(f"      AND {wp}")

                recursive_cases.append("\n".join(rec_sql))

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
               sink.id AS __b_id,
               sink.name AS __b_name,
               source.id AS __a_id,
               source.name AS __a_name,
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

        # Get target node's table info
        target_entity = target_op.entity
        target_table = self._graph_def.get_sql_table_descriptors(
            target_entity.entity_name
        )
        if not target_table:
            raise TranspilerInternalErrorException(
                f"No table descriptor for {target_entity.entity_name}"
            )

        # Get target node's ID column and schema
        target_node_schema = self._graph_def.get_node_definition(target_entity.entity_name)
        if target_node_schema and target_node_schema.node_id_property:
            target_id_col = target_node_schema.node_id_property.property_name
        else:
            target_id_col = "id"

        # Get alias for target node (sink) and source node
        target_alias = target_entity.alias or "n"
        source_alias = recursive_op.source_alias or "src"

        # Get source node's table info (may be same type as target)
        source_node_type = recursive_op.source_node_type
        source_table = self._graph_def.get_sql_table_descriptors(source_node_type)
        if not source_table:
            # Fallback to target table if source not found
            source_table = target_table

        # Get source node's ID column and schema
        source_node_schema = self._graph_def.get_node_definition(source_node_type)
        if source_node_schema and source_node_schema.node_id_property:
            source_id_col = source_node_schema.node_id_property.property_name
        else:
            source_id_col = "id"

        lines: list[str] = []
        lines.append(f"{indent}SELECT")

        # Project fields from TARGET node (sink)
        field_lines: list[str] = []
        if target_node_schema:
            field_lines.append(f"sink.{target_id_col} AS __{target_alias}_{target_id_col}")
            for prop in target_node_schema.properties:
                prop_name = prop.property_name
                if prop_name != target_id_col:
                    field_lines.append(f"sink.{prop_name} AS __{target_alias}_{prop_name}")
        else:
            field_lines.append(f"sink.{target_id_col} AS __{target_alias}_id")

        # Project fields from SOURCE node
        if source_node_schema:
            field_lines.append(f"source.{source_id_col} AS __{source_alias}_{source_id_col}")
            for prop in source_node_schema.properties:
                prop_name = prop.property_name
                if prop_name != source_id_col:
                    field_lines.append(f"source.{prop_name} AS __{source_alias}_{prop_name}")
        else:
            field_lines.append(f"source.{source_id_col} AS __{source_alias}_id")

        # Include path info from CTE
        field_lines.append("p.start_node")
        field_lines.append("p.end_node")
        field_lines.append("p.depth")
        field_lines.append("p.path")
        # Include path_edges if edge collection is enabled (for relationships(path))
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

    def _indent(self, depth: int) -> str:
        """Get indentation string for a given depth."""
        return "  " * depth

    def _render_data_source(self, op: DataSourceOperator, depth: int) -> str:
        """Render a data source operator."""
        lines: list[str] = []
        indent = self._indent(depth)

        if not op.output_schema:
            return ""

        entity_field = op.output_schema[0]
        if not isinstance(entity_field, EntityField):
            return ""

        # Get SQL table descriptor
        table_desc = self._graph_def.get_sql_table_descriptors(
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

        # Add implicit filter if defined (e.g., edge_type = 'KNOWS')
        if table_desc.filter:
            filters.append(table_desc.filter)

        # Add pushed-down filter from optimizer (e.g., p.name = 'Alice')
        if op.filter_expression:
            # Render the filter expression using raw column names
            # (not aliased names like __p_name)
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

        Unlike _render_expression which uses aliased names like __p_name,
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
            return self._render_recursive_join(op, left_op, right_op, depth)

        lines.append(f"{indent}SELECT")

        # Determine output fields from both sides
        output_fields = self._get_join_output_fields(
            op, left_op, right_op, left_var, right_var
        )
        for i, field_line in enumerate(output_fields):
            prefix = " " if i == 0 else ","
            lines.append(f"{indent}  {prefix}{field_line}")

        # FROM left subquery
        lines.append(f"{indent}FROM (")
        lines.append(self._render_operator(left_op, depth + 1))
        lines.append(f"{indent}) AS {left_var}")

        # JOIN type and right subquery
        if op.join_type == JoinType.CROSS:
            lines.append(f"{indent}CROSS JOIN (")
            lines.append(self._render_operator(right_op, depth + 1))
            lines.append(f"{indent}) AS {right_var}")
        else:
            join_keyword = (
                "INNER JOIN" if op.join_type == JoinType.INNER else "LEFT JOIN"
            )
            lines.append(f"{indent}{join_keyword} (")
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

        # Collect field aliases from both sides
        left_aliases = {f.field_alias for f in left_op.output_schema}
        right_aliases = {f.field_alias for f in right_op.output_schema}

        for field in op.output_schema:
            is_from_left = field.field_alias in left_aliases
            var_name = left_var if is_from_left else right_var

            if isinstance(field, EntityField):
                # Entity field - output join keys
                if field.entity_type == EntityType.NODE:
                    if field.node_join_field:
                        key_name = self._get_field_name(
                            field.field_alias, field.node_join_field.field_alias
                        )
                        fields.append(f"{var_name}.{key_name} AS {key_name}")
                else:
                    if field.rel_source_join_field:
                        src_key = self._get_field_name(
                            field.field_alias,
                            field.rel_source_join_field.field_alias,
                        )
                        fields.append(f"{var_name}.{src_key} AS {src_key}")
                    if field.rel_sink_join_field:
                        sink_key = self._get_field_name(
                            field.field_alias,
                            field.rel_sink_join_field.field_alias,
                        )
                        fields.append(f"{var_name}.{sink_key} AS {sink_key}")

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
                        field_alias = self._get_field_name(
                            field.field_alias, encap_field.field_alias
                        )
                        # Column pruning: only include if required or pruning disabled
                        if (
                            not self._enable_column_pruning
                            or not self._required_columns
                            or field_alias in self._required_columns
                        ):
                            fields.append(f"{var_name}.{field_alias} AS {field_alias}")

            elif isinstance(field, ValueField):
                # Column pruning for value fields
                if (
                    not self._enable_column_pruning
                    or not self._required_columns
                    or field.field_alias in self._required_columns
                ):
                    fields.append(f"{var_name}.{field.field_alias} AS {field.field_alias}")

        return fields

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
                node_key = self._get_field_name(
                    node_alias,
                    (
                        node_field.node_join_field.field_alias
                        if node_field.node_join_field
                        else "id"
                    ),
                )

                if pair.pair_type == JoinKeyPairType.SOURCE:
                    rel_key = self._get_field_name(
                        rel_alias,
                        (
                            rel_field.rel_source_join_field.field_alias
                            if rel_field.rel_source_join_field
                            else "source_id"
                        ),
                    )
                elif pair.pair_type == JoinKeyPairType.SINK:
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
                    rel_key = self._get_field_name(
                        rel_alias,
                        (
                            rel_field.node_join_field.field_alias
                            if rel_field.node_join_field
                            else "id"
                        ),
                    )
                else:
                    # EITHER - undirected relationship, needs to match both directions
                    # Generate: (node.id = rel.source_id OR node.id = rel.sink_id)
                    source_key = None
                    sink_key = None

                    if rel_field.rel_source_join_field:
                        source_key = self._get_field_name(
                            rel_alias, rel_field.rel_source_join_field.field_alias
                        )
                    if rel_field.rel_sink_join_field:
                        sink_key = self._get_field_name(
                            rel_alias, rel_field.rel_sink_join_field.field_alias
                        )

                    if source_key and sink_key:
                        # Both directions available - generate OR condition
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
        for i, (alias, expr) in enumerate(op.projections):
            rendered = self._render_expression(expr, op)
            prefix = " " if i == 0 else ","
            lines.append(f"{indent}  {prefix}{rendered} AS {alias}")

        lines.append(f"{indent}FROM (")
        lines.append(self._render_operator(op.in_operator, depth + 1))
        lines.append(f"{indent}) AS _proj")

        # WHERE clause (from flattened SelectionOperator)
        # Applied BEFORE GROUP BY - filters individual rows
        if op.filter_expression:
            filter_sql = self._render_expression(op.filter_expression, op)
            lines.append(f"{indent}WHERE {filter_sql}")

        # Group by for aggregations
        has_aggregation = any(
            self._has_aggregation(expr) for _, expr in op.projections
        )
        if has_aggregation:
            non_agg_exprs = [
                self._render_expression(expr, op)
                for alias, expr in op.projections
                if not self._has_aggregation(expr)
            ]
            if non_agg_exprs:
                group_by = ", ".join(non_agg_exprs)
                lines.append(f"{indent}GROUP BY {group_by}")

        # HAVING clause (filter on aggregated columns)
        # Applied AFTER GROUP BY - filters groups
        if op.having_expression:
            having_sql = self._render_expression(op.having_expression, op)
            lines.append(f"{indent}HAVING {having_sql}")

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
        """Render a property access."""
        if expr.property_name:
            field_alias = self._get_field_name(expr.variable_name, expr.property_name)
            return field_alias
        return expr.variable_name

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

        left = self._render_expression(expr.left_expression, context_op)
        right = self._render_expression(expr.right_expression, context_op)

        pattern = OPERATOR_PATTERNS.get(expr.operator.name, "({0}) ? ({1})")
        return pattern.format(left, right)

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
            return "path" if params else "ARRAY()"
        elif func == Function.RELATIONSHIPS:
            # relationships(path) -> path_edges (array of edge structs from CTE)
            # The parameter should be a path variable reference
            return "path_edges" if params else "ARRAY()"
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
        if expr.operator.name != BinaryOperator.EQ.name:
            return None

        left = expr.left_expression
        right = expr.right_expression

        # Check if left is just the variable and right is the value
        if (
            isinstance(left, QueryExpressionProperty)
            and left.variable_name == var_name
            and not left.property_name
        ):
            # Pattern: x = value
            return self._render_expression(right, context_op)

        # Check if right is just the variable and left is the value
        if (
            isinstance(right, QueryExpressionProperty)
            and right.variable_name == var_name
            and not right.property_name
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
            left = self._render_list_predicate_filter(
                expr.left_expression, var_name, context_op
            )
            right = self._render_list_predicate_filter(
                expr.right_expression, var_name, context_op
            )
            from gsql2rsql.renderer.sql_renderer import OPERATOR_PATTERNS
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

        # Render the reducer expression, replacing accumulator and variable references
        reducer_sql = self._render_reduce_expression(
            expr.reducer_expression, acc_name, var_name, context_op
        )

        return f"AGGREGATE({list_sql}, {initial_sql}, ({acc_name}, {var_name}) -> {reducer_sql})"

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
            if relationship.direction == RelationshipDirection.BACKWARD:
                edge_id = EdgeSchema.get_edge_id(
                    rel_name, target_entity_name, source_entity_name
                )
            else:
                edge_id = EdgeSchema.get_edge_id(
                    rel_name, source_entity_name, target_entity_name
                )
            rel_table_desc = self._graph_def.get_sql_table_descriptors(edge_id)
            if relationship.direction == RelationshipDirection.BACKWARD:
                edge_schema = self._graph_def.get_edge_definition(
                    rel_name, target_entity_name, source_entity_name
                )
            else:
                edge_schema = self._graph_def.get_edge_definition(
                    rel_name, source_entity_name, target_entity_name
                )
        else:
            # Source entity unknown - use fallback lookup by verb
            # This handles EXISTS patterns where source is a variable reference
            result = self._graph_def.find_edge_by_verb(rel_name, target_entity_name)
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
            target_table_desc = self._graph_def.get_sql_table_descriptors(
                target_node.entity_name
            )
            if target_table_desc:
                target_table = target_table_desc.full_table_name
                # Get target node's ID column
                target_node_schema = self._graph_def.get_node_definition(
                    target_node.entity_name
                )
                target_node_id_col = "id"
                if target_node_schema and target_node_schema.node_id_property:
                    target_node_id_col = target_node_schema.node_id_property.property_name

                # Join direction depends on relationship direction
                if relationship.direction == RelationshipDirection.BACKWARD:
                    lines.append(
                        f"JOIN {target_table} _exists_target "
                        f"ON _exists_rel.{source_id_col} = _exists_target.{target_node_id_col}"
                    )
                else:
                    lines.append(
                        f"JOIN {target_table} _exists_target "
                        f"ON _exists_rel.{target_id_col} = _exists_target.{target_node_id_col}"
                    )

        # Correlate with outer query using source node's ID
        # The source node's ID field should be in the outer context
        source_alias = source_node.alias or "_src"
        # Get the source node's ID column from schema
        source_node_schema = self._graph_def.get_node_definition(
            source_node.entity_name
        ) if source_node.entity_name else None
        source_node_id_col = "id"
        if source_node_schema and source_node_schema.node_id_property:
            source_node_id_col = source_node_schema.node_id_property.property_name

        outer_field = f"__{source_alias}_{source_node_id_col}"

        # WHERE clause: correlate with outer query
        if relationship.direction == RelationshipDirection.BACKWARD:
            correlation = f"_exists_rel.{target_id_col} = {outer_field}"
        else:
            correlation = f"_exists_rel.{source_id_col} = {outer_field}"

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

    def _get_field_name(self, prefix: str, field_name: str) -> str:
        """Generate a field name with entity prefix."""
        clean_prefix = "".join(
            c if c.isalnum() or c == "_" else "" for c in prefix
        )
        return f"__{clean_prefix}_{field_name}"

    def _render_edge_filter_expression(self, expr: QueryExpression) -> str:
        """Render an edge filter expression using DIRECT column references.

        This method is specifically for rendering predicates that have been
        pushed down into the recursive CTE. Unlike _render_expression, this
        renders property accesses as direct SQL column references (e.g., e.amount)
        rather than entity-prefixed aliases (e.g., __e_amount).

        This is necessary because inside the CTE, we reference edge columns
        directly from the edge table (aliased as 'e'), not through the
        entity field aliasing system used for JOINs.

        Example:
            Input: QueryExpressionProperty(variable_name="e", property_name="amount")
            _render_expression output: "__e_amount" (wrong for CTE)
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
