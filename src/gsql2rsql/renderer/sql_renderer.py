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
    NodeEntity,
    QueryExpression,
    QueryExpressionAggregationFunction,
    QueryExpressionBinary,
    QueryExpressionCaseExpression,
    QueryExpressionExists,
    QueryExpressionFunction,
    QueryExpressionList,
    QueryExpressionProperty,
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
)
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

                # Get column names from first edge schema
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

        # Build the recursive CTE
        lines: list[str] = []
        lines.append(f"  {cte_name} AS (")

        # If min_depth is 0, add zero-length path base case first
        if min_depth == 0:
            # Get source node table descriptor
            source_table = self._graph_def.get_sql_table_descriptors(op.source_node_type)
            if not source_table:
                raise TranspilerInternalErrorException(
                    f"No table descriptor for source node: {op.source_node_type}"
                )

            lines.append("    -- Base case: Zero-length paths (depth = 0)")
            lines.append("    SELECT")
            lines.append(f"      n.{op.source_id_column} AS start_node,")
            lines.append(f"      n.{op.source_id_column} AS end_node,")
            lines.append("      0 AS depth,")
            lines.append(f"      ARRAY(n.{op.source_id_column}) AS path,")
            lines.append("      ARRAY() AS visited")
            lines.append(f"    FROM {source_table.full_table_name} n")

            # Add start_node_filter if present
            if op.start_node_filter:
                filter_sql = self._render_expression(op.start_node_filter, op)
                lines.append(f"    WHERE n.{op.source_id_column} = {filter_sql}")

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
            base_sql.append(f"      ARRAY(e.{source_id_col}) AS visited")
            base_sql.append(f"    FROM {table_name} e")

            # Build WHERE clause
            where_parts = []
            if combined_filter:
                where_parts.append(f"({combined_filter})")
            if op.start_node_filter:
                where_parts.append(f"e.{source_id_col} = {op.start_node_filter}")
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
                base_sql.append(f"      ARRAY(e.{source_id_col}) AS visited")
                base_sql.append(f"    FROM {edge_table.full_table_name} e")

                filters = []
                if edge_table.filter:
                    filters.append(edge_table.filter)
                if op.start_node_filter:
                    filters.append(f"e.{source_id_col} = {op.start_node_filter}")
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
        Render a JOIN between recursive CTE and target node table.

        Generates proper JOIN condition connecting end_node to target id.
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

        # Get target node's ID column
        node_schema = self._graph_def.get_node_definition(target_entity.entity_name)
        if node_schema and node_schema.node_id_property:
            target_id_col = node_schema.node_id_property.property_name
        else:
            target_id_col = "id"

        # Get alias for target node
        target_alias = target_entity.alias or "n"
        # Get alias for source node from recursive op
        source_alias = recursive_op.source_alias or "src"

        lines: list[str] = []
        lines.append(f"{indent}SELECT")

        # Project fields from target node
        # Get all properties from the node schema
        field_lines: list[str] = []
        if node_schema:
            field_lines.append(f"n.{target_id_col} AS __{target_alias}_{target_id_col}")
            for prop in node_schema.properties:
                prop_name = prop.property_name
                if prop_name != target_id_col:
                    field_lines.append(f"n.{prop_name} AS __{target_alias}_{prop_name}")
        else:
            field_lines.append(f"n.{target_id_col} AS __{target_alias}_id")

        # Project source node's id (start_node) with proper alias for WHERE
        field_lines.append(f"p.start_node AS __{source_alias}_id")

        # Also include path info from CTE
        field_lines.append("p.start_node")
        field_lines.append("p.end_node")
        field_lines.append("p.depth")
        field_lines.append("p.path")

        for i, field in enumerate(field_lines):
            prefix = " " if i == 0 else ","
            lines.append(f"{indent}  {prefix}{field}")

        # FROM recursive CTE
        lines.append(f"{indent}FROM {cte_name} p")

        # JOIN with target node table
        lines.append(f"{indent}JOIN {target_table.full_table_name} n")
        lines.append(f"{indent}  ON n.{target_id_col} = p.end_node")

        # WHERE clause for depth bounds
        where_parts = [f"p.depth >= {min_depth}"]
        if recursive_op.max_hops is not None:
            where_parts.append(f"p.depth <= {recursive_op.max_hops}")
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

        # Add implicit filter if defined (e.g., edge_type = 'KNOWS')
        if table_desc.filter:
            lines.append(f"{indent}WHERE {table_desc.filter}")

        return "\n".join(lines)

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
                        fields.append(f"{var_name}.{field_alias} AS {field_alias}")

            elif isinstance(field, ValueField):
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
                    # Either - try source first
                    if rel_field.rel_source_join_field:
                        rel_key = self._get_field_name(
                            rel_alias, rel_field.rel_source_join_field.field_alias
                        )
                    elif rel_field.rel_sink_join_field:
                        rel_key = self._get_field_name(
                            rel_alias, rel_field.rel_sink_join_field.field_alias
                        )
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

    def _render_projection(self, op: ProjectionOperator, depth: int) -> str:
        """Render a projection (SELECT) operator."""
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
        else:
            # Unknown function
            params_str = ", ".join(params)
            return f"{func.name}({params_str})"

    def _render_aggregation(
        self, expr: QueryExpressionAggregationFunction, context_op: LogicalOperator
    ) -> str:
        """Render an aggregation function."""
        inner = (
            self._render_expression(expr.inner_expression, context_op)
            if expr.inner_expression
            else "*"
        )

        if expr.is_distinct:
            inner = f"DISTINCT {inner}"

        pattern = AGGREGATION_PATTERNS.get(expr.aggregation_function, "{0}")
        return pattern.format(inner)

    def _render_list(
        self, expr: QueryExpressionList, context_op: LogicalOperator
    ) -> str:
        """Render a list expression."""
        items = [self._render_expression(item, context_op) for item in expr.items]
        return f"({', '.join(items)})"

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
        clean_prefix = "".join(c if c.isalnum() or c == "_" else "" for c in prefix)
        return f"__{clean_prefix}_{field_name}"
