"""SQL Renderer - Converts logical plan to T-SQL."""

from __future__ import annotations

from enum import Enum, auto
from typing import Any

from opencypher_transpiler.common.exceptions import (
    TranspilerInternalErrorException,
    TranspilerNotSupportedException,
)
from opencypher_transpiler.common.logging import ILoggable
from opencypher_transpiler.parser.ast import (
    QueryExpression,
    QueryExpressionAggregationFunction,
    QueryExpressionBinary,
    QueryExpressionCaseExpression,
    QueryExpressionFunction,
    QueryExpressionList,
    QueryExpressionProperty,
    QueryExpressionValue,
)
from opencypher_transpiler.parser.operators import (
    AggregationFunction,
    BinaryOperator,
    Function,
)
from opencypher_transpiler.planner.logical_plan import LogicalPlan
from opencypher_transpiler.planner.operators import (
    DataSourceOperator,
    JoinKeyPairType,
    JoinOperator,
    JoinType,
    LogicalOperator,
    ProjectionOperator,
    SelectionOperator,
    SetOperationType,
    SetOperator,
)
from opencypher_transpiler.planner.schema import EntityField, EntityType, ValueField
from opencypher_transpiler.renderer.schema_provider import ISQLDBSchemaProvider


class SqlDbType(Enum):
    """SQL Server data types."""

    INT = auto()
    SMALLINT = auto()
    BIGINT = auto()
    FLOAT = auto()
    NVARCHAR = auto()
    DATETIME2 = auto()
    BIT = auto()
    UNIQUEIDENTIFIER = auto()
    TINYINT = auto()
    BINARY = auto()
    DECIMAL = auto()


# Mapping from Python types to SQL types
TYPE_TO_SQL_TYPE: dict[type[Any], SqlDbType] = {
    int: SqlDbType.BIGINT,
    float: SqlDbType.FLOAT,
    str: SqlDbType.NVARCHAR,
    bool: SqlDbType.BIT,
    bytes: SqlDbType.BINARY,
}

# Mapping from SQL types to their string representations
SQL_TYPE_RENDERING: dict[SqlDbType, str] = {
    SqlDbType.INT: "int",
    SqlDbType.SMALLINT: "smallint",
    SqlDbType.BIGINT: "bigint",
    SqlDbType.FLOAT: "float",
    SqlDbType.NVARCHAR: "nvarchar(MAX)",
    SqlDbType.DATETIME2: "datetime2",
    SqlDbType.BIT: "bit",
    SqlDbType.UNIQUEIDENTIFIER: "uniqueidentifier",
    SqlDbType.TINYINT: "tinyint",
    SqlDbType.BINARY: "binary",
    SqlDbType.DECIMAL: "decimal",
}

# Mapping from binary operators to SQL patterns
OPERATOR_PATTERNS: dict[BinaryOperator, str] = {
    BinaryOperator.PLUS: "({0})+({1})",
    BinaryOperator.MINUS: "({0})-({1})",
    BinaryOperator.MULTIPLY: "({0})*({1})",
    BinaryOperator.DIVIDE: "({0})/({1})",
    BinaryOperator.MODULO: "({0})%({1})",
    BinaryOperator.EXPONENTIATION: "CAST(POWER({0},{1}) AS float)",
    BinaryOperator.AND: "({0}) AND ({1})",
    BinaryOperator.OR: "({0}) OR ({1})",
    BinaryOperator.XOR: "(({0}) AND NOT ({1})) OR (NOT ({0}) AND ({1}))",
    BinaryOperator.LT: "({0})<({1})",
    BinaryOperator.LEQ: "({0})<=({1})",
    BinaryOperator.GT: "({0})>({1})",
    BinaryOperator.GEQ: "({0})>=({1})",
    BinaryOperator.EQ: "({0})=({1})",
    BinaryOperator.NEQ: "({0})!=({1})",
    BinaryOperator.REGMATCH: "PATINDEX('%{1}%', {0})",
    BinaryOperator.IN: "({0}) IN {1}",
}

# Mapping from aggregation functions to SQL patterns
AGGREGATION_PATTERNS: dict[AggregationFunction, str] = {
    AggregationFunction.AVG: "AVG(CAST({0} AS float))",
    AggregationFunction.SUM: "SUM({0})",
    AggregationFunction.MIN: "MIN({0})",
    AggregationFunction.MAX: "MAX({0})",
    AggregationFunction.FIRST: "MIN({0})",
    AggregationFunction.LAST: "MAX({0})",
    AggregationFunction.STDEV: "STDEV({0})",
    AggregationFunction.STDEVP: "STDEVP({0})",
    AggregationFunction.COUNT: "COUNT({0})",
}


class SQLRenderer:
    """
    Renders a logical plan to T-SQL.

    This class converts the relational algebra operators in the logical plan
    into equivalent T-SQL query statements.
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

    def render_plan(self, plan: LogicalPlan) -> str:
        """
        Render a logical plan to T-SQL.

        Args:
            plan: The logical plan to render.

        Returns:
            The rendered T-SQL query string.
        """
        if not plan.terminal_operators:
            return ""

        # Render from the terminal operator
        terminal_op = plan.terminal_operators[0]
        return self._render_operator(terminal_op, depth=0)

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
        else:
            raise TranspilerNotSupportedException(
                f"Operator type {type(op).__name__}"
            )

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
        for ref_name in entity_field.referenced_field_aliases:
            # Skip key fields (already added)
            if entity_field.node_join_field and ref_name == entity_field.node_join_field.field_alias:
                continue
            if entity_field.rel_source_join_field and ref_name == entity_field.rel_source_join_field.field_alias:
                continue
            if entity_field.rel_sink_join_field and ref_name == entity_field.rel_sink_join_field.field_alias:
                continue

            field_alias = self._get_field_name(entity_field.field_alias, ref_name)
            field_lines.append(f"{ref_name} AS {field_alias}")

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

        lines.append(f"{indent}SELECT")

        # Determine output fields from both sides
        output_fields = self._get_join_output_fields(op, left_op, right_op, left_var, right_var)
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
            join_keyword = "INNER JOIN" if op.join_type == JoinType.INNER else "LEFT JOIN"
            lines.append(f"{indent}{join_keyword} (")
            lines.append(self._render_operator(right_op, depth + 1))
            lines.append(f"{indent}) AS {right_var} ON")

            # Render join conditions
            conditions = self._render_join_conditions(
                op, left_op, right_op, left_var, right_var
            )
            for i, cond in enumerate(conditions):
                prefix = "  " if i == 0 else "  AND "
                lines.append(f"{indent}{prefix}{cond}")

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
                            field.field_alias, field.rel_source_join_field.field_alias
                        )
                        fields.append(f"{var_name}.{src_key} AS {src_key}")
                    if field.rel_sink_join_field:
                        sink_key = self._get_field_name(
                            field.field_alias, field.rel_sink_join_field.field_alias
                        )
                        fields.append(f"{var_name}.{sink_key} AS {sink_key}")

                # Output referenced fields
                for ref_name in field.referenced_field_aliases:
                    if field.node_join_field and ref_name == field.node_join_field.field_alias:
                        continue
                    if field.rel_source_join_field and ref_name == field.rel_source_join_field.field_alias:
                        continue
                    if field.rel_sink_join_field and ref_name == field.rel_sink_join_field.field_alias:
                        continue
                    field_alias = self._get_field_name(field.field_alias, ref_name)
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
                None
            )
            rel_field = next(
                (f for f in op.input_schema if f.field_alias == rel_alias),
                None
            )

            if not node_field or not rel_field:
                continue

            if isinstance(node_field, EntityField) and isinstance(rel_field, EntityField):
                node_key = self._get_field_name(
                    node_alias,
                    node_field.node_join_field.field_alias if node_field.node_join_field else "id"
                )

                if pair.pair_type == JoinKeyPairType.SOURCE:
                    rel_key = self._get_field_name(
                        rel_alias,
                        rel_field.rel_source_join_field.field_alias if rel_field.rel_source_join_field else "source_id"
                    )
                elif pair.pair_type == JoinKeyPairType.SINK:
                    rel_key = self._get_field_name(
                        rel_alias,
                        rel_field.rel_sink_join_field.field_alias if rel_field.rel_sink_join_field else "sink_id"
                    )
                elif pair.pair_type == JoinKeyPairType.NODE_ID:
                    # Node to node join
                    rel_key = self._get_field_name(
                        rel_alias,
                        rel_field.node_join_field.field_alias if rel_field.node_join_field else "id"
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

        return conditions if conditions else ["1 = 1"]

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
            where_clause = self._render_expression(op.filter_expression, op)
            lines.append(f"{indent}WHERE {where_clause}")

        return "\n".join(lines)

    def _render_projection(self, op: ProjectionOperator, depth: int) -> str:
        """Render a projection (SELECT/RETURN) operator."""
        lines: list[str] = []
        indent = self._indent(depth)

        if not op.in_operator:
            return ""

        distinct = "DISTINCT " if op.is_distinct else ""
        lines.append(f"{indent}SELECT {distinct}")

        # Render projected fields
        field_lines: list[str] = []
        for alias, expr in op.projections:
            rendered = self._render_expression(expr, op)
            field_lines.append(f"{rendered} AS [{alias}]")

        for i, field_line in enumerate(field_lines):
            prefix = " " if i == 0 else ","
            lines.append(f"{indent}  {prefix}{field_line}")

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

        # Limit and skip (using OFFSET FETCH for SQL Server 2012+)
        if op.limit is not None or op.skip is not None:
            # Need ORDER BY for OFFSET FETCH
            if not op.order_by:
                lines.append(f"{indent}ORDER BY (SELECT NULL)")

            skip_val = op.skip or 0
            lines.append(f"{indent}OFFSET {skip_val} ROWS")

            if op.limit is not None:
                lines.append(f"{indent}FETCH NEXT {op.limit} ROWS ONLY")

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
        else:
            return str(expr)

    def _render_value(self, expr: QueryExpressionValue) -> str:
        """Render a literal value."""
        if expr.value is None:
            return "NULL"
        if isinstance(expr.value, str):
            escaped = expr.value.replace("'", "''")
            return f"N'{escaped}'"
        if isinstance(expr.value, bool):
            return "1" if expr.value else "0"
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
        """Render a function call."""
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
            return f"CAST({params[0]} AS nvarchar(MAX))" if params else "NULL"
        elif func == Function.TO_INTEGER:
            return f"CAST({params[0]} AS int)" if params else "NULL"
        elif func == Function.TO_FLOAT:
            return f"CAST({params[0]} AS float)" if params else "NULL"
        elif func == Function.TO_BOOLEAN:
            return f"CAST({params[0]} AS bit)" if params else "NULL"
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
            return f"LEN({params[0]})" if params else "NULL"
        elif func == Function.STRING_LEFT:
            return f"LEFT({params[0]}, {params[1]})" if len(params) >= 2 else "NULL"
        elif func == Function.STRING_RIGHT:
            return f"RIGHT({params[0]}, {params[1]})" if len(params) >= 2 else "NULL"
        elif func == Function.STRING_STARTS_WITH:
            if len(params) >= 2:
                return f"({params[0]} LIKE {params[1]} + '%')"
            return "NULL"
        elif func == Function.STRING_ENDS_WITH:
            if len(params) >= 2:
                return f"({params[0]} LIKE '%' + {params[1]})"
            return "NULL"
        elif func == Function.STRING_CONTAINS:
            if len(params) >= 2:
                return f"({params[0]} LIKE '%' + {params[1]} + '%')"
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

        pattern = AGGREGATION_PATTERNS.get(
            expr.aggregation_function, "{0}"
        )
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

    def _has_aggregation(self, expr: QueryExpression) -> bool:
        """Check if an expression contains an aggregation function."""
        if isinstance(expr, QueryExpressionAggregationFunction):
            return True
        if isinstance(expr, QueryExpressionBinary):
            left_has = self._has_aggregation(expr.left_expression) if expr.left_expression else False
            right_has = self._has_aggregation(expr.right_expression) if expr.right_expression else False
            return left_has or right_has
        if isinstance(expr, QueryExpressionFunction):
            return any(self._has_aggregation(p) for p in expr.parameters)
        return False

    def _get_field_name(self, prefix: str, field_name: str) -> str:
        """Generate a field name with entity prefix."""
        clean_prefix = "".join(c if c.isalnum() or c == "_" else "" for c in prefix)
        return f"__{clean_prefix}_{field_name}"
