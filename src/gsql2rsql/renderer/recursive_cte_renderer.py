"""Recursive CTE renderer — WITH RECURSIVE generation for variable-length paths.

Handles rendering of recursive CTEs for BFS/DFS variable-length path
traversals, bidirectional BFS (both recursive and unrolled variants),
edge table lookups, filter clause generation, and aggregation boundary CTEs.
"""

from __future__ import annotations

from dataclasses import dataclass

from gsql2rsql.common.exceptions import (
    TranspilerInternalErrorException,
    TranspilerNotSupportedException,
)
from gsql2rsql.common.schema import EdgeSchema, WILDCARD_EDGE_TYPE
from gsql2rsql.parser.ast import (
    QueryExpression,
    RelationshipDirection,
)
from gsql2rsql.planner.operators import (
    AggregationBoundaryOperator,
    RecursiveTraversalOperator,
)
from gsql2rsql.planner.path_analyzer import rewrite_predicate_for_edge_alias
from gsql2rsql.planner.schema import Schema

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gsql2rsql.renderer.expression_renderer import ExpressionRenderer
    from gsql2rsql.renderer.render_context import RenderContext
    from gsql2rsql.renderer.schema_provider import SQLTableDescriptor


@dataclass
class EdgeInfo:
    """Parameter object replacing closure-captured variables in CTE generation.

    Holds all resolved edge table information, column names, filters, and
    directional flags needed by the base-case and recursive-case builders.
    """

    cte_name: str
    edge_tables: list[tuple[str, "SQLTableDescriptor"]]
    source_id_col: str
    target_id_col: str
    edge_props: list[str]
    single_table: bool
    single_table_name: str | None
    single_table_filter: str | None
    min_depth: int
    max_depth: int
    is_backward: bool
    needs_union_for_undirected: bool
    edge_filter_sql: str | None = None
    source_node_filter_sql: str | None = None
    source_node_table: "SQLTableDescriptor | None" = None


class RecursiveCTERenderer:
    """Renders WITH RECURSIVE CTEs for variable-length path traversal.

    Receives a ``RenderContext`` for shared state and an ``ExpressionRenderer``
    for rendering edge filter expressions within CTE bodies.
    """

    def __init__(
        self,
        ctx: "RenderContext",
        expr: "ExpressionRenderer",
        render_operator_fn=None,
    ) -> None:
        self._ctx = ctx
        self._expr = expr
        self._render_operator = render_operator_fn

    def _render_recursive_cte(self, op: RecursiveTraversalOperator) -> str:
        """Render a recursive CTE for variable-length path traversal.

        Generates Databricks SQL WITH RECURSIVE for BFS/DFS traversal.
        Supports multiple edge types, predicate pushdown (edge and source
        node filters), and undirected traversal via internal UNION ALL.
        """
        # Bidirectional BFS dispatch
        if op.bidirectional_bfs_mode == "recursive":
            return self._render_bidirectional_recursive_cte(op)
        elif op.bidirectional_bfs_mode == "unrolling":
            return self._render_bidirectional_unrolling_cte(op)

        # Resolve edge tables and build EdgeInfo parameter object
        ei = self._resolve_edge_tables(op)

        # Resolve filter clauses (edge predicate pushdown + source node filter)
        self._resolve_filter_clauses(op, ei)

        # Assemble CTE
        lines: list[str] = []
        lines.append(f"  {ei.cte_name} AS (")

        # Zero-length path base case (min_depth == 0)
        if ei.min_depth == 0:
            self._append_zero_length_base_case(op, ei, lines)

        # Base case: direct edges (depth = 1)
        lines.append("    -- Base case: direct edges (depth = 1)")
        self._append_base_cases(op, ei, lines)

        lines.append("")
        lines.append("    UNION ALL")
        lines.append("")
        lines.append("    -- Recursive case: extend paths")

        # Recursive case: extend paths
        self._append_recursive_cases(op, ei, lines)

        lines.append("  )")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # _render_recursive_cte sub-methods
    # ------------------------------------------------------------------

    def _resolve_edge_tables(
        self, op: RecursiveTraversalOperator
    ) -> EdgeInfo:
        """Resolve edge table descriptors and build an EdgeInfo."""
        self._ctx.cte_counter += 1
        cte_name = f"paths_{self._ctx.cte_counter}"
        op.cte_name = cte_name

        edge_types = op.edge_types if op.edge_types else []
        edge_tables: list[tuple[str, SQLTableDescriptor]] = []
        source_id_col = op.source_id_column or "source_id"
        target_id_col = op.target_id_column or "target_id"
        edge_props: list[str] = list(op.edge_properties) if op.edge_properties else []

        if edge_types:
            for edge_type in edge_types:
                edge_table = None
                edge_schema = None

                if not op.source_node_type or not op.target_node_type:
                    edges = self._ctx.db_schema.find_edges_by_verb(
                        edge_type,
                        from_node_name=op.source_node_type or None,
                        to_node_name=op.target_node_type or None,
                    )
                    if edges:
                        edge_schema = edges[0]
                        edge_table = self._ctx.db_schema.get_sql_table_descriptors(
                            edge_schema.id
                        )
                else:
                    edge_id = EdgeSchema.get_edge_id(
                        edge_type,
                        op.source_node_type,
                        op.target_node_type,
                    )
                    edge_table = self._ctx.db_schema.get_sql_table_descriptors(edge_id)
                    if not edge_table:
                        edge_table = self._ctx.db_schema.get_sql_table_descriptors(edge_type)

                    edge_schema = self._ctx.db_schema.get_edge_definition(
                        edge_type,
                        op.source_node_type,
                        op.target_node_type,
                    )

                if edge_table:
                    edge_tables.append((edge_type, edge_table))

                if len(edge_tables) == 1 and edge_schema:
                    if edge_schema.source_id_property:
                        source_id_col = edge_schema.source_id_property.property_name
                    if edge_schema.sink_id_property:
                        target_id_col = edge_schema.sink_id_property.property_name
                    if op.collect_edges and not edge_props:
                        for prop in edge_schema.properties:
                            if prop.property_name not in (source_id_col, target_id_col):
                                edge_props.append(prop.property_name)
        else:
            wildcard_edge_table = self._ctx.db_schema.get_wildcard_edge_table_descriptor()
            if not wildcard_edge_table:
                raise TranspilerInternalErrorException(
                    "Variable-length path without edge type requires wildcard edge support. "
                    "Please specify at least one edge type, e.g., -[:KNOWS*1..3]->"
                )
            edge_tables.append((WILDCARD_EDGE_TYPE, wildcard_edge_table))

            wildcard_edge_schema = self._ctx.db_schema.get_wildcard_edge_definition()
            if wildcard_edge_schema:
                if wildcard_edge_schema.source_id_property:
                    source_id_col = wildcard_edge_schema.source_id_property.property_name
                if wildcard_edge_schema.sink_id_property:
                    target_id_col = wildcard_edge_schema.sink_id_property.property_name
                if op.collect_edges and not edge_props:
                    for prop in wildcard_edge_schema.properties:
                        if prop.property_name not in (source_id_col, target_id_col):
                            edge_props.append(prop.property_name)

        if not edge_tables:
            edge_type_str = "|".join(edge_types)
            raise TranspilerInternalErrorException(
                f"No table descriptor for edges: {edge_type_str}"
            )

        # Determine single-table optimisation
        table_to_filters: dict[str, list[str]] = {}
        for edge_type, edge_table in edge_tables:
            tname = edge_table.full_table_name
            if tname not in table_to_filters:
                table_to_filters[tname] = []
            if edge_table.filter:
                table_to_filters[tname].append(edge_table.filter)

        single_table = len(table_to_filters) == 1
        single_table_name: str | None = None
        single_table_filter: str | None = None
        if single_table:
            single_table_name = list(table_to_filters.keys())[0]
            filters_list = table_to_filters[single_table_name]
            if len(filters_list) > 1:
                single_table_filter = " OR ".join(f"({f})" for f in filters_list)
            elif len(filters_list) == 1:
                single_table_filter = filters_list[0]

        return EdgeInfo(
            cte_name=cte_name,
            edge_tables=edge_tables,
            source_id_col=source_id_col,
            target_id_col=target_id_col,
            edge_props=edge_props,
            single_table=single_table,
            single_table_name=single_table_name,
            single_table_filter=single_table_filter,
            min_depth=op.min_hops if op.min_hops is not None else 1,
            max_depth=op.max_hops or 10,
            is_backward=op.swap_source_sink,
            needs_union_for_undirected=op.use_internal_union_for_bidirectional,
        )

    def _resolve_filter_clauses(
        self, op: RecursiveTraversalOperator, ei: EdgeInfo
    ) -> None:
        """Resolve edge and source-node filter clauses into *ei*."""
        # Edge predicate pushdown
        if op.edge_filter and op.edge_filter_lambda_var:
            rewritten = rewrite_predicate_for_edge_alias(
                op.edge_filter, op.edge_filter_lambda_var, edge_alias="e",
            )
            ei.edge_filter_sql = self._expr._render_edge_filter_expression(rewritten)

        # Source node filter pushdown
        if op.start_node_filter:
            source_table = self._ctx.get_table_descriptor_with_wildcard(
                op.source_node_type
            )
            if source_table:
                rewritten = rewrite_predicate_for_edge_alias(
                    op.start_node_filter, op.source_alias, edge_alias="src",
                )
                ei.source_node_filter_sql = self._expr._render_edge_filter_expression(
                    rewritten
                )
                ei.source_node_table = source_table

    def _build_edge_struct(self, ei: EdgeInfo, alias: str = "e") -> str:
        """Build NAMED_STRUCT expression for an edge with its properties."""
        struct_parts = [
            f"'{ei.source_id_col}', {alias}.{ei.source_id_col}",
            f"'{ei.target_id_col}', {alias}.{ei.target_id_col}",
        ]
        for prop in ei.edge_props:
            struct_parts.append(f"'{prop}', {alias}.{prop}")
        return f"NAMED_STRUCT({', '.join(struct_parts)})"

    def _append_zero_length_base_case(
        self,
        op: RecursiveTraversalOperator,
        ei: EdgeInfo,
        lines: list[str],
    ) -> None:
        """Append zero-length path base case (depth = 0) to *lines*."""
        source_table = self._ctx.get_table_descriptor_with_wildcard(
            op.source_node_type
        )
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
        if op.collect_edges:
            lines.append("      ARRAY() AS path_edges,")
        lines.append("      ARRAY() AS visited")
        lines.append(f"    FROM {source_table.full_table_name} n")

        if op.start_node_filter:
            rewritten = rewrite_predicate_for_edge_alias(
                op.start_node_filter, op.source_alias, edge_alias="n",
            )
            filter_sql = self._expr._render_edge_filter_expression(rewritten)
            lines.append(f"    WHERE {filter_sql}")

        lines.append("")
        lines.append("    UNION ALL")
        lines.append("")

    def _build_base_case_select(
        self,
        op: RecursiveTraversalOperator,
        ei: EdgeInfo,
        table_name: str,
        src_col: str,
        dst_col: str,
        filter_clause: str | None,
        direction_label: str = "",
    ) -> str:
        """Build a single base case SELECT for one direction."""
        base_sql: list[str] = []
        if direction_label:
            base_sql.append(f"    -- {direction_label}")
        base_sql.append("    SELECT")
        base_sql.append(f"      e.{src_col} AS start_node,")
        base_sql.append(f"      e.{dst_col} AS end_node,")
        base_sql.append("      1 AS depth,")
        base_sql.append(f"      ARRAY(e.{src_col}, e.{dst_col}) AS path,")
        if op.collect_edges:
            base_sql.append(f"      ARRAY({self._build_edge_struct(ei)}) AS path_edges,")
        base_sql.append(f"      ARRAY(e.{src_col}) AS visited")
        base_sql.append(f"    FROM {table_name} e")

        if ei.source_node_filter_sql and ei.source_node_table:
            base_sql.append(
                f"    JOIN {ei.source_node_table.full_table_name} src "
                f"ON src.{op.source_id_column} = e.{src_col}"
            )

        where_parts: list[str] = []
        if filter_clause:
            where_parts.append(f"({filter_clause})")
        if ei.source_node_filter_sql:
            where_parts.append(ei.source_node_filter_sql)
        if ei.edge_filter_sql:
            where_parts.append(ei.edge_filter_sql)
        if where_parts:
            base_sql.append(f"    WHERE {' AND '.join(where_parts)}")

        return "\n".join(base_sql)

    def _append_base_cases(
        self,
        op: RecursiveTraversalOperator,
        ei: EdgeInfo,
        lines: list[str],
    ) -> None:
        """Append base case SELECT(s) to *lines*."""
        src = ei.source_id_col
        dst = ei.target_id_col

        if ei.single_table:
            assert ei.single_table_name is not None
            if ei.needs_union_for_undirected:
                fwd = self._build_base_case_select(
                    op, ei, ei.single_table_name, src, dst,
                    ei.single_table_filter, "Forward direction",
                )
                bwd = self._build_base_case_select(
                    op, ei, ei.single_table_name, dst, src,
                    ei.single_table_filter, "Backward direction",
                )
                lines.append("    SELECT * FROM (")
                lines.append(fwd)
                lines.append("")
                lines.append("      UNION ALL")
                lines.append("")
                lines.append(bwd)
                lines.append("    )")
            elif ei.is_backward:
                lines.append(self._build_base_case_select(
                    op, ei, ei.single_table_name, dst, src, ei.single_table_filter,
                ))
            else:
                lines.append(self._build_base_case_select(
                    op, ei, ei.single_table_name, src, dst, ei.single_table_filter,
                ))
        else:
            base_cases: list[str] = []
            for edge_type, edge_table in ei.edge_tables:
                fc = edge_table.filter
                if ei.needs_union_for_undirected:
                    base_cases.append(self._build_base_case_select(
                        op, ei, edge_table.full_table_name, src, dst,
                        fc, f"Forward: {edge_type}",
                    ))
                    base_cases.append(self._build_base_case_select(
                        op, ei, edge_table.full_table_name, dst, src,
                        fc, f"Backward: {edge_type}",
                    ))
                elif ei.is_backward:
                    base_cases.append(self._build_base_case_select(
                        op, ei, edge_table.full_table_name, dst, src, fc,
                    ))
                else:
                    base_cases.append(self._build_base_case_select(
                        op, ei, edge_table.full_table_name, src, dst, fc,
                    ))
            lines.append("    SELECT * FROM (")
            lines.append("\n      UNION ALL\n".join(base_cases))
            lines.append("    )")

    def _build_recursive_case_select(
        self,
        op: RecursiveTraversalOperator,
        ei: EdgeInfo,
        table_name: str,
        join_col: str,
        end_col: str,
        visited_col: str,
        filter_clause: str | None,
        direction_label: str = "",
    ) -> str:
        """Build a single recursive case SELECT for one direction."""
        rec: list[str] = []
        if direction_label:
            rec.append(f"    -- {direction_label}")
        rec.append("    SELECT")
        rec.append("      p.start_node,")
        rec.append(f"      e.{end_col} AS end_node,")
        rec.append("      p.depth + 1 AS depth,")
        rec.append(f"      CONCAT(p.path, ARRAY(e.{end_col})) AS path,")
        if op.collect_edges:
            rec.append(
                f"      ARRAY_APPEND(p.path_edges, {self._build_edge_struct(ei)}) AS path_edges,"
            )
        rec.append(f"      CONCAT(p.visited, ARRAY(e.{visited_col})) AS visited")
        rec.append(f"    FROM {ei.cte_name} p")
        rec.append(f"    JOIN {table_name} e")
        rec.append(f"      ON p.end_node = e.{join_col}")

        where_parts = [
            f"p.depth < {ei.max_depth}",
            f"NOT ARRAY_CONTAINS(p.visited, e.{end_col})",
        ]
        if filter_clause:
            where_parts.append(f"({filter_clause})")
        if ei.edge_filter_sql:
            where_parts.append(ei.edge_filter_sql)
        rec.append(f"    WHERE {where_parts[0]}")
        for wp in where_parts[1:]:
            rec.append(f"      AND {wp}")

        return "\n".join(rec)

    def _append_recursive_cases(
        self,
        op: RecursiveTraversalOperator,
        ei: EdgeInfo,
        lines: list[str],
    ) -> None:
        """Append recursive case SELECT(s) to *lines*."""
        src = ei.source_id_col
        dst = ei.target_id_col

        if ei.single_table:
            assert ei.single_table_name is not None
            if ei.needs_union_for_undirected:
                fwd = self._build_recursive_case_select(
                    op, ei, ei.single_table_name, src, dst, src,
                    ei.single_table_filter, "Forward direction",
                )
                bwd = self._build_recursive_case_select(
                    op, ei, ei.single_table_name, dst, src, dst,
                    ei.single_table_filter, "Backward direction",
                )
                lines.append("    SELECT * FROM (")
                lines.append(fwd)
                lines.append("")
                lines.append("      UNION ALL")
                lines.append("")
                lines.append(bwd)
                lines.append("    )")
            elif ei.is_backward:
                lines.append(self._build_recursive_case_select(
                    op, ei, ei.single_table_name, dst, src, dst,
                    ei.single_table_filter,
                ))
            else:
                lines.append(self._build_recursive_case_select(
                    op, ei, ei.single_table_name, src, dst, src,
                    ei.single_table_filter,
                ))
        else:
            recursive_cases: list[str] = []
            for edge_type, edge_table in ei.edge_tables:
                fc = edge_table.filter
                if ei.needs_union_for_undirected:
                    recursive_cases.append(self._build_recursive_case_select(
                        op, ei, edge_table.full_table_name, src, dst, src,
                        fc, f"Forward: {edge_type}",
                    ))
                    recursive_cases.append(self._build_recursive_case_select(
                        op, ei, edge_table.full_table_name, dst, src, dst,
                        fc, f"Backward: {edge_type}",
                    ))
                elif ei.is_backward:
                    recursive_cases.append(self._build_recursive_case_select(
                        op, ei, edge_table.full_table_name, dst, src, dst, fc,
                    ))
                else:
                    recursive_cases.append(self._build_recursive_case_select(
                        op, ei, edge_table.full_table_name, src, dst, src, fc,
                    ))
            lines.append("    SELECT * FROM (")
            lines.append("\n      UNION ALL\n".join(recursive_cases))
            lines.append("    )")

    def _render_bidirectional_recursive_cte(
        self, op: RecursiveTraversalOperator
    ) -> str:
        """Render bidirectional BFS using WITH RECURSIVE forward/backward CTEs.

        This implements the recursive CTE approach for bidirectional BFS:
        - forward CTE: explores from source toward target
        - backward CTE: explores from target toward source
        - final: JOINs forward and backward where they meet

        The depth split is approximately half for each direction.

        Mathematical speedup:
        - Unidirectional: O(b^d) where b=branching factor, d=depth
        - Bidirectional: O(2 * b^(d/2))
        - Speedup: ~b^(d/2) / 2 (e.g., 500x for b=10, d=6)
        """

        self._ctx.cte_counter += 1
        cte_name = f"paths_{self._ctx.cte_counter}"
        op.cte_name = cte_name

        # Get configuration
        forward_depth = op.bidirectional_depth_forward or 5
        backward_depth = op.bidirectional_depth_backward or 5
        target_value = op.bidirectional_target_value
        min_depth = op.min_hops if op.min_hops is not None else 1
        max_depth = op.max_hops or 10

        # Get edge table info
        edge_table_name = self._get_edge_table_name(op)
        edge_filter_clause = self._get_edge_filter_clause(op)

        # Get edge column names from schema (src, dst columns on edges table)
        edge_src_col, edge_dst_col = self._get_edge_column_names(op)

        # Node ID column (for node table joins)
        node_id_col = op.source_id_column or "id"

        # Get source node table for filter
        source_node_table = self._ctx.get_table_descriptor_with_wildcard(
            op.source_node_type
        )
        source_filter_sql = self._render_source_filter_for_bidirectional(
            op, node_id_col
        )
        target_filter_sql = self._render_target_filter_for_bidirectional(
            op, node_id_col, target_value
        )

        # Build edge struct for path_edges collection
        edge_props: list[str] = list(op.edge_properties) if op.edge_properties else []
        struct_fields = [f"e.{edge_src_col} AS src", f"e.{edge_dst_col} AS dst"]
        for prop in edge_props:
            struct_fields.append(f"e.{prop}")
        edge_struct = f"NAMED_STRUCT({', '.join(repr(f.split(' AS ')[1] if ' AS ' in f else f.split('.')[1]) + ', ' + f.split(' AS ')[0] for f in struct_fields)})"
        # Simpler: just STRUCT with src, dst
        edge_struct = f"STRUCT(e.{edge_src_col} AS src, e.{edge_dst_col} AS dst)"

        lines: list[str] = []

        # Forward CTE: starts from source, explores forward
        lines.append(f"  forward_{cte_name} AS (")
        lines.append("    -- Depth 0: source node itself (for meeting with backward)")
        lines.append("    SELECT")
        lines.append(f"      src.{node_id_col} AS current_node,")
        lines.append("      0 AS depth,")
        lines.append(f"      ARRAY(src.{node_id_col}) AS path,")
        lines.append(
            "      CAST(ARRAY() AS ARRAY<STRUCT<src: STRING, dst: STRING>>) AS path_edges"
        )
        if source_node_table:
            lines.append(f"    FROM {source_node_table.full_table_name} src")
        else:
            # Fallback - use edge table to find source
            lines.append(f"    FROM {edge_table_name} e")
            lines.append(
                f"    JOIN (SELECT DISTINCT {edge_src_col} AS {node_id_col} "
                f"FROM {edge_table_name}) src ON 1=1"
            )
        if source_filter_sql:
            lines.append(f"    WHERE {source_filter_sql}")
        lines.append("")
        lines.append("    UNION ALL")
        lines.append("")

        # Check if undirected (explore both edge directions)
        is_undirected = op.direction == RelationshipDirection.BOTH

        # Depth 1+: explore outgoing edges from source
        lines.append("    -- Depth 1+: explore edges from source")
        if is_undirected:
            lines.append("    SELECT * FROM (")
        lines.append("    SELECT")
        lines.append(f"      e.{edge_dst_col} AS current_node,")
        lines.append("      1 AS depth,")
        lines.append(f"      ARRAY(e.{edge_src_col}, e.{edge_dst_col}) AS path,")
        lines.append(f"      ARRAY({edge_struct}) AS path_edges")
        lines.append(f"    FROM {edge_table_name} e")
        if source_node_table:
            lines.append(
                f"    JOIN {source_node_table.full_table_name} src "
                f"ON src.{node_id_col} = e.{edge_src_col}"
            )
        where_parts = []
        if edge_filter_clause:
            where_parts.append(f"({edge_filter_clause})")
        if source_filter_sql:
            where_parts.append(source_filter_sql)
        if where_parts:
            lines.append(f"    WHERE {' AND '.join(where_parts)}")

        # For undirected: add UNION ALL with reverse direction
        if is_undirected:
            lines.append("")
            lines.append("      UNION ALL")
            lines.append("")
            lines.append("    -- Reverse direction for undirected")
            lines.append("    SELECT")
            lines.append(f"      e.{edge_src_col} AS current_node,")
            lines.append("      1 AS depth,")
            lines.append(f"      ARRAY(e.{edge_dst_col}, e.{edge_src_col}) AS path,")
            lines.append(f"      ARRAY({edge_struct}) AS path_edges")
            lines.append(f"    FROM {edge_table_name} e")
            if source_node_table:
                lines.append(
                    f"    JOIN {source_node_table.full_table_name} src "
                    f"ON src.{node_id_col} = e.{edge_dst_col}"
                )
            where_parts_rev = []
            if edge_filter_clause:
                where_parts_rev.append(f"({edge_filter_clause})")
            if source_filter_sql:
                where_parts_rev.append(source_filter_sql)
            if where_parts_rev:
                lines.append(f"    WHERE {' AND '.join(where_parts_rev)}")
            lines.append("    )")

        lines.append("")
        lines.append("    UNION ALL")
        lines.append("")

        # Recursive case: extend forward
        lines.append("    -- Recursive case: extend forward")
        if is_undirected:
            lines.append("    SELECT * FROM (")
        lines.append("    SELECT")
        lines.append(f"      e.{edge_dst_col} AS current_node,")
        lines.append("      f.depth + 1 AS depth,")
        lines.append(f"      CONCAT(f.path, ARRAY(e.{edge_dst_col})) AS path,")
        lines.append(f"      CONCAT(f.path_edges, ARRAY({edge_struct})) AS path_edges")
        lines.append(f"    FROM forward_{cte_name} f")
        lines.append(f"    JOIN {edge_table_name} e")
        lines.append(f"      ON f.current_node = e.{edge_src_col}")
        lines.append(f"    WHERE f.depth < {forward_depth}")
        lines.append(f"      AND NOT ARRAY_CONTAINS(f.path, e.{edge_dst_col})")
        if edge_filter_clause:
            lines.append(f"      AND ({edge_filter_clause})")

        # For undirected: add UNION ALL with reverse direction
        if is_undirected:
            lines.append("")
            lines.append("      UNION ALL")
            lines.append("")
            lines.append("    -- Reverse direction for undirected")
            lines.append("    SELECT")
            lines.append(f"      e.{edge_src_col} AS current_node,")
            lines.append("      f.depth + 1 AS depth,")
            lines.append(f"      CONCAT(f.path, ARRAY(e.{edge_src_col})) AS path,")
            lines.append(f"      CONCAT(f.path_edges, ARRAY({edge_struct})) AS path_edges")
            lines.append(f"    FROM forward_{cte_name} f")
            lines.append(f"    JOIN {edge_table_name} e")
            lines.append(f"      ON f.current_node = e.{edge_dst_col}")
            lines.append(f"    WHERE f.depth < {forward_depth}")
            lines.append(f"      AND NOT ARRAY_CONTAINS(f.path, e.{edge_src_col})")
            if edge_filter_clause:
                lines.append(f"      AND ({edge_filter_clause})")
            lines.append("    )")

        lines.append("  ),")
        lines.append("")

        # Backward CTE: starts from target, explores backward
        # Get target node table for filter
        target_node_table = self._ctx.get_table_descriptor_with_wildcard(
            op.target_node_type
        )
        lines.append(f"  backward_{cte_name} AS (")
        lines.append("    -- Depth 0: target node itself (for meeting with forward)")
        lines.append("    SELECT")
        lines.append(f"      tgt.{node_id_col} AS current_node,")
        lines.append("      0 AS depth,")
        lines.append(f"      ARRAY(tgt.{node_id_col}) AS path,")
        lines.append(
            "      CAST(ARRAY() AS ARRAY<STRUCT<src: STRING, dst: STRING>>) AS path_edges"
        )
        if target_node_table:
            lines.append(f"    FROM {target_node_table.full_table_name} tgt")
        else:
            # Fallback - use edge table to find target
            lines.append(f"    FROM {edge_table_name} e")
            lines.append(
                f"    JOIN (SELECT DISTINCT {edge_dst_col} AS {node_id_col} "
                f"FROM {edge_table_name}) tgt ON 1=1"
            )
        if target_filter_sql:
            lines.append(f"    WHERE {target_filter_sql}")
        lines.append("")
        lines.append("    UNION ALL")
        lines.append("")

        # Depth 1+: explore incoming edges to target
        lines.append("    -- Depth 1+: explore edges to target")
        if is_undirected:
            lines.append("    SELECT * FROM (")
        lines.append("    SELECT")
        lines.append(f"      e.{edge_src_col} AS current_node,")
        lines.append("      1 AS depth,")
        lines.append(f"      ARRAY(e.{edge_src_col}, e.{edge_dst_col}) AS path,")
        lines.append(f"      ARRAY({edge_struct}) AS path_edges")
        lines.append(f"    FROM {edge_table_name} e")
        if target_node_table:
            lines.append(
                f"    JOIN {target_node_table.full_table_name} tgt "
                f"ON tgt.{node_id_col} = e.{edge_dst_col}"
            )
        where_parts_bwd = []
        if edge_filter_clause:
            where_parts_bwd.append(f"({edge_filter_clause})")
        if target_filter_sql:
            where_parts_bwd.append(target_filter_sql)
        if where_parts_bwd:
            lines.append(f"    WHERE {' AND '.join(where_parts_bwd)}")

        # For undirected: add UNION ALL with reverse direction
        if is_undirected:
            lines.append("")
            lines.append("      UNION ALL")
            lines.append("")
            lines.append("    -- Reverse direction for undirected")
            lines.append("    SELECT")
            lines.append(f"      e.{edge_dst_col} AS current_node,")
            lines.append("      1 AS depth,")
            lines.append(f"      ARRAY(e.{edge_dst_col}, e.{edge_src_col}) AS path,")
            lines.append(f"      ARRAY({edge_struct}) AS path_edges")
            lines.append(f"    FROM {edge_table_name} e")
            if target_node_table:
                lines.append(
                    f"    JOIN {target_node_table.full_table_name} tgt "
                    f"ON tgt.{node_id_col} = e.{edge_src_col}"
                )
            where_parts_bwd_rev = []
            if edge_filter_clause:
                where_parts_bwd_rev.append(f"({edge_filter_clause})")
            if target_filter_sql:
                where_parts_bwd_rev.append(target_filter_sql)
            if where_parts_bwd_rev:
                lines.append(f"    WHERE {' AND '.join(where_parts_bwd_rev)}")
            lines.append("    )")

        lines.append("")
        lines.append("    UNION ALL")
        lines.append("")

        # Recursive case: extend backward
        lines.append("    -- Recursive case: extend backward")
        if is_undirected:
            lines.append("    SELECT * FROM (")
        lines.append("    SELECT")
        lines.append(f"      e.{edge_src_col} AS current_node,")
        lines.append("      b.depth + 1 AS depth,")
        lines.append(f"      CONCAT(ARRAY(e.{edge_src_col}), b.path) AS path,")
        lines.append(f"      CONCAT(ARRAY({edge_struct}), b.path_edges) AS path_edges")
        lines.append(f"    FROM backward_{cte_name} b")
        lines.append(f"    JOIN {edge_table_name} e")
        lines.append(f"      ON b.current_node = e.{edge_dst_col}")
        lines.append(f"    WHERE b.depth < {backward_depth}")
        lines.append(f"      AND NOT ARRAY_CONTAINS(b.path, e.{edge_src_col})")
        if edge_filter_clause:
            lines.append(f"      AND ({edge_filter_clause})")

        # For undirected: add UNION ALL with reverse direction
        if is_undirected:
            lines.append("")
            lines.append("      UNION ALL")
            lines.append("")
            lines.append("    -- Reverse direction for undirected")
            lines.append("    SELECT")
            lines.append(f"      e.{edge_dst_col} AS current_node,")
            lines.append("      b.depth + 1 AS depth,")
            lines.append(f"      CONCAT(ARRAY(e.{edge_dst_col}), b.path) AS path,")
            lines.append(f"      CONCAT(ARRAY({edge_struct}), b.path_edges) AS path_edges")
            lines.append(f"    FROM backward_{cte_name} b")
            lines.append(f"    JOIN {edge_table_name} e")
            lines.append(f"      ON b.current_node = e.{edge_src_col}")
            lines.append(f"    WHERE b.depth < {backward_depth}")
            lines.append(f"      AND NOT ARRAY_CONTAINS(b.path, e.{edge_dst_col})")
            if edge_filter_clause:
                lines.append(f"      AND ({edge_filter_clause})")
            lines.append("    )")

        lines.append("  ),")
        lines.append("")

        # Final CTE: join forward and backward where they meet
        lines.append(f"  {cte_name} AS (")
        lines.append("    -- Intersection: paths that meet in the middle")
        lines.append("    -- Use DISTINCT to deduplicate paths found via different meeting points")
        lines.append("    SELECT DISTINCT")
        lines.append("      f.path[0] AS start_node,")
        lines.append("      b.path[SIZE(b.path) - 1] AS end_node,")
        lines.append("      f.depth + b.depth AS depth,")
        lines.append("      -- Combine paths: forward path (except last) + backward path")
        lines.append(
            "      CONCAT(SLICE(f.path, 1, SIZE(f.path) - 1), b.path) AS path,"
        )
        lines.append("      -- Combine path_edges from forward and backward")
        lines.append("      CONCAT(f.path_edges, b.path_edges) AS path_edges")
        lines.append(f"    FROM forward_{cte_name} f")
        lines.append(f"    JOIN backward_{cte_name} b")
        lines.append("      ON f.current_node = b.current_node")
        lines.append(f"    WHERE f.depth + b.depth >= {min_depth}")
        lines.append(f"      AND f.depth + b.depth <= {max_depth}")
        lines.append("      -- Prevent duplicate nodes in combined path")
        lines.append(
            "      AND SIZE(ARRAY_INTERSECT(SLICE(f.path, 1, SIZE(f.path) - 1), b.path)) = 0"
        )
        lines.append("  )")

        return "\n".join(lines)

    def _render_bidirectional_unrolling_cte(
        self, op: RecursiveTraversalOperator
    ) -> str:
        """Render bidirectional BFS using unrolled CTEs (one per level).

        This implements the unrolling approach for bidirectional BFS:
        - fwd_0, fwd_1, fwd_2, ...: forward CTEs, one per depth level
        - bwd_0, bwd_1, bwd_2, ...: backward CTEs, one per depth level
        - final: UNION ALL of all valid (fwd_i, bwd_j) combinations

        Benefits over recursive:
        - TRUE frontier behavior (each level only sees previous level)
        - Potentially better memory usage
        - No recursive CTE overhead

        Drawbacks:
        - SQL size grows O(depth^2)
        - Fixed depth at transpile time
        """

        self._ctx.cte_counter += 1
        cte_name = f"paths_{self._ctx.cte_counter}"
        op.cte_name = cte_name

        # Get configuration
        forward_depth = op.bidirectional_depth_forward or 3
        backward_depth = op.bidirectional_depth_backward or 3
        target_value = op.bidirectional_target_value
        min_depth = op.min_hops if op.min_hops is not None else 1
        max_depth = op.max_hops or 6

        # Get edge table info - use edge column names, not node ID columns
        edge_src_col, edge_dst_col = self._get_edge_column_names(op)
        edge_table_name = self._get_edge_table_name(op)
        edge_filter_clause = self._get_edge_filter_clause(op)

        # Get source/target filters (these use node ID columns for node table filtering)
        source_id_col = op.source_id_column or "node_id"
        target_id_col = op.target_id_column or "node_id"
        source_filter_sql = self._render_source_filter_for_bidirectional(
            op, source_id_col
        )
        target_filter_sql = self._render_target_filter_for_bidirectional(
            op, target_id_col, target_value
        )

        source_node_table = self._ctx.get_table_descriptor_with_wildcard(
            op.source_node_type
        )
        target_node_table = self._ctx.get_table_descriptor_with_wildcard(
            op.target_node_type
        )

        # Build edge struct for path_edges collection
        edge_struct = f"STRUCT(e.{edge_src_col} AS src, e.{edge_dst_col} AS dst)"

        lines: list[str] = []

        # Generate forward CTEs: fwd_0, fwd_1, ..., fwd_n
        for level in range(forward_depth + 1):
            if level == 0:
                # Base case: fwd_0 = source node (no edges yet)
                lines.append(f"  fwd_{level}_{cte_name} AS (")
                lines.append("    SELECT")
                lines.append(f"      src.{op.source_id_column} AS current_node,")
                lines.append(f"      ARRAY(src.{op.source_id_column}) AS path,")
                lines.append(
                    "      CAST(ARRAY() AS ARRAY<STRUCT<src: STRING, dst: STRING>>) AS path_edges"
                )
                if source_node_table:
                    lines.append(f"    FROM {source_node_table.full_table_name} src")
                    if source_filter_sql:
                        lines.append(f"    WHERE {source_filter_sql}")
                lines.append("  ),")
            else:
                # Recursive level: fwd_i = extend fwd_{i-1}
                lines.append(f"  fwd_{level}_{cte_name} AS (")
                lines.append("    SELECT")
                lines.append(f"      e.{edge_dst_col} AS current_node,")
                lines.append(f"      CONCAT(f.path, ARRAY(e.{edge_dst_col})) AS path,")
                lines.append(f"      CONCAT(f.path_edges, ARRAY({edge_struct})) AS path_edges")
                lines.append(f"    FROM fwd_{level - 1}_{cte_name} f")
                lines.append(f"    JOIN {edge_table_name} e")
                lines.append(f"      ON f.current_node = e.{edge_src_col}")
                where_parts = [f"NOT ARRAY_CONTAINS(f.path, e.{edge_dst_col})"]
                if edge_filter_clause:
                    where_parts.append(f"({edge_filter_clause})")
                lines.append(f"    WHERE {' AND '.join(where_parts)}")
                lines.append("  ),")
            lines.append("")

        # Generate backward CTEs: bwd_0, bwd_1, ..., bwd_n
        for level in range(backward_depth + 1):
            if level == 0:
                # Base case: bwd_0 = target node (no edges yet)
                lines.append(f"  bwd_{level}_{cte_name} AS (")
                lines.append("    SELECT")
                lines.append(f"      tgt.{op.target_id_column} AS current_node,")
                lines.append(f"      ARRAY(tgt.{op.target_id_column}) AS path,")
                lines.append(
                    "      CAST(ARRAY() AS ARRAY<STRUCT<src: STRING, dst: STRING>>) AS path_edges"
                )
                if target_node_table:
                    lines.append(f"    FROM {target_node_table.full_table_name} tgt")
                    if target_filter_sql:
                        lines.append(f"    WHERE {target_filter_sql}")
                lines.append("  ),")
            else:
                # Recursive level: bwd_i = extend bwd_{i-1} backward
                lines.append(f"  bwd_{level}_{cte_name} AS (")
                lines.append("    SELECT")
                lines.append(f"      e.{edge_src_col} AS current_node,")
                lines.append(
                    f"      CONCAT(ARRAY(e.{edge_src_col}), b.path) AS path,"
                )
                lines.append(
                    f"      CONCAT(ARRAY({edge_struct}), b.path_edges) AS path_edges"
                )
                lines.append(f"    FROM bwd_{level - 1}_{cte_name} b")
                lines.append(f"    JOIN {edge_table_name} e")
                lines.append(f"      ON b.current_node = e.{edge_dst_col}")
                where_parts = [f"NOT ARRAY_CONTAINS(b.path, e.{edge_src_col})"]
                if edge_filter_clause:
                    where_parts.append(f"({edge_filter_clause})")
                lines.append(f"    WHERE {' AND '.join(where_parts)}")
                lines.append("  ),")
            lines.append("")

        # Generate final CTE: UNION ALL of all valid combinations
        lines.append(f"  {cte_name} AS (")
        union_parts: list[str] = []

        for fwd_level in range(forward_depth + 1):
            for bwd_level in range(backward_depth + 1):
                # Total path length = fwd_level + bwd_level
                # (fwd_0 has 1 node, fwd_1 has 2 nodes, etc.)
                total_length = fwd_level + bwd_level
                if total_length < min_depth or total_length > max_depth:
                    continue
                if fwd_level == 0 and bwd_level == 0:
                    # Both at base = direct source=target (skip if min > 0)
                    if min_depth > 0:
                        continue

                union_sql = []
                union_sql.append("    SELECT")
                union_sql.append("      f.path[0] AS start_node,")
                union_sql.append("      b.path[SIZE(b.path) - 1] AS end_node,")
                union_sql.append(f"      {total_length} AS depth,")
                if fwd_level == 0:
                    # Only backward path and path_edges
                    union_sql.append("      b.path AS path,")
                    union_sql.append("      b.path_edges AS path_edges")
                elif bwd_level == 0:
                    # Only forward path and path_edges
                    union_sql.append("      f.path AS path,")
                    union_sql.append("      f.path_edges AS path_edges")
                else:
                    # Combine: forward (except meeting node) + backward
                    union_sql.append(
                        "      CONCAT(SLICE(f.path, 1, SIZE(f.path) - 1), b.path) AS path,"
                    )
                    union_sql.append(
                        "      CONCAT(f.path_edges, b.path_edges) AS path_edges"
                    )
                union_sql.append(f"    FROM fwd_{fwd_level}_{cte_name} f")
                union_sql.append(f"    JOIN bwd_{bwd_level}_{cte_name} b")
                union_sql.append("      ON f.current_node = b.current_node")
                # Prevent duplicate nodes in combined path
                if fwd_level > 0 and bwd_level > 0:
                    union_sql.append(
                        "    WHERE SIZE(ARRAY_INTERSECT("
                        "SLICE(f.path, 1, SIZE(f.path) - 1), b.path)) = 0"
                    )

                union_parts.append("\n".join(union_sql))

        if union_parts:
            # Use UNION (not UNION ALL) to deduplicate paths found via different meeting points
            # E.g., path A→B→C→D can be found with fwd=1/bwd=2 or fwd=2/bwd=1
            lines.append("\n    UNION\n".join(union_parts))
        else:
            # Fallback: empty result if no valid combinations
            lines.append("    SELECT")
            lines.append("      NULL AS start_node,")
            lines.append("      NULL AS end_node,")
            lines.append("      0 AS depth,")
            lines.append("      ARRAY() AS path,")
            lines.append(
                "      CAST(ARRAY() AS ARRAY<STRUCT<src: STRING, dst: STRING>>) AS path_edges"
            )
            lines.append("    WHERE FALSE")

        lines.append("  )")

        return "\n".join(lines)

    def _get_edge_column_names(
        self, op: RecursiveTraversalOperator
    ) -> tuple[str, str]:
        """Get the edge source and destination column names.

        Returns:
            Tuple of (source_col, dest_col) for the edges table
        """

        edge_types = op.edge_types if op.edge_types else []
        if edge_types and op.source_node_type and op.target_node_type:
            edge_type = edge_types[0]
            edge_schema = self._ctx.db_schema.get_edge_definition(
                edge_type, op.source_node_type, op.target_node_type
            )
            if edge_schema and edge_schema.source_id_property and edge_schema.sink_id_property:
                src_col = edge_schema.source_id_property.property_name
                dst_col = edge_schema.sink_id_property.property_name
                return (src_col, dst_col)

        # Try partial lookup
        if edge_types:
            edges = self._ctx.db_schema.find_edges_by_verb(
                edge_types[0],
                from_node_name=op.source_node_type or None,
                to_node_name=op.target_node_type or None,
            )
            if edges:
                edge_schema = edges[0]
                if edge_schema.source_id_property and edge_schema.sink_id_property:
                    src_col = edge_schema.source_id_property.property_name
                    dst_col = edge_schema.sink_id_property.property_name
                    return (src_col, dst_col)

        # Fallback to wildcard edge
        wildcard_edge = self._ctx.db_schema.get_wildcard_edge_definition()
        if wildcard_edge and wildcard_edge.source_id_property and wildcard_edge.sink_id_property:
            src_col = wildcard_edge.source_id_property.property_name
            dst_col = wildcard_edge.sink_id_property.property_name
            return (src_col, dst_col)

        # Default fallback
        return ("src", "dst")

    def _get_edge_table_name(self, op: RecursiveTraversalOperator) -> str:
        """Get the edge table name for a RecursiveTraversalOperator."""

        edge_types = op.edge_types if op.edge_types else []
        if edge_types:
            edge_type = edge_types[0]
            # Try exact lookup
            if op.source_node_type and op.target_node_type:
                edge_id = EdgeSchema.get_edge_id(
                    edge_type, op.source_node_type, op.target_node_type
                )
                edge_table = self._ctx.db_schema.get_sql_table_descriptors(edge_id)
                if edge_table:
                    return edge_table.full_table_name
            # Try partial lookup
            edges = self._ctx.db_schema.find_edges_by_verb(
                edge_type,
                from_node_name=op.source_node_type or None,
                to_node_name=op.target_node_type or None,
            )
            if edges:
                edge_schema = edges[0]
                edge_table = self._ctx.db_schema.get_sql_table_descriptors(edge_schema.id)
                if edge_table:
                    return edge_table.full_table_name

        # Fallback to wildcard edge table
        wildcard = self._ctx.db_schema.get_wildcard_edge_table_descriptor()
        if wildcard:
            return wildcard.full_table_name

        raise TranspilerInternalErrorException(
            "No edge table found for bidirectional traversal"
        )

    def _get_edge_filter_clause(self, op: RecursiveTraversalOperator) -> str | None:
        """Get the edge type filter clause (e.g., relationship_type = 'KNOWS')."""
        edge_types = op.edge_types if op.edge_types else []
        if not edge_types:
            return None

        if len(edge_types) == 1:
            # Single type lookup
            edge_type = edge_types[0]
            if op.source_node_type and op.target_node_type:
        
                edge_id = EdgeSchema.get_edge_id(
                    edge_type, op.source_node_type, op.target_node_type
                )
                edge_table = self._ctx.db_schema.get_sql_table_descriptors(edge_id)
                if edge_table and edge_table.filter:
                    return edge_table.filter

        # Multiple types or lookup failed - build OR clause
        filters = []
        for edge_type in edge_types:
            if op.source_node_type and op.target_node_type:
        
                edge_id = EdgeSchema.get_edge_id(
                    edge_type, op.source_node_type, op.target_node_type
                )
                edge_table = self._ctx.db_schema.get_sql_table_descriptors(edge_id)
                if edge_table and edge_table.filter:
                    filters.append(f"({edge_table.filter})")

        if filters:
            return " OR ".join(filters)
        return None

    def _render_source_filter_for_bidirectional(
        self, op: RecursiveTraversalOperator, source_id_col: str
    ) -> str | None:
        """Render source node filter for bidirectional base case."""
        if not op.start_node_filter:
            return None

        # Rewrite filter: p.name -> src.name
        rewritten = rewrite_predicate_for_edge_alias(
            op.start_node_filter,
            op.source_alias,
            edge_alias="src",
        )
        return self._expr._render_edge_filter_expression(rewritten)

    def _render_target_filter_for_bidirectional(
        self,
        op: RecursiveTraversalOperator,
        target_id_col: str,
        target_value: str | None,
    ) -> str | None:
        """Render target node filter for bidirectional backward base case."""
        if not op.sink_node_filter:
            return None

        # Rewrite filter: b.id -> tgt.id
        rewritten = rewrite_predicate_for_edge_alias(
            op.sink_node_filter,
            op.target_alias,
            edge_alias="tgt",
        )
        return self._expr._render_edge_filter_expression(rewritten)

    def _render_recursive_reference(
        self, op: RecursiveTraversalOperator, depth: int
    ) -> str:
        """Render a reference to a recursive CTE."""
        indent = self._ctx.indent(depth)
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
            rendered_expr = self._expr._render_expression(expr, context_op)
            select_items.append(f"    {rendered_expr} AS `{alias}`")

        # Aggregates
        for alias, expr in op.aggregates:
            rendered_expr = self._expr._render_expression(expr, context_op)
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
                rendered_expr = self._expr._render_expression(expr, context_op)
                group_by_exprs.append(rendered_expr)
            lines.append(f"  GROUP BY {', '.join(group_by_exprs)}")

        # Render HAVING clause
        if op.having_filter:
            having_sql = self._expr._render_expression(op.having_filter, context_op)
            lines.append(f"  HAVING {having_sql}")

        # Render ORDER BY clause
        if op.order_by:
            order_parts = []
            for expr, is_desc in op.order_by:
                rendered_expr = self._expr._render_expression(expr, context_op)
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
        indent = self._ctx.indent(depth)
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
