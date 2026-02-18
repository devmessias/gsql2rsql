"""Unit tests for procedural BFS rendering mode.

Tests verify SQL text output (structure, keywords, patterns) without PySpark.
Uses SimpleSQLSchemaProvider pattern from existing tests.

Two test classes:
- TestNumberedViewsRendering: EXECUTE IMMEDIATE + numbered views (PySpark 4.2)
- TestTempTablesRendering: CREATE TEMPORARY TABLE + INSERT INTO (Databricks)
"""

import pytest

from gsql2rsql.common.exceptions import TranspilerNotSupportedException
from gsql2rsql.common.schema import (
    EdgeSchema,
    EntityProperty,
    NodeSchema,
)
from gsql2rsql.parser.opencypher_parser import OpenCypherParser
from gsql2rsql.planner.logical_plan import LogicalPlan
from gsql2rsql.planner.pass_manager import optimize_plan
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)
from gsql2rsql.renderer.sql_renderer import SQLRenderer


# ------------------------------------------------------------------
# Shared schema setup
# ------------------------------------------------------------------

def _make_schema() -> SimpleSQLSchemaProvider:
    """Create a schema with Person nodes and KNOWS edges."""
    schema = SimpleSQLSchemaProvider()

    schema.add_node(
        NodeSchema(
            name="Person",
            properties=[
                EntityProperty("node_id", str),
                EntityProperty("name", str),
                EntityProperty("age", int),
            ],
            node_id_property=EntityProperty("node_id", str),
        ),
        SQLTableDescriptor(
            table_name="nodes",
            node_id_columns=["node_id"],
            filter="node_type = 'Person'",
        ),
    )

    schema.add_edge(
        EdgeSchema(
            name="KNOWS",
            source_node_id="Person",
            sink_node_id="Person",
            source_id_property=EntityProperty("src", str),
            sink_id_property=EntityProperty("dst", str),
            properties=[
                EntityProperty("src", str),
                EntityProperty("dst", str),
                EntityProperty("amount", int),
            ],
        ),
        SQLTableDescriptor(
            entity_id="Person@KNOWS@Person",
            table_name="edges",
            node_id_columns=["src", "dst"],
            filter="relationship_type = 'KNOWS'",
        ),
    )

    return schema


def _transpile(
    query: str,
    schema: SimpleSQLSchemaProvider,
    *,
    materialization: str = "temp_tables",
    vlp_mode: str = "procedural",
) -> str:
    """Transpile a Cypher query with the given strategy."""
    parser = OpenCypherParser()
    renderer = SQLRenderer(
        db_schema_provider=schema,
        vlp_rendering_mode=vlp_mode,
        materialization_strategy=materialization,
    )
    ast = parser.parse(query)
    plan = LogicalPlan.process_query_tree(ast, schema)
    optimize_plan(plan)
    plan.resolve(original_query=query)
    return renderer.render_plan(plan)


BASIC_QUERY = """
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
WHERE a.node_id = 'Alice'
RETURN b.node_id
"""


# ======================================================================
# numbered_views strategy (PySpark 4.2)
# ======================================================================


class TestNumberedViewsRendering:
    """Verify SQL text for materialization_strategy='numbered_views'."""

    def setup_method(self) -> None:
        self.schema = _make_schema()

    def _sql(self, query: str = BASIC_QUERY) -> str:
        return _transpile(
            query, self.schema, materialization="numbered_views",
        )

    # Structure
    def test_produces_begin_end(self) -> None:
        sql = self._sql()
        assert sql.strip().startswith("BEGIN")
        assert sql.strip().endswith("END")

    def test_contains_while_loop(self) -> None:
        sql = self._sql()
        assert "WHILE" in sql
        assert "END WHILE" in sql

    def test_contains_execute_immediate(self) -> None:
        sql = self._sql()
        assert "EXECUTE IMMEDIATE" in sql

    def test_contains_declare_statements(self) -> None:
        sql = self._sql()
        assert "DECLARE bfs_depth_" in sql
        assert "DECLARE bfs_frontier_count_" in sql
        assert "DECLARE bfs_union_sql_" in sql

    # Frontier / visited init
    def test_frontier_init_has_start_filter(self) -> None:
        sql = self._sql()
        assert "bfs_frontier_" in sql
        assert "_0 AS" in sql
        assert "node_type = 'Person'" in sql

    def test_visited_init_from_frontier(self) -> None:
        sql = self._sql()
        assert "bfs_visited_" in sql
        assert "SELECT node FROM bfs_frontier_" in sql

    # Direction
    def test_directed_forward_join_on_src(self) -> None:
        assert "e.src = f.node" in self._sql()

    def test_directed_backward_join_on_dst(self) -> None:
        query = """
        MATCH (a:Person)<-[:KNOWS*1..3]-(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        assert "e.dst = f.node" in self._sql(query)

    def test_undirected_or_join_with_case(self) -> None:
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]-(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        sql = self._sql(query)
        assert "OR" in sql
        assert "CASE WHEN" in sql

    # Edge filter
    def test_edge_type_filter_in_expansion(self) -> None:
        assert "relationship_type" in self._sql()

    # min_hops
    def test_min_hops_skips_early_levels(self) -> None:
        query = """
        MATCH (a:Person)-[:KNOWS*2..4]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        sql = self._sql(query)
        assert "bfs_depth_" in sql
        assert ">= 2" in sql

    # Final view
    def test_final_view_cross_join_frontier(self) -> None:
        assert "CROSS JOIN bfs_frontier_" in self._sql()

    def test_final_view_has_start_node_end_node(self) -> None:
        sql = self._sql()
        assert "start_node" in sql
        assert "end_node" in sql

    # No numbered suffix uses depth-based naming
    def test_numbered_suffix_in_views(self) -> None:
        """Views use depth suffix like bfs_frontier_N_0."""
        sql = self._sql()
        assert "_0 AS" in sql  # frontier_N_0, visited_N_0

    # collect_edges support
    def test_collect_edges_produces_path_edges(self) -> None:
        """relationships(path) should produce path_edges column."""
        query = """
        MATCH path = (a:Person)-[r:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN relationships(path)
        """
        sql = self._sql(query)
        assert "path_edges" in sql
        assert "NAMED_STRUCT" in sql

    def test_unwind_relationships_produces_path_edges(self) -> None:
        """UNWIND relationships(path) should work with procedural BFS."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        UNWIND relationships(path) AS r
        RETURN r.src, r.dst
        """
        sql = self._sql(query)
        assert "path_edges" in sql
        assert "EXPLODE" in sql

    # CTE mode unchanged
    def test_cte_mode_unchanged(self) -> None:
        sql = _transpile(
            BASIC_QUERY, self.schema,
            vlp_mode="cte", materialization="numbered_views",
        )
        assert "WITH RECURSIVE" in sql
        assert "BEGIN" not in sql


# ======================================================================
# temp_tables strategy (Databricks)
# ======================================================================


class TestTempTablesRendering:
    """Verify SQL text for materialization_strategy='temp_tables'."""

    def setup_method(self) -> None:
        self.schema = _make_schema()

    def _sql(self, query: str = BASIC_QUERY) -> str:
        return _transpile(
            query, self.schema, materialization="temp_tables",
        )

    # Structure
    def test_produces_begin_end(self) -> None:
        sql = self._sql()
        assert sql.strip().startswith("BEGIN")
        assert sql.strip().endswith("END")

    def test_contains_while_loop(self) -> None:
        sql = self._sql()
        assert "WHILE" in sql
        assert "END WHILE" in sql

    def test_contains_create_temporary_table(self) -> None:
        sql = self._sql()
        assert "CREATE TEMPORARY TABLE" in sql

    def test_contains_insert_into(self) -> None:
        sql = self._sql()
        assert "INSERT INTO bfs_visited_" in sql
        assert "INSERT INTO bfs_result_" in sql

    def test_contains_drop_temporary_table(self) -> None:
        sql = self._sql()
        assert "DROP TEMPORARY TABLE" in sql

    def test_no_execute_immediate(self) -> None:
        """temp_tables should NOT use EXECUTE IMMEDIATE."""
        sql = self._sql()
        assert "EXECUTE IMMEDIATE" not in sql

    def test_no_union_sql_variable(self) -> None:
        """temp_tables should NOT need bfs_union_sql string variable."""
        sql = self._sql()
        assert "bfs_union_sql_" not in sql

    def test_declare_current_depth(self) -> None:
        sql = self._sql()
        assert "DECLARE current_depth_" in sql
        assert "DECLARE rows_in_frontier_" in sql

    # Setup
    def test_setup_creates_visited_table(self) -> None:
        sql = self._sql()
        assert "CREATE TEMPORARY TABLE bfs_visited_" in sql

    def test_setup_creates_frontier_from_node_table(self) -> None:
        sql = self._sql()
        assert "CREATE TEMPORARY TABLE bfs_frontier_" in sql
        assert "FROM nodes n" in sql

    def test_setup_seeds_visited_from_frontier(self) -> None:
        sql = self._sql()
        assert "INSERT INTO bfs_visited_" in sql
        assert "SELECT node FROM bfs_frontier_" in sql

    def test_setup_creates_empty_result_table(self) -> None:
        sql = self._sql()
        assert "CREATE TEMPORARY TABLE bfs_result_" in sql

    def test_setup_saves_frontier_init(self) -> None:
        sql = self._sql()
        assert "bfs_frontier_" in sql
        assert "_init" in sql

    def test_start_filter_in_setup(self) -> None:
        sql = self._sql()
        assert "node_type = 'Person'" in sql

    # Direction
    def test_directed_forward_join_on_src(self) -> None:
        assert "e.src = f.node" in self._sql()

    def test_directed_backward_join_on_dst(self) -> None:
        query = """
        MATCH (a:Person)<-[:KNOWS*1..3]-(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        assert "e.dst = f.node" in self._sql(query)

    def test_undirected_or_join_with_case(self) -> None:
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]-(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        sql = self._sql(query)
        assert "OR" in sql
        assert "CASE WHEN" in sql

    # Edge filter
    def test_edge_type_filter(self) -> None:
        assert "relationship_type" in self._sql()

    # WHILE loop internals
    def test_while_drops_and_creates_edges(self) -> None:
        sql = self._sql()
        assert "DROP TEMPORARY TABLE IF EXISTS bfs_edges_" in sql
        assert "CREATE TEMPORARY TABLE bfs_edges_" in sql

    def test_while_counts_edges_with_set(self) -> None:
        """SET rows_in_frontier = (SELECT COUNT...)."""
        sql = self._sql()
        assert "SET rows_in_frontier_" in sql
        assert "SELECT COUNT(1) FROM bfs_edges_" in sql

    def test_while_inserts_into_visited(self) -> None:
        sql = self._sql()
        assert "INSERT INTO bfs_visited_" in sql
        assert "SELECT DISTINCT _next_node FROM bfs_edges_" in sql

    def test_while_replaces_frontier(self) -> None:
        """Frontier replaced via DROP + CREATE TABLE AS."""
        sql = self._sql()
        assert "DROP TEMPORARY TABLE bfs_frontier_" in sql
        assert "SELECT DISTINCT _next_node AS node FROM bfs_edges_" in sql

    def test_while_inserts_into_result(self) -> None:
        sql = self._sql()
        assert "INSERT INTO bfs_result_" in sql

    # min_hops
    def test_min_hops_conditional_insert(self) -> None:
        """*2..4 should guard INSERT INTO result with IF depth >= 2."""
        query = """
        MATCH (a:Person)-[:KNOWS*2..4]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        sql = self._sql(query)
        assert "current_depth_" in sql
        assert ">= 2" in sql

    # Final view
    def test_final_view_cross_join_frontier_init(self) -> None:
        sql = self._sql()
        assert "CROSS JOIN bfs_frontier_" in sql
        assert "_init f0" in sql

    def test_final_view_has_start_node_end_node(self) -> None:
        sql = self._sql()
        assert "start_node" in sql
        assert "end_node" in sql

    def test_final_view_selects_from_result(self) -> None:
        sql = self._sql()
        assert "FROM bfs_result_" in sql

    # No numbered suffixes (fixed table names)
    def test_no_numbered_suffix(self) -> None:
        """Table names are bfs_visited_N, NOT bfs_visited_N_0."""
        sql = self._sql()
        # Should NOT contain the numbered_views pattern of _N_0
        # (except for bfs_frontier_N_init which is different)
        lines = sql.split("\n")
        for line in lines:
            if "bfs_visited_" in line and "DECLARE" not in line:
                # visited tables should NOT have depth suffix
                assert "_0 AS" not in line or "bfs_frontier_" in line

    # collect_edges support
    def test_collect_edges_produces_path_edges(self) -> None:
        """relationships(path) should produce path_edges column."""
        query = """
        MATCH path = (a:Person)-[r:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN relationships(path)
        """
        sql = self._sql(query)
        assert "path_edges" in sql
        assert "NAMED_STRUCT" in sql
        # temp_tables uses direct SQL, no EXECUTE IMMEDIATE
        assert "ARRAY(NAMED_STRUCT(" in sql

    def test_unwind_relationships_produces_path_edges(self) -> None:
        """UNWIND relationships(path) should work with procedural BFS."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        UNWIND relationships(path) AS r
        RETURN r.src, r.dst
        """
        sql = self._sql(query)
        assert "path_edges" in sql
        assert "EXPLODE" in sql


# ======================================================================
# Cross-strategy equivalence
# ======================================================================


class TestStrategyEquivalence:
    """Verify both strategies produce the same final view schema."""

    def setup_method(self) -> None:
        self.schema = _make_schema()

    def test_both_strategies_produce_same_final_columns(self) -> None:
        """Both should have start_node, end_node, depth in final view."""
        sql_tt = _transpile(
            BASIC_QUERY, self.schema, materialization="temp_tables",
        )
        sql_nv = _transpile(
            BASIC_QUERY, self.schema, materialization="numbered_views",
        )
        for sql, name in [(sql_tt, "temp_tables"), (sql_nv, "numbered_views")]:
            assert "start_node" in sql, f"{name} missing start_node"
            assert "end_node" in sql, f"{name} missing end_node"
            assert "depth" in sql, f"{name} missing depth"

    def test_both_strategies_use_cross_join(self) -> None:
        sql_tt = _transpile(
            BASIC_QUERY, self.schema, materialization="temp_tables",
        )
        sql_nv = _transpile(
            BASIC_QUERY, self.schema, materialization="numbered_views",
        )
        assert "CROSS JOIN" in sql_tt
        assert "CROSS JOIN" in sql_nv

    def test_cte_mode_ignores_materialization(self) -> None:
        """When vlp_mode='cte', materialization_strategy is irrelevant."""
        sql_tt = _transpile(
            BASIC_QUERY, self.schema,
            vlp_mode="cte", materialization="temp_tables",
        )
        sql_nv = _transpile(
            BASIC_QUERY, self.schema,
            vlp_mode="cte", materialization="numbered_views",
        )
        assert "WITH RECURSIVE" in sql_tt
        assert "WITH RECURSIVE" in sql_nv
        assert "BEGIN" not in sql_tt
        assert "BEGIN" not in sql_nv
