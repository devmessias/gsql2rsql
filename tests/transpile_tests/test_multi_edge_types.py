"""Test multi-edge-type support for relationship queries.

This test file validates that the transpiler correctly handles queries
with multiple relationship types using the pipe syntax: [:TYPE1|TYPE2].

Current status:
- Variable-length paths with multi-edge types: SUPPORTED
- Fixed-length relationships with multi-edge types: NOT YET SUPPORTED
"""

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)

from tests.utils.sql_assertions import (
    assert_has_select,
    assert_has_recursive_cte,
    assert_cycle_detection,
    SQLStructure,
)


class TestMultiEdgeTypeSupport:
    """Test multi-edge-type support for various query patterns."""

    def setup_method(self) -> None:
        """Set up test fixtures with single edge table schema."""
        # SQL schema with two edge types between Person nodes
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Person"),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor(
                table_name="graph.edges",
                filter="edge_type = 'KNOWS'",
            ),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="FOLLOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor(
                table_name="graph.edges",
                filter="edge_type = 'FOLLOWS'",
            ),
        )

    def _transpile(self, cypher: str) -> str:
        """Helper to transpile a Cypher query."""
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        return renderer.render_plan(plan)

    # =========================================================================
    # Variable-Length Multi-Edge Type Tests (SUPPORTED)
    # =========================================================================

    def test_variable_length_multi_edge_generates_recursive_cte(self) -> None:
        """Test that [:KNOWS|FOLLOWS*1..3] generates WITH RECURSIVE."""
        cypher = """
        MATCH (p:Person)-[:KNOWS|FOLLOWS*1..3]->(f:Person)
        RETURN DISTINCT f.name
        """
        sql = self._transpile(cypher)

        assert_has_recursive_cte(sql)
        assert_has_select(sql)

    def test_variable_length_multi_edge_includes_both_types(self) -> None:
        """Test that SQL references both edge types."""
        cypher = """
        MATCH (p:Person)-[:KNOWS|FOLLOWS*1..3]->(f:Person)
        RETURN f.id
        """
        sql = self._transpile(cypher)

        # Should contain references to both edge types
        assert "KNOWS" in sql, "Expected KNOWS edge type in SQL"
        assert "FOLLOWS" in sql, "Expected FOLLOWS edge type in SQL"

    def test_variable_length_multi_edge_uses_or_filter(self) -> None:
        """Test that multi-edge uses combined OR filter for single table."""
        cypher = """
        MATCH (p:Person)-[:KNOWS|FOLLOWS*1..3]->(f:Person)
        RETURN f.name
        """
        sql = self._transpile(cypher)

        # When both types are in same table, should use OR
        # Either explicit OR or IN clause is acceptable
        sql_upper = sql.upper()
        has_or = " OR " in sql_upper
        has_in = "IN (" in sql_upper

        assert has_or or has_in, (
            "Expected OR filter or IN clause for combined edge types"
        )

    def test_variable_length_multi_edge_has_cycle_detection(self) -> None:
        """Test that recursive query includes cycle detection."""
        cypher = """
        MATCH (p:Person)-[:KNOWS|FOLLOWS*1..5]->(f:Person)
        RETURN f.id
        """
        sql = self._transpile(cypher)

        assert_cycle_detection(sql)

    def test_variable_length_multi_edge_respects_depth(self) -> None:
        """Test that recursive query respects max depth."""
        cypher = """
        MATCH (p:Person)-[:KNOWS|FOLLOWS*1..4]->(f:Person)
        RETURN f.id
        """
        sql = self._transpile(cypher)

        # Should have depth < 4 check
        assert "depth < 4" in sql.lower(), "Expected depth < 4 limit"

    def test_variable_length_single_edge_type_still_works(self) -> None:
        """Test that single edge type in variable-length still works."""
        cypher = """
        MATCH (p:Person)-[:KNOWS*1..3]->(f:Person)
        RETURN f.name
        """
        sql = self._transpile(cypher)

        assert_has_recursive_cte(sql)
        assert "KNOWS" in sql
        # Should NOT have FOLLOWS when only KNOWS is specified
        assert "FOLLOWS" not in sql

    # =========================================================================
    # Edge Case Tests
    # =========================================================================

    def test_multi_edge_with_where_clause(self) -> None:
        """Test multi-edge traversal with WHERE filter."""
        cypher = """
        MATCH (p:Person)-[:KNOWS|FOLLOWS*1..3]->(f:Person)
        WHERE p.id = 1
        RETURN f.name
        """
        sql = self._transpile(cypher)

        assert_has_recursive_cte(sql)
        # Should have WHERE somewhere in the query
        structure = SQLStructure(raw_sql=sql)
        # Note: WHERE might be in CTE or outer query
        assert "WHERE" in sql.upper()

    def test_multi_edge_with_distinct(self) -> None:
        """Test multi-edge traversal with DISTINCT."""
        cypher = """
        MATCH (p:Person)-[:KNOWS|FOLLOWS*1..3]->(f:Person)
        RETURN DISTINCT f.id
        """
        sql = self._transpile(cypher)

        assert_has_recursive_cte(sql)
        assert "DISTINCT" in sql.upper()
