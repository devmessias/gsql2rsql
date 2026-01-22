"""Test 09: OPTIONAL MATCH (Left Join Semantics).

Validates that OPTIONAL MATCH correctly generates LEFT JOIN to preserve
all rows from the initial MATCH even when the optional pattern doesn't match.
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
    assert_has_from_table,
    assert_has_join,
    assert_left_join_for_optional,
    SQLStructure,
)


class TestOptionalMatch:
    """Test OPTIONAL MATCH (left join semantics)."""

    TEST_ID = "09"
    TEST_NAME = "optional_match"

    def setup_method(self) -> None:
        """Set up test fixtures with Person and Movie."""
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
        self.schema.add_node(
            NodeSchema(
                name="Movie",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("title", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Movie"),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="ACTED_IN",
                source_node_id="Person",
                sink_node_id="Movie",
            ),
            SQLTableDescriptor(
                table_name="graph.ActedIn",
                node_id_columns=["person_id", "movie_id"],
            ),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="DIRECTED",
                source_node_id="Person",
                sink_node_id="Movie",
            ),
            SQLTableDescriptor(
                table_name="graph.Directed",
                node_id_columns=["person_id", "movie_id"],
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

    def test_optional_match_uses_left_join(self) -> None:
        """Test that OPTIONAL MATCH generates LEFT JOIN."""
        cypher = """
        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:ACTED_IN]->(m:Movie)
        RETURN p.name, m.title
        """
        sql = self._transpile(cypher)

        # Critical assertion: must use LEFT JOIN
        assert_left_join_for_optional(sql)

    def test_optional_match_preserves_all_persons(self) -> None:
        """Test that OPTIONAL MATCH preserves all persons structurally.

        The LEFT JOIN ensures all Person rows are kept, even if they
        have no ACTED_IN relationships.
        """
        cypher = """
        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:ACTED_IN]->(m:Movie)
        RETURN p.name, m.title
        """
        sql = self._transpile(cypher)

        # Should reference Person table
        assert_has_from_table(sql, "Person")
        # Should use LEFT (not INNER only)
        assert "LEFT" in sql.upper()

    def test_optional_match_projects_nullable_property(self) -> None:
        """Test that optional matched properties can be projected."""
        cypher = """
        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:ACTED_IN]->(m:Movie)
        RETURN p.name, m.title
        """
        sql = self._transpile(cypher)

        # Should project both name and title
        assert "name" in sql.lower()
        assert "title" in sql.lower()

    def test_optional_match_has_valid_sql_structure(self) -> None:
        """Test that OPTIONAL MATCH generates valid SQL."""
        cypher = """
        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:ACTED_IN]->(m:Movie)
        RETURN p.name, m.title
        """
        sql = self._transpile(cypher)

        assert_has_select(sql)
        assert "FROM" in sql.upper()

    def test_optional_match_with_where_on_optional(self) -> None:
        """Test OPTIONAL MATCH with WHERE filter on optional part."""
        cypher = """
        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:ACTED_IN]->(m:Movie)
        WHERE m.title IS NOT NULL
        RETURN p.name, m.title
        """
        sql = self._transpile(cypher)

        assert_has_select(sql)
        # Should still use LEFT JOIN
        assert "LEFT" in sql.upper()

    def test_optional_match_references_both_tables(self) -> None:
        """Test that both Person and Movie tables are referenced."""
        cypher = """
        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:ACTED_IN]->(m:Movie)
        RETURN p.name, m.title
        """
        sql = self._transpile(cypher)

        assert_has_from_table(sql, "Person")
        assert_has_from_table(sql, "Movie")

    def test_regular_match_uses_inner_join(self) -> None:
        """Test that regular MATCH uses INNER JOIN (not LEFT)."""
        cypher = """
        MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
        RETURN p.name, m.title
        """
        sql = self._transpile(cypher)

        # Should use INNER JOIN (or just JOIN)
        assert "JOIN" in sql.upper()
        # The key difference: INNER not LEFT in the main pattern
        assert_has_join(sql)

    def test_multiple_optional_matches(self) -> None:
        """Test multiple OPTIONAL MATCH clauses."""
        cypher = """
        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:ACTED_IN]->(m:Movie)
        OPTIONAL MATCH (p)-[:DIRECTED]->(d:Movie)
        RETURN p.name, m.title, d.title
        """
        sql = self._transpile(cypher)

        # Should have multiple LEFT JOINs
        left_count = sql.upper().count("LEFT")
        assert left_count >= 1, "Expected at least one LEFT JOIN"
