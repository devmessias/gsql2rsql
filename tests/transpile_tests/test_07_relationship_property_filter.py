"""Test 07: Relationship match with property filter.

Validates that relationships can be filtered by their properties using WHERE clause.
"""

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    SimpleGraphSchemaProvider,
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
    assert_has_where,
    assert_has_join,
    SQLStructure,
)


class TestRelationshipPropertyFilter:
    """Test relationship property filtering."""

    TEST_ID = "07"
    TEST_NAME = "relationship_property_filter"

    def setup_method(self) -> None:
        """Set up test fixtures with edge properties."""
        # Graph schema
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
                properties=[
                    EntityProperty("since", int),
                    EntityProperty("weight", float),
                ],
            )
        )

        # SQL schema
        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
            NodeSchema(
                name="Person",
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Person"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
                properties=[
                    EntityProperty("since", int),
                    EntityProperty("weight", float),
                ],
            ),
            SQLTableDescriptor(table_name="graph.Knows"),
        )

    def _transpile(self, cypher: str) -> str:
        """Helper to transpile a Cypher query."""
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def test_relationship_property_filter_has_where(self) -> None:
        """Test that relationship property filter generates WHERE clause."""
        cypher = """
        MATCH (p:Person)-[r:KNOWS]->(f:Person)
        WHERE r.since > 2020
        RETURN p.name, f.name
        """
        sql = self._transpile(cypher)

        assert_has_where(sql)

    def test_relationship_property_filter_includes_value(self) -> None:
        """Test that WHERE clause contains filter value."""
        cypher = """
        MATCH (p:Person)-[r:KNOWS]->(f:Person)
        WHERE r.since > 2020
        RETURN p.name, f.name
        """
        sql = self._transpile(cypher)

        # Should contain the filter value
        assert "2020" in sql, "Expected filter value 2020 in SQL"

    def test_relationship_property_projected(self) -> None:
        """Test that relationship properties can be projected."""
        cypher = """
        MATCH (p:Person)-[r:KNOWS]->(f:Person)
        RETURN p.name, f.name, r.since
        """
        sql = self._transpile(cypher)

        # Should project the 'since' property
        assert "since" in sql.lower(), "Expected 'since' property in projection"

    def test_relationship_has_joins(self) -> None:
        """Test that relationship generates JOINs."""
        cypher = """
        MATCH (p:Person)-[r:KNOWS]->(f:Person)
        WHERE r.since > 2020
        RETURN p.name, f.name
        """
        sql = self._transpile(cypher)

        assert_has_join(sql)

    def test_relationship_references_edge_table(self) -> None:
        """Test that SQL references the edge table."""
        cypher = """
        MATCH (p:Person)-[r:KNOWS]->(f:Person)
        RETURN p.name, f.name
        """
        sql = self._transpile(cypher)

        assert_has_from_table(sql, "Knows")

    def test_multiple_relationship_properties(self) -> None:
        """Test filtering on multiple relationship properties."""
        cypher = """
        MATCH (p:Person)-[r:KNOWS]->(f:Person)
        WHERE r.since > 2020 AND r.weight > 0.5
        RETURN p.name, f.name
        """
        sql = self._transpile(cypher)

        assert_has_where(sql)
        assert "since" in sql.lower()
        assert "weight" in sql.lower()
        assert "2020" in sql
        assert "0.5" in sql

    def test_relationship_property_equality(self) -> None:
        """Test equality filter on relationship property."""
        cypher = """
        MATCH (p:Person)-[r:KNOWS]->(f:Person)
        WHERE r.since = 2022
        RETURN p.name, f.name
        """
        sql = self._transpile(cypher)

        assert_has_where(sql)
        assert "2022" in sql
        assert "=" in sql
