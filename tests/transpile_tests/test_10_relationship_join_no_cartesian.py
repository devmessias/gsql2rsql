"""Test 10: Relationship joins should not generate cartesian products.

Validates that MATCH (a)-[r]->(b) generates proper join conditions
instead of cartesian joins (ON TRUE).
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


class TestRelationshipJoinNoCartesian:
    """Test that relationship joins generate proper conditions."""

    TEST_ID = "10"
    TEST_NAME = "relationship_join_no_cartesian"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Graph schema
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("age", int),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                properties=[],
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            )
        )

        # SQL schema
        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
            NodeSchema(
                name="Person",
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(
                table_name="dbo.graph.Person",
                node_id_columns=["id"],
            ),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
            ),
            SQLTableDescriptor(
                entity_id="Person@KNOWS@Person",
                table_name="dbo.graph.Knows",
                node_id_columns=["source_id", "target_id"],
            ),
        )

    def test_directed_relationship_no_cartesian_join(self) -> None:
        """Test MATCH (a)-[:KNOWS]->(b) generates proper join conditions."""
        cypher = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.id, b.id"

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        sql = renderer.render_plan(plan)

        # Should have proper joins, not cartesian
        assert "ON TRUE" not in sql.upper()

        # Should have join conditions that connect nodes through relationships
        # The exact condition depends on the schema, but should not be TRUE
        assert "ON" in sql.upper()

        # Should reference the relationship table
        assert "Knows" in sql

        # Should have meaningful join conditions (not just TRUE)
        # Look for patterns like "a.id = r.source_id" or similar
        sql_upper = sql.upper()
        assert (
            "SOURCE_ID" in sql_upper or "TARGET_ID" in sql_upper
        ), "Should reference relationship columns"

    def test_undirected_relationship_no_cartesian_join(self) -> None:
        """Test MATCH (a)-[:KNOWS]-(b) generates proper join conditions."""
        cypher = "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a.id, b.id"

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        sql = renderer.render_plan(plan)

        # Should have proper joins, not cartesian
        assert "ON TRUE" not in sql.upper()

        # Should have join conditions
        assert "ON" in sql.upper()

    def test_multiple_hops_no_cartesian_joins(self) -> None:
        """Test longer paths don't generate cartesian joins."""
        cypher = (
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN a.id, b.id, c.id"
        )

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        sql = renderer.render_plan(plan)

        # Should have proper joins throughout
        assert "ON TRUE" not in sql.upper()

        # Should have multiple joins
        sql_upper = sql.upper()
        join_count = sql_upper.count("JOIN")
        assert join_count >= 2, f"Expected at least 2 joins, got {join_count}"

        # Should have meaningful conditions
        assert "ON" in sql_upper
