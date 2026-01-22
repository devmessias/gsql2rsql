"""Test 06: Single-hop directed relationship match."""

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


class TestSingleHopRelationship:
    """Test single-hop directed relationship matching."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
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
            SQLTableDescriptor(
                table_name="dbo.Person",
                node_id_columns=["id"],
            ),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
            ),
            SQLTableDescriptor(
                entity_id="Person@KNOWS@Person",
                table_name="dbo.Knows",
                node_id_columns=["person1_id", "person2_id"],
            ),
        )

    def test_single_hop_directed_relationship(self) -> None:
        """Test MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p, f."""
        cypher = "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name"

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # Should have JOIN (connecting Person to Knows to Person)
        assert "JOIN" in sql.upper()

        # Should reference both Person and Knows tables
        assert "Person" in sql
        assert "Knows" in sql or "KNOWS" in sql.upper()

        # Should have SELECT
        assert "SELECT" in sql

    def test_single_hop_has_joins(self) -> None:
        """Test that relationship pattern generates joins."""
        cypher = "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.id, f.id"

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # Should have JOIN keywords
        assert "JOIN" in sql.upper()

        # Should have ON keyword for join condition
        assert " ON" in sql or " on" in sql or "ON\n" in sql

        # Should NOT have cartesian joins (ON TRUE)
        assert "ON TRUE" not in sql.upper()
