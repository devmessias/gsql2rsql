"""Tests for logical plan dump functionality.

TDD: These tests verify that the LogicalPlan.dump_graph() method produces
structured output suitable for debugging and artifact persistence.
"""

import pytest

from gsql2rsql import LogicalPlan, OpenCypherParser
from gsql2rsql.common.schema import EdgeSchema, EntityProperty, NodeSchema
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor


@pytest.fixture
def simple_schema() -> SimpleSQLSchemaProvider:
    """Create a simple schema for testing."""
    provider = SimpleSQLSchemaProvider()

    # Add Person node
    person_schema = NodeSchema(
        name="Person",
        properties=[
            EntityProperty(property_name="name", data_type=str),
            EntityProperty(property_name="age", data_type=int),
        ],
        node_id_property=EntityProperty(property_name="id", data_type=int),
    )
    provider.add_node(person_schema, SQLTableDescriptor(table_or_view_name="dbo.Person"))

    # Add KNOWS edge (Person -> Person)
    knows_schema = EdgeSchema(
        name="KNOWS",
        source_node_id="Person",
        sink_node_id="Person",
        source_id_property=EntityProperty(property_name="person_id", data_type=int),
        sink_id_property=EntityProperty(property_name="friend_id", data_type=int),
        properties=[],
    )
    provider.add_edge(knows_schema, SQLTableDescriptor(table_or_view_name="dbo.Knows"))

    return provider


class TestLogicalPlanDump:
    """Tests for LogicalPlan.dump_graph() method."""

    def test_dump_graph_returns_string(self, simple_schema: SimpleSQLSchemaProvider) -> None:
        """Test that dump_graph() returns a non-empty string."""
        query = "MATCH (p:Person) RETURN p.name"
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, simple_schema)

        result = plan.dump_graph()

        assert isinstance(result, str)
        assert len(result) > 0

    def test_dump_graph_contains_operator_info(self, simple_schema: SimpleSQLSchemaProvider) -> None:
        """Test that dump_graph() contains operator information."""
        query = "MATCH (p:Person) RETURN p.name"
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, simple_schema)

        result = plan.dump_graph()

        # Should contain operator class names
        assert "DataSourceOperator" in result or "Projection" in result
        # Should contain level/depth information
        assert "Level" in result
        # Should contain operator IDs
        assert "OpId=" in result

    def test_dump_graph_contains_join_info_for_relationship(
        self, simple_schema: SimpleSQLSchemaProvider
    ) -> None:
        """Test that dump_graph() shows join operators for relationships."""
        query = "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name"
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, simple_schema)

        result = plan.dump_graph()

        # Should show join operator for the relationship traversal
        assert "Join" in result
        # Should reference both entities
        assert "Person" in result or "__p" in result or "__f" in result

    def test_dump_graph_different_from_repr(self, simple_schema: SimpleSQLSchemaProvider) -> None:
        """Test that dump_graph() is different from str/repr."""
        query = "MATCH (p:Person) RETURN p.name"
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, simple_schema)

        dump_result = plan.dump_graph()
        str_result = str(plan)
        repr_result = repr(plan)

        # dump_graph should NOT be the same as str/repr (which show object address)
        assert dump_result != str_result
        assert dump_result != repr_result
        # dump_graph should not contain "object at 0x"
        assert "object at 0x" not in dump_result

    def test_dump_graph_for_aggregation_query(
        self, simple_schema: SimpleSQLSchemaProvider
    ) -> None:
        """Test dump_graph() for query with aggregation."""
        query = "MATCH (p:Person) RETURN COUNT(p) AS total"
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, simple_schema)

        result = plan.dump_graph()

        # Should still produce structured output
        assert isinstance(result, str)
        assert len(result) > 0
        assert "Level" in result

    def test_dump_graph_has_consistent_structure(
        self, simple_schema: SimpleSQLSchemaProvider
    ) -> None:
        """Test that dump_graph() has consistent structure with dashes and levels."""
        query = "MATCH (p:Person)-[:KNOWS]->(f:Person) WHERE p.name = 'Alice' RETURN f.name"
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, simple_schema)

        result = plan.dump_graph()

        # Should have level markers
        assert "Level 0:" in result
        # Should have separator lines
        assert "------" in result
        # Should have InOpIds and OutOpIds
        assert "InOpIds=" in result
        assert "OutOpIds=" in result
