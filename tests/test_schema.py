"""Tests for the graph schema."""

import pytest

from gsql2rsql.common.schema import (
    EdgeSchema,
    EntityProperty,
    NodeSchema,
)


class TestEntityProperty:
    """Tests for EntityProperty."""

    def test_create_property(self) -> None:
        """Test property creation."""
        prop = EntityProperty(property_name="name", data_type=str)
        assert prop.property_name == "name"
        assert prop.data_type == str

    def test_empty_name_raises(self) -> None:
        """Test that empty property name raises."""
        with pytest.raises(ValueError):
            EntityProperty(property_name="", data_type=str)


class TestNodeSchema:
    """Tests for NodeSchema."""

    def test_create_node_schema(self) -> None:
        """Test node schema creation."""
        schema = NodeSchema(
            name="Person",
            properties=[
                EntityProperty(property_name="name", data_type=str),
                EntityProperty(property_name="age", data_type=int),
            ],
        )
        assert schema.name == "Person"
        assert schema.id == "Person"
        assert len(schema.properties) == 2


class TestEdgeSchema:
    """Tests for EdgeSchema."""

    def test_create_edge_schema(self) -> None:
        """Test edge schema creation."""
        schema = EdgeSchema(
            name="KNOWS",
            source_node_id="Person",
            sink_node_id="Person",
        )
        assert schema.name == "KNOWS"
        assert schema.source_node_id == "Person"
        assert schema.sink_node_id == "Person"

    def test_edge_id(self) -> None:
        """Test edge ID generation."""
        schema = EdgeSchema(
            name="ACTED_IN",
            source_node_id="Person",
            sink_node_id="Movie",
        )
        expected_id = "Person@ACTED_IN@Movie"
        assert schema.id == expected_id

    def test_get_edge_id_class_method(self) -> None:
        """Test static edge ID generation."""
        edge_id = EdgeSchema.get_edge_id("KNOWS", "Person", "Person")
        assert edge_id == "Person@KNOWS@Person"
