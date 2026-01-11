"""Pytest configuration and fixtures."""

from typing import Any
import pytest

from opencypher_transpiler.common.schema import (
    EdgeSchema,
    NodeSchema,
    SimpleGraphSchemaProvider,
)
from opencypher_transpiler.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)


@pytest.fixture
def movie_graph_schema() -> SimpleGraphSchemaProvider:
    """Create a movie graph schema fixture.

    This matches the movie graph from the C# test data.
    """
    schema = SimpleGraphSchemaProvider()

    # Nodes
    schema.add_node(NodeSchema(name="Person"))
    schema.add_node(NodeSchema(name="Movie"))

    # Edges
    schema.add_edge(
        EdgeSchema(
            name="ACTED_IN",
            source_node_id="Person",
            sink_node_id="Movie",
        )
    )
    schema.add_edge(
        EdgeSchema(
            name="DIRECTED",
            source_node_id="Person",
            sink_node_id="Movie",
        )
    )
    schema.add_edge(
        EdgeSchema(
            name="PRODUCED",
            source_node_id="Person",
            sink_node_id="Movie",
        )
    )
    schema.add_edge(
        EdgeSchema(
            name="WROTE",
            source_node_id="Person",
            sink_node_id="Movie",
        )
    )
    schema.add_edge(
        EdgeSchema(
            name="FOLLOWS",
            source_node_id="Person",
            sink_node_id="Person",
        )
    )
    schema.add_edge(
        EdgeSchema(
            name="REVIEWED",
            source_node_id="Person",
            sink_node_id="Movie",
        )
    )

    return schema


@pytest.fixture
def movie_sql_schema() -> SimpleSQLSchemaProvider:
    """Create SQL schema mappings for the movie graph."""
    schema = SimpleSQLSchemaProvider()

    # Node tables
    schema.add_table(
        SQLTableDescriptor(
            entity_id="Person",
            table_name="dbo.Person",
            node_id_columns=["id"],
        )
    )
    schema.add_table(
        SQLTableDescriptor(
            entity_id="Movie",
            table_name="dbo.Movie",
            node_id_columns=["id"],
        )
    )

    # Edge tables
    schema.add_table(
        SQLTableDescriptor(
            entity_id="Person@ACTED_IN@Movie",
            table_name="dbo.ActedIn",
            node_id_columns=["person_id", "movie_id"],
        )
    )
    schema.add_table(
        SQLTableDescriptor(
            entity_id="Person@DIRECTED@Movie",
            table_name="dbo.Directed",
            node_id_columns=["person_id", "movie_id"],
        )
    )
    schema.add_table(
        SQLTableDescriptor(
            entity_id="Person@PRODUCED@Movie",
            table_name="dbo.Produced",
            node_id_columns=["person_id", "movie_id"],
        )
    )
    schema.add_table(
        SQLTableDescriptor(
            entity_id="Person@WROTE@Movie",
            table_name="dbo.Wrote",
            node_id_columns=["person_id", "movie_id"],
        )
    )
    schema.add_table(
        SQLTableDescriptor(
            entity_id="Person@FOLLOWS@Person",
            table_name="dbo.Follows",
            node_id_columns=["follower_id", "followed_id"],
        )
    )
    schema.add_table(
        SQLTableDescriptor(
            entity_id="Person@REVIEWED@Movie",
            table_name="dbo.Reviewed",
            node_id_columns=["person_id", "movie_id"],
        )
    )

    return schema
