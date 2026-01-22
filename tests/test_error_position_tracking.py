"""Test error position tracking in ColumnResolutionError.

This module tests that errors show the correct line and column position
in the query text when column resolution fails.
"""

import pytest

from gsql2rsql import OpenCypherParser, LogicalPlan
from gsql2rsql.common.exceptions import ColumnResolutionError
from gsql2rsql.common.schema import (
    EntityProperty,
    NodeSchema,
    EdgeSchema,
)
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider


def test_undefined_variable_shows_correct_position():
    """Test that undefined variable errors show correct position in query."""
    # Setup schema
    schema = SimpleSQLSchemaProvider()
    person_node = NodeSchema(
        name="Person",
        properties=[
            EntityProperty("name", str),
            EntityProperty("age", int),
        ],
    )
    schema.add_node(person_node)

    knows_edge = EdgeSchema(
        name="KNOWS",
        source_node_id="Person",
        sink_node_id="Person",
        properties=[EntityProperty("since", int)],
    )
    schema.add_edge(knows_edge)

    # Query with undefined variable 'x' on line 3
    query = """MATCH (p:Person)-[:KNOWS]->(f:Person)
WITH p, COUNT(f) AS friends
WHERE x.age > 30
RETURN p.name, friends"""

    # Parse and plan
    parser = OpenCypherParser()
    ast = parser.parse(query)
    plan = LogicalPlan.process_query_tree(ast, schema)

    # Try to resolve - should fail with ColumnResolutionError
    with pytest.raises(ColumnResolutionError) as exc_info:
        plan.resolve(original_query=query)

    error = exc_info.value
    context = error.context

    # Verify error context has position information
    assert context.query_text == query
    assert context.error_offset > 0, "Error offset should be set"

    # Verify the error points to 'x' (should be somewhere in line 3)
    # Find position of 'x' in the query
    expected_offset = query.find("WHERE x")
    if expected_offset != -1:
        expected_offset += len("WHERE ")  # Point to 'x'

    # The offset should be close to where 'x' appears
    # (might not be exact due to first-occurrence heuristic)
    assert abs(context.error_offset - expected_offset) < 50, \
        f"Error offset {context.error_offset} should be near 'x' at {expected_offset}"

    # Verify error message contains query with pointer
    error_str = str(error)
    assert "WHERE x.age > 30" in error_str, "Error should show the line with the error"
    assert "▲" in error_str or "ERROR:" in error_str, "Error should have a pointer"

    # Verify it shows line 3 (not line 1 empty)
    assert "  3 │" in error_str or "  3 |" in error_str, \
        "Error should show line 3 where the error occurs"


def test_invalid_property_shows_correct_position():
    """Test that invalid property errors show correct position in query."""
    # Setup schema
    schema = SimpleSQLSchemaProvider()
    person_node = NodeSchema(
        name="Person",
        properties=[
            EntityProperty("name", str),
            EntityProperty("age", int),
        ],  # Note: no 'email' property
    )
    schema.add_node(person_node)

    # Query with invalid property 'email' on line 2
    query = """MATCH (p:Person)
WHERE p.email = 'test@example.com'
RETURN p.name"""

    # Parse and plan
    parser = OpenCypherParser()
    ast = parser.parse(query)
    plan = LogicalPlan.process_query_tree(ast, schema)

    # Try to resolve - should fail with ColumnResolutionError
    with pytest.raises(ColumnResolutionError) as exc_info:
        plan.resolve(original_query=query)

    error = exc_info.value
    context = error.context

    # Verify error context has position information
    assert context.query_text == query
    assert context.error_offset > 0, "Error offset should be set"

    # Verify error message contains the problematic line
    error_str = str(error)
    assert "p.email" in error_str, "Error should mention p.email"
    assert "▲" in error_str or "ERROR:" in error_str, "Error should have a pointer"

    # Verify it shows line 2 where the error occurs
    assert "  2 │" in error_str or "  2 |" in error_str, \
        "Error should show line 2 where p.email appears"


def test_multiline_query_correct_line_calculation():
    """Test that line numbers are calculated correctly for multiline queries."""
    # Setup schema
    schema = SimpleSQLSchemaProvider()
    person_node = NodeSchema(
        name="Person",
        properties=[EntityProperty("name", str)],
    )
    schema.add_node(person_node)

    city_node = NodeSchema(
        name="City",
        properties=[EntityProperty("name", str)],
    )
    schema.add_node(city_node)

    lives_in_edge = EdgeSchema(
        name="LIVES_IN",
        source_node_id="Person",
        sink_node_id="City",
        properties=[],
    )
    schema.add_edge(lives_in_edge)

    # Query with undefined variable on line 5
    query = """MATCH (p:Person)
WITH p, p.name AS pname
MATCH (c:City)
WITH c, c.name AS cname
WHERE unknown_var = 'test'
RETURN pname, cname"""

    # Parse and plan
    parser = OpenCypherParser()
    ast = parser.parse(query)
    plan = LogicalPlan.process_query_tree(ast, schema)

    # Try to resolve - should fail
    with pytest.raises(ColumnResolutionError) as exc_info:
        plan.resolve(original_query=query)

    error_str = str(exc_info.value)

    # Verify it shows line 5 where the error occurs
    assert "  5 │" in error_str or "  5 |" in error_str, \
        "Error should show line 5 where unknown_var appears"
    assert "unknown_var" in error_str, "Error should show the problematic variable"


def test_error_with_no_query_text():
    """Test that errors still work when query text is not available."""
    # This is a defensive test - errors should still be raised even without position info

    schema = SimpleSQLSchemaProvider()
    person_node = NodeSchema(
        name="Person",
        properties=[EntityProperty("name", str)],
    )
    schema.add_node(person_node)

    query = "MATCH (p:Person) WHERE x.name = 'test' RETURN p"

    parser = OpenCypherParser()
    ast = parser.parse(query)
    plan = LogicalPlan.process_query_tree(ast, schema)

    # Resolve without passing original_query (edge case)
    with pytest.raises(ColumnResolutionError) as exc_info:
        plan.resolve(original_query="")  # Empty query text

    error = exc_info.value
    # Error should still be raised, even if position info is missing
    assert "Variable 'x' is not defined" in str(error)
