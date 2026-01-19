"""Test that symbols don't appear in both available and out-of-scope lists.

This test verifies the fix for the symbol duplication bug where variables
could appear in both the available symbols list and the out-of-scope symbols
list simultaneously.
"""

import pytest

from gsql2rsql import OpenCypherParser, LogicalPlan
from gsql2rsql.common.exceptions import ColumnResolutionError
from gsql2rsql.common.schema import EntityProperty, NodeSchema, EdgeSchema, SimpleGraphSchemaProvider


def test_symbol_not_in_both_available_and_out_of_scope():
    """Test that re-defined symbols are removed from out-of-scope list."""
    # Setup schema
    schema = SimpleGraphSchemaProvider()
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

    # Query with aggregation followed by MATCH
    # This causes 'p' to go out of scope, then be re-introduced by the second MATCH
    query = """MATCH (p:Person)
WITH p, COUNT(*) AS person_count
MATCH (c:City)<-[r:LIVES_IN]-(p)
WHERE unknown_var = 'test'
RETURN c.name, person_count"""

    # Parse and plan
    parser = OpenCypherParser()
    ast = parser.parse(query)
    plan = LogicalPlan.process_query_tree(ast, schema)

    # Try to resolve - should fail with ColumnResolutionError
    with pytest.raises(ColumnResolutionError) as exc_info:
        plan.resolve(original_query=query)

    error = exc_info.value
    context = error.context

    # Get available and out-of-scope symbol names
    available_names = {sym.name for sym in context.available_symbols}
    out_of_scope_names = {sym.name for sym, _ in context.out_of_scope_symbols}

    # Check that no symbol appears in both lists
    overlap = available_names & out_of_scope_names
    assert not overlap, (
        f"Symbols should not appear in both available and out-of-scope lists. "
        f"Found overlap: {overlap}"
    )

    # Verify that 'p' is available (re-defined by second MATCH) but NOT out-of-scope
    assert 'p' in available_names, "'p' should be available (from second MATCH)"
    assert 'p' not in out_of_scope_names, "'p' should NOT be in out-of-scope list after being re-defined"

    # Verify that 'c' and 'r' are available but NOT out-of-scope
    assert 'c' in available_names, "'c' should be available"
    assert 'c' not in out_of_scope_names, "'c' should NOT be in out-of-scope list"
    assert 'r' in available_names, "'r' should be available"
    assert 'r' not in out_of_scope_names, "'r' should NOT be in out-of-scope list"


def test_no_duplicate_entries_in_out_of_scope_list():
    """Test that out-of-scope list doesn't contain duplicate entries."""
    # Setup schema
    schema = SimpleGraphSchemaProvider()
    person_node = NodeSchema(
        name="Person",
        properties=[EntityProperty("name", str), EntityProperty("age", int)],
    )
    schema.add_node(person_node)

    # Query with undefined variable
    query = """MATCH (p:Person)
WITH p, COUNT(*) AS cnt
WHERE undefined_var = 'test'
RETURN p.name, cnt"""

    # Parse and plan
    parser = OpenCypherParser()
    ast = parser.parse(query)
    plan = LogicalPlan.process_query_tree(ast, schema)

    # Try to resolve
    with pytest.raises(ColumnResolutionError) as exc_info:
        plan.resolve(original_query=query)

    error = exc_info.value
    out_of_scope = error.context.out_of_scope_symbols

    # Check for duplicate entries (same name appearing multiple times)
    from collections import Counter
    names = [sym.name for sym, _ in out_of_scope]
    counts = Counter(names)

    duplicates = {name: count for name, count in counts.items() if count > 1}
    assert not duplicates, (
        f"Out-of-scope list should not contain duplicates. "
        f"Found: {duplicates}"
    )
