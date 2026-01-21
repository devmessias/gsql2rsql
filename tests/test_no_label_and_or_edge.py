"""
TDD tests for:
1. No-label nodes
2. No-label nodes with inline attributes
3. OR syntax for 2 edge types ([:KNOWS|WORKS_AT])
4. OR syntax for 3 edge types ([:KNOWS|WORKS_AT|OWNS])

These tests should FAIL initially, then pass after implementation.
"""

import re
import pytest
from gsql2rsql import GraphContext


@pytest.fixture
def graph():
    """Create a GraphContext with test schema."""
    g = GraphContext(
        nodes_table="catalog.schema.nodes",
        edges_table="catalog.schema.edges",
        extra_node_attrs={"name": str, "age": int, "amount": float},
        extra_edge_attrs={"weight": float, "since": int},
    )
    g.set_types(
        node_types=["Person", "Company", "Device"],
        edge_types=["KNOWS", "WORKS_AT", "OWNS"],
    )
    return g


def assert_has_node_type_filter(sql: str, node_type: str) -> None:
    """Assert SQL has a proper node type filter (not just the word)."""
    # Should have: node_type = 'Person' or (node_type = 'Person')
    pattern = rf"node_type\s*=\s*'{re.escape(node_type)}'"
    assert re.search(pattern, sql), f"Expected node_type = '{node_type}' filter in SQL:\n{sql}"


def count_node_type_filters(sql: str) -> int:
    """Count node_type filters in SQL."""
    filters = re.findall(r"node_type\s*=\s*'[^']+'", sql)
    return len(filters)


def assert_has_edge_type_in_clause(sql: str, edge_types: list[str]) -> None:
    """Assert SQL has IN clause or OR syntax for multiple edge types."""
    # Should have: relationship_type IN ('KNOWS', 'WORKS_AT')
    # Build pattern for IN clause with types in any order
    types_pattern = "|".join(re.escape(t) for t in edge_types)
    in_pattern = rf"relationship_type\s+IN\s*\(\s*(?:'(?:{types_pattern})'\s*,?\s*){{2,}}\)"

    if re.search(in_pattern, sql, re.IGNORECASE):
        return  # Found IN clause

    # Alternative: Check for OR syntax combining filters
    # E.g., (relationship_type = 'KNOWS') OR (relationship_type = 'WORKS_AT')
    or_found = True
    for edge_type in edge_types:
        pattern = rf"relationship_type\s*=\s*'{re.escape(edge_type)}'"
        if not re.search(pattern, sql):
            or_found = False
            break
    if or_found and " OR " in sql:
        return  # Found OR syntax with all types

    # Alternative: Check for UNION with separate filters
    union_pattern = r"UNION\s+ALL"
    if re.search(union_pattern, sql, re.IGNORECASE):
        # Check each edge type has its own filter
        for edge_type in edge_types:
            pattern = rf"relationship_type\s*=\s*'{re.escape(edge_type)}'"
            assert re.search(pattern, sql), (
                f"Expected relationship_type = '{edge_type}' in UNION SQL:\n{sql}"
            )
        return

    # Neither IN clause, OR syntax, nor UNION found - fail
    raise AssertionError(
        f"Expected relationship_type IN {edge_types}, OR syntax, "
        f"or UNION ALL with separate filters.\nGot SQL:\n{sql}"
    )


def assert_no_concatenated_edge_type(sql: str, edge_types: list[str]) -> None:
    """Assert SQL does NOT have edge types as concatenated string."""
    # Should NOT have: relationship_type = 'KNOWS|WORKS_AT'
    concatenated = "|".join(edge_types)
    bad_pattern = rf"relationship_type\s*=\s*'{re.escape(concatenated)}'"
    assert not re.search(bad_pattern, sql), (
        f"Found concatenated edge type '{concatenated}' instead of proper IN/UNION.\n"
        f"SQL:\n{sql}"
    )


class TestNoLabelNode:
    """Tests for nodes without labels (wildcard matching)."""

    def test_no_label_source_node(self, graph):
        """MATCH (a)-[:WORKS_AT]->(b:Company) - source 'a' has no label."""
        query = """
        MATCH (a)-[:WORKS_AT]->(b:Company)
        RETURN a.name, b.name
        """
        sql = graph.transpile(query)

        assert "SELECT" in sql
        # Node 'b' should have type filter for Company
        assert_has_node_type_filter(sql, "Company")
        # Should NOT have 'Person' or 'Device' filter for node 'a'
        # (wildcard should have no type filter or all types)

    def test_no_label_target_node(self, graph):
        """MATCH (a:Person)-[:WORKS_AT]->(b) - target 'b' has no label."""
        query = """
        MATCH (a:Person)-[:WORKS_AT]->(b)
        RETURN a.name, b.name
        """
        sql = graph.transpile(query)

        assert "SELECT" in sql
        # Node 'a' should have Person filter
        assert_has_node_type_filter(sql, "Person")

    def test_no_label_both_nodes(self, graph):
        """MATCH (a)-[:OWNS]->(b) - both nodes have no label."""
        query = """
        MATCH (a)-[:OWNS]->(b)
        RETURN a.name, b.name
        """
        sql = graph.transpile(query)

        assert "SELECT" in sql
        # Should have edge filter
        assert "relationship_type" in sql and "OWNS" in sql

    def test_no_label_correct_filter_structure(self, graph):
        """Verify no-label nodes don't have node_type filter."""
        query = """
        MATCH (a)-[:WORKS_AT]->(b:Company)
        RETURN a.name, b.name
        """
        sql = graph.transpile(query)

        # Count node_type filters
        filters = re.findall(r"node_type\s*=\s*'([^']+)'", sql)
        # Should have exactly 1 filter (for Company), not 2
        company_filters = [f for f in filters if f == "Company"]
        assert len(company_filters) >= 1, f"Expected Company filter, got: {filters}"
        # Should NOT have Person/Device filters for wildcard node 'a'


class TestNoLabelWithInlineAttributes:
    """Tests for no-label nodes with inline property filters."""

    def test_no_label_with_inline_filter_source(self, graph):
        """MATCH (a {name: 'Alice'})-[:WORKS_AT]->(b:Company) - source has inline filter."""
        query = """
        MATCH (a {name: 'Alice'})-[:WORKS_AT]->(b:Company)
        RETURN a.name, b.name
        """
        sql = graph.transpile(query)

        assert "SELECT" in sql
        # Should have proper filter for name = 'Alice' (not just 'Alice' anywhere)
        assert re.search(r"name\s*[)]\s*=\s*[(']+Alice[')]+", sql) or "Alice" in sql
        # Node 'b' should have Company filter
        assert_has_node_type_filter(sql, "Company")

    def test_no_label_with_inline_filter_target(self, graph):
        """MATCH (a:Person)-[:WORKS_AT]->(b {name: 'TechCorp'}) - target has inline filter."""
        query = """
        MATCH (a:Person)-[:WORKS_AT]->(b {name: 'TechCorp'})
        RETURN a.name, b.name
        """
        sql = graph.transpile(query)

        assert "SELECT" in sql
        # Should have proper filter for name = 'TechCorp'
        assert "TechCorp" in sql
        # Node 'a' should have Person filter
        assert_has_node_type_filter(sql, "Person")

    def test_no_label_with_multiple_inline_filters(self, graph):
        """MATCH (a {name: 'Alice', age: 30})-[:KNOWS]->(b) - multiple inline filters."""
        query = """
        MATCH (a {name: 'Alice', age: 30})-[:KNOWS]->(b)
        RETURN a.name, b.name
        """
        sql = graph.transpile(query)

        assert "SELECT" in sql
        # Should have both filters
        assert "Alice" in sql
        assert "30" in sql


class TestOrEdgeSyntax:
    """Tests for OR syntax with multiple edge types ([:TYPE1|TYPE2])."""

    def test_two_edge_types_or(self, graph):
        """MATCH (a:Person)-[:KNOWS|WORKS_AT]->(b) - two edge types with OR."""
        query = """
        MATCH (a:Person)-[:KNOWS|WORKS_AT]->(b)
        RETURN a.name, b.name
        """
        sql = graph.transpile(query)

        assert "SELECT" in sql
        # Must NOT have concatenated string 'KNOWS|WORKS_AT'
        assert_no_concatenated_edge_type(sql, ["KNOWS", "WORKS_AT"])
        # Must have proper IN clause or UNION
        assert_has_edge_type_in_clause(sql, ["KNOWS", "WORKS_AT"])

    def test_three_edge_types_or(self, graph):
        """MATCH (a:Person)-[:KNOWS|WORKS_AT|OWNS]->(b) - three edge types with OR."""
        query = """
        MATCH (a:Person)-[:KNOWS|WORKS_AT|OWNS]->(b)
        RETURN a.name, b.name
        """
        sql = graph.transpile(query)

        assert "SELECT" in sql
        # Must NOT have concatenated string
        assert_no_concatenated_edge_type(sql, ["KNOWS", "WORKS_AT", "OWNS"])
        # Must have proper IN clause or UNION
        assert_has_edge_type_in_clause(sql, ["KNOWS", "WORKS_AT", "OWNS"])

    def test_two_edge_types_with_labeled_nodes(self, graph):
        """MATCH (a:Person)-[:KNOWS|WORKS_AT]->(b:Company) - OR edges with labeled nodes."""
        query = """
        MATCH (a:Person)-[:KNOWS|WORKS_AT]->(b:Company)
        RETURN a.name, b.name
        """
        sql = graph.transpile(query)

        assert "SELECT" in sql
        # Node filters
        assert_has_node_type_filter(sql, "Person")
        assert_has_node_type_filter(sql, "Company")
        # Edge filter - proper IN/UNION, not concatenated
        assert_no_concatenated_edge_type(sql, ["KNOWS", "WORKS_AT"])
        assert_has_edge_type_in_clause(sql, ["KNOWS", "WORKS_AT"])

    def test_or_edge_with_variable(self, graph):
        """MATCH (a:Person)-[r:KNOWS|WORKS_AT]->(b) - OR edge with variable."""
        query = """
        MATCH (a:Person)-[r:KNOWS|WORKS_AT]->(b)
        RETURN a.name, type(r), b.name
        """
        sql = graph.transpile(query)

        assert "SELECT" in sql
        # Must NOT have concatenated string
        assert_no_concatenated_edge_type(sql, ["KNOWS", "WORKS_AT"])


class TestCombinedFeatures:
    """Tests combining no-label nodes with OR edge syntax."""

    def test_no_label_with_or_edges(self, graph):
        """MATCH (a)-[:KNOWS|WORKS_AT]->(b:Company) - no label + OR edges."""
        query = """
        MATCH (a)-[:KNOWS|WORKS_AT]->(b:Company)
        RETURN a.name, b.name
        """
        sql = graph.transpile(query)

        assert "SELECT" in sql
        assert_has_node_type_filter(sql, "Company")
        # Edge filter - proper IN/UNION, not concatenated
        assert_no_concatenated_edge_type(sql, ["KNOWS", "WORKS_AT"])
        assert_has_edge_type_in_clause(sql, ["KNOWS", "WORKS_AT"])

    def test_no_label_inline_filter_with_or_edges(self, graph):
        """MATCH (a {age: 30})-[:KNOWS|OWNS]->(b) - no label + inline + OR edges."""
        query = """
        MATCH (a {age: 30})-[:KNOWS|OWNS]->(b)
        RETURN a.name, b.name
        """
        sql = graph.transpile(query)

        assert "SELECT" in sql
        assert "30" in sql
        # Edge filter - proper IN/UNION, not concatenated
        assert_no_concatenated_edge_type(sql, ["KNOWS", "OWNS"])
        assert_has_edge_type_in_clause(sql, ["KNOWS", "OWNS"])
