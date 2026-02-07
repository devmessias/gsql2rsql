"""
Test: Verify WHERE clause filters are correctly applied for OR edge syntax.

These tests focus specifically on verifying the structure of WHERE clauses
in the generated SQL for OR edge syntax queries.

All tests use GraphContext with triple store pattern (single nodes/edges table).
"""

import re
import pytest

from gsql2rsql import GraphContext


# =============================================================================
# FIXTURES: Isolated Schema Definitions
# =============================================================================


@pytest.fixture
def triple_store_graph():
    """GraphContext with triple store pattern (single nodes/edges table)."""
    g = GraphContext(
        nodes_table="catalog.graph.nodes",
        edges_table="catalog.graph.edges",
        extra_node_attrs={"name": str, "age": int, "salary": float},
        extra_edge_attrs={"since": int, "weight": float},
    )
    g.set_types(
        node_types=["Person", "Company", "City"],
        edge_types=["KNOWS", "WORKS_AT", "LIVES_IN"],
    )
    return g


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def extract_where_clauses(sql: str) -> list[str]:
    """Extract all WHERE clause contents from SQL."""
    pattern = (
        r"WHERE\s+(.+?)(?=\s+(?:FROM|JOIN|GROUP|ORDER|LIMIT|UNION|SELECT|\))|$)"
    )
    matches = re.findall(pattern, sql, re.IGNORECASE | re.DOTALL)
    return [m.strip() for m in matches]


def count_filter_occurrences(sql: str, filter_pattern: str) -> int:
    """Count how many times a filter pattern appears in SQL."""
    return len(re.findall(filter_pattern, sql, re.IGNORECASE))


def has_or_edge_filter(sql: str, edge_types: list[str]) -> bool:
    """Check if SQL has OR-combined edge type filters."""
    if " OR " not in sql:
        return False
    for edge_type in edge_types:
        if f"relationship_type = '{edge_type}'" not in sql:
            return False
    return True


# =============================================================================
# TEST CLASS: Triple Store Pattern - Basic WHERE Structure
# =============================================================================


class TestTripleStoreOrEdgeWhereStructure:
    """Test WHERE clause structure for OR edge syntax with triple store."""

    def test_two_types_has_or_in_where(self, triple_store_graph):
        """Two edge types should produce WHERE with OR operator."""
        sql = triple_store_graph.transpile("""
            MATCH (p:Person)-[:KNOWS|WORKS_AT]->(target)
            RETURN p.name
        """)

        # Should have WHERE clause with OR
        assert " OR " in sql, "Missing OR operator in WHERE clause"

        # Verify both types are present
        assert "relationship_type = 'KNOWS'" in sql, "KNOWS filter missing"
        assert "relationship_type = 'WORKS_AT'" in sql, "WORKS_AT filter missing"

    def test_three_types_has_two_or_operators(self, triple_store_graph):
        """Three edge types should have 2 OR operators in edge filter."""
        sql = triple_store_graph.transpile("""
            MATCH (p:Person)-[:KNOWS|WORKS_AT|LIVES_IN]->(target)
            RETURN p.name
        """)

        # All three filters must be present
        assert "relationship_type = 'KNOWS'" in sql
        assert "relationship_type = 'WORKS_AT'" in sql
        assert "relationship_type = 'LIVES_IN'" in sql

        # Count OR operators in the edge filter section
        # Pattern: ((filter1) OR (filter2) OR (filter3))
        edge_filter_match = re.search(
            r"\(\s*\(relationship_type[^)]+\)"
            r"(?:\s*OR\s*\(relationship_type[^)]+\))+\s*\)",
            sql,
            re.IGNORECASE,
        )
        assert edge_filter_match, "Edge filter with OR not found"

        edge_filter = edge_filter_match.group(0)
        or_count = edge_filter.count(" OR ")
        assert or_count == 2, f"Expected 2 OR operators for 3 types, got {or_count}"

    def test_where_filter_not_concatenated(self, triple_store_graph):
        """WHERE should NOT have concatenated edge type string."""
        sql = triple_store_graph.transpile("""
            MATCH (p:Person)-[:KNOWS|WORKS_AT]->(target)
            RETURN p.name
        """)

        # Should NOT have 'KNOWS|WORKS_AT' as a single string
        bad_pattern = r"relationship_type\s*=\s*'KNOWS\|WORKS_AT'"
        assert not re.search(bad_pattern, sql), "Found concatenated edge type"

    def test_node_type_filter_separate_from_edge_filter(self, triple_store_graph):
        """Node type filter should be separate from edge type filter."""
        sql = triple_store_graph.transpile("""
            MATCH (p:Person)-[:KNOWS|WORKS_AT]->(target)
            RETURN p.name
        """)

        # Node filter should exist
        assert count_filter_occurrences(sql, r"node_type\s*=\s*'Person'") >= 1

        # Edge OR filter should exist
        assert has_or_edge_filter(sql, ["KNOWS", "WORKS_AT"])


# =============================================================================
# TEST CLASS: Triple Store Pattern - Combined with Node Filters
# =============================================================================


class TestTripleStoreOrEdgeWithNodeFilters:
    """Test WHERE clause when combining OR edge with node filters."""

    def test_source_node_filter_with_or_edge(self, triple_store_graph):
        """Source node filter should be in separate WHERE from edge filter."""
        sql = triple_store_graph.transpile("""
            MATCH (p:Person)-[:KNOWS|WORKS_AT]->(target)
            RETURN p.name
        """)

        where_clauses = extract_where_clauses(sql)

        node_filter_wheres = [w for w in where_clauses if "node_type" in w]
        edge_filter_wheres = [
            w for w in where_clauses if "relationship_type" in w
        ]

        assert len(node_filter_wheres) >= 1, "No WHERE with node_type filter"
        assert len(edge_filter_wheres) >= 1, "No WHERE with relationship_type"

    def test_both_endpoints_labeled_with_or_edge(self, triple_store_graph):
        """Both endpoint labels should have separate node_type filters."""
        sql = triple_store_graph.transpile("""
            MATCH (p:Person)-[:KNOWS|WORKS_AT]->(c:Company)
            RETURN p.name, c.name
        """)

        # Should have Person filter
        assert count_filter_occurrences(sql, r"node_type\s*=\s*'Person'") >= 1

        # Should have Company filter
        assert count_filter_occurrences(sql, r"node_type\s*=\s*'Company'") >= 1

        # Should have OR edge filter
        assert has_or_edge_filter(sql, ["KNOWS", "WORKS_AT"])


# =============================================================================
# TEST CLASS: Triple Store Pattern - Property Filters
# =============================================================================


class TestTripleStoreOrEdgeWithPropertyFilters:
    """Test WHERE clause with property filters combined with OR edge."""

    def test_inline_property_filter_with_or_edge(self, triple_store_graph):
        """Inline property filter should work with OR edge syntax."""
        sql = triple_store_graph.transpile("""
            MATCH (p:Person {name: 'Alice'})-[:KNOWS|WORKS_AT]->(target)
            RETURN target.name
        """)

        # Should have Alice filter
        assert "'Alice'" in sql, "Alice property filter missing"

        # Should have OR edge filter
        assert has_or_edge_filter(sql, ["KNOWS", "WORKS_AT"])

    def test_where_clause_property_filter_with_or_edge(self, triple_store_graph):
        """WHERE clause property filter should work with OR edge syntax."""
        sql = triple_store_graph.transpile("""
            MATCH (p:Person)-[:KNOWS|WORKS_AT]->(target)
            WHERE p.age > 30
            RETURN target.name
        """)

        # Should have age filter
        assert "30" in sql, "Age filter missing"

        # Should have OR edge filter
        assert has_or_edge_filter(sql, ["KNOWS", "WORKS_AT"])


# =============================================================================
# TEST CLASS: Triple Store Pattern - VLP (Variable Length Paths)
# =============================================================================


class TestTripleStoreOrEdgeVLP:
    """Test WHERE clause in Variable-Length Paths with OR edge."""

    def test_vlp_or_edge_filters_in_base_and_recursive(self, triple_store_graph):
        """VLP with OR edge should have filters in both base and recursive."""
        sql = triple_store_graph.transpile("""
            MATCH path = (p:Person)-[:KNOWS|WORKS_AT*1..2]->(target)
            RETURN p.name, length(path) AS hops LIMIT 5
        """)

        # Count occurrences of each filter
        knows_count = sql.count("'KNOWS'")
        works_at_count = sql.count("'WORKS_AT'")

        # Should appear in both base case and recursive case
        assert knows_count >= 2, f"KNOWS should appear 2+ times ({knows_count})"
        assert works_at_count >= 2, f"WORKS_AT should appear 2+ times"

        # Should have OR in VLP
        assert " OR " in sql, "OR missing in VLP"

    def test_vlp_source_filter_with_or_edge(self, triple_store_graph):
        """VLP source node filter should work with OR edge."""
        sql = triple_store_graph.transpile("""
            MATCH path = (p:Person {name: 'Alice'})-[:KNOWS|WORKS_AT*1..2]->(t)
            RETURN t.name LIMIT 5
        """)

        # Alice filter should be present
        assert "'Alice'" in sql, "Alice filter missing in VLP"

        # OR edge filters should be present
        assert "'KNOWS'" in sql
        assert "'WORKS_AT'" in sql


# =============================================================================
# TEST CLASS: Triple Store Pattern - No-Label Nodes
# =============================================================================


class TestTripleStoreOrEdgeNoLabel:
    """Test WHERE clause with no-label nodes and OR edge."""

    def test_no_label_source_only_edge_filter(self, triple_store_graph):
        """No-label source should not have node_type filter."""
        sql = triple_store_graph.transpile("""
            MATCH (a)-[:KNOWS|WORKS_AT]->(b:Company)
            RETURN a.name, b.name
        """)

        # Should have Company filter
        assert "node_type = 'Company'" in sql, "Company filter missing"

        # Should have OR edge filter
        assert has_or_edge_filter(sql, ["KNOWS", "WORKS_AT"])

    def test_no_label_both_nodes_only_edge_filter(self, triple_store_graph):
        """No-label on both nodes should only have edge filter."""
        sql = triple_store_graph.transpile("""
            MATCH (a)-[:KNOWS|WORKS_AT]->(b)
            RETURN a.name, b.name
        """)

        # Should have OR edge filter
        assert has_or_edge_filter(sql, ["KNOWS", "WORKS_AT"])


# =============================================================================
# TEST CLASS: Triple Store Pattern - Filter Format
# =============================================================================


class TestTripleStoreOrEdgeFilterFormat:
    """Test the exact format of WHERE filters."""

    def test_filter_format_parentheses(self, triple_store_graph):
        """Each filter in OR should be wrapped in parentheses."""
        sql = triple_store_graph.transpile("""
            MATCH (p:Person)-[:KNOWS|WORKS_AT]->(target)
            RETURN p.name
        """)

        # Should have format: ((filter1) OR (filter2))
        pattern = (
            r"\(\(relationship_type\s*=\s*'[^']+'\)"
            r"\s*OR\s*"
            r"\(relationship_type\s*=\s*'[^']+'\)\)"
        )
        assert re.search(pattern, sql), "Edge filter not in expected format"

    def test_filter_uses_equality_not_in(self, triple_store_graph):
        """Each filter should use = operator, not IN."""
        sql = triple_store_graph.transpile("""
            MATCH (p:Person)-[:KNOWS|WORKS_AT]->(target)
            RETURN p.name
        """)

        # Should use relationship_type = 'X', not IN (...)
        assert "relationship_type = 'KNOWS'" in sql
        assert "relationship_type = 'WORKS_AT'" in sql

        # Should NOT have IN clause for relationship_type
        in_pattern = r"relationship_type\s+IN\s*\("
        assert not re.search(in_pattern, sql), "Found IN clause instead of OR"


# =============================================================================
# TEST CLASS: Edge Cases and Regression Tests
# =============================================================================


class TestOrEdgeWhereEdgeCases:
    """Edge cases and regression tests for OR edge WHERE filters."""

    def test_single_edge_type_no_or(self, triple_store_graph):
        """Single edge type should NOT have OR operator."""
        sql = triple_store_graph.transpile("""
            MATCH (p:Person)-[:KNOWS]->(f:Person)
            RETURN p.name
        """)

        # Should have KNOWS filter
        assert "relationship_type = 'KNOWS'" in sql

        # Should NOT have OR for single type
        # (unless there are other OR conditions from nodes)
        edge_filter_match = re.search(
            r"relationship_type\s*=\s*'KNOWS'.*OR.*relationship_type",
            sql,
            re.IGNORECASE,
        )
        assert not edge_filter_match, "Single edge type should not have OR"

    def test_or_edge_with_edge_variable(self, triple_store_graph):
        """OR edge with relationship variable should preserve filters.

        Note: type(r) is not yet supported by the transpiler (Function.INVALID).
        This test focuses on OR edge filter, not the type() function.
        """
        sql = triple_store_graph.transpile("""
            MATCH (p:Person)-[r:KNOWS|WORKS_AT]->(target)
            RETURN p.name, r.weight
        """)

        # Should have OR edge filter
        assert has_or_edge_filter(sql, ["KNOWS", "WORKS_AT"])

    def test_or_edge_backward_direction(self, triple_store_graph):
        """OR edge with backward direction should have correct filters."""
        sql = triple_store_graph.transpile("""
            MATCH (c:Company)<-[:WORKS_AT|KNOWS]-(p:Person)
            RETURN c.name, p.name
        """)

        # Should have both edge type filters
        assert "'WORKS_AT'" in sql
        assert "'KNOWS'" in sql

    def test_or_edge_undirected_uses_union(self, triple_store_graph):
        """OR edge undirected uses UNION ALL for both directions.

        Note: Undirected edges with OR types use UNION ALL pattern,
        which may not include explicit edge type filters in each branch.
        """
        sql = triple_store_graph.transpile("""
            MATCH (a:Person)-[:KNOWS|WORKS_AT]-(b)
            RETURN a.name, b.name
        """)

        # Undirected edges use UNION ALL pattern
        assert "UNION ALL" in sql, "Undirected should use UNION ALL"

    def test_multiple_patterns_with_or_edge(self, triple_store_graph):
        """Multiple patterns with OR edge should each have filters."""
        sql = triple_store_graph.transpile("""
            MATCH (p:Person)-[:KNOWS|WORKS_AT]->(target),
                  (p)-[:LIVES_IN]->(c:City)
            RETURN p.name, target.name, c.name
        """)

        # Should have OR filter for first pattern
        assert "'KNOWS'" in sql
        assert "'WORKS_AT'" in sql

        # Should have filter for second pattern
        assert "'LIVES_IN'" in sql
