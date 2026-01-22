"""
TDD Tests: Untyped Edges Support

These tests validate that edges without explicit type work correctly:
- Regular MATCH: (a:Person)-[]-(b)
- VLP: (a:Person)-[*1..3]-(b)

Expected behavior: Untyped edges should NOT have relationship_type filter,
similar to how no-label nodes don't have node_type filter.

Run with: uv run pytest tests/test_untyped_edges.py -v
"""

import pytest

from gsql2rsql import GraphContext


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def graph_multi_edge():
    """GraphContext with multiple edge types."""
    g = GraphContext(
        nodes_table="catalog.graph.nodes",
        edges_table="catalog.graph.edges",
        extra_node_attrs={"name": str, "age": int},
        extra_edge_attrs={"since": int, "weight": float},
    )
    g.set_types(
        node_types=["Person", "Company", "City"],
        edge_types=["KNOWS", "WORKS_AT", "LIVES_IN"],
    )
    return g


@pytest.fixture
def graph_single_edge():
    """GraphContext with single edge type."""
    g = GraphContext(
        nodes_table="catalog.graph.nodes",
        edges_table="catalog.graph.edges",
        extra_node_attrs={"name": str},
    )
    g.set_types(
        node_types=["Person"],
        edge_types=["KNOWS"],
    )
    return g


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def has_node_type_filter(sql: str, node_type: str) -> bool:
    """Check if SQL has filter for specific node type."""
    return f"node_type = '{node_type}'" in sql


def has_relationship_type_filter(sql: str) -> bool:
    """Check if SQL has ANY relationship_type filter."""
    return "relationship_type = " in sql


# =============================================================================
# TEST CLASS: Regular MATCH with Untyped Edge
# =============================================================================


class TestUntypedEdgeRegularMatch:
    """Test regular MATCH patterns with untyped edges."""

    def test_untyped_edge_basic(self, graph_multi_edge):
        """Untyped edge should NOT have relationship_type filter.

        Query: MATCH (a:Person)-[]-(b) RETURN a.name
        """
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)-[]-(b)
            RETURN a.name
        """)

        assert "SELECT" in sql
        assert has_node_type_filter(sql, "Person")
        # Key assertion: NO relationship_type filter
        assert not has_relationship_type_filter(sql), (
            "Untyped edge should NOT have relationship_type filter"
        )

    def test_untyped_edge_forward_direction(self, graph_multi_edge):
        """Untyped edge with forward direction."""
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)-[]->(b)
            RETURN a.name
        """)

        assert "SELECT" in sql
        assert not has_relationship_type_filter(sql)

    def test_untyped_edge_backward_direction(self, graph_multi_edge):
        """Untyped edge with backward direction."""
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)<-[]-(b)
            RETURN a.name
        """)

        assert "SELECT" in sql
        assert not has_relationship_type_filter(sql)

    def test_untyped_edge_double_dash_syntax(self, graph_multi_edge):
        """Double dash syntax (--) should also work."""
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)--(b)
            RETURN a.name
        """)

        assert "SELECT" in sql
        assert not has_relationship_type_filter(sql)

    def test_untyped_edge_both_nodes_labeled(self, graph_multi_edge):
        """Untyped edge with both endpoints labeled."""
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)-[]-(b:Company)
            RETURN a.name
        """)

        assert "SELECT" in sql
        assert has_node_type_filter(sql, "Person")
        assert has_node_type_filter(sql, "Company")
        assert not has_relationship_type_filter(sql)

    def test_untyped_edge_with_variable(self, graph_multi_edge):
        """Untyped edge with relationship variable compiles without error."""
        # Note: type(r) function is not yet implemented, so we just test
        # that the query with a relationship variable compiles
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)-[r]->(b)
            RETURN a.name
        """)

        assert "SELECT" in sql
        # Untyped edge should not have relationship_type filter
        assert not has_relationship_type_filter(sql)

    def test_untyped_edge_single_edge_schema(self, graph_single_edge):
        """Untyped edge with schema that has only one edge type."""
        sql = graph_single_edge.transpile("""
            MATCH (a:Person)-[]-(b)
            RETURN a.name
        """)

        assert "SELECT" in sql
        # Even with single edge type, untyped should not filter
        assert not has_relationship_type_filter(sql)


# =============================================================================
# TEST CLASS: VLP with Untyped Edge
# =============================================================================


class TestUntypedEdgeVLP:
    """Test Variable Length Path with untyped edges."""

    def test_vlp_untyped_basic(self, graph_multi_edge):
        """VLP without edge type should use recursive CTE without type filter.

        Query: MATCH (a:Person)-[*1..3]-(b) RETURN a.name
        """
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)-[*1..3]-(b)
            RETURN a.name
        """)

        assert "WITH RECURSIVE" in sql
        # No relationship_type filter in VLP
        assert not has_relationship_type_filter(sql)

    def test_vlp_untyped_with_direction(self, graph_multi_edge):
        """VLP without edge type, with direction."""
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)-[*1..2]->(b)
            RETURN a.name
        """)

        assert "WITH RECURSIVE" in sql
        assert not has_relationship_type_filter(sql)

    def test_vlp_untyped_with_path_variable(self, graph_multi_edge):
        """VLP without edge type, with path variable."""
        sql = graph_multi_edge.transpile("""
            MATCH path = (a:Person)-[*1..2]-(b)
            RETURN length(path) AS hops
        """)

        assert "WITH RECURSIVE" in sql

    def test_vlp_untyped_with_labeled_target(self, graph_multi_edge):
        """VLP without edge type, with labeled target compiles."""
        # Note: VLP currently doesn't apply node type filters in the final join.
        # This is a known limitation - the filter would be on the final WHERE.
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)-[*1..2]-(b:City)
            RETURN a.name
        """)

        assert "WITH RECURSIVE" in sql
        # No relationship_type filter for untyped edges
        assert not has_relationship_type_filter(sql)

    def test_vlp_untyped_both_no_label(self, graph_multi_edge):
        """VLP without edge type, both endpoints no-label."""
        sql = graph_multi_edge.transpile("""
            MATCH (a)-[*1..2]-(b)
            RETURN a.name
        """)

        assert "WITH RECURSIVE" in sql
        # No node_type filters either
        assert "node_type = " not in sql
        assert not has_relationship_type_filter(sql)

    def test_vlp_untyped_min_max_bounds(self, graph_multi_edge):
        """VLP without edge type, various min/max bounds."""
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)-[*2..5]-(b)
            RETURN a.name
        """)

        assert "WITH RECURSIVE" in sql
        assert "2" in sql  # min depth
        assert "5" in sql  # max depth


# =============================================================================
# TEST CLASS: Combined with Property Filters
# =============================================================================


class TestUntypedEdgeWithFilters:
    """Test untyped edges combined with property filters."""

    def test_untyped_edge_with_inline_node_filter(self, graph_multi_edge):
        """Untyped edge with inline property filter on node."""
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person {name: 'Alice'})-[]-(b)
            RETURN b.name
        """)

        assert "SELECT" in sql
        assert "'Alice'" in sql
        assert not has_relationship_type_filter(sql)

    def test_untyped_edge_with_where_clause(self, graph_multi_edge):
        """Untyped edge with WHERE clause filter."""
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)-[]-(b)
            WHERE a.age > 30
            RETURN b.name
        """)

        assert "SELECT" in sql
        assert "30" in sql
        assert not has_relationship_type_filter(sql)

    def test_vlp_untyped_with_where_on_endpoint(self, graph_multi_edge):
        """VLP untyped with WHERE on endpoint."""
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)-[*1..2]-(b)
            WHERE b.name = 'Bob'
            RETURN a.name
        """)

        assert "WITH RECURSIVE" in sql
        assert "'Bob'" in sql


# =============================================================================
# TEST CLASS: Edge Cases
# =============================================================================


class TestUntypedEdgeEdgeCases:
    """Edge cases for untyped edges."""

    def test_untyped_followed_by_typed(self, graph_multi_edge):
        """Multiple patterns: untyped followed by typed edge (directed)."""
        sql = graph_multi_edge.transpile("""
            MATCH (a)-[]->(b)-[:KNOWS]->(c)
            RETURN a.name
        """)

        assert "SELECT" in sql
        # KNOWS should have filter, untyped should not
        assert "relationship_type = 'KNOWS'" in sql

    def test_typed_followed_by_untyped(self, graph_multi_edge):
        """Multiple patterns: typed followed by untyped edge (directed)."""
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)-[:KNOWS]->(b)-[]->(c)
            RETURN a.name
        """)

        assert "SELECT" in sql
        assert "relationship_type = 'KNOWS'" in sql

    def test_multiple_untyped_edges(self, graph_multi_edge):
        """Multiple untyped edges in pattern."""
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)-[]-(b)-[]-(c)
            RETURN a.name
        """)

        assert "SELECT" in sql


# =============================================================================
# TEST CLASS: Comparison with Typed Edges
# =============================================================================


class TestUntypedVsTypedComparison:
    """Compare untyped edges with typed edges."""

    def test_typed_edge_has_filter(self, graph_multi_edge):
        """Typed edge SHOULD have relationship_type filter (directed)."""
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)-[:KNOWS]->(b)
            RETURN a.name
        """)

        assert "SELECT" in sql
        assert "relationship_type = 'KNOWS'" in sql

    def test_or_edge_has_multiple_filters(self, graph_multi_edge):
        """OR edge syntax SHOULD have filters for each type (directed)."""
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)-[:KNOWS|WORKS_AT]->(b)
            RETURN a.name
        """)

        assert "SELECT" in sql
        assert "relationship_type = 'KNOWS'" in sql
        assert "relationship_type = 'WORKS_AT'" in sql
        assert " OR " in sql

    def test_untyped_no_filter(self, graph_multi_edge):
        """Untyped edge should NOT have relationship_type filter."""
        sql = graph_multi_edge.transpile("""
            MATCH (a:Person)-[]-(b)
            RETURN a.name
        """)

        assert "SELECT" in sql
        assert not has_relationship_type_filter(sql)
