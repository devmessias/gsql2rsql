"""
Test: OR edge syntax with triple store pattern.

Tests verify that OR edge syntax like [:KNOWS|LIVES_IN|WORKS_AT] generates
correct SQL filters for all edge types, including when edge types have
different target node types.

All tests use isolated GraphContext with triple store pattern.
"""

import re
import pytest

from gsql2rsql import GraphContext


@pytest.fixture
def triple_store_graph():
    """Create GraphContext with triple store pattern."""
    graph = GraphContext(
        nodes_table="catalog.graph.nodes",
        edges_table="catalog.graph.edges",
        extra_node_attrs={"name": str, "population": int, "industry": str},
    )
    graph.set_types(
        node_types=["Person", "City", "Company", "Movie"],
        edge_types=["KNOWS", "LIVES_IN", "WORKS_AT", "ACTED_IN", "DIRECTED"],
    )
    return graph


class TestOrEdgeTripleStoreDifferentTargets:
    """Tests with edge types having different target node types."""

    def test_three_edge_types_different_targets(self, triple_store_graph):
        """OR edge with 3 types having different target node types."""
        query = """
        MATCH (p:Person)-[:KNOWS|LIVES_IN|WORKS_AT]->(target)
        RETURN p.name, target.name LIMIT 5
        """
        sql = triple_store_graph.transpile(query)

        # All three edge types must be present
        assert "relationship_type = 'KNOWS'" in sql, "KNOWS filter missing"
        assert "relationship_type = 'LIVES_IN'" in sql, "LIVES_IN filter missing"
        assert "relationship_type = 'WORKS_AT'" in sql, "WORKS_AT filter missing"

        # Must use OR syntax
        assert " OR " in sql, "OR syntax missing"

        # Must NOT have only one filter
        or_count = sql.count(" OR ")
        assert or_count >= 2, f"Expected 2+ OR operators for 3 types, got {or_count}"

    def test_two_edge_types_different_targets(self, triple_store_graph):
        """OR edge with 2 types having different target node types."""
        query = """
        MATCH (p:Person)-[:KNOWS|WORKS_AT]->(target)
        RETURN p.name, target.name LIMIT 5
        """
        sql = triple_store_graph.transpile(query)

        assert "relationship_type = 'KNOWS'" in sql, "KNOWS filter missing"
        assert "relationship_type = 'WORKS_AT'" in sql, "WORKS_AT filter missing"
        assert " OR " in sql, "OR syntax missing"

    def test_vlp_or_edge_different_targets(self, triple_store_graph):
        """VLP with OR edge types having different target node types."""
        query = """
        MATCH path = (p:Person)-[:KNOWS|LIVES_IN*1..2]->(target)
        RETURN p.name, target.name, length(path) AS hops LIMIT 5
        """
        sql = triple_store_graph.transpile(query)

        # Both edge types must appear in the SQL (in base and recursive case)
        knows_count = sql.count("'KNOWS'")
        lives_in_count = sql.count("'LIVES_IN'")

        assert knows_count >= 1, "KNOWS not found in VLP SQL"
        assert lives_in_count >= 1, "LIVES_IN not found in VLP SQL"

        # Must have OR syntax in VLP
        assert " OR " in sql, "OR syntax missing in VLP"


class TestOrEdgeSameTargetType:
    """Tests where all edge types have the same target type."""

    def test_same_target_type(self, triple_store_graph):
        """OR edge where all types go to same target type (Person→Movie).

        ACTED_IN and DIRECTED both go Person→Movie.
        """
        query = """
        MATCH (p:Person)-[:ACTED_IN|DIRECTED]->(m:Movie)
        RETURN p.name, m.name LIMIT 5
        """
        sql = triple_store_graph.transpile(query)

        assert "relationship_type = 'ACTED_IN'" in sql, "ACTED_IN filter missing"
        assert "relationship_type = 'DIRECTED'" in sql, "DIRECTED filter missing"
        assert " OR " in sql, "OR syntax missing"

    def test_same_source_and_target(self, triple_store_graph):
        """OR edge where edges have same source AND target (Person→Person)."""
        # Create a graph with two Person→Person edge types
        graph = GraphContext(
            nodes_table="nodes",
            edges_table="edges",
            extra_node_attrs={"name": str},
        )
        graph.set_types(
            node_types=["Person"],
            edge_types=["KNOWS", "FOLLOWS"],
        )

        query = """
        MATCH (a:Person)-[:KNOWS|FOLLOWS]->(b:Person)
        RETURN a.name, b.name
        """
        sql = graph.transpile(query)

        assert "relationship_type = 'KNOWS'" in sql, "KNOWS filter missing"
        assert "relationship_type = 'FOLLOWS'" in sql, "FOLLOWS filter missing"
        assert " OR " in sql, "OR syntax missing"


class TestOrEdgeFilterCounts:
    """Test OR operator count matches edge type count - 1."""

    def test_two_types_one_or(self):
        """2 edge types should have 1 OR operator."""
        graph = GraphContext(
            nodes_table="nodes",
            edges_table="edges",
            extra_node_attrs={"name": str},
        )
        graph.set_types(
            node_types=["A", "B", "C", "D"],
            edge_types=["R1", "R2", "R3", "R4"],
        )

        sql = graph.transpile("MATCH (a:A)-[:R1|R2]->(b) RETURN a.name")
        or_count = len(re.findall(r"\bOR\b", sql))
        assert or_count >= 1, f"2 types should have 1+ OR, got {or_count}"

    def test_three_types_two_ors(self):
        """3 edge types should have 2 OR operators."""
        graph = GraphContext(
            nodes_table="nodes",
            edges_table="edges",
            extra_node_attrs={"name": str},
        )
        graph.set_types(
            node_types=["A", "B", "C", "D"],
            edge_types=["R1", "R2", "R3", "R4"],
        )

        sql = graph.transpile("MATCH (a:A)-[:R1|R2|R3]->(b) RETURN a.name")
        or_count = len(re.findall(r"\bOR\b", sql))
        assert or_count >= 2, f"3 types should have 2+ ORs, got {or_count}"

    def test_four_types_three_ors(self):
        """4 edge types should have 3 OR operators."""
        graph = GraphContext(
            nodes_table="nodes",
            edges_table="edges",
            extra_node_attrs={"name": str},
        )
        graph.set_types(
            node_types=["A", "B", "C", "D"],
            edge_types=["R1", "R2", "R3", "R4"],
        )

        sql = graph.transpile("MATCH (a:A)-[:R1|R2|R3|R4]->(b) RETURN a.name")
        or_count = len(re.findall(r"\bOR\b", sql))
        assert or_count >= 3, f"4 types should have 3+ ORs, got {or_count}"


class TestOrEdgeFilterNotConcatenated:
    """Regression tests: edge types should not be concatenated."""

    def test_no_concatenated_string(self, triple_store_graph):
        """Should NOT have 'KNOWS|WORKS_AT' as a single string."""
        query = """
        MATCH (p:Person)-[:KNOWS|WORKS_AT]->(target)
        RETURN p.name
        """
        sql = triple_store_graph.transpile(query)

        bad_pattern = r"relationship_type\s*=\s*'KNOWS\|WORKS_AT'"
        assert not re.search(bad_pattern, sql), "Found concatenated edge type"

    def test_no_concatenated_three_types(self, triple_store_graph):
        """Should NOT have 'KNOWS|LIVES_IN|WORKS_AT' as a single string."""
        query = """
        MATCH (p:Person)-[:KNOWS|LIVES_IN|WORKS_AT]->(target)
        RETURN p.name
        """
        sql = triple_store_graph.transpile(query)

        bad_pattern = r"relationship_type\s*=\s*'KNOWS\|LIVES_IN\|WORKS_AT'"
        assert not re.search(bad_pattern, sql), "Found concatenated edge type"
