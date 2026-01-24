"""Test: SQL alias uniqueness to prevent Databricks optimizer issues.

This test ensures that generated SQL uses unique aliases for nested
subqueries, preventing issues with aggressive query optimizers like
Databricks that may flatten queries and confuse repeated aliases.

Background:
- Databricks optimizer can flatten nested subqueries
- If aliases like `_left` are reused at different nesting levels,
  the optimizer may resolve columns to the wrong source
- This caused bugs where `src == dst` in production

Solution:
- Use depth-based unique aliases: `_left_1`, `_left_2`, etc.
- This test verifies that aliases are unique within each SQL query
"""

import re
import pytest
from gsql2rsql import GraphContext


@pytest.fixture
def graph_context():
    """Create GraphContext for testing."""
    graph = GraphContext(
        spark=None,
        nodes_table="nodes",
        edges_table="edges",
        node_type_col="type",
        node_id_col="node_id",
    )
    graph.set_types(
        node_types=["Person", "Company"],
        edge_types=["KNOWS", "WORKS_AT"],
    )
    return graph


def extract_table_aliases(sql: str) -> list[str]:
    """Extract all table aliases from SQL (e.g., 'AS _left_1', 'AS _right_2')."""
    # Match patterns like ") AS _left_1" or ") AS _right_2"
    pattern = r'\)\s+AS\s+(\w+)'
    matches = re.findall(pattern, sql, re.IGNORECASE)
    return matches


def check_alias_uniqueness_in_scope(sql: str) -> tuple[bool, list[str]]:
    """Check that aliases used for JOIN sides are unique.

    Returns:
        (is_unique, duplicate_aliases)
    """
    aliases = extract_table_aliases(sql)

    # Filter to only _left* and _right* aliases (the ones we care about)
    join_aliases = [a for a in aliases if a.startswith('_left') or a.startswith('_right')]

    # Check for duplicates
    seen = set()
    duplicates = []
    for alias in join_aliases:
        if alias in seen:
            duplicates.append(alias)
        seen.add(alias)

    return len(duplicates) == 0, duplicates


class TestSQLAliasUniqueness:
    """Tests to ensure SQL aliases are unique to prevent optimizer issues."""

    def test_single_hop_has_unique_aliases(self, graph_context):
        """Single-hop query should use unique aliases."""
        query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        RETURN a.node_id AS src, b.node_id AS dst
        """

        sql = graph_context.transpile(query)
        is_unique, duplicates = check_alias_uniqueness_in_scope(sql)

        assert is_unique, (
            f"Duplicate aliases found: {duplicates}\n"
            f"This can cause issues with Databricks optimizer.\n"
            f"SQL:\n{sql}"
        )

    def test_multi_hop_has_unique_aliases(self, graph_context):
        """Multi-hop query should use unique aliases at each level."""
        query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
        RETURN a.node_id, b.node_id, c.node_id
        """

        sql = graph_context.transpile(query)
        is_unique, duplicates = check_alias_uniqueness_in_scope(sql)

        assert is_unique, (
            f"Duplicate aliases found: {duplicates}\n"
            f"Multi-hop queries are especially prone to alias collision.\n"
            f"SQL:\n{sql}"
        )

    def test_no_bare_left_right_aliases(self, graph_context):
        """Ensure we don't use bare '_left' or '_right' without suffix."""
        query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        RETURN a.node_id, b.node_id
        """

        sql = graph_context.transpile(query)
        aliases = extract_table_aliases(sql)

        bare_aliases = [a for a in aliases if a in ('_left', '_right')]

        assert len(bare_aliases) == 0, (
            f"Found bare aliases without depth suffix: {bare_aliases}\n"
            f"All _left/_right aliases should have numeric suffix (e.g., _left_1).\n"
            f"SQL:\n{sql}"
        )

    def test_aliases_follow_depth_pattern(self, graph_context):
        """Aliases should follow _left_N, _right_N pattern."""
        query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)
        RETURN a.node_id, c.node_id
        """

        sql = graph_context.transpile(query)
        aliases = extract_table_aliases(sql)

        join_aliases = [a for a in aliases if a.startswith('_left') or a.startswith('_right')]

        for alias in join_aliases:
            # Should match _left_N or _right_N where N is a number
            assert re.match(r'^_(left|right)_\d+$', alias), (
                f"Alias '{alias}' doesn't follow expected pattern '_left_N' or '_right_N'.\n"
                f"SQL:\n{sql}"
            )

    def test_complex_query_unique_aliases(self, graph_context):
        """Complex query with multiple patterns should have unique aliases."""
        query = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        WHERE p.node_id = 'Alice'
        MATCH (f)-[:WORKS_AT]->(c:Company)
        RETURN p.node_id, f.node_id, c.node_id
        """

        sql = graph_context.transpile(query)
        is_unique, duplicates = check_alias_uniqueness_in_scope(sql)

        assert is_unique, (
            f"Duplicate aliases in complex query: {duplicates}\n"
            f"SQL:\n{sql}"
        )


class TestAliasUniquenessRegression:
    """Regression tests for the src==dst bug caused by alias collision."""

    def test_directed_query_no_alias_collision(self, graph_context):
        """The exact pattern that caused src==dst bug in Databricks."""
        query = """
        MATCH (origem)-[:KNOWS]->(destino)
        RETURN origem.node_id AS src, destino.node_id AS dst
        """

        sql = graph_context.transpile(query)

        # 1. Check unique aliases
        is_unique, duplicates = check_alias_uniqueness_in_scope(sql)
        assert is_unique, f"Duplicate aliases: {duplicates}"

        # 2. Verify the SQL structure references different sources for src and dst
        # src should come from origem (joined on edge.src)
        # dst should come from destino (joined on edge.dst)
        assert '_gsql2rsql_origem_node_id' in sql, "Missing origem binding"
        assert '_gsql2rsql_destino_node_id' in sql, "Missing destino binding"

        # 3. No bare _left/_right
        assert 'AS _left\n' not in sql and 'AS _left ' not in sql, "Found bare _left alias"
        assert 'AS _right\n' not in sql and 'AS _right ' not in sql, "Found bare _right alias"

        print(f"✅ SQL passes alias uniqueness check:\n{sql}")
