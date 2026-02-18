"""Tests for undirected relationship optimization (UNION ALL of edges).

This test module follows TDD principles for the UNION ALL edge expansion
strategy.
"""

import pytest

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
from gsql2rsql.planner.operators import JoinOperator
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)


class TestUndirectedJoinOptimization:
    """Tests for UNION ALL edge expansion optimization."""

    def setup_method(self) -> None:
        """Set up test fixtures with undirected relationship."""
        # SQL schema (includes graph schema information)
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(
                table_name="graph.Person",
                node_id_columns=["id"],
            ),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("person_id", int),
                sink_id_property=EntityProperty("friend_id", int),
                properties=[EntityProperty("since", int)],
            ),
            SQLTableDescriptor(table_name="graph.Knows"),
        )

    def test_undirected_join_generates_union_all_when_enabled(self):
        """
        Test that undirected joins generate UNION ALL subquery when
        optimization is enabled (default).

        Query: MATCH (p:Person)-[:KNOWS]-(f:Person) RETURN p.name
        Expected SQL:
          - Edge table wrapped in UNION ALL (forward + reverse)
          - Simple equality joins (no OR conditions)
        """
        cypher = (
            "MATCH (p:Person)-[:KNOWS]-(f:Person) "
            "RETURN p.name, f.name"
        )

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)

        # Render with default config (optimization enabled)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # TDD: These assertions will fail until implementation
        assert (
            "UNION ALL" in sql
        ), "Expected UNION ALL for undirected edge expansion"
        assert (
            " OR " not in sql.upper()
        ), "Should not have OR in JOIN when UNION ALL is used"

    def test_undirected_join_uses_or_when_disabled(self):
        """
        Test that undirected joins use OR condition when use_union_for_undirected
        is set to False on the JoinKeyPair.

        The decision to use UNION ALL or OR is now made by the planner via
        JoinKeyPair.use_union_for_undirected field, following SoC principles.
        """
        cypher = "MATCH (p:Person)-[:KNOWS]-(f:Person) RETURN p.name"

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)

        # Disable UNION optimization by setting use_union_for_undirected=False
        # on all JoinKeyPairs in the plan (simulates planner decision)
        for op in plan.all_operators():
            if isinstance(op, JoinOperator):
                for pair in op.join_pairs:
                    pair.use_union_for_undirected = False

        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # Should use OR, not UNION ALL
        assert " OR " in sql.upper(), (
            "Expected OR condition when use_union_for_undirected=False"
        )
        assert "UNION ALL" not in sql, (
            "Should not have UNION ALL when use_union_for_undirected=False"
        )

    def test_directed_joins_unchanged(self):
        """
        Test that directed joins are not affected by undirected
        optimization.

        Directed relationships should continue to use simple equality
        joins.
        """
        # Directed query: Person -[:KNOWS]-> Person
        cypher = "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name"

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)

        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # Directed joins should NOT have UNION ALL
        assert "UNION ALL" not in sql, (
            "Directed joins should not use UNION ALL"
        )
        assert " OR " not in sql.upper(), (
            "Directed joins should use simple equality"
        )

    def test_union_all_includes_edge_properties(self):
        """
        Test that UNION ALL branches include all edge properties.

        Edge properties must appear in both forward and reverse branches.
        """
        # Query with edge property
        cypher = (
            "MATCH (p:Person)-[k:KNOWS]-(f:Person) "
            "RETURN k.since"
        )

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)

        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # Should have UNION ALL with edge properties in both branches
        assert "UNION ALL" in sql, "Expected UNION ALL for undirected join"
        # Property 'since' should appear multiple times
        # (once per UNION branch + projection)
        assert sql.count("since") >= 2, (
            "Edge property should appear in both UNION branches"
        )

    def test_multi_hop_undirected_no_query_explosion(self):
        """
        Test that multi-hop undirected queries don't explode into multiple
        queries.

        With Option A, multi-hop should still be a single query with
        multiple UNION ALL subqueries.
        """
        # 2-hop undirected query
        cypher = (
            "MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person) "
            "RETURN c.name"
        )

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)

        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # Should have UNION ALL (possibly multiple for each hop)
        assert "UNION ALL" in sql, "Expected UNION ALL for undirected hops"
        # Count UNION ALL instances - should be 2 (one per undirected hop)
        union_all_count = sql.count("UNION ALL")
        assert union_all_count == 2, (
            f"Expected 2 UNION ALL (one per hop), got {union_all_count}"
        )

        # Verify it's a single query, not multiple top-level queries
        # With Option A: Single query with nested SELECTs and UNION ALL
        # With Option B (bad): Multiple top-level queries (2^N explosion)
        # A 2-hop query should have ~12-15 SELECT statements (linear growth)
        # while Option B would require 2^2 = 4 separate top-level queries
        select_count = sql.upper().count("SELECT")
        # Linear growth: ~5-7 SELECTs per hop + overhead
        # Exponential (bad): would be way higher or structured differently
        assert select_count < 20, (
            f"Too many SELECT statements ({select_count}), "
            "possible query explosion (expected ~12-15 for 2-hop)"
        )

    def test_self_loops_deduplicated(self):
        """
        Test that self-loops are handled correctly (not duplicated).

        The reverse branch of UNION ALL excludes self-loops via
        WHERE src != dst, so they only appear once (from the forward branch).
        """
        cypher = "MATCH (p:Person)-[:KNOWS]-(f:Person) RETURN p.name, f.name"

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)

        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # The reverse branch should have a self-loop exclusion filter
        assert "!=" in sql or "<>" in sql, (
            "Expected self-loop exclusion (src != dst) in reverse UNION ALL branch"
        )
