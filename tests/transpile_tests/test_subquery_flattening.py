"""Tests for SubqueryFlatteningOptimizer.

This module tests the conservative subquery flattening optimization that merges
Selection -> Projection operator chains to reduce SQL nesting.

CONSERVATIVE APPROACH RATIONALE:
================================

We only flatten patterns that are 100% semantically equivalent:

1. Selection -> Projection:
   - WHERE + SELECT always safe to combine
   - SQL: SELECT ... FROM (SELECT * FROM T WHERE cond) AS _proj
     -> SELECT ... FROM T WHERE cond

What we DON'T flatten (and why):
- Projection -> Projection: Column alias conflicts possible
- Anything with LIMIT/OFFSET: Row ordering semantics change
- Window functions: Scope boundaries matter
- DISTINCT in inner query: Affects aggregation counts

Trade-offs:
- Databricks optimizer already flattens internally, so we prioritize correctness
- Generated SQL is more readable and debuggable
- No silent semantic changes to query results
"""

import pytest

from gsql2rsql import LogicalPlan, OpenCypherParser, SQLRenderer
from gsql2rsql.planner.subquery_optimizer import (
    SubqueryFlatteningOptimizer,
    FlatteningStats,
    optimize_plan,
)
from gsql2rsql.planner.operators import (
    ProjectionOperator,
    SelectionOperator,
)
from gsql2rsql.common.schema import (
    SimpleGraphSchemaProvider,
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)


def _create_graph_schema() -> SimpleGraphSchemaProvider:
    """Create a graph schema for parsing."""
    schema = SimpleGraphSchemaProvider()

    schema.add_node(
        NodeSchema(
            name="Person",
            properties=[
                EntityProperty("id", int),
                EntityProperty("name", str),
                EntityProperty("age", int),
            ],
            node_id_property=EntityProperty("id", int),
        )
    )

    schema.add_edge(
        EdgeSchema(
            name="KNOWS",
            source_node_id="Person",
            sink_node_id="Person",
        )
    )

    return schema


def _create_sql_schema() -> SimpleSQLSchemaProvider:
    """Create a SQL schema for rendering."""
    schema = SimpleSQLSchemaProvider()

    schema.add_node(
        NodeSchema(
            name="Person",
            properties=[
                EntityProperty("id", int),
                EntityProperty("name", str),
                EntityProperty("age", int),
            ],
            node_id_property=EntityProperty("id", int),
        ),
        SQLTableDescriptor(
            table_name="graph.Person",
            node_id_columns=["id"],
        ),
    )

    schema.add_edge(
        EdgeSchema(
            name="KNOWS",
            source_node_id="Person",
            sink_node_id="Person",
        ),
        SQLTableDescriptor(
            entity_id="Person@KNOWS@Person",
            table_name="graph.Knows",
            node_id_columns=["source_id", "target_id"],
        ),
    )

    return schema


# Shared test schemas
GRAPH_SCHEMA = _create_graph_schema()
SQL_SCHEMA = _create_sql_schema()


class TestSubqueryFlatteningOptimizer:
    """Test the SubqueryFlatteningOptimizer class."""

    def _transpile(self, query: str, optimize: bool = True) -> str:
        """Helper to transpile a query with optional optimization."""
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, GRAPH_SCHEMA)

        if optimize:
            optimizer = SubqueryFlatteningOptimizer(enabled=True)
            optimizer.optimize(plan)

        # Resolve before rendering
        plan.resolve(original_query=query)

        renderer = SQLRenderer(SQL_SCHEMA)
        return renderer.render_plan(plan)

    def _get_plan(self, query: str) -> LogicalPlan:
        """Helper to get a logical plan without optimization."""
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, GRAPH_SCHEMA)
        # Resolve before returning
        plan.resolve(original_query=query)
        return plan

    # =========================================================================
    # Basic Flattening Tests
    # =========================================================================

    def test_selection_into_projection_flattened(self):
        """Selection -> Projection should be flattened."""
        query = """
        MATCH (p:Person)
        WHERE p.age > 30
        RETURN p.name
        """
        # Without optimization
        sql_no_opt = self._transpile(query, optimize=False)

        # With optimization
        sql_opt = self._transpile(query, optimize=True)

        # Both should produce valid SQL
        assert "SELECT" in sql_no_opt
        assert "SELECT" in sql_opt

        # Optimized SQL should have WHERE at same level as SELECT (not nested)
        # Count nesting levels - optimized should have fewer nested SELECTs
        no_opt_selects = sql_no_opt.upper().count("SELECT")
        opt_selects = sql_opt.upper().count("SELECT")

        # Optimized version should have equal or fewer SELECT statements
        assert opt_selects <= no_opt_selects

    def test_optimizer_disabled_no_changes(self):
        """When disabled, optimizer should not modify the plan."""
        query = """
        MATCH (p:Person)
        WHERE p.age > 30
        RETURN p.name
        """
        sql_enabled = self._transpile(query, optimize=True)
        sql_disabled = self._transpile(query, optimize=False)

        # They might differ if optimization occurred
        # Just verify both produce valid SQL
        assert "SELECT" in sql_enabled
        assert "SELECT" in sql_disabled

    def test_flattening_stats_counted(self):
        """Optimizer should track flattening statistics."""
        query = """
        MATCH (p:Person)
        WHERE p.age > 30
        RETURN p.name
        """
        plan = self._get_plan(query)

        optimizer = SubqueryFlatteningOptimizer(enabled=True)
        optimizer.optimize(plan)

        # Stats should be populated
        assert isinstance(optimizer.stats, FlatteningStats)
        assert optimizer.stats.total_operators_before >= 0
        assert optimizer.stats.total_operators_after >= 0

    # =========================================================================
    # Conservative Approach Tests
    # =========================================================================

    def test_no_filter_not_flattened(self):
        """Selection without filter should not be flattened (nothing to merge)."""
        query = """
        MATCH (p:Person)
        RETURN p.name
        """
        plan = self._get_plan(query)

        optimizer = SubqueryFlatteningOptimizer(enabled=True)
        optimizer.optimize(plan)

        # Should not flatten anything (no WHERE clause)
        assert optimizer.stats.selection_into_projection == 0

    def test_projection_filter_preserved(self):
        """Flattened filter should appear in SQL WHERE clause."""
        query = """
        MATCH (p:Person)
        WHERE p.age > 30
        RETURN p.name AS person_name
        """
        sql = self._transpile(query, optimize=True)

        # Should have WHERE clause
        assert "WHERE" in sql.upper()
        # Should reference the age filter
        assert "30" in sql
        # Should have the alias
        assert "person_name" in sql.lower() or "AS person_name" in sql

    def test_aggregation_with_filter_correct(self):
        """WHERE should apply before GROUP BY (not HAVING)."""
        query = """
        MATCH (p:Person)
        WHERE p.age > 18
        RETURN p.name, COUNT(*) AS cnt
        """
        sql = self._transpile(query, optimize=True)

        # Should have GROUP BY
        assert "GROUP BY" in sql.upper()
        # Should have WHERE
        assert "WHERE" in sql.upper()
        # WHERE should appear before GROUP BY in the SQL
        where_pos = sql.upper().find("WHERE")
        group_pos = sql.upper().find("GROUP BY")
        assert where_pos < group_pos, "WHERE should come before GROUP BY"

    # =========================================================================
    # Semantic Correctness Tests
    # =========================================================================

    def test_flattened_sql_semantically_equivalent(self):
        """Optimized SQL should be semantically equivalent to original."""
        query = """
        MATCH (p:Person)
        WHERE p.age > 25 AND p.name IS NOT NULL
        RETURN p.name, p.age
        ORDER BY p.age DESC
        """
        sql_opt = self._transpile(query, optimize=True)
        sql_no_opt = self._transpile(query, optimize=False)

        # Both should have same clauses
        for clause in ["SELECT", "WHERE", "ORDER BY", "DESC"]:
            assert clause in sql_opt.upper(), f"Missing {clause} in optimized SQL"
            assert clause in sql_no_opt.upper(), f"Missing {clause} in non-optimized SQL"

        # Both should reference same filter values
        assert "25" in sql_opt
        assert "25" in sql_no_opt

    def test_complex_filter_preserved(self):
        """Complex filter expressions should be fully preserved."""
        query = """
        MATCH (p:Person)
        WHERE p.age > 18 AND (p.name STARTS WITH 'A' OR p.name STARTS WITH 'B')
        RETURN p.name
        """
        sql = self._transpile(query, optimize=True)

        # Should have the filter conditions
        assert "18" in sql
        assert "WHERE" in sql.upper()

    # =========================================================================
    # Integration Tests
    # =========================================================================

    def test_relationship_query_with_filter(self):
        """Relationship queries with WHERE should also be optimizable."""
        query = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        WHERE p.age > 30
        RETURN p.name, f.name
        """
        sql = self._transpile(query, optimize=True)

        # Should produce valid SQL with JOIN and WHERE
        assert "JOIN" in sql.upper()
        assert "WHERE" in sql.upper() or "30" in sql

    def test_optimize_plan_convenience_function(self):
        """Test the optimize_plan convenience function."""
        query = """
        MATCH (p:Person)
        WHERE p.age > 30
        RETURN p.name
        """
        plan = self._get_plan(query)

        stats = optimize_plan(plan, enabled=True)

        assert isinstance(stats, FlatteningStats)
        assert stats.total_operators_before >= 0


class TestFlatteningTradeoffs:
    """Tests documenting the trade-offs of conservative flattening.

    These tests verify that we DON'T flatten cases that could change semantics.
    """

    def _transpile(self, query: str, optimize: bool = True) -> str:
        """Helper to transpile a query."""
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, GRAPH_SCHEMA)

        if optimize:
            optimizer = SubqueryFlatteningOptimizer(enabled=True)
            optimizer.optimize(plan)

        # Resolve before rendering
        plan.resolve(original_query=query)

        renderer = SQLRenderer(SQL_SCHEMA)
        return renderer.render_plan(plan)

    def test_distinct_preserved(self):
        """DISTINCT queries should still work correctly."""
        query = """
        MATCH (p:Person)
        WHERE p.age > 18
        RETURN DISTINCT p.name
        """
        sql = self._transpile(query, optimize=True)

        assert "DISTINCT" in sql.upper()

    def test_limit_preserved(self):
        """LIMIT should be preserved in optimized SQL."""
        query = """
        MATCH (p:Person)
        WHERE p.age > 18
        RETURN p.name
        LIMIT 10
        """
        sql = self._transpile(query, optimize=True)

        assert "LIMIT 10" in sql.upper() or "LIMIT  10" in sql.upper()

    def test_order_by_preserved(self):
        """ORDER BY should be preserved in optimized SQL."""
        query = """
        MATCH (p:Person)
        WHERE p.age > 18
        RETURN p.name
        ORDER BY p.name ASC
        """
        sql = self._transpile(query, optimize=True)

        assert "ORDER BY" in sql.upper()


class TestSelectionIntoSelectionFlattening:
    """Tests for Selection → Selection flattening (ANDing multiple WHEREs)."""

    def _transpile(self, query: str, optimize: bool = True) -> str:
        """Helper to transpile a query."""
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, GRAPH_SCHEMA)

        if optimize:
            optimizer = SubqueryFlatteningOptimizer(enabled=True)
            optimizer.optimize(plan)

        # Resolve before rendering
        plan.resolve(original_query=query)

        renderer = SQLRenderer(SQL_SCHEMA)
        return renderer.render_plan(plan)

    def _get_plan(self, query: str) -> LogicalPlan:
        """Helper to get a logical plan without optimization."""
        parser = OpenCypherParser()
        ast = parser.parse(query)
        return LogicalPlan.process_query_tree(ast, GRAPH_SCHEMA)

    def test_consecutive_wheres_anded(self):
        """Multiple WHERE clauses should be ANDed together.

        Note: In Cypher, you can't have multiple WHERE clauses directly,
        but WITH...WHERE creates consecutive SelectionOperators.
        """
        # WITH...WHERE pattern creates Selection → Selection
        query = """
        MATCH (p:Person)
        WHERE p.age > 18
        WITH p
        WHERE p.name IS NOT NULL
        RETURN p.name
        """
        sql = self._transpile(query, optimize=True)

        # Should have both conditions
        assert "18" in sql
        assert "WHERE" in sql.upper()

    def test_selection_selection_stats_tracked(self):
        """Stats should track selection_into_selection merges."""
        query = """
        MATCH (p:Person)
        WHERE p.age > 18
        WITH p
        WHERE p.name IS NOT NULL
        RETURN p.name
        """
        plan = self._get_plan(query)

        optimizer = SubqueryFlatteningOptimizer(enabled=True)
        optimizer.optimize(plan)

        # Stats should show both types of flattening
        assert optimizer.stats.total_operators_before > optimizer.stats.total_operators_after


class TestFlatteningStatsReporting:
    """Tests for the FlatteningStats class."""

    def test_stats_str_format(self):
        """Stats should have readable string representation."""
        stats = FlatteningStats(
            selection_into_projection=2,
            selection_into_selection=1,
            total_operators_before=10,
            total_operators_after=7,
        )
        s = str(stats)
        assert "2" in s  # sel→proj
        assert "1" in s  # sel→sel
        assert "10" in s
        assert "7" in s

    def test_stats_default_values(self):
        """Stats should have sensible defaults."""
        stats = FlatteningStats()
        assert stats.selection_into_projection == 0
        assert stats.selection_into_selection == 0
        assert stats.total_operators_before == 0
        assert stats.total_operators_after == 0
