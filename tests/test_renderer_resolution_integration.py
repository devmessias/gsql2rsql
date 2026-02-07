"""TDD tests for SQLRenderer integration with ColumnResolver.

These tests define the expected behavior when the renderer uses
ResolutionResult instead of guessing/inferring column information.

Test Organization:
- TestRendererRequiresResolution: Renderer rejects unresolved plans
- TestRendererWithResolution: Renderer uses resolution when available
- TestRendererPropertyResolution: Property access uses ResolvedColumnRef
- TestRendererJoinResolution: Join conditions use resolved sources
- TestRendererColumnPruning: Column pruning from ResolutionResult
- TestRendererDeterminism: Output determinism verification
"""

import pytest

from gsql2rsql import LogicalPlan, OpenCypherParser, SQLRenderer
from gsql2rsql.common.schema import EdgeSchema, EntityProperty, NodeSchema
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def movie_schema_provider() -> SimpleSQLSchemaProvider:
    """Create a schema provider with Person, Movie, and relationships."""
    provider = SimpleSQLSchemaProvider()

    # Person node
    person_schema = NodeSchema(
        name="Person",
        properties=[
            EntityProperty(property_name="name", data_type=str),
            EntityProperty(property_name="age", data_type=int),
            EntityProperty(property_name="born", data_type=int),
        ],
        node_id_property=EntityProperty(property_name="id", data_type=int),
    )
    provider.add_node(
        person_schema,
        SQLTableDescriptor(
            table_or_view_name="Person",
            node_id_columns=["id"],
        ),
    )

    # Movie node
    movie_schema = NodeSchema(
        name="Movie",
        properties=[
            EntityProperty(property_name="title", data_type=str),
            EntityProperty(property_name="released", data_type=int),
        ],
        node_id_property=EntityProperty(property_name="id", data_type=int),
    )
    provider.add_node(
        movie_schema,
        SQLTableDescriptor(
            table_or_view_name="Movie",
            node_id_columns=["id"],
        ),
    )

    # ACTED_IN relationship
    acted_in_schema = EdgeSchema(
        name="ACTED_IN",
        source_node_id="Person",
        sink_node_id="Movie",
        properties=[
            EntityProperty(property_name="role", data_type=str),
        ],
    )
    provider.add_edge(
        acted_in_schema,
        SQLTableDescriptor(
            table_or_view_name="ActedIn",
            node_id_columns=["source_id", "target_id"],
        ),
    )

    # KNOWS relationship
    knows_schema = EdgeSchema(
        name="KNOWS",
        source_node_id="Person",
        sink_node_id="Person",
        properties=[
            EntityProperty(property_name="since", data_type=int),
        ],
    )
    provider.add_edge(
        knows_schema,
        SQLTableDescriptor(
            table_or_view_name="Knows",
            node_id_columns=["source_id", "target_id"],
        ),
    )

    return provider


def parse_and_plan(query: str, schema_provider: SimpleSQLSchemaProvider) -> LogicalPlan:
    """Helper to parse query and create logical plan."""
    parser = OpenCypherParser()
    ast = parser.parse(query)
    return LogicalPlan.process_query_tree(ast, schema_provider)


# =============================================================================
# Phase 1: Backwards Compatibility Tests
# =============================================================================


class TestRendererRequiresResolution:
    """Test that renderer REQUIRES resolution (no longer backwards compatible).

    The renderer is now "stupid and safe" - it refuses to work without resolution.
    This enforces separation of concerns: ColumnResolver = semantic, Renderer = mechanical.
    """

    def test_renderer_rejects_unresolved_plan(self, movie_schema_provider) -> None:
        """Renderer should REJECT plans that haven't been resolved."""
        query = "MATCH (p:Person) RETURN p.name"
        plan = parse_and_plan(query, movie_schema_provider)

        # Explicitly do NOT resolve
        assert not plan.is_resolved

        # Renderer should REJECT
        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        with pytest.raises(ValueError, match="SQLRenderer requires a resolved plan"):
            renderer.render_plan(plan)

    def test_renderer_rejects_simple_query_unresolved(self, movie_schema_provider) -> None:
        """Simple queries without resolution should be rejected."""
        query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name, p.age"
        plan = parse_and_plan(query, movie_schema_provider)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        with pytest.raises(ValueError, match="SQLRenderer requires a resolved plan"):
            renderer.render_plan(plan)

    def test_renderer_rejects_join_query_unresolved(self, movie_schema_provider) -> None:
        """Join queries without resolution should be rejected."""
        query = "MATCH (p:Person)-[r:ACTED_IN]->(m:Movie) RETURN p.name, m.title"
        plan = parse_and_plan(query, movie_schema_provider)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        with pytest.raises(ValueError, match="SQLRenderer requires a resolved plan"):
            renderer.render_plan(plan)


# =============================================================================
# Phase 1: Resolution Acceptance Tests
# =============================================================================


class TestRendererWithResolution:
    """Test that renderer accepts and uses ResolutionResult.

    TRADE-OFF: Renderer checks is_resolved to decide which path to use.
    LIMITATION: Some legacy code paths remain until Phase 5.
    """

    def test_renderer_accepts_resolved_plan(self, movie_schema_provider) -> None:
        """Renderer should accept a resolved plan."""
        query = "MATCH (p:Person) RETURN p.name"
        plan = parse_and_plan(query, movie_schema_provider)

        # Resolve the plan
        plan.resolve(original_query=query)
        assert plan.is_resolved
        assert plan.resolution_result is not None

        # Renderer should work with resolved plan
        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        assert sql is not None
        assert "_gsql2rsql_p_name" in sql

    def test_resolution_is_required(
        self, movie_schema_provider
    ) -> None:
        """Resolution is now required - unresolved plans are rejected.

        This enforces separation of concerns between ColumnResolver
        (semantic) and SQLRenderer (mechanical).
        """
        query = "MATCH (p:Person) RETURN p.name AS name"

        # Unresolved plan should be rejected
        plan1 = parse_and_plan(query, movie_schema_provider)
        renderer1 = SQLRenderer(db_schema_provider=movie_schema_provider)
        with pytest.raises(ValueError, match="requires a resolved plan"):
            renderer1.render_plan(plan1)

        # Resolved plan should work
        plan2 = parse_and_plan(query, movie_schema_provider)
        plan2.resolve(original_query=query)
        renderer2 = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql2 = renderer2.render_plan(plan2)

        # Should produce valid SQL
        assert sql2 is not None
        assert "SELECT" in sql2

    def test_resolution_result_accessible_in_renderer(
        self, movie_schema_provider
    ) -> None:
        """Renderer should be able to access resolution_result from plan."""
        query = "MATCH (p:Person) RETURN p.name"
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        # Internal: renderer should be able to get resolution result
        # This tests the mechanism, not the usage
        assert plan.resolution_result is not None
        assert plan.resolution_result.total_references_resolved > 0

        # Verify renderer can use it
        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)
        assert sql is not None


# =============================================================================
# Phase 2: Property Resolution Tests
# =============================================================================


class TestRendererPropertyResolution:
    """Test that property access uses ResolvedColumnRef.sql_column_name.

    TRADE-OFF: We use resolution when available, fallback when not.
    RISK: Divergence between old and new paths.
    MITIGATION: Compare outputs in tests.
    """

    def test_property_renders_correct_column_name(self, movie_schema_provider) -> None:
        """p.name should render as _gsql2rsql_p_name."""
        query = "MATCH (p:Person) RETURN p.name"
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        # Should use the standard column name format
        assert "_gsql2rsql_p_name" in sql

    def test_multiple_properties_same_entity(self, movie_schema_provider) -> None:
        """Multiple properties from same entity should all resolve correctly."""
        query = "MATCH (p:Person) RETURN p.name, p.age, p.born"
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        assert "_gsql2rsql_p_name" in sql
        assert "_gsql2rsql_p_age" in sql
        assert "_gsql2rsql_p_born" in sql

    def test_properties_from_different_entities(self, movie_schema_provider) -> None:
        """Properties from different entities should use correct prefixes."""
        query = "MATCH (p:Person)-[r:ACTED_IN]->(m:Movie) RETURN p.name, r.role, m.title"
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        assert "_gsql2rsql_p_name" in sql
        assert "_gsql2rsql_r_role" in sql
        assert "_gsql2rsql_m_title" in sql

    def test_where_clause_property_resolution(self, movie_schema_provider) -> None:
        """Properties in WHERE clause should resolve correctly."""
        query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name"
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        assert "_gsql2rsql_p_age" in sql
        assert "_gsql2rsql_p_name" in sql
        assert "30" in sql

    def test_aliased_property_in_return(self, movie_schema_provider) -> None:
        """Aliased properties should use the alias in output."""
        query = "MATCH (p:Person) RETURN p.name AS person_name"
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        # The output alias should be person_name
        assert "AS person_name" in sql


# =============================================================================
# Phase 3: Join Resolution Tests
# =============================================================================


class TestRendererJoinResolution:
    """Test that join conditions use resolved source information.

    TRADE-OFF: Join resolution still uses schema for join keys,
    but column sources should come from resolution.
    LIMITATION: Complex multi-way joins may need more work.
    """

    def test_simple_join_renders_correctly(self, movie_schema_provider) -> None:
        """Simple two-entity join should render correct SQL."""
        query = "MATCH (p:Person)-[r:ACTED_IN]->(m:Movie) RETURN p.name, m.title"
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        # Should have JOIN with correct conditions
        assert "JOIN" in sql
        assert "_gsql2rsql_p_name" in sql
        assert "_gsql2rsql_m_title" in sql

    def test_join_with_filter_on_both_sides(self, movie_schema_provider) -> None:
        """Join with WHERE referencing both sides."""
        query = """
        MATCH (p:Person)-[r:ACTED_IN]->(m:Movie)
        WHERE p.age > 30 AND m.released > 2000
        RETURN p.name, m.title
        """
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        assert "_gsql2rsql_p_age" in sql
        assert "_gsql2rsql_m_released" in sql
        assert "30" in sql
        assert "2000" in sql

    def test_same_label_different_aliases(self, movie_schema_provider) -> None:
        """Two Person nodes with different aliases should be distinguished."""
        query = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        RETURN p.name, f.name
        """
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        # Both should be present with different prefixes
        assert "_gsql2rsql_p_name" in sql
        assert "_gsql2rsql_f_name" in sql

    def test_three_way_join(self, movie_schema_provider) -> None:
        """Three-way join should resolve all columns correctly."""
        query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)-[:ACTED_IN]->(m:Movie)
        RETURN a.name, b.name, m.title
        """
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        assert "_gsql2rsql_a_name" in sql
        assert "_gsql2rsql_b_name" in sql
        assert "_gsql2rsql_m_title" in sql


# =============================================================================
# Phase 4: Column Pruning Tests
# =============================================================================


class TestRendererColumnPruning:
    """Test that column pruning works with resolution.

    TRADE-OFF: Column pruning optimization may produce different SQL
    but semantically equivalent results.
    """

    def test_only_required_columns_in_output(self, movie_schema_provider) -> None:
        """Only columns referenced in RETURN should be in final SELECT."""
        query = "MATCH (p:Person) RETURN p.name"
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        # name should be in output
        assert "_gsql2rsql_p_name" in sql

    def test_where_columns_included_in_subquery(self, movie_schema_provider) -> None:
        """Columns in WHERE should be available even if not in RETURN."""
        query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name"
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        # age should be available for WHERE even though not in final output
        assert "_gsql2rsql_p_age" in sql
        assert "_gsql2rsql_p_name" in sql


# =============================================================================
# Phase 5: Assertion Mode Tests
# =============================================================================


class TestRendererDeterminism:
    """Test that renderer produces deterministic output.

    Since renderer now requires resolution, these tests verify that
    resolving the same query multiple times produces identical SQL.
    """

    def test_deterministic_output_simple_query(
        self, movie_schema_provider
    ) -> None:
        """Resolving same query twice should produce identical SQL."""
        query = "MATCH (p:Person) RETURN p.name"

        # First resolution
        plan1 = parse_and_plan(query, movie_schema_provider)
        plan1.resolve(original_query=query)
        renderer1 = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql1 = renderer1.render_plan(plan1)

        # Second resolution
        plan2 = parse_and_plan(query, movie_schema_provider)
        plan2.resolve(original_query=query)
        renderer2 = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql2 = renderer2.render_plan(plan2)

        assert sql1 == sql2, f"Non-deterministic:\n{sql1}\n\nvs\n\n{sql2}"

    def test_deterministic_output_join_query(
        self, movie_schema_provider
    ) -> None:
        """Join query should produce deterministic SQL."""
        query = "MATCH (p:Person)-[:ACTED_IN]->(m:Movie) RETURN p.name, m.title"

        # First resolution
        plan1 = parse_and_plan(query, movie_schema_provider)
        plan1.resolve(original_query=query)
        renderer1 = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql1 = renderer1.render_plan(plan1)

        # Second resolution
        plan2 = parse_and_plan(query, movie_schema_provider)
        plan2.resolve(original_query=query)
        renderer2 = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql2 = renderer2.render_plan(plan2)

        assert sql1 == sql2, f"Non-deterministic:\n{sql1}\n\nvs\n\n{sql2}"

    def test_deterministic_output_where_clause(
        self, movie_schema_provider
    ) -> None:
        """WHERE clause query should produce deterministic SQL."""
        query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name"

        # First resolution
        plan1 = parse_and_plan(query, movie_schema_provider)
        plan1.resolve(original_query=query)
        renderer1 = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql1 = renderer1.render_plan(plan1)

        # Second resolution
        plan2 = parse_and_plan(query, movie_schema_provider)
        plan2.resolve(original_query=query)
        renderer2 = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql2 = renderer2.render_plan(plan2)

        assert sql1 == sql2, f"Non-deterministic:\n{sql1}\n\nvs\n\n{sql2}"


# =============================================================================
# Edge Cases and Regression Tests
# =============================================================================


class TestRendererEdgeCases:
    """Edge cases that might cause issues during migration.

    TODO: Add more edge cases as they are discovered.
    """

    def test_aggregation_with_resolution(self, movie_schema_provider) -> None:
        """Aggregation should work with resolution."""
        query = "MATCH (p:Person) RETURN COUNT(p) AS total"
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        assert "COUNT" in sql
        assert "total" in sql

    def test_order_by_with_resolution(self, movie_schema_provider) -> None:
        """ORDER BY should work with resolution."""
        query = "MATCH (p:Person) RETURN p.name ORDER BY p.name"
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        assert "ORDER BY" in sql
        assert "_gsql2rsql_p_name" in sql

    def test_distinct_with_resolution(self, movie_schema_provider) -> None:
        """DISTINCT should work with resolution."""
        query = "MATCH (p:Person) RETURN DISTINCT p.name"
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        # DISTINCT uses GROUP BY TO_JSON workaround for Spark MAP type compat
        assert "GROUP BY" in sql and "FIRST(" in sql

    def test_limit_with_resolution(self, movie_schema_provider) -> None:
        """LIMIT should work with resolution."""
        query = "MATCH (p:Person) RETURN p.name LIMIT 10"
        plan = parse_and_plan(query, movie_schema_provider)
        plan.resolve(original_query=query)

        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        assert "LIMIT 10" in sql
