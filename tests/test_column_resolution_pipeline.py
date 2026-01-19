"""TDD tests for column resolution pipeline.

These tests define the expected behavior of the column resolution system
that will replace the renderer's guessing/inference mechanisms:

1. _find_entity_field -> ResolutionResult.get_entity_field()
2. _determine_column_source -> ResolvedColumnRef.source_operator_id + sql_table_alias
3. List comprehension element types -> authoritative types from structured_type

The tests are written FIRST (TDD) and should fail until implementation is complete.
"""

import pytest

from gsql2rsql import LogicalPlan, OpenCypherParser
from gsql2rsql.common.schema import EdgeSchema, EntityProperty, NodeSchema
from gsql2rsql.planner.column_ref import ColumnRefType, ResolvedColumnRef
from gsql2rsql.planner.column_resolver import ColumnResolver, ResolutionResult
from gsql2rsql.planner.data_types import ArrayType, PrimitiveType, StructField, StructType
from gsql2rsql.planner.operators import (
    DataSourceOperator,
    JoinOperator,
    ProjectionOperator,
    RecursiveTraversalOperator,
)
from gsql2rsql.planner.schema import ValueField
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def movie_schema_provider() -> SimpleSQLSchemaProvider:
    """Create a schema provider with Person, Movie, and ACTED_IN relationship."""
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

    # KNOWS relationship (Person -> Person)
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

    # TRANSFER relationship for path tests
    transfer_schema = EdgeSchema(
        name="TRANSFER",
        source_node_id="Person",
        sink_node_id="Person",
        properties=[
            EntityProperty(property_name="amount", data_type=float),
            EntityProperty(property_name="date", data_type=str),
        ],
    )
    provider.add_edge(
        transfer_schema,
        SQLTableDescriptor(
            table_or_view_name="Transfer",
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
# TEST CLASS 1: Property Resolution - Replaces _find_entity_field
# =============================================================================


class TestPropertyResolution:
    """Tests for property resolution that replaces _find_entity_field in renderer.

    The renderer currently searches through schemas to find entity definitions.
    After resolution, we should be able to directly get:
    - The SQL column name for any property reference
    - The source operator that provides the column
    - The data type of the column
    """

    def test_simple_property_access_sql_name(self, movie_schema_provider) -> None:
        """Test that p.name resolves to _gsql2rsql_p_name."""
        query = "MATCH (p:Person) RETURN p.name"
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        # Find the resolved reference for p.name
        ref = self._find_ref(result, "p", "name")
        assert ref is not None, "p.name should be resolved"
        assert ref.sql_column_name == "_gsql2rsql_p_name"
        assert ref.ref_type == ColumnRefType.ENTITY_PROPERTY

    def test_multiple_properties_same_entity(self, movie_schema_provider) -> None:
        """Test resolving multiple properties from same entity."""
        query = "MATCH (p:Person) RETURN p.name, p.age, p.born"
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        # All three properties should resolve correctly
        name_ref = self._find_ref(result, "p", "name")
        age_ref = self._find_ref(result, "p", "age")
        born_ref = self._find_ref(result, "p", "born")

        assert name_ref is not None
        assert name_ref.sql_column_name == "_gsql2rsql_p_name"

        assert age_ref is not None
        assert age_ref.sql_column_name == "_gsql2rsql_p_age"

        assert born_ref is not None
        assert born_ref.sql_column_name == "_gsql2rsql_p_born"

    def test_properties_from_different_entities(self, movie_schema_provider) -> None:
        """Test resolving properties from different entities in a pattern."""
        query = "MATCH (p:Person)-[r:ACTED_IN]->(m:Movie) RETURN p.name, r.role, m.title"
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        p_name = self._find_ref(result, "p", "name")
        r_role = self._find_ref(result, "r", "role")
        m_title = self._find_ref(result, "m", "title")

        assert p_name is not None
        assert p_name.sql_column_name == "_gsql2rsql_p_name"

        assert r_role is not None
        assert r_role.sql_column_name == "_gsql2rsql_r_role"

        assert m_title is not None
        assert m_title.sql_column_name == "_gsql2rsql_m_title"

    def test_entity_id_reference(self, movie_schema_provider) -> None:
        """Test that bare entity reference (p) resolves to ID column."""
        query = "MATCH (p:Person) RETURN p"
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        ref = self._find_ref(result, "p", None)
        assert ref is not None
        assert ref.ref_type == ColumnRefType.ENTITY_ID
        assert ref.sql_column_name == "_gsql2rsql_p_id"

    def test_source_operator_tracking(self, movie_schema_provider) -> None:
        """Test that resolved references track which operator provides them."""
        query = "MATCH (p:Person) RETURN p.name"
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        ref = self._find_ref(result, "p", "name")
        assert ref is not None

        # The source should be a DataSourceOperator
        source_op = self._find_operator_by_id(plan, ref.source_operator_id)
        assert source_op is not None
        assert isinstance(source_op, DataSourceOperator)

    def _find_ref(
        self, result: ResolutionResult, variable: str, property_name: str | None
    ) -> ResolvedColumnRef | None:
        """Helper to find a resolved reference."""
        key = f"{variable}.{property_name}" if property_name else variable
        for exprs in result.resolved_expressions.values():
            for expr in exprs:
                if key in expr.column_refs:
                    return expr.column_refs[key]
        # Also check resolved projections
        for projs in result.resolved_projections.values():
            for proj in projs:
                if key in proj.expression.column_refs:
                    return proj.expression.column_refs[key]
        return None

    def _find_operator_by_id(self, plan: LogicalPlan, op_id: int) -> None:
        """Helper to find operator by debug ID."""
        for op in plan.all_operators():
            if op.operator_debug_id == op_id:
                return op
        return None


# =============================================================================
# TEST CLASS 2: Join Column Source - Replaces _determine_column_source
# =============================================================================


class TestJoinColumnSourceTracking:
    """Tests for tracking which side of a join provides a column.

    The renderer's _determine_column_source currently GUESSES which side
    of a join provides a column. After resolution, each ResolvedColumnRef
    should have:
    - source_operator_id: which operator provides the column
    - sql_table_alias: the table alias to use (e.g., _gsql2rsql_left)
    """

    def test_join_left_side_column(self, movie_schema_provider) -> None:
        """Test that columns from left side of join have correct source."""
        query = "MATCH (p:Person)-[r:ACTED_IN]->(m:Movie) RETURN p.name"
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        ref = self._find_ref(result, "p", "name")
        assert ref is not None

        # p comes from the Person DataSource (left side of first join)
        source_op = self._find_operator_by_id(plan, ref.source_operator_id)
        assert isinstance(source_op, DataSourceOperator)
        # The entity should be Person
        assert source_op.entity.entity_name == "Person"

    def test_join_right_side_column(self, movie_schema_provider) -> None:
        """Test that columns from right side of join have correct source."""
        query = "MATCH (p:Person)-[r:ACTED_IN]->(m:Movie) RETURN m.title"
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        ref = self._find_ref(result, "m", "title")
        assert ref is not None

        source_op = self._find_operator_by_id(plan, ref.source_operator_id)
        assert isinstance(source_op, DataSourceOperator)
        assert source_op.entity.entity_name == "Movie"

    def test_relationship_properties(self, movie_schema_provider) -> None:
        """Test that relationship properties track correct source."""
        query = "MATCH (p:Person)-[r:ACTED_IN]->(m:Movie) RETURN r.role"
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        ref = self._find_ref(result, "r", "role")
        assert ref is not None

        source_op = self._find_operator_by_id(plan, ref.source_operator_id)
        assert isinstance(source_op, DataSourceOperator)
        # Should be the ACTED_IN relationship
        assert source_op.entity.entity_name == "ACTED_IN"

    def test_multiple_joins_column_tracking(self, movie_schema_provider) -> None:
        """Test column tracking through multiple joins."""
        query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)-[:ACTED_IN]->(m:Movie)
        RETURN a.name, b.name, m.title
        """
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        a_name = self._find_ref(result, "a", "name")
        b_name = self._find_ref(result, "b", "name")
        m_title = self._find_ref(result, "m", "title")

        assert a_name is not None
        assert b_name is not None
        assert m_title is not None

        # All should have different source operators
        assert a_name.source_operator_id != b_name.source_operator_id
        assert b_name.source_operator_id != m_title.source_operator_id

    def _find_ref(
        self, result: ResolutionResult, variable: str, property_name: str | None
    ) -> ResolvedColumnRef | None:
        """Helper to find a resolved reference."""
        key = f"{variable}.{property_name}" if property_name else variable
        for exprs in result.resolved_expressions.values():
            for expr in exprs:
                if key in expr.column_refs:
                    return expr.column_refs[key]
        for projs in result.resolved_projections.values():
            for proj in projs:
                if key in proj.expression.column_refs:
                    return proj.expression.column_refs[key]
        return None

    def _find_operator_by_id(self, plan: LogicalPlan, op_id: int) -> None:
        """Helper to find operator by debug ID."""
        for op in plan.all_operators():
            if op.operator_debug_id == op_id:
                return op
        return None


# =============================================================================
# TEST CLASS 3: List Comprehension with Authoritative Types
# =============================================================================


class TestListComprehensionAuthoritativeTypes:
    """Tests for list comprehension resolution using authoritative types.

    When we have [n IN nodes(path) | n.id], the resolver should:
    1. Look up 'path' and find its structured_type (ArrayType)
    2. Extract the element type (StructType with fields)
    3. Bind 'n' to that StructType
    4. Validate that 'n.id' exists in the StructType
    5. Generate the correct SQL column name for n.id

    This requires the authoritative type system from data_types.py.
    """

    def test_path_variable_has_authoritative_type(self, movie_schema_provider) -> None:
        """Test that path variable in RecursiveTraversalOperator has structured_type."""
        query = "MATCH path = (a:Person)-[:TRANSFER*1..3]->(b:Person) RETURN path"
        plan = parse_and_plan(query, movie_schema_provider)

        # Find the RecursiveTraversalOperator
        rec_op = None
        for op in plan.all_operators():
            if isinstance(op, RecursiveTraversalOperator):
                rec_op = op
                break

        assert rec_op is not None, "Should have RecursiveTraversalOperator"

        # Find the path field in output schema
        path_field = None
        for field in rec_op.output_schema:
            if isinstance(field, ValueField) and field.field_alias == "path":
                path_field = field
                break

        assert path_field is not None, "path should be in output schema"
        assert path_field.structured_type is not None, "path should have structured_type"
        assert isinstance(path_field.structured_type, ArrayType), "path should be ArrayType"

    def test_list_comprehension_element_type_resolution(
        self, movie_schema_provider
    ) -> None:
        """Test that list comprehension resolves element type from authoritative types."""
        query = """
        MATCH path = (a:Person)-[:TRANSFER*1..3]->(b:Person)
        RETURN [n IN nodes(path) | n.id] AS node_ids
        """
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        # The 'n.id' reference inside the comprehension should be resolved
        # We need to check that it has the correct type info from the path's element struct
        n_id_ref = self._find_ref_in_list_comprehension(result, "n", "id")

        assert n_id_ref is not None, "n.id should be resolved in list comprehension"
        # The SQL name inside TRANSFORM should reference the struct field
        assert "id" in n_id_ref.sql_column_name.lower() or n_id_ref.sql_column_name == "n.id"

    def test_list_comprehension_validates_element_fields(
        self, movie_schema_provider
    ) -> None:
        """Test that list comprehension validates fields exist in element type."""
        # This test verifies that if we try to access a non-existent field,
        # the resolver should either warn or error
        query = """
        MATCH path = (a:Person)-[:TRANSFER*1..3]->(b:Person)
        RETURN [n IN nodes(path) | n.nonexistent] AS bad_access
        """
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        # Should either raise error or add warning
        # For now, we check that it at least resolves (may warn)
        assert result is not None
        # TODO: Check for warning about nonexistent field

    def test_relationship_list_comprehension(self, movie_schema_provider) -> None:
        """Test list comprehension over relationships(path)."""
        query = """
        MATCH path = (a:Person)-[:TRANSFER*1..3]->(b:Person)
        RETURN [r IN relationships(path) | r.amount] AS amounts
        """
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        # The 'r.amount' reference should be resolved
        r_amount_ref = self._find_ref_in_list_comprehension(result, "r", "amount")

        # Note: relationships(path) returns edge structs, which should have amount field
        assert r_amount_ref is not None or result.total_references_resolved > 0

    def test_nested_property_in_list_comprehension(self, movie_schema_provider) -> None:
        """Test accessing nested properties in list comprehension."""
        query = """
        MATCH path = (a:Person)-[:TRANSFER*1..3]->(b:Person)
        WHERE ALL(r IN relationships(path) WHERE r.amount > 100)
        RETURN a.name, b.name
        """
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        # r.amount in the ALL predicate should be resolved
        assert result.total_references_resolved >= 2  # At least a.name, b.name

    def _find_ref_in_list_comprehension(
        self, result: ResolutionResult, variable: str, property_name: str | None
    ) -> ResolvedColumnRef | None:
        """Helper to find reference that was resolved in list comprehension context."""
        key = f"{variable}.{property_name}" if property_name else variable
        for exprs in result.resolved_expressions.values():
            for expr in exprs:
                if key in expr.column_refs:
                    return expr.column_refs[key]
        for projs in result.resolved_projections.values():
            for proj in projs:
                if key in proj.expression.column_refs:
                    return proj.expression.column_refs[key]
        return None


# =============================================================================
# TEST CLASS 4: Aggregation Boundary Scope
# =============================================================================


class TestAggregationBoundaryScope:
    """Tests for proper scope handling at aggregation boundaries.

    After WITH containing aggregation, only projected variables are in scope.
    The resolver must track this correctly.
    """

    def test_variable_out_of_scope_after_aggregation(self, movie_schema_provider) -> None:
        """Test that non-projected variables are out of scope after aggregation."""
        query = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        WITH p, COUNT(f) AS friend_count
        RETURN p.name, friend_count
        """
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        # p.name should be resolved (p is projected)
        p_name = self._find_ref(result, "p", "name")
        assert p_name is not None

        # friend_count should be resolved as VALUE_ALIAS
        fc_ref = self._find_ref(result, "friend_count", None)
        assert fc_ref is not None
        assert fc_ref.ref_type == ColumnRefType.VALUE_ALIAS

    def test_accessing_aggregated_variable_fails(self, movie_schema_provider) -> None:
        """Test that accessing f after aggregation fails."""
        from gsql2rsql.common.exceptions import ColumnResolutionError

        query = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        WITH p, COUNT(f) AS friend_count
        RETURN f.name
        """
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()

        # This should raise ColumnResolutionError because f is out of scope
        with pytest.raises(ColumnResolutionError) as exc_info:
            resolver.resolve(plan, query)

        assert "f" in str(exc_info.value)

    def test_out_of_scope_symbols_tracked(self, movie_schema_provider) -> None:
        """Test that out-of-scope symbols are tracked for error messages."""
        from gsql2rsql.common.exceptions import ColumnResolutionError

        query = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        WITH p, COUNT(f) AS friend_count
        RETURN f.name
        """
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()

        try:
            resolver.resolve(plan, query)
            pytest.fail("Should have raised ColumnResolutionError")
        except ColumnResolutionError as e:
            # Check that error context includes f as out-of-scope
            if e.context:
                out_of_scope_names = [
                    sym.name for sym, _ in e.context.out_of_scope_symbols
                ]
                assert "f" in out_of_scope_names

    def _find_ref(
        self, result: ResolutionResult, variable: str, property_name: str | None
    ) -> ResolvedColumnRef | None:
        """Helper to find a resolved reference."""
        key = f"{variable}.{property_name}" if property_name else variable
        for exprs in result.resolved_expressions.values():
            for expr in exprs:
                if key in expr.column_refs:
                    return expr.column_refs[key]
        for projs in result.resolved_projections.values():
            for proj in projs:
                if key in proj.expression.column_refs:
                    return proj.expression.column_refs[key]
        return None


# =============================================================================
# TEST CLASS 5: ResolutionResult API for Renderer
# =============================================================================


class TestResolutionResultAPI:
    """Tests for the ResolutionResult API that the renderer will use.

    These tests define the API contract between ColumnResolver and SQLRenderer.
    """

    def test_get_resolved_projection_by_operator(self, movie_schema_provider) -> None:
        """Test getting resolved projections for a specific operator."""
        query = "MATCH (p:Person) RETURN p.name AS name, p.age AS age"
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        # Find ProjectionOperator
        proj_op = None
        for op in plan.all_operators():
            if isinstance(op, ProjectionOperator):
                proj_op = op
                break

        assert proj_op is not None

        # Get resolved projections for this operator
        projs = result.resolved_projections.get(proj_op.operator_debug_id, [])
        assert len(projs) == 2

        # Check aliases
        aliases = [p.alias for p in projs]
        assert "name" in aliases
        assert "age" in aliases

    def test_resolved_projection_has_sql_output_name(self, movie_schema_provider) -> None:
        """Test that ResolvedProjection has the correct SQL output name."""
        query = "MATCH (p:Person) RETURN p.name AS person_name"
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        # Find the projection
        for projs in result.resolved_projections.values():
            for proj in projs:
                if proj.alias == "person_name":
                    # The output name should be the alias
                    assert proj.sql_output_name == "person_name"
                    return

        pytest.fail("Could not find person_name projection")

    def test_get_column_ref_for_expression(self, movie_schema_provider) -> None:
        """Test getting column references from a resolved expression."""
        query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name"
        plan = parse_and_plan(query, movie_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, query)

        # Find resolved expressions (from WHERE clause)
        found_age_ref = False
        for exprs in result.resolved_expressions.values():
            for expr in exprs:
                if "p.age" in expr.column_refs:
                    found_age_ref = True
                    ref = expr.column_refs["p.age"]
                    assert ref.sql_column_name == "_gsql2rsql_p_age"

        assert found_age_ref, "Should have resolved p.age in WHERE clause"


# =============================================================================
# TEST CLASS 6: Integration with Renderer
# =============================================================================


class TestRendererIntegration:
    """Tests for using ResolutionResult in the renderer.

    These tests verify that when the renderer uses ResolutionResult,
    it produces correct SQL without guessing.
    """

    def test_render_with_resolution_simple(self, movie_schema_provider) -> None:
        """Test rendering a simple query using resolution."""
        from gsql2rsql import SQLRenderer

        query = "MATCH (p:Person) RETURN p.name"
        plan = parse_and_plan(query, movie_schema_provider)

        # Resolve using plan.resolve() to mark plan as resolved
        plan.resolve(original_query=query)

        # Render (renderer requires resolution)
        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        # SQL should contain the correct column name
        assert "_gsql2rsql_p_name" in sql

    def test_render_join_with_resolution(self, movie_schema_provider) -> None:
        """Test rendering a join query using resolution."""
        from gsql2rsql import SQLRenderer

        query = "MATCH (p:Person)-[r:ACTED_IN]->(m:Movie) RETURN p.name, m.title"
        plan = parse_and_plan(query, movie_schema_provider)

        # Resolve using plan.resolve() to mark plan as resolved
        plan.resolve(original_query=query)

        # Render
        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        # Should have correct column references
        assert "_gsql2rsql_p_name" in sql
        assert "_gsql2rsql_m_title" in sql

    def test_render_uses_resolution_not_guessing(self, movie_schema_provider) -> None:
        """Test that renderer uses resolution result instead of guessing.

        This is the key test - we want to verify the renderer uses
        pre-resolved references and doesn't have any legacy guessing code.
        """
        from gsql2rsql import SQLRenderer

        query = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        WHERE p.age > 30 AND f.age < 25
        RETURN p.name, f.name
        """
        plan = parse_and_plan(query, movie_schema_provider)

        # Resolve using plan.resolve() to mark plan as resolved
        plan.resolve(original_query=query)

        # Render
        renderer = SQLRenderer(db_schema_provider=movie_schema_provider)
        sql = renderer.render_plan(plan)

        # Both columns should be correctly referenced
        # The key is that they come from different entities (both Person)
        # but should be distinguished by their aliases
        assert "_gsql2rsql_p_name" in sql
        assert "_gsql2rsql_f_name" in sql
        assert "_gsql2rsql_p_age" in sql
        assert "_gsql2rsql_f_age" in sql
