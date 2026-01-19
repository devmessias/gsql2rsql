"""Unit tests for the ColumnResolver class."""

import pytest

from gsql2rsql.common.exceptions import ColumnResolutionError, levenshtein_distance
from gsql2rsql.planner.column_ref import (
    ColumnRefType,
    ResolvedColumnRef,
    compute_sql_column_name,
    compute_sql_table_alias,
    compute_cte_name,
)
from gsql2rsql.planner.column_resolver import ColumnResolver, resolve_plan


class TestLevenshteinDistance:
    """Tests for the levenshtein_distance function."""

    def test_identical_strings(self) -> None:
        """Test distance between identical strings."""
        assert levenshtein_distance("hello", "hello") == 0

    def test_empty_strings(self) -> None:
        """Test distance with empty strings."""
        assert levenshtein_distance("", "") == 0
        assert levenshtein_distance("abc", "") == 3
        assert levenshtein_distance("", "abc") == 3

    def test_single_char_diff(self) -> None:
        """Test single character difference."""
        assert levenshtein_distance("cat", "car") == 1
        assert levenshtein_distance("cat", "cats") == 1
        assert levenshtein_distance("cat", "at") == 1

    def test_multiple_diffs(self) -> None:
        """Test multiple character differences."""
        assert levenshtein_distance("kitten", "sitting") == 3
        assert levenshtein_distance("sunday", "saturday") == 3


class TestResolvedColumnRef:
    """Tests for ResolvedColumnRef dataclass."""

    def test_create_property_ref(self) -> None:
        """Test creating a property reference."""
        ref = ResolvedColumnRef(
            original_variable="p",
            original_property="name",
            ref_type=ColumnRefType.ENTITY_PROPERTY,
            source_operator_id=1,
            sql_column_name="_gsql2rsql_p_name",
            data_type=str,
            data_type_name="STRING",
        )

        assert ref.original_variable == "p"
        assert ref.original_property == "name"
        assert ref.ref_type == ColumnRefType.ENTITY_PROPERTY
        assert ref.original_text == "p.name"
        assert ref.full_sql_reference == "_gsql2rsql_p_name"

    def test_create_entity_id_ref(self) -> None:
        """Test creating an entity ID reference."""
        ref = ResolvedColumnRef(
            original_variable="p",
            original_property=None,
            ref_type=ColumnRefType.ENTITY_ID,
            source_operator_id=1,
            sql_column_name="_gsql2rsql_p_id",
        )

        assert ref.original_text == "p"
        assert ref.ref_type == ColumnRefType.ENTITY_ID

    def test_full_sql_reference_with_alias(self) -> None:
        """Test full SQL reference includes table alias."""
        ref = ResolvedColumnRef(
            original_variable="p",
            original_property="name",
            sql_column_name="_gsql2rsql_p_name",
            sql_table_alias="_gsql2rsql_left",
        )

        assert ref.full_sql_reference == "_gsql2rsql_left._gsql2rsql_p_name"

    def test_str_representation(self) -> None:
        """Test string representation."""
        ref = ResolvedColumnRef(
            original_variable="p",
            original_property="name",
            ref_type=ColumnRefType.ENTITY_PROPERTY,
            sql_column_name="_gsql2rsql_p_name",
        )

        s = str(ref)
        assert "p.name" in s
        assert "_gsql2rsql_p_name" in s


class TestComputeSqlNames:
    """Tests for SQL name computation functions."""

    def test_compute_column_name_with_property(self) -> None:
        """Test computing column name for property access."""
        name = compute_sql_column_name("person", "name")
        assert name == "_gsql2rsql_person_name"

    def test_compute_column_name_entity_id(self) -> None:
        """Test computing column name for entity ID."""
        name = compute_sql_column_name("p")
        assert name == "_gsql2rsql_p_id"

    def test_compute_column_name_custom_prefix(self) -> None:
        """Test computing column name with custom prefix."""
        name = compute_sql_column_name("p", "age", prefix="__")
        assert name == "__p_age"

    def test_compute_column_name_cleans_special_chars(self) -> None:
        """Test that special characters are cleaned from names."""
        name = compute_sql_column_name("my-var", "my.prop")
        assert name == "_gsql2rsql_myvar_myprop"

    def test_compute_table_alias(self) -> None:
        """Test computing table alias."""
        assert compute_sql_table_alias("left") == "_gsql2rsql_left"
        assert compute_sql_table_alias("right") == "_gsql2rsql_right"
        assert compute_sql_table_alias("sq", 1) == "_gsql2rsql_sq_1"

    def test_compute_cte_name(self) -> None:
        """Test computing CTE name."""
        assert compute_cte_name("agg", 1) == "_gsql2rsql_agg_1"
        assert compute_cte_name("recursive", 2) == "_gsql2rsql_recursive_2"


class TestColumnResolutionError:
    """Tests for ColumnResolutionError exception."""

    def test_simple_error(self) -> None:
        """Test creating a simple error."""
        error = ColumnResolutionError("Variable 'x' not defined")

        assert "Variable 'x' not defined" in str(error)

    def test_get_simple_message(self) -> None:
        """Test getting simple error message."""
        from gsql2rsql.common.exceptions import ColumnResolutionErrorContext

        context = ColumnResolutionErrorContext(
            error_type="UndefinedVariable",
            message="Variable 'x' not defined",
            error_line=3,
            error_column=5,
        )
        error = ColumnResolutionError("Variable 'x' not defined", context=context)

        simple = error.get_simple_message()
        assert "UndefinedVariable" in simple
        assert "line 3" in simple
        assert "column 5" in simple


class TestColumnResolver:
    """Tests for ColumnResolver class."""

    @pytest.fixture
    def simple_schema_provider(self):
        """Create a simple schema provider for testing."""
        from gsql2rsql.renderer.schema_provider import (
            SimpleSQLSchemaProvider,
            SQLTableDescriptor,
        )
        from gsql2rsql.common.schema import NodeSchema, EntityProperty

        provider = SimpleSQLSchemaProvider()

        # Add Person node
        person_schema = NodeSchema(
            name="Person",
            properties=[
                EntityProperty(property_name="name", data_type=str),
                EntityProperty(property_name="age", data_type=int),
            ],
            node_id_property=EntityProperty(property_name="id", data_type=int),
        )
        provider.add_node(
            person_schema,
            SQLTableDescriptor(table_or_view_name="Person"),
        )

        return provider

    def test_resolver_init(self) -> None:
        """Test resolver initialization."""
        resolver = ColumnResolver()
        assert resolver is not None

    def test_resolve_simple_query(self, simple_schema_provider) -> None:
        """Test resolving a simple query."""
        from gsql2rsql import LogicalPlan, OpenCypherParser

        query = "MATCH (p:Person) RETURN p.name"
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, simple_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, original_query=query)

        assert result is not None
        assert result.total_references_resolved > 0
        assert result.symbol_table is not None

    def test_resolve_detects_available_symbols(self, simple_schema_provider) -> None:
        """Test that resolver builds symbol table correctly."""
        from gsql2rsql import LogicalPlan, OpenCypherParser

        query = "MATCH (p:Person) RETURN p.name AS name"
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, simple_schema_provider)

        resolver = ColumnResolver()
        result = resolver.resolve(plan, original_query=query)

        # Check symbol table contains 'p'
        assert result.symbol_table is not None
        assert result.symbol_table.is_defined("p")

    def test_resolve_plan_convenience_function(self, simple_schema_provider) -> None:
        """Test the convenience resolve_plan function."""
        from gsql2rsql import LogicalPlan, OpenCypherParser

        query = "MATCH (p:Person) RETURN p"
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, simple_schema_provider)

        result = resolve_plan(plan, original_query=query)

        assert result is not None
        assert result.symbol_table is not None


class TestLogicalPlanResolve:
    """Tests for LogicalPlan.resolve() integration."""

    @pytest.fixture
    def simple_schema_provider(self):
        """Create a simple schema provider for testing."""
        from gsql2rsql.renderer.schema_provider import (
            SimpleSQLSchemaProvider,
            SQLTableDescriptor,
        )
        from gsql2rsql.common.schema import NodeSchema, EntityProperty

        provider = SimpleSQLSchemaProvider()

        person_schema = NodeSchema(
            name="Person",
            properties=[
                EntityProperty(property_name="name", data_type=str),
                EntityProperty(property_name="age", data_type=int),
            ],
            node_id_property=EntityProperty(property_name="id", data_type=int),
        )
        provider.add_node(
            person_schema,
            SQLTableDescriptor(table_or_view_name="Person"),
        )

        return provider

    def test_plan_resolve_method(self, simple_schema_provider) -> None:
        """Test calling resolve() on LogicalPlan."""
        from gsql2rsql import LogicalPlan, OpenCypherParser

        query = "MATCH (p:Person) RETURN p.name"
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, simple_schema_provider)

        # Initially not resolved
        assert not plan.is_resolved
        assert plan.resolution_result is None

        # Resolve
        result = plan.resolve(original_query=query)

        # Now resolved
        assert plan.is_resolved
        assert plan.resolution_result is result
        assert result.symbol_table is not None

    def test_all_operators_method(self, simple_schema_provider) -> None:
        """Test the all_operators() helper method."""
        from gsql2rsql import LogicalPlan, OpenCypherParser

        query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name"
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, simple_schema_provider)

        ops = plan.all_operators()

        assert len(ops) > 0
        # Should have at least DataSource, possibly Selection, and Projection
        op_types = [type(op).__name__ for op in ops]
        assert "DataSourceOperator" in op_types
        assert "ProjectionOperator" in op_types
