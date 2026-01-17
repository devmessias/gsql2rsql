"""Tests for the logical planner."""

import pytest

from gsql2rsql.planner.operators import (
    DataSourceOperator,
    JoinOperator,
    JoinType,
    ProjectionOperator,
    SelectionOperator,
    SetOperator,
    SetOperationType,
)
from gsql2rsql.planner.schema import (
    EntityField,
    Schema,
    ValueField,
)


class TestSchema:
    """Tests for Schema class."""

    def test_empty_schema(self) -> None:
        """Test empty schema creation."""
        schema = Schema()
        assert len(schema.fields) == 0

    def test_add_value_field(self) -> None:
        """Test adding a value field."""
        schema = Schema()
        field = ValueField(field_alias="count", data_type=int)
        schema.add_field(field)

        assert len(schema.fields) == 1
        assert schema.get_field("count") is field

    def test_add_entity_field(self) -> None:
        """Test adding an entity field."""
        schema = Schema()
        field = EntityField(field_alias="p", entity_name="Person")
        schema.add_field(field)

        assert len(schema.fields) == 1
        result = schema.get_field("p")
        assert result is not None
        assert isinstance(result, EntityField)
        assert result.entity_name == "Person"

    def test_merge_schemas(self) -> None:
        """Test merging two schemas."""
        schema1 = Schema()
        schema1.add_field(ValueField(field_alias="a", data_type=str))

        schema2 = Schema()
        schema2.add_field(ValueField(field_alias="b", data_type=int))

        merged = Schema.merge(schema1, schema2)

        assert len(merged.fields) == 2
        assert merged.get_field("a") is not None
        assert merged.get_field("b") is not None


class TestDataSourceOperator:
    """Tests for DataSourceOperator."""

    def test_create_data_source(self) -> None:
        """Test creating a data source operator."""
        op = DataSourceOperator()
        op.entity_alias = "p"

        assert op.entity_alias == "p"

    def test_data_source_depth(self) -> None:
        """Test data source operator depth is 0 (start operator)."""
        op = DataSourceOperator()
        assert op.depth == 0


class TestJoinOperator:
    """Tests for JoinOperator."""

    def test_create_join_operator(self) -> None:
        """Test creating a join operator."""
        left = DataSourceOperator()
        right = DataSourceOperator()

        join = JoinOperator()
        join.set_in_operators(left, right)

        assert join.in_operator_left is left
        assert join.in_operator_right is right

    def test_join_type_default(self) -> None:
        """Test default join type is INNER."""
        join = JoinOperator()
        assert join.join_type == JoinType.INNER

    def test_join_type_left(self) -> None:
        """Test LEFT join type."""
        join = JoinOperator(join_type=JoinType.LEFT)
        assert join.join_type == JoinType.LEFT


class TestSelectionOperator:
    """Tests for SelectionOperator."""

    def test_create_selection_operator(self) -> None:
        """Test creating a selection operator."""
        source = DataSourceOperator()

        selection = SelectionOperator()
        selection.add_in_operator(source)
        source.add_out_operator(selection)

        assert selection.in_operator is source

    def test_selection_with_filter(self) -> None:
        """Test selection with filter expression."""
        selection = SelectionOperator()
        # filter_expression is optional, defaults to None
        assert selection.filter_expression is None


class TestProjectionOperator:
    """Tests for ProjectionOperator."""

    def test_create_projection_operator(self) -> None:
        """Test creating a projection operator."""
        source = DataSourceOperator()

        projection = ProjectionOperator()
        projection.add_in_operator(source)
        source.add_out_operator(projection)

        assert projection.in_operator is source
        assert projection.is_distinct is False

    def test_projection_with_distinct(self) -> None:
        """Test projection with DISTINCT."""
        projection = ProjectionOperator(is_distinct=True)
        assert projection.is_distinct is True

    def test_projection_with_limit(self) -> None:
        """Test projection with LIMIT."""
        projection = ProjectionOperator(limit=10)
        assert projection.limit == 10


class TestSetOperator:
    """Tests for SetOperator."""

    def test_create_union_operator(self) -> None:
        """Test creating a UNION operator."""
        left = DataSourceOperator()
        right = DataSourceOperator()

        union = SetOperator(set_operation=SetOperationType.UNION)
        union.set_in_operators(left, right)

        assert union.in_operator_left is left
        assert union.in_operator_right is right
        assert union.set_operation == SetOperationType.UNION

    def test_create_union_all_operator(self) -> None:
        """Test creating a UNION ALL operator."""
        union_all = SetOperator(set_operation=SetOperationType.UNION_ALL)
        assert union_all.set_operation == SetOperationType.UNION_ALL

    def test_default_set_operation(self) -> None:
        """Test default set operation is UNION."""
        union = SetOperator()
        assert union.set_operation == SetOperationType.UNION
