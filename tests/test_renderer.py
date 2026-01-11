"""Tests for the SQL renderer."""

import pytest

from opencypher_transpiler.parser.ast import (
    QueryExpressionValue,
    QueryExpressionProperty,
)
from opencypher_transpiler.planner.operators import (
    DataSourceOperator,
    JoinOperator,
    JoinType,
    ProjectionOperator,
    SelectionOperator,
)
from opencypher_transpiler.planner.schema import (
    EntityField,
    Schema,
    ValueField,
)
from opencypher_transpiler.renderer.sql_renderer import SQLRenderer
from opencypher_transpiler.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)
from opencypher_transpiler.common.schema import (
    NodeSchema,
    EdgeSchema,
    SimpleGraphSchemaProvider,
)


class TestSQLRenderer:
    """Tests for SQLRenderer."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Graph schema
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(NodeSchema(name="Person"))
        self.graph_schema.add_node(NodeSchema(name="Movie"))
        self.graph_schema.add_edge(
            EdgeSchema(
                name="ACTED_IN",
                source_node_id="Person",
                sink_node_id="Movie",
            )
        )

        # SQL schema
        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_table(
            SQLTableDescriptor(
                entity_id="Person",
                table_name="Person",
                node_id_columns=["id"],
            )
        )
        self.sql_schema.add_table(
            SQLTableDescriptor(
                entity_id="Movie",
                table_name="Movie",
                node_id_columns=["id"],
            )
        )
        self.sql_schema.add_table(
            SQLTableDescriptor(
                entity_id="Person@ACTED_IN@Movie",
                table_name="ActedIn",
                node_id_columns=["person_id", "movie_id"],
            )
        )

        self.renderer = SQLRenderer(
            graph_schema_provider=self.graph_schema,
            db_schema_provider=self.sql_schema,
        )

    def test_render_literal_value(self) -> None:
        """Test rendering literal values."""
        # String value
        expr = QueryExpressionValue(value="hello", value_type=str)
        result = self.renderer._render_value(expr)
        assert result == "N'hello'"

        # Integer value
        expr = QueryExpressionValue(value=42, value_type=int)
        result = self.renderer._render_value(expr)
        assert result == "42"

        # Float value
        expr = QueryExpressionValue(value=3.14, value_type=float)
        result = self.renderer._render_value(expr)
        assert result == "3.14"

        # Boolean values
        expr = QueryExpressionValue(value=True, value_type=bool)
        result = self.renderer._render_value(expr)
        assert result == "1"

        expr = QueryExpressionValue(value=False, value_type=bool)
        result = self.renderer._render_value(expr)
        assert result == "0"

        # Null value
        expr = QueryExpressionValue(value=None, value_type=type(None))
        result = self.renderer._render_value(expr)
        assert result == "NULL"

    def test_render_property_expression(self) -> None:
        """Test rendering property expressions."""
        expr = QueryExpressionProperty(
            variable_name="p",
            property_name="name",
        )
        # The renderer generates internal field names with prefix
        result = self.renderer._get_field_name(expr.variable_name, expr.property_name)
        assert result == "__p_name"


class TestSchemaRendering:
    """Tests for schema-related rendering."""

    def test_value_field(self) -> None:
        """Test ValueField creation and properties."""
        field = ValueField(field_alias="total", data_type=int)
        assert field.field_alias == "total"
        assert field.data_type == int

    def test_entity_field(self) -> None:
        """Test EntityField creation and properties."""
        field = EntityField(field_alias="p", entity_name="Person")
        assert field.field_alias == "p"
        assert field.entity_name == "Person"

    def test_schema_field_lookup(self) -> None:
        """Test Schema field lookup by alias."""
        schema = Schema()
        field1 = ValueField(field_alias="name", data_type=str)
        field2 = EntityField(field_alias="p", entity_name="Person")
        schema.add_field(field1)
        schema.add_field(field2)

        result = schema.get_field("name")
        assert result is field1

        result = schema.get_field("p")
        assert result is field2

        result = schema.get_field("unknown")
        assert result is None
