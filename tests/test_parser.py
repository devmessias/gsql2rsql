"""Tests for the AST parser."""

import pytest

from gsql2rsql.parser.ast import (
    NodeEntity,
    QueryExpressionProperty,
    QueryExpressionValue,
    RelationshipDirection,
    RelationshipEntity,
    SingleQueryNode,
)


class TestQueryExpressionValue:
    """Tests for QueryExpressionValue."""

    def test_null_value(self) -> None:
        """Test NULL value rendering."""
        expr = QueryExpressionValue(value=None, value_type=type(None))
        assert str(expr) == "NULL"

    def test_string_value(self) -> None:
        """Test string value rendering."""
        expr = QueryExpressionValue(value="hello", value_type=str)
        assert str(expr) == "'hello'"

    def test_integer_value(self) -> None:
        """Test integer value rendering."""
        expr = QueryExpressionValue(value=42, value_type=int)
        assert str(expr) == "42"

    def test_float_value(self) -> None:
        """Test float value rendering."""
        expr = QueryExpressionValue(value=3.14, value_type=float)
        assert str(expr) == "3.14"

    def test_boolean_true(self) -> None:
        """Test boolean true value rendering."""
        expr = QueryExpressionValue(value=True, value_type=bool)
        assert str(expr) == "true"

    def test_boolean_false(self) -> None:
        """Test boolean false value rendering."""
        expr = QueryExpressionValue(value=False, value_type=bool)
        assert str(expr) == "false"

    def test_evaluate_type(self) -> None:
        """Test type evaluation."""
        expr = QueryExpressionValue(value=42, value_type=int)
        assert expr.evaluate_type() == int


class TestQueryExpressionProperty:
    """Tests for QueryExpressionProperty."""

    def test_variable_only(self) -> None:
        """Test variable-only property."""
        expr = QueryExpressionProperty(variable_name="n")
        assert str(expr) == "n"

    def test_property_access(self) -> None:
        """Test property access rendering."""
        expr = QueryExpressionProperty(variable_name="n", property_name="name")
        assert str(expr) == "n.name"


class TestNodeEntity:
    """Tests for NodeEntity."""

    def test_create_node(self) -> None:
        """Test node creation."""
        node = NodeEntity(alias="n", entity_name="Person")
        assert node.alias == "n"
        assert node.entity_name == "Person"
        assert str(node) == "n:Person"


class TestRelationshipEntity:
    """Tests for RelationshipEntity."""

    def test_forward_relationship(self) -> None:
        """Test forward relationship."""
        rel = RelationshipEntity(
            alias="r",
            entity_name="KNOWS",
            direction=RelationshipDirection.FORWARD,
        )
        assert "->" in str(rel)

    def test_backward_relationship(self) -> None:
        """Test backward relationship."""
        rel = RelationshipEntity(
            alias="r",
            entity_name="KNOWS",
            direction=RelationshipDirection.BACKWARD,
        )
        assert "<-" in str(rel)

    def test_bidirectional_relationship(self) -> None:
        """Test bidirectional relationship."""
        rel = RelationshipEntity(
            alias="r",
            entity_name="KNOWS",
            direction=RelationshipDirection.BOTH,
        )
        assert str(rel) == "[r:KNOWS]-"


class TestSingleQueryNode:
    """Tests for SingleQueryNode."""

    def test_empty_query(self) -> None:
        """Test empty query creation."""
        query = SingleQueryNode()
        assert len(query.parts) == 0

    def test_children(self) -> None:
        """Test children property."""
        query = SingleQueryNode(parts=[])
        assert list(query.children) == []
