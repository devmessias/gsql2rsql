"""Unit tests for is_terminator() parser recognition and validation.

Verifies that is_terminator() is parsed as a QueryExpressionFunction with
Function.IS_TERMINATOR and one parameter (the inner predicate).
Also verifies that using is_terminator() outside a VLP raises an error.
"""

from __future__ import annotations

import pytest

from gsql2rsql.common.exceptions import TranspilerNotSupportedException
from gsql2rsql.common.schema import EdgeSchema, EntityProperty, NodeSchema
from gsql2rsql.parser.ast import (
    QueryExpressionFunction,
)
from gsql2rsql.parser.operators import Function
from gsql2rsql.parser.opencypher_parser import OpenCypherParser
from gsql2rsql.planner.logical_plan import LogicalPlan
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor


@pytest.fixture
def parser():
    return OpenCypherParser()


def _make_schema() -> SimpleSQLSchemaProvider:
    schema = SimpleSQLSchemaProvider()
    schema.add_node(
        NodeSchema(
            name="Person",
            properties=[
                EntityProperty("node_id", str),
                EntityProperty("age", int),
            ],
            node_id_property=EntityProperty("node_id", str),
        ),
        SQLTableDescriptor(
            table_name="nodes",
            node_id_columns=["node_id"],
            filter="node_type = 'Person'",
        ),
    )
    schema.add_edge(
        EdgeSchema(
            name="KNOWS",
            source_node_id="Person",
            sink_node_id="Person",
            source_id_property=EntityProperty("src", str),
            sink_id_property=EntityProperty("dst", str),
            properties=[
                EntityProperty("src", str),
                EntityProperty("dst", str),
            ],
        ),
        SQLTableDescriptor(
            entity_id="Person@KNOWS@Person",
            table_name="edges",
            node_id_columns=["src", "dst"],
            filter="relationship_type = 'KNOWS'",
        ),
    )
    return schema


def _find_is_terminator(expr, depth=0):
    """Recursively find is_terminator function in expression tree."""
    if isinstance(expr, QueryExpressionFunction):
        if expr.function == Function.IS_TERMINATOR:
            return expr
    if hasattr(expr, "children"):
        for child in expr.children:
            result = _find_is_terminator(child, depth + 1)
            if result is not None:
                return result
    return None


class TestIsTerminatorParsing:
    def test_is_terminator_parsed_as_function(self, parser):
        """is_terminator() should parse as QueryExpressionFunction."""
        cypher = """
        MATCH path = (a:Station)-[:LINK*1..3]->(b:Station)
        WHERE is_terminator(b.is_hub = true)
        RETURN b.node_id
        """
        ast = parser.parse(cypher)
        where = ast.parts[0].match_clauses[0].where_expression
        func = _find_is_terminator(where)
        assert func is not None, "is_terminator() should be in the AST"
        assert func.function == Function.IS_TERMINATOR
        assert len(func.parameters) == 1

    def test_is_terminator_with_comparison(self, parser):
        """is_terminator(b.degree > 1000) should parse."""
        cypher = """
        MATCH path = (a:Station)-[:LINK*1..3]->(b:Station)
        WHERE is_terminator(b.degree > 1000)
        RETURN b.node_id
        """
        ast = parser.parse(cypher)
        where = ast.parts[0].match_clauses[0].where_expression
        func = _find_is_terminator(where)
        assert func is not None
        assert func.function == Function.IS_TERMINATOR
        assert len(func.parameters) == 1

    def test_is_terminator_coexists_with_other_filters(self, parser):
        """is_terminator inside AND with other filters."""
        cypher = """
        MATCH path = (a:Station)-[:LINK*1..3]->(b:Station)
        WHERE a.node_id = 'N1' AND is_terminator(b.is_hub = true) AND b.score > 0.5
        RETURN b.node_id
        """
        ast = parser.parse(cypher)
        where = ast.parts[0].match_clauses[0].where_expression
        func = _find_is_terminator(where)
        assert func is not None
        assert func.function == Function.IS_TERMINATOR
        assert len(func.parameters) == 1


class TestIsTerminatorValidation:
    """Verify is_terminator() raises an error outside VLP queries."""

    def test_is_terminator_outside_vlp_raises(self) -> None:
        """is_terminator() in a non-VLP MATCH must raise TranspilerNotSupportedException."""
        query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE is_terminator(b.age > 30)
        RETURN b.node_id
        """
        schema = _make_schema()
        parser = OpenCypherParser()
        ast = parser.parse(query)
        with pytest.raises(TranspilerNotSupportedException, match="is_terminator"):
            LogicalPlan.process_query_tree(ast, schema)

    def test_is_terminator_outside_vlp_with_other_filters_raises(self) -> None:
        """is_terminator() combined with other filters in non-VLP must also raise."""
        query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE a.node_id = 'X' AND is_terminator(b.age > 30)
        RETURN b.node_id
        """
        schema = _make_schema()
        parser = OpenCypherParser()
        ast = parser.parse(query)
        with pytest.raises(TranspilerNotSupportedException, match="is_terminator"):
            LogicalPlan.process_query_tree(ast, schema)

    def test_is_terminator_anonymous_target_raises(self) -> None:
        """is_terminator(b.x) when target node is anonymous () must raise."""
        query = """
        MATCH p = (a:Person)-[:KNOWS*1..2]-()
        WHERE is_terminator(b.age > 30)
        RETURN a.node_id
        """
        schema = _make_schema()
        parser = OpenCypherParser()
        ast = parser.parse(query)
        with pytest.raises(
            TranspilerNotSupportedException,
            match="is_terminator.*target node",
        ):
            LogicalPlan.process_query_tree(ast, schema)

    def test_is_terminator_wrong_variable_raises(self) -> None:
        """is_terminator(a.x) referencing source instead of target must raise."""
        query = """
        MATCH p = (a:Person)-[:KNOWS*1..2]->(b:Person)
        WHERE is_terminator(a.age > 30)
        RETURN b.node_id
        """
        schema = _make_schema()
        parser = OpenCypherParser()
        ast = parser.parse(query)
        with pytest.raises(
            TranspilerNotSupportedException,
            match="is_terminator.*target node",
        ):
            LogicalPlan.process_query_tree(ast, schema)

    def test_is_terminator_nonexistent_variable_raises(self) -> None:
        """is_terminator(x.prop) with completely undefined variable must raise."""
        query = """
        MATCH p = (a:Person)-[:KNOWS*1..2]->(b:Person)
        WHERE is_terminator(x.age > 30)
        RETURN b.node_id
        """
        schema = _make_schema()
        parser = OpenCypherParser()
        ast = parser.parse(query)
        with pytest.raises(
            TranspilerNotSupportedException,
            match="is_terminator.*target node",
        ):
            LogicalPlan.process_query_tree(ast, schema)
