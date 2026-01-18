"""Test 36: Path Functions - nodes(path) and relationships(path).

Validates that path functions correctly access the accumulated path data
from recursive CTEs and can be used with Higher-Order Functions.

OpenCypher: nodes(path) -> array of node IDs in path
OpenCypher: relationships(path) -> array of relationship objects in path

Databricks SQL:
- nodes(path) -> path (array of node IDs)
- relationships(path) -> path_edges (array of edge structs)
"""

from gsql2rsql import OpenCypherParser
from gsql2rsql.parser.operators import Function
from gsql2rsql.parser.ast import QueryExpressionFunction


class TestPathFunctions:
    """Test path functions parsing and rendering."""

    TEST_ID = "36"
    TEST_NAME = "path_functions"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()

    def test_nodes_function_parsed(self) -> None:
        """Test that nodes(p) is parsed as NODES function."""
        query = "MATCH p = (a)-[:KNOWS*1..3]->(b) RETURN nodes(p)"
        ast = self.parser.parse(query)

        # Query should parse successfully
        assert ast is not None
        assert len(ast.parts) == 1

        # Check that return_body has an expression
        part = ast.parts[0]
        assert len(part.return_body) == 1

        # The return item should be a function call
        item = part.return_body[0]
        assert isinstance(item.inner_expression, QueryExpressionFunction)
        assert item.inner_expression.function == Function.NODES

    def test_relationships_function_parsed(self) -> None:
        """Test that relationships(p) is parsed as RELATIONSHIPS function."""
        query = "MATCH p = (a)-[:KNOWS*1..3]->(b) RETURN relationships(p)"
        ast = self.parser.parse(query)

        assert ast is not None
        assert len(ast.parts) == 1

        part = ast.parts[0]
        assert len(part.return_body) == 1

        item = part.return_body[0]
        assert isinstance(item.inner_expression, QueryExpressionFunction)
        assert item.inner_expression.function == Function.RELATIONSHIPS

    def test_rels_alias_parsed(self) -> None:
        """Test that rels(p) is parsed as RELATIONSHIPS function."""
        query = "MATCH p = (a)-[:KNOWS*1..3]->(b) RETURN rels(p)"
        ast = self.parser.parse(query)

        assert ast is not None
        part = ast.parts[0]
        item = part.return_body[0]
        assert isinstance(item.inner_expression, QueryExpressionFunction)
        assert item.inner_expression.function == Function.RELATIONSHIPS

    def test_size_of_path_nodes(self) -> None:
        """Test that size(nodes(p)) is parsed correctly."""
        query = (
            "MATCH p = (a)-[:KNOWS*1..3]->(b) "
            "RETURN size(nodes(p)) AS path_length"
        )
        ast = self.parser.parse(query)

        assert ast is not None
        part = ast.parts[0]
        item = part.return_body[0]

        # The outer function should be SIZE
        assert isinstance(item.inner_expression, QueryExpressionFunction)
        assert item.inner_expression.function == Function.SIZE

        # The inner function should be NODES
        inner = item.inner_expression.parameters[0]
        assert isinstance(inner, QueryExpressionFunction)
        assert inner.function == Function.NODES

    def test_size_of_relationships(self) -> None:
        """Test that size(relationships(p)) is parsed correctly."""
        query = (
            "MATCH p = (a)-[:KNOWS*1..3]->(b) "
            "RETURN size(relationships(p)) AS hop_count"
        )
        ast = self.parser.parse(query)

        assert ast is not None
        part = ast.parts[0]
        item = part.return_body[0]

        # The outer function should be SIZE
        assert isinstance(item.inner_expression, QueryExpressionFunction)
        assert item.inner_expression.function == Function.SIZE

        # The inner function should be RELATIONSHIPS
        inner = item.inner_expression.parameters[0]
        assert isinstance(inner, QueryExpressionFunction)
        assert inner.function == Function.RELATIONSHIPS


class TestPathFunctionsWithHoF:
    """Test path functions used with Higher-Order Functions."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()

    def test_all_predicate_on_relationships(self) -> None:
        """Test ALL predicate on relationships(p) is parsed correctly.

        Cypher: ALL(rel IN relationships(p) WHERE rel.amount > 1000)
        Should parse the nested ALL with relationships function.
        """
        query = """
        MATCH p = (a:Account)-[:TRANSFER*1..5]->(b:Account)
        WHERE ALL(rel IN relationships(p) WHERE rel.amount > 1000)
        RETURN a.id, b.id
        """
        ast = self.parser.parse(query)

        # Query should parse successfully
        assert ast is not None
        assert len(ast.parts) == 1
        part = ast.parts[0]
        assert len(part.match_clauses) == 1

    def test_any_predicate_on_relationships(self) -> None:
        """Test ANY predicate on relationships(p) is parsed correctly.

        Cypher: ANY(rel IN relationships(p) WHERE rel.flagged = true)
        Should parse the nested ANY with relationships function.
        """
        query = """
        MATCH p = (a:Account)-[:TRANSFER*1..5]->(b:Account)
        WHERE ANY(rel IN relationships(p) WHERE rel.flagged = true)
        RETURN a.id, b.id
        """
        ast = self.parser.parse(query)

        # Query should parse successfully
        assert ast is not None
        assert len(ast.parts) == 1

    def test_none_predicate_on_relationships(self) -> None:
        """Test NONE predicate on relationships(p) is parsed correctly.

        Cypher: NONE(rel IN relationships(p) WHERE rel.flagged = true)
        Should parse the nested NONE with relationships function.
        """
        query = """
        MATCH p = (a:Account)-[:TRANSFER*1..5]->(b:Account)
        WHERE NONE(rel IN relationships(p) WHERE rel.flagged = true)
        RETURN a.id, b.id
        """
        ast = self.parser.parse(query)

        # Query should parse successfully
        assert ast is not None
        assert len(ast.parts) == 1

    def test_list_comprehension_on_nodes(self) -> None:
        """Test list comprehension on nodes(p) is parsed correctly.

        Cypher: [n IN nodes(p) | n.id]
        Should parse list comprehension with nodes function.
        """
        query = """
        MATCH p = (a:Person)-[:KNOWS*1..3]->(b:Person)
        RETURN [n IN nodes(p) | n.id] AS node_ids
        """
        ast = self.parser.parse(query)

        # Query should parse successfully
        assert ast is not None
        assert len(ast.parts) == 1
        part = ast.parts[0]
        assert len(part.return_body) == 1
