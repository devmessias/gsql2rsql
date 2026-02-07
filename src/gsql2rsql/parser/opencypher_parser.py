"""openCypher parser using ANTLR4."""

from gsql2rsql.common.logging import ILoggable
from gsql2rsql.parser.ast import QueryNode, SingleQueryNode
from gsql2rsql.parser.error_listener import SyntaxErrorCollector


class OpenCypherParser:
    """
    Parser for openCypher queries using ANTLR4.

    This class parses openCypher query strings and constructs an Abstract Syntax Tree (AST).
    """

    def __init__(self, logger: ILoggable | None = None) -> None:
        """
        Initialize the parser.

        Args:
            logger: Optional logger for debugging.
        """
        self._logger = logger

    def parse(self, query_string: str) -> QueryNode:
        """
        Parse an openCypher query string into an AST.

        Args:
            query_string: The openCypher query to parse.

        Returns:
            The root QueryNode of the AST.

        Raises:
            TranspilerSyntaxErrorException: If the query has syntax errors.
        """
        # Import here to avoid circular imports and allow for lazy loading
        # of ANTLR generated code
        from gsql2rsql.parser.visitor import CypherVisitor

        try:
            from antlr4 import CommonTokenStream, InputStream

            from gsql2rsql.parser.grammar.CypherLexer import CypherLexer
            from gsql2rsql.parser.grammar.CypherParser import CypherParser
        except ImportError as e:
            raise ImportError(
                "ANTLR4 runtime or generated parser not found. "
                "Please run 'antlr4 -Dlanguage=Python3 -visitor Cypher.g4' "
                "in the grammar directory."
            ) from e

        # Create lexer and parser with custom error collector
        input_stream = InputStream(query_string)
        lexer = CypherLexer(input_stream)
        token_stream = CommonTokenStream(lexer)
        parser = CypherParser(token_stream)

        # Replace default stderr error listeners with our collector
        error_collector = SyntaxErrorCollector()
        lexer.removeErrorListeners()
        lexer.addErrorListener(error_collector)
        parser.removeErrorListeners()
        parser.addErrorListener(error_collector)

        # Parse — errors are collected, not printed
        tree = parser.oC_Cypher()

        # Fail fast with structured error if ANTLR found syntax problems
        error_collector.raise_if_errors(query_string)

        # Visit the parse tree to build our AST
        visitor = CypherVisitor(self._logger)
        result = visitor.visit(tree)

        if isinstance(result, QueryNode):
            return result

        # If we get here, something went wrong
        return SingleQueryNode()
