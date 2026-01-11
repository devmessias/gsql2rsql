"""openCypher parser using ANTLR4."""

from opencypher_transpiler.common.logging import ILoggable
from opencypher_transpiler.parser.ast import QueryNode, SingleQueryNode


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
        from opencypher_transpiler.parser.visitor import CypherVisitor

        try:
            from antlr4 import CommonTokenStream, InputStream

            from opencypher_transpiler.parser.grammar.CypherLexer import CypherLexer
            from opencypher_transpiler.parser.grammar.CypherParser import CypherParser
        except ImportError as e:
            raise ImportError(
                "ANTLR4 runtime or generated parser not found. "
                "Please run 'antlr4 -Dlanguage=Python3 -visitor Cypher.g4' "
                "in the grammar directory."
            ) from e

        # Create lexer and parser
        input_stream = InputStream(query_string)
        lexer = CypherLexer(input_stream)
        token_stream = CommonTokenStream(lexer)
        parser = CypherParser(token_stream)

        # Visit the parse tree
        visitor = CypherVisitor(self._logger)
        result = visitor.visit(parser.oC_Cypher())

        if isinstance(result, QueryNode):
            return result

        # If we get here, something went wrong
        return SingleQueryNode()
