"""Common exceptions for the openCypher transpiler."""

from typing import Any


class TranspilerException(Exception):
    """Base exception for all transpiler errors."""

    def __init__(self, message: str, *args: Any) -> None:
        self.message = message
        super().__init__(message, *args)

    def __str__(self) -> str:
        return self.message


class TranspilerSyntaxErrorException(TranspilerException):
    """Exception for syntax errors in the openCypher query."""

    def __init__(self, message: str) -> None:
        super().__init__(f"Syntax error: {message}")


class TranspilerBindingException(TranspilerException):
    """Exception for binding errors (e.g., unknown node/edge types)."""

    def __init__(self, message: str) -> None:
        super().__init__(f"Binding error: {message}")


class TranspilerNotSupportedException(TranspilerException):
    """Exception for unsupported features."""

    def __init__(self, message: str) -> None:
        super().__init__(f"Not supported: {message}")


class TranspilerInternalErrorException(TranspilerException):
    """Exception for internal transpiler errors (bugs)."""

    def __init__(self, message: str) -> None:
        super().__init__(f"Internal error: {message}")


class UnsupportedQueryPatternError(TranspilerException):
    """Exception for query patterns that are syntactically valid but not yet supported.

    This is distinct from TranspilerNotSupportedException which is for features
    we don't plan to support. This exception is for patterns we intend to support
    but haven't implemented yet.

    Example: MATCH after aggregating WITH
        MATCH (a)-[:R1]->(b)
        WITH a, COUNT(b) AS cnt    -- Aggregation creates materialization boundary
        MATCH (a)-[:R2]->(c)       -- MATCH after aggregation not yet supported
        RETURN a, cnt, COUNT(c)
    """

    def __init__(self, message: str) -> None:
        super().__init__(f"Unsupported query pattern: {message}")
