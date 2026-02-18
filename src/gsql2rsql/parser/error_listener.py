"""Custom ANTLR error listener for structured syntax error reporting."""

from __future__ import annotations

from dataclasses import dataclass, field

from antlr4.error.ErrorListener import ErrorListener

from gsql2rsql.common.exceptions import TranspilerSyntaxErrorException


@dataclass
class SyntaxError:
    """A single syntax error with location information."""

    line: int
    column: int
    message: str
    offending_symbol: str | None = None

    def __str__(self) -> str:
        symbol = f" near '{self.offending_symbol}'" if self.offending_symbol else ""
        return f"line {self.line}:{self.column}{symbol} {self.message}"


@dataclass
class SyntaxErrorCollector(ErrorListener):  # type: ignore[misc]
    """ANTLR error listener that collects syntax errors instead of printing to stderr.

    Usage:
        collector = SyntaxErrorCollector()
        lexer.removeErrorListeners()
        lexer.addErrorListener(collector)
        parser.removeErrorListeners()
        parser.addErrorListener(collector)
        tree = parser.oC_Cypher()
        collector.raise_if_errors(query_string)
    """

    errors: list[SyntaxError] = field(default_factory=list)

    # ANTLR ErrorListener interface methods

    def syntaxError(  # noqa: N802 — ANTLR naming convention
        self,
        recognizer: object,
        offendingSymbol: object,  # noqa: N803
        line: int,
        column: int,
        msg: str,
        e: object,
    ) -> None:
        """Called by ANTLR on each syntax error."""
        symbol_text = None
        if offendingSymbol is not None and hasattr(offendingSymbol, "text"):
            symbol_text = getattr(offendingSymbol, "text", None)
        self.errors.append(
            SyntaxError(
                line=line,
                column=column,
                message=msg,
                offending_symbol=symbol_text,
            )
        )

    def reportAmbiguity(  # noqa: N802, N803
        self, recognizer: object, dfa: object, startIndex: object,
        stopIndex: object, exact: object, ambigAlts: object, configs: object,
    ) -> None:
        pass

    def reportAttemptingFullContext(  # noqa: N802, N803
        self, recognizer: object, dfa: object, startIndex: object,
        stopIndex: object, conflictingAlts: object, configs: object,
    ) -> None:
        pass

    def reportContextSensitivity(  # noqa: N802, N803
        self, recognizer: object, dfa: object, startIndex: object,
        stopIndex: object, prediction: object, configs: object,
    ) -> None:
        pass

    # Public API

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def raise_if_errors(self, query_string: str = "") -> None:
        """Raise TranspilerSyntaxErrorException if any errors were collected."""
        if not self.errors:
            return

        parts: list[str] = []
        for err in self.errors:
            parts.append(str(err))

        # Show the query with a pointer to the first error
        if query_string and self.errors:
            first = self.errors[0]
            lines = query_string.split("\n")
            if 0 < first.line <= len(lines):
                parts.append("")
                parts.append(f"  {lines[first.line - 1]}")
                parts.append(f"  {' ' * first.column}^")

        raise TranspilerSyntaxErrorException("\n".join(parts))
