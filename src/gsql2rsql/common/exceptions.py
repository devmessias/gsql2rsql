"""Common exceptions for the openCypher transpiler."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from gsql2rsql.planner.symbol_table import SymbolInfo


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


@dataclass
class ColumnResolutionErrorContext:
    """Full context for a column resolution error.

    Contains all information needed to produce detailed, helpful error messages.
    """

    # Core error info
    error_type: str  # e.g., "UndefinedVariable", "InvalidProperty"
    message: str     # Human-readable summary

    # Location in query
    query_text: str = ""           # Full original query
    error_offset: int = 0          # Character offset where error occurs
    error_length: int = 0          # Length of problematic token
    error_line: int = 0            # Line number (1-indexed)
    error_column: int = 0          # Column number (1-indexed)
    query_part_index: int = 0      # Which PartialQueryNode (0-indexed)

    # Scope context
    available_symbols: list[SymbolInfo] = field(default_factory=list)
    out_of_scope_symbols: list[tuple[SymbolInfo, str]] = field(default_factory=list)

    # Suggestions
    suggestions: list[str] = field(default_factory=list)
    hints: list[str] = field(default_factory=list)

    # Debug info
    operator_id: int = 0           # Which operator was being processed
    operator_type: str = ""        # Type of operator
    resolution_phase: str = ""     # "symbol_lookup", "property_validation", etc.
    symbol_table_dump: str = ""    # Full symbol table state


class ColumnResolutionError(TranspilerException):
    """Error raised when column resolution fails.

    Provides full context for debugging including:
    - Exact error location in query with visual pointer
    - Full scope/symbol table dump
    - Available variables and their types
    - "Did you mean...?" suggestions
    - Hints about common mistakes
    - Debug information

    Example output:
        ╔══════════════════════════════════════════════════════════════════════════════╗
        ║ ColumnResolutionError: Undefined variable 'x'                                ║
        ╚══════════════════════════════════════════════════════════════════════════════╝

        ━━━ Query ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

          1 │ MATCH (p:Person)-[:KNOWS]->(f:Person)
          2 │ WITH p, COUNT(f) AS friends
          3 │ WHERE x.age > 30
            │       ▲
            │       └── ERROR: Variable 'x' is not defined
          4 │ MATCH (p)-[:WORKS_AT]->(c:Company)
          5 │ RETURN p.name, friends, c.name

        ...
    """

    def __init__(
        self,
        message: str,
        context: ColumnResolutionErrorContext | None = None,
    ) -> None:
        self.context = context or ColumnResolutionErrorContext(
            error_type="Unknown",
            message=message,
        )
        super().__init__(message)

    def __str__(self) -> str:
        """Format the error with full context."""
        return self._format_full_error()

    def _format_full_error(self) -> str:
        """Format the complete error message with all context."""
        ctx = self.context
        lines: list[str] = []

        # Header box
        header = f"ColumnResolutionError: {ctx.message}"
        box_width = max(80, len(header) + 4)
        lines.append("╔" + "═" * (box_width - 2) + "╗")
        lines.append("║ " + header.ljust(box_width - 4) + " ║")
        lines.append("╚" + "═" * (box_width - 2) + "╝")
        lines.append("")

        # Query section with error pointer
        if ctx.query_text:
            lines.append("━━━ Query " + "━" * 70)
            lines.append("")
            lines.extend(self._format_query_with_pointer())
            lines.append("")

        # Available variables
        if ctx.available_symbols:
            scope_info = f" (Scope Level {max(s.scope_level for s in ctx.available_symbols)})" if ctx.available_symbols else ""
            lines.append(f"━━━ Available Variables{scope_info} " + "━" * 50)
            lines.append("")
            lines.append("  Name         Type      Data Type   Defined At              Properties")
            lines.append("  " + "─" * 75)
            for sym in ctx.available_symbols:
                props = ", ".join(sym.properties) if sym.properties else "-"
                data_type = sym.data_type or "unknown"
                lines.append(
                    f"  {sym.name:<12} {sym.symbol_type:<8} {data_type:<12} "
                    f"{sym.definition_location:<24} {props}"
                )
            lines.append("")

        # Out-of-scope variables
        if ctx.out_of_scope_symbols:
            lines.append("━━━ Out-of-Scope Variables " + "━" * 50)
            lines.append("")
            lines.append("  Name         Type      Reason")
            lines.append("  " + "─" * 75)
            for sym, reason in ctx.out_of_scope_symbols:
                lines.append(f"  {sym.name:<12} {sym.symbol_type:<8} {reason}")
            lines.append("")

        # Suggestions
        if ctx.suggestions:
            lines.append("━━━ Suggestions " + "━" * 60)
            lines.append("")
            for suggestion in ctx.suggestions:
                lines.append(f"  • {suggestion}")
            lines.append("")

        # Hints
        if ctx.hints:
            lines.append("━━━ Hints " + "━" * 65)
            lines.append("")
            for hint in ctx.hints:
                # Format multi-line hints with proper indentation
                hint_lines = hint.split("\n")
                for i, hint_line in enumerate(hint_lines):
                    prefix = "  💡 " if i == 0 else "     "
                    lines.append(f"{prefix}{hint_line}")
            lines.append("")

        # Debug information
        if ctx.operator_id or ctx.resolution_phase or ctx.symbol_table_dump:
            lines.append("━━━ Debug Information " + "━" * 55)
            lines.append("")
            if ctx.operator_type:
                lines.append(f"  Operator:         {ctx.operator_type} (id={ctx.operator_id})")
            if ctx.resolution_phase:
                lines.append(f"  Resolution Phase: {ctx.resolution_phase}")
            if ctx.query_part_index:
                lines.append(f"  Query Part:       {ctx.query_part_index}")
            if ctx.symbol_table_dump:
                lines.append("  Symbol Table:")
                for dump_line in ctx.symbol_table_dump.split("\n"):
                    lines.append(f"    {dump_line}")
            lines.append("")

        return "\n".join(lines)

    def _format_query_with_pointer(self) -> list[str]:
        """Format the query with line numbers and error pointer."""
        ctx = self.context
        lines: list[str] = []

        if not ctx.query_text:
            return lines

        query_lines = ctx.query_text.split("\n")

        # Calculate which line contains the error
        error_line = ctx.error_line if ctx.error_line > 0 else 1
        if ctx.error_offset > 0:
            char_count = 0
            for i, line in enumerate(query_lines):
                if char_count + len(line) + 1 > ctx.error_offset:
                    error_line = i + 1
                    ctx.error_column = ctx.error_offset - char_count + 1
                    break
                char_count += len(line) + 1  # +1 for newline

        # Show context: 2 lines before and after error
        start_line = max(0, error_line - 3)
        end_line = min(len(query_lines), error_line + 2)

        for i in range(start_line, end_line):
            line_num = i + 1
            line_content = query_lines[i]
            lines.append(f"  {line_num:3} │ {line_content}")

            # Add error pointer
            if line_num == error_line:
                error_col = ctx.error_column if ctx.error_column > 0 else 1
                lines.append(f"      │ {' ' * (error_col - 1)}▲")
                lines.append(f"      │ {' ' * (error_col - 1)}└── ERROR: {ctx.message}")

        return lines

    def get_simple_message(self) -> str:
        """Get a simple one-line error message.

        Useful for logging or contexts where full output is not appropriate.
        """
        ctx = self.context
        location = ""
        if ctx.error_line > 0:
            location = f" at line {ctx.error_line}"
            if ctx.error_column > 0:
                location += f", column {ctx.error_column}"
        return f"{ctx.error_type}: {ctx.message}{location}"


def levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate the Levenshtein (edit) distance between two strings.

    Used for "Did you mean...?" suggestions.

    Args:
        s1: First string
        s2: Second string

    Returns:
        The minimum number of edits (insertions, deletions, substitutions)
        needed to transform s1 into s2.
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row: list[int] = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row: list[int] = [i + 1]
        for j, c2 in enumerate(s2):
            # Cost is 0 if characters match, 1 otherwise
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]
