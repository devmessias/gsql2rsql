"""Symbol table for tracking variable definitions and scopes.

This module provides a centralized mechanism for tracking Cypher variables,
their types, and their scopes throughout the transpilation process.

The SymbolTable supports:
- Nested scopes (WITH clauses create new scopes)
- Variable definitions (entities, values, paths)
- Scope-aware lookup
- Full context for error messages
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gsql2rsql.planner.schema import EntityField, ValueField


class SymbolType(Enum):
    """Type of symbol in the symbol table."""

    ENTITY = auto()  # Node or relationship entity (e.g., p:Person, r:KNOWS)
    VALUE = auto()   # Scalar value (e.g., COUNT(x), p.name)
    PATH = auto()    # Path variable (e.g., path = (a)-[*]->(b))


@dataclass
class SymbolEntry:
    """An entry in the symbol table representing a defined variable.

    Attributes:
        name: Variable name as written in query (e.g., "p", "friends")
        symbol_type: Whether this is an entity, value, or path
        definition_operator_id: ID of the operator where this was defined
        definition_location: Human-readable location (e.g., "MATCH on line 1")
        scope_level: Nesting level (0 = outermost scope)
        entity_info: For ENTITY type: the EntityField with full metadata
        value_info: For VALUE type: the ValueField with type info
        data_type_name: Human-readable type name (e.g., "Person", "INTEGER")
        properties: For entities: list of available property names
        is_aggregated: True if this symbol was created by aggregation
        source_expression_text: Original expression text for debugging
    """

    name: str
    symbol_type: SymbolType
    definition_operator_id: int
    definition_location: str
    scope_level: int

    # Type-specific information
    entity_info: EntityField | None = None
    value_info: ValueField | None = None

    # For error messages
    data_type_name: str | None = None
    properties: list[str] = field(default_factory=list)
    is_aggregated: bool = False
    source_expression_text: str = ""

    # VLP relationship variable marker
    # When True, this symbol represents the list of relationships in a VLP
    # and should be resolved to path_edges instead of normal entity columns
    is_vlp_relationship: bool = False

    def __str__(self) -> str:
        """Human-readable representation for debugging."""
        type_str = self.symbol_type.name.lower()
        data_type = self.data_type_name or "unknown"
        return f"{self.name}: {type_str}({data_type}) @ scope {self.scope_level}"

    def to_symbol_info(self) -> SymbolInfo:
        """Convert to SymbolInfo for error messages."""
        return SymbolInfo(
            name=self.name,
            symbol_type=self.symbol_type.name.lower(),
            data_type=self.data_type_name,
            definition_location=self.definition_location,
            scope_level=self.scope_level,
            properties=self.properties if self.properties else None,
        )


@dataclass
class SymbolInfo:
    """Simplified symbol information for error messages.

    This is a serializable subset of SymbolEntry used in error reporting.
    """

    name: str
    symbol_type: str  # "entity", "value", "path"
    data_type: str | None
    definition_location: str
    scope_level: int
    properties: list[str] | None = None

    def format_for_error(self) -> str:
        """Format for display in error messages."""
        props = ", ".join(self.properties) if self.properties else "-"
        return f"{self.name:<12} {self.symbol_type:<8} {self.data_type or 'unknown':<12} {self.definition_location:<24} {props}"


@dataclass
class ScopeInfo:
    """Information about a scope for debugging."""

    level: int
    reason: str  # e.g., "global", "WITH aggregation", "subquery"
    symbols: dict[str, SymbolEntry] = field(default_factory=dict)


class SymbolTable:
    """Tracks variable definitions and their scopes.

    The symbol table maintains a stack of scopes, where each scope contains
    variable definitions. Scopes are created by:
    - The outermost query (global scope)
    - WITH clauses (especially aggregating WITH)
    - Subqueries (EXISTS, pattern comprehensions)

    Usage:
        table = SymbolTable()

        # Define a variable
        table.define("p", SymbolEntry(
            name="p",
            symbol_type=SymbolType.ENTITY,
            definition_operator_id=1,
            definition_location="MATCH on line 1",
            scope_level=0,
            entity_info=entity_field,
            data_type_name="Person",
            properties=["name", "age", "city"]
        ))

        # Look up a variable
        entry = table.lookup("p")

        # Enter a new scope (e.g., after aggregating WITH)
        table.enter_scope("WITH aggregation")

        # Define variables in new scope
        table.define("friends", SymbolEntry(...))

        # Exit scope
        table.exit_scope()
    """

    def __init__(self) -> None:
        """Initialize with empty global scope."""
        self._scopes: list[ScopeInfo] = [ScopeInfo(level=0, reason="global")]
        self._current_level: int = 0

        # Track out-of-scope symbols for error messages
        self._out_of_scope: list[tuple[SymbolEntry, str]] = []  # (entry, reason)

    @property
    def current_level(self) -> int:
        """Current scope nesting level."""
        return self._current_level

    def enter_scope(self, reason: str = "nested") -> None:
        """Enter a new scope.

        Args:
            reason: Human-readable reason for the scope (for debugging)
        """
        self._current_level += 1
        self._scopes.append(ScopeInfo(
            level=self._current_level,
            reason=reason,
        ))

    def exit_scope(self) -> None:
        """Exit the current scope.

        All symbols defined in the current scope become out-of-scope.
        """
        if self._current_level == 0:
            raise RuntimeError("Cannot exit global scope")

        # Record symbols going out of scope for error messages
        current_scope = self._scopes[-1]
        for entry in current_scope.symbols.values():
            self._out_of_scope.append((
                entry,
                f"Went out of scope after {current_scope.reason}"
            ))

        self._scopes.pop()
        self._current_level -= 1

    def clear_scope_for_aggregation(
        self, reason: str = "WITH aggregation", max_operator_id: int | None = None
    ) -> None:
        """Clear current scope symbols for aggregation boundary.

        In Cypher, after an aggregating WITH, only the projected variables
        are visible. This method:
        1. Records all current symbols as out-of-scope
        2. Clears the current scope
        3. New symbols should be defined for the projected variables

        Args:
            reason: Explanation for why symbols went out of scope
            max_operator_id: If provided, only clear symbols defined by operators
                with ID <= max_operator_id. This prevents clearing symbols from
                downstream operators that were defined in Phase 1.
        """
        # Collect symbols to move to out-of-scope
        symbols_to_clear: list[tuple[str, SymbolEntry]] = []

        for scope in self._scopes:
            for name, entry in list(scope.symbols.items()):
                # If max_operator_id is specified, only clear symbols from upstream operators
                if max_operator_id is None or entry.definition_operator_id <= max_operator_id:
                    symbols_to_clear.append((name, entry))
                    self._out_of_scope.append((entry, f"Not projected through {reason}"))

        # Clear the collected symbols
        for scope in self._scopes:
            for name, entry in symbols_to_clear:
                if name in scope.symbols and scope.symbols[name] == entry:
                    del scope.symbols[name]

    def define(self, name: str, entry: SymbolEntry) -> None:
        """Define a symbol in the current scope.

        Args:
            name: Variable name
            entry: Symbol entry with full metadata

        Raises:
            ValueError: If symbol is already defined in current scope
        """
        current_scope = self._scopes[-1]

        if name in current_scope.symbols:
            existing = current_scope.symbols[name]
            raise ValueError(
                f"Symbol '{name}' already defined in current scope "
                f"(at {existing.definition_location})"
            )

        entry.scope_level = self._current_level
        current_scope.symbols[name] = entry

    def define_or_update(self, name: str, entry: SymbolEntry) -> None:
        """Define or update a symbol in the current scope.

        Unlike define(), this allows redefining existing symbols.
        Useful for correlated subqueries where the same entity appears multiple times.

        IMPORTANT: If this symbol was previously out-of-scope, remove it from
        the out-of-scope list to avoid showing it in both available and
        out-of-scope lists.

        Args:
            name: Variable name
            entry: Symbol entry with full metadata
        """
        entry.scope_level = self._current_level
        self._scopes[-1].symbols[name] = entry

        # Remove from out-of-scope list if it was previously out-of-scope
        # This prevents the symbol from appearing in both available and out-of-scope lists
        self._out_of_scope = [
            (sym, reason) for sym, reason in self._out_of_scope
            if sym.name != name
        ]

    def lookup(self, name: str) -> SymbolEntry | None:
        """Look up a symbol, searching from innermost to outermost scope.

        Args:
            name: Variable name to look up

        Returns:
            SymbolEntry if found, None otherwise
        """
        for scope in reversed(self._scopes):
            if name in scope.symbols:
                return scope.symbols[name]
        return None

    def is_defined(self, name: str) -> bool:
        """Check if a symbol is defined in any visible scope.

        Args:
            name: Variable name

        Returns:
            True if the symbol is defined and in scope
        """
        return self.lookup(name) is not None

    def all_names(self) -> list[str]:
        """Get all symbol names currently in scope.

        Returns:
            List of all visible symbol names
        """
        names: list[str] = []
        for scope in self._scopes:
            names.extend(scope.symbols.keys())
        return names

    def all_entries(self) -> list[SymbolEntry]:
        """Get all symbol entries currently in scope.

        Returns:
            List of all visible symbol entries
        """
        entries: list[SymbolEntry] = []
        for scope in self._scopes:
            entries.extend(scope.symbols.values())
        return entries

    def get_available_symbols(self) -> list[SymbolInfo]:
        """Get all available symbols as SymbolInfo for error messages.

        Returns:
            List of SymbolInfo objects for all visible symbols
        """
        return [entry.to_symbol_info() for entry in self.all_entries()]

    def lookup_out_of_scope(self, name: str) -> SymbolEntry | None:
        """Look up a symbol in the out-of-scope list.

        This is used for special cases like AggregationBoundaryOperator where
        we need to resolve expressions that reference pre-aggregation variables.

        Args:
            name: Variable name to look up

        Returns:
            SymbolEntry if found in out-of-scope list, None otherwise
        """
        for entry, _ in self._out_of_scope:
            if entry.name == name:
                return entry
        return None

    def get_out_of_scope_symbols(self) -> list[tuple[SymbolInfo, str]]:
        """Get symbols that went out of scope, with reasons.

        Returns:
            List of (SymbolInfo, reason) tuples
        """
        return [
            (entry.to_symbol_info(), reason)
            for entry, reason in self._out_of_scope
        ]

    def get_entity_symbols(self) -> list[SymbolEntry]:
        """Get all entity symbols currently in scope.

        Returns:
            List of entity symbol entries
        """
        return [
            entry for entry in self.all_entries()
            if entry.symbol_type == SymbolType.ENTITY
        ]

    def get_value_symbols(self) -> list[SymbolEntry]:
        """Get all value symbols currently in scope.

        Returns:
            List of value symbol entries
        """
        return [
            entry for entry in self.all_entries()
            if entry.symbol_type == SymbolType.VALUE
        ]

    def dump(self) -> str:
        """Dump full symbol table state for debugging.

        Returns:
            Human-readable dump of all scopes and symbols
        """
        lines: list[str] = ["Symbol Table Dump:"]

        for scope in self._scopes:
            lines.append(f"  Scope {scope.level} ({scope.reason}):")
            if not scope.symbols:
                lines.append("    [empty]")
            else:
                for name, entry in scope.symbols.items():
                    lines.append(f"    {entry}")

        if self._out_of_scope:
            lines.append("  Out-of-scope symbols:")
            for entry, reason in self._out_of_scope[-5:]:  # Last 5 for brevity
                lines.append(f"    {entry.name}: {reason}")
            if len(self._out_of_scope) > 5:
                lines.append(f"    ... and {len(self._out_of_scope) - 5} more")

        return "\n".join(lines)

    def clone(self) -> SymbolTable:
        """Create a deep copy of the symbol table.

        Useful for speculative resolution (e.g., trying different interpretations).

        Returns:
            A new SymbolTable with the same state
        """
        new_table = SymbolTable()
        new_table._current_level = self._current_level
        new_table._scopes = [
            ScopeInfo(
                level=scope.level,
                reason=scope.reason,
                symbols=dict(scope.symbols),
            )
            for scope in self._scopes
        ]
        new_table._out_of_scope = list(self._out_of_scope)
        return new_table

    def __repr__(self) -> str:
        """Compact representation for debugging."""
        symbol_count = sum(len(scope.symbols) for scope in self._scopes)
        return f"SymbolTable(scopes={len(self._scopes)}, symbols={symbol_count})"
