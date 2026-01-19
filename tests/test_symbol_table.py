"""Unit tests for the SymbolTable class."""

import pytest

from gsql2rsql.planner.symbol_table import (
    ScopeInfo,
    SymbolEntry,
    SymbolInfo,
    SymbolTable,
    SymbolType,
)


class TestSymbolEntry:
    """Tests for SymbolEntry dataclass."""

    def test_create_entity_symbol(self) -> None:
        """Test creating an entity symbol entry."""
        entry = SymbolEntry(
            name="p",
            symbol_type=SymbolType.ENTITY,
            definition_operator_id=1,
            definition_location="MATCH (p:Person)",
            scope_level=0,
            data_type_name="Person",
            properties=["name", "age", "city"],
        )

        assert entry.name == "p"
        assert entry.symbol_type == SymbolType.ENTITY
        assert entry.definition_operator_id == 1
        assert entry.data_type_name == "Person"
        assert "name" in entry.properties

    def test_create_value_symbol(self) -> None:
        """Test creating a value symbol entry."""
        entry = SymbolEntry(
            name="count_result",
            symbol_type=SymbolType.VALUE,
            definition_operator_id=5,
            definition_location="WITH COUNT(x) AS count_result",
            scope_level=1,
            data_type_name="INTEGER",
            is_aggregated=True,
        )

        assert entry.name == "count_result"
        assert entry.symbol_type == SymbolType.VALUE
        assert entry.is_aggregated is True

    def test_to_symbol_info(self) -> None:
        """Test converting SymbolEntry to SymbolInfo."""
        entry = SymbolEntry(
            name="p",
            symbol_type=SymbolType.ENTITY,
            definition_operator_id=1,
            definition_location="MATCH (p:Person)",
            scope_level=0,
            data_type_name="Person",
            properties=["name", "age"],
        )

        info = entry.to_symbol_info()

        assert isinstance(info, SymbolInfo)
        assert info.name == "p"
        assert info.symbol_type == "entity"
        assert info.data_type == "Person"
        assert info.properties == ["name", "age"]

    def test_str_representation(self) -> None:
        """Test string representation of SymbolEntry."""
        entry = SymbolEntry(
            name="p",
            symbol_type=SymbolType.ENTITY,
            definition_operator_id=1,
            definition_location="MATCH",
            scope_level=0,
            data_type_name="Person",
        )

        s = str(entry)
        assert "p" in s
        assert "entity" in s
        assert "Person" in s


class TestSymbolTable:
    """Tests for SymbolTable class."""

    def test_empty_table(self) -> None:
        """Test empty symbol table."""
        table = SymbolTable()

        assert table.current_level == 0
        assert table.lookup("nonexistent") is None
        assert not table.is_defined("nonexistent")
        assert table.all_names() == []

    def test_define_and_lookup(self) -> None:
        """Test defining and looking up symbols."""
        table = SymbolTable()

        entry = SymbolEntry(
            name="p",
            symbol_type=SymbolType.ENTITY,
            definition_operator_id=1,
            definition_location="MATCH",
            scope_level=0,
        )
        table.define("p", entry)

        result = table.lookup("p")
        assert result is not None
        assert result.name == "p"
        assert table.is_defined("p")

    def test_define_duplicate_raises(self) -> None:
        """Test that defining a duplicate symbol raises ValueError."""
        table = SymbolTable()

        entry1 = SymbolEntry(
            name="p",
            symbol_type=SymbolType.ENTITY,
            definition_operator_id=1,
            definition_location="MATCH 1",
            scope_level=0,
        )
        table.define("p", entry1)

        entry2 = SymbolEntry(
            name="p",
            symbol_type=SymbolType.ENTITY,
            definition_operator_id=2,
            definition_location="MATCH 2",
            scope_level=0,
        )

        with pytest.raises(ValueError, match="already defined"):
            table.define("p", entry2)

    def test_define_or_update(self) -> None:
        """Test define_or_update allows redefining."""
        table = SymbolTable()

        entry1 = SymbolEntry(
            name="p",
            symbol_type=SymbolType.ENTITY,
            definition_operator_id=1,
            definition_location="MATCH 1",
            scope_level=0,
        )
        table.define_or_update("p", entry1)

        entry2 = SymbolEntry(
            name="p",
            symbol_type=SymbolType.VALUE,
            definition_operator_id=2,
            definition_location="WITH",
            scope_level=0,
        )
        table.define_or_update("p", entry2)

        result = table.lookup("p")
        assert result is not None
        assert result.symbol_type == SymbolType.VALUE
        assert result.definition_operator_id == 2

    def test_enter_and_exit_scope(self) -> None:
        """Test entering and exiting scopes."""
        table = SymbolTable()
        assert table.current_level == 0

        table.enter_scope("WITH clause")
        assert table.current_level == 1

        table.enter_scope("subquery")
        assert table.current_level == 2

        table.exit_scope()
        assert table.current_level == 1

        table.exit_scope()
        assert table.current_level == 0

    def test_exit_global_scope_raises(self) -> None:
        """Test that exiting global scope raises RuntimeError."""
        table = SymbolTable()

        with pytest.raises(RuntimeError, match="Cannot exit global scope"):
            table.exit_scope()

    def test_nested_scope_lookup(self) -> None:
        """Test looking up symbols in nested scopes."""
        table = SymbolTable()

        # Define in global scope
        entry_p = SymbolEntry(
            name="p",
            symbol_type=SymbolType.ENTITY,
            definition_operator_id=1,
            definition_location="MATCH",
            scope_level=0,
        )
        table.define("p", entry_p)

        # Enter new scope and define another symbol
        table.enter_scope("WITH")
        entry_q = SymbolEntry(
            name="q",
            symbol_type=SymbolType.VALUE,
            definition_operator_id=2,
            definition_location="WITH",
            scope_level=1,
        )
        table.define("q", entry_q)

        # Both should be visible from inner scope
        assert table.lookup("p") is not None
        assert table.lookup("q") is not None

        # Exit scope
        table.exit_scope()

        # p still visible, q is out of scope
        assert table.lookup("p") is not None
        assert table.lookup("q") is None

    def test_scope_shadows_outer(self) -> None:
        """Test that inner scope can shadow outer scope."""
        table = SymbolTable()

        # Define p in global scope
        entry_p1 = SymbolEntry(
            name="p",
            symbol_type=SymbolType.ENTITY,
            definition_operator_id=1,
            definition_location="MATCH 1",
            scope_level=0,
            data_type_name="Person",
        )
        table.define("p", entry_p1)

        # Enter new scope and redefine p
        table.enter_scope("WITH")
        entry_p2 = SymbolEntry(
            name="p",
            symbol_type=SymbolType.VALUE,
            definition_operator_id=2,
            definition_location="WITH",
            scope_level=1,
            data_type_name="INTEGER",
        )
        table.define("p", entry_p2)

        # Inner p should be found first
        result = table.lookup("p")
        assert result is not None
        assert result.symbol_type == SymbolType.VALUE
        assert result.data_type_name == "INTEGER"

    def test_clear_scope_for_aggregation(self) -> None:
        """Test clearing scope for aggregation boundary."""
        table = SymbolTable()

        # Define some symbols
        entry_p = SymbolEntry(
            name="p",
            symbol_type=SymbolType.ENTITY,
            definition_operator_id=1,
            definition_location="MATCH",
            scope_level=0,
        )
        table.define("p", entry_p)

        entry_f = SymbolEntry(
            name="f",
            symbol_type=SymbolType.ENTITY,
            definition_operator_id=2,
            definition_location="MATCH",
            scope_level=0,
        )
        table.define("f", entry_f)

        # Clear for aggregation
        table.clear_scope_for_aggregation("WITH COUNT")

        # All symbols should be gone
        assert table.lookup("p") is None
        assert table.lookup("f") is None

        # But they should be in out-of-scope list
        out_of_scope = table.get_out_of_scope_symbols()
        assert len(out_of_scope) == 2
        names = [info.name for info, _ in out_of_scope]
        assert "p" in names
        assert "f" in names

    def test_all_names(self) -> None:
        """Test getting all symbol names."""
        table = SymbolTable()

        table.define("a", SymbolEntry(
            name="a", symbol_type=SymbolType.ENTITY,
            definition_operator_id=1, definition_location="", scope_level=0,
        ))
        table.define("b", SymbolEntry(
            name="b", symbol_type=SymbolType.VALUE,
            definition_operator_id=2, definition_location="", scope_level=0,
        ))

        table.enter_scope("nested")
        table.define("c", SymbolEntry(
            name="c", symbol_type=SymbolType.PATH,
            definition_operator_id=3, definition_location="", scope_level=1,
        ))

        names = table.all_names()
        assert set(names) == {"a", "b", "c"}

    def test_get_entity_symbols(self) -> None:
        """Test filtering to entity symbols only."""
        table = SymbolTable()

        table.define("p", SymbolEntry(
            name="p", symbol_type=SymbolType.ENTITY,
            definition_operator_id=1, definition_location="", scope_level=0,
        ))
        table.define("count", SymbolEntry(
            name="count", symbol_type=SymbolType.VALUE,
            definition_operator_id=2, definition_location="", scope_level=0,
        ))
        table.define("r", SymbolEntry(
            name="r", symbol_type=SymbolType.ENTITY,
            definition_operator_id=3, definition_location="", scope_level=0,
        ))

        entities = table.get_entity_symbols()
        assert len(entities) == 2
        names = [e.name for e in entities]
        assert "p" in names
        assert "r" in names
        assert "count" not in names

    def test_get_available_symbols(self) -> None:
        """Test getting available symbols as SymbolInfo."""
        table = SymbolTable()

        table.define("p", SymbolEntry(
            name="p", symbol_type=SymbolType.ENTITY,
            definition_operator_id=1, definition_location="MATCH",
            scope_level=0, data_type_name="Person",
        ))

        available = table.get_available_symbols()
        assert len(available) == 1
        assert available[0].name == "p"
        assert available[0].symbol_type == "entity"

    def test_dump(self) -> None:
        """Test dumping symbol table state."""
        table = SymbolTable()

        table.define("p", SymbolEntry(
            name="p", symbol_type=SymbolType.ENTITY,
            definition_operator_id=1, definition_location="MATCH",
            scope_level=0, data_type_name="Person",
        ))

        dump = table.dump()
        assert "Symbol Table Dump" in dump
        assert "Scope 0" in dump
        assert "p" in dump

    def test_clone(self) -> None:
        """Test cloning symbol table."""
        table = SymbolTable()

        table.define("p", SymbolEntry(
            name="p", symbol_type=SymbolType.ENTITY,
            definition_operator_id=1, definition_location="MATCH",
            scope_level=0,
        ))

        cloned = table.clone()

        # Cloned table should have same symbols
        assert cloned.lookup("p") is not None

        # Modifying clone shouldn't affect original
        cloned.define("q", SymbolEntry(
            name="q", symbol_type=SymbolType.VALUE,
            definition_operator_id=2, definition_location="WITH",
            scope_level=0,
        ))

        assert cloned.lookup("q") is not None
        assert table.lookup("q") is None

    def test_repr(self) -> None:
        """Test repr of symbol table."""
        table = SymbolTable()
        table.define("p", SymbolEntry(
            name="p", symbol_type=SymbolType.ENTITY,
            definition_operator_id=1, definition_location="",
            scope_level=0,
        ))

        r = repr(table)
        assert "SymbolTable" in r
        assert "scopes=1" in r
        assert "symbols=1" in r


class TestSymbolInfo:
    """Tests for SymbolInfo dataclass."""

    def test_format_for_error(self) -> None:
        """Test formatting for error messages."""
        info = SymbolInfo(
            name="p",
            symbol_type="entity",
            data_type="Person",
            definition_location="MATCH (p:Person)",
            scope_level=0,
            properties=["name", "age"],
        )

        formatted = info.format_for_error()
        assert "p" in formatted
        assert "entity" in formatted
        assert "Person" in formatted
        assert "name, age" in formatted

    def test_format_without_properties(self) -> None:
        """Test formatting when no properties."""
        info = SymbolInfo(
            name="count",
            symbol_type="value",
            data_type="INTEGER",
            definition_location="WITH COUNT",
            scope_level=1,
            properties=None,
        )

        formatted = info.format_for_error()
        assert "count" in formatted
        assert "-" in formatted  # No properties indicator
