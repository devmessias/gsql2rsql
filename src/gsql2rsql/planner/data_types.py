"""Authoritative data type definitions for schema propagation.

This module defines the type system used by operators to declare their output schemas.
These types are AUTHORITATIVE - operators must declare exactly what they produce,
and downstream consumers (ColumnResolver, Renderer) must trust these declarations.

Design Philosophy:
------------------
- Operators are the source of truth for schema information
- Types are explicit and structural (no inference, no guessing)
- If a type cannot be determined, the system fails fast with a clear error
- The renderer never guesses - it only consumes resolved type information

Type Hierarchy:
---------------
DataType (base)
├── PrimitiveType (INT, STRING, BOOLEAN, FLOAT, UNKNOWN)
├── StructType (named fields with types - e.g., node/edge structure)
└── ArrayType (collection with element type)

Usage:
------
    # Path variable in recursive traversal (authoritative declaration)
    path_element_type = StructType(
        name="PathNode",
        fields={
            "id": PrimitiveType.INT,
            "label": PrimitiveType.STRING,
        }
    )
    path_type = ArrayType(element_type=path_element_type)

    # This is authoritative: the operator GUARANTEES this structure
    path_field = ValueField(
        field_alias="path",
        field_name="_gsql2rsql_path",
        data_type=path_type,
    )
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any


class PrimitiveKind(Enum):
    """Enumeration of primitive data types.

    UNKNOWN is used when type cannot be determined at compile time,
    but MUST be documented with a TODO explaining why.
    """

    INT = auto()
    BIGINT = auto()
    FLOAT = auto()
    DOUBLE = auto()
    STRING = auto()
    BOOLEAN = auto()
    DATE = auto()
    TIMESTAMP = auto()
    BINARY = auto()
    # UNKNOWN should be used sparingly and documented
    UNKNOWN = auto()


@dataclass(frozen=True)
class DataType(ABC):
    """Base class for all data types.

    DataType instances are immutable and hashable for use in caches/sets.

    This is the authoritative type declaration system:
    - Operators MUST declare types, not describe them
    - ColumnResolver MUST trust these types, not infer them
    - Renderer MUST use these types, not guess
    """

    @abstractmethod
    def sql_type_name(self) -> str:
        """Return the SQL type name for rendering.

        Returns:
            SQL type string (e.g., 'INT', 'ARRAY<STRUCT<id: INT>>')
        """
        ...

    @abstractmethod
    def is_collection(self) -> bool:
        """Check if this type represents a collection.

        Returns:
            True if this type is a collection (ArrayType)
        """
        ...

    @abstractmethod
    def is_struct(self) -> bool:
        """Check if this type is a structured type.

        Returns:
            True if this type is a StructType
        """
        ...

    @abstractmethod
    def clone(self) -> DataType:
        """Create a deep copy of this type."""
        ...


@dataclass(frozen=True)
class PrimitiveType(DataType):
    """Primitive (scalar) data type.

    Represents basic SQL types like INT, STRING, BOOLEAN, etc.

    Attributes:
        kind: The primitive type kind
        nullable: Whether the value can be NULL (default True)

    Note on UNKNOWN:
        UNKNOWN should only be used when the type truly cannot be determined
        at compile time. Every use of UNKNOWN should be accompanied by a
        TODO comment explaining why and what would be needed to resolve it.

    Class Constants:
        Use PrimitiveType.INT, PrimitiveType.STRING, etc. for common types.
        These are initialized after the class definition.
    """

    kind: PrimitiveKind
    nullable: bool = True

    def sql_type_name(self) -> str:
        """Return SQL type name."""
        type_map = {
            PrimitiveKind.INT: "INT",
            PrimitiveKind.BIGINT: "BIGINT",
            PrimitiveKind.FLOAT: "FLOAT",
            PrimitiveKind.DOUBLE: "DOUBLE",
            PrimitiveKind.STRING: "STRING",
            PrimitiveKind.BOOLEAN: "BOOLEAN",
            PrimitiveKind.DATE: "DATE",
            PrimitiveKind.TIMESTAMP: "TIMESTAMP",
            PrimitiveKind.BINARY: "BINARY",
            PrimitiveKind.UNKNOWN: "STRING",  # Fallback for unknown
        }
        return type_map.get(self.kind, "STRING")

    def is_collection(self) -> bool:
        return False

    def is_struct(self) -> bool:
        return False

    def clone(self) -> PrimitiveType:
        return PrimitiveType(kind=self.kind, nullable=self.nullable)

    def __str__(self) -> str:
        null_str = "" if self.nullable else " NOT NULL"
        return f"{self.kind.name}{null_str}"


# Initialize class-level constants after class definition
PrimitiveType.INT = PrimitiveType(PrimitiveKind.INT)
PrimitiveType.BIGINT = PrimitiveType(PrimitiveKind.BIGINT)
PrimitiveType.FLOAT = PrimitiveType(PrimitiveKind.FLOAT)
PrimitiveType.DOUBLE = PrimitiveType(PrimitiveKind.DOUBLE)
PrimitiveType.STRING = PrimitiveType(PrimitiveKind.STRING)
PrimitiveType.BOOLEAN = PrimitiveType(PrimitiveKind.BOOLEAN)
PrimitiveType.DATE = PrimitiveType(PrimitiveKind.DATE)
PrimitiveType.TIMESTAMP = PrimitiveType(PrimitiveKind.TIMESTAMP)
PrimitiveType.UNKNOWN = PrimitiveType(PrimitiveKind.UNKNOWN)


@dataclass(frozen=True)
class StructField:
    """A field within a StructType.

    Attributes:
        name: Field name (e.g., 'id', 'name')
        data_type: The field's data type
        sql_name: Optional SQL column name (if different from name)
    """

    name: str
    data_type: DataType
    sql_name: str | None = None

    def get_sql_name(self) -> str:
        """Get the SQL column name for this field."""
        return self.sql_name or self.name

    def clone(self) -> StructField:
        return StructField(
            name=self.name,
            data_type=self.data_type.clone(),
            sql_name=self.sql_name,
        )

    def __str__(self) -> str:
        return f"{self.name}: {self.data_type}"


@dataclass(frozen=True)
class StructType(DataType):
    """Structured type with named fields.

    Represents node/edge structures in path traversals,
    or any record-like type with named fields.

    This is AUTHORITATIVE: when an operator declares a StructType,
    it guarantees that these fields exist and have these types.
    The resolver and renderer MUST trust this declaration.

    Attributes:
        name: Optional struct name (e.g., 'PathNode', 'PersonNode')
        fields: Tuple of StructField instances (ordered, immutable)

    Example:
        # A node in a path traversal
        node_struct = StructType(
            name="PathNode",
            fields=(
                StructField("id", PrimitiveType.INT, sql_name="_gsql2rsql_id"),
                StructField("label", PrimitiveType.STRING),
            )
        )
    """

    name: str = ""
    fields: tuple[StructField, ...] = field(default_factory=tuple)

    def sql_type_name(self) -> str:
        """Return SQL STRUCT type declaration."""
        if not self.fields:
            return "STRUCT<>"
        field_strs = [f"{f.name}: {f.data_type.sql_type_name()}" for f in self.fields]
        return f"STRUCT<{', '.join(field_strs)}>"

    def is_collection(self) -> bool:
        return False

    def is_struct(self) -> bool:
        return True

    def clone(self) -> StructType:
        return StructType(
            name=self.name,
            fields=tuple(f.clone() for f in self.fields),
        )

    def get_field(self, name: str) -> StructField | None:
        """Get a field by name.

        Args:
            name: Field name to look up

        Returns:
            StructField if found, None otherwise
        """
        for f in self.fields:
            if f.name == name:
                return f
        return None

    def has_field(self, name: str) -> bool:
        """Check if a field exists.

        Args:
            name: Field name to check

        Returns:
            True if field exists
        """
        return self.get_field(name) is not None

    def field_names(self) -> list[str]:
        """Get all field names.

        Returns:
            List of field names in order
        """
        return [f.name for f in self.fields]

    def __str__(self) -> str:
        name_part = f"{self.name}: " if self.name else ""
        fields_str = ", ".join(str(f) for f in self.fields)
        return f"{name_part}STRUCT<{fields_str}>"


@dataclass(frozen=True)
class ArrayType(DataType):
    """Array (collection) type with typed elements.

    Represents collections like path arrays, UNWIND results, etc.

    This is AUTHORITATIVE: when an operator declares an ArrayType,
    it guarantees the element type. The resolver MUST use this
    element type when binding lambda variables in list comprehensions.

    Attributes:
        element_type: The type of elements in the array

    Example:
        # Path is an array of node structs
        path_type = ArrayType(
            element_type=StructType(
                name="PathNode",
                fields=(StructField("id", PrimitiveType.INT),)
            )
        )

        # When resolving [n IN nodes(path) | n.id]:
        # 1. nodes(path) has type ArrayType(StructType(...))
        # 2. Bind 'n' to element_type (StructType)
        # 3. Resolve 'n.id' using StructType.get_field('id')
        # 4. Emit TRANSFORM(path, n -> n.id)
    """

    element_type: DataType

    def sql_type_name(self) -> str:
        """Return SQL ARRAY type declaration."""
        return f"ARRAY<{self.element_type.sql_type_name()}>"

    def is_collection(self) -> bool:
        return True

    def is_struct(self) -> bool:
        return False

    def clone(self) -> ArrayType:
        return ArrayType(element_type=self.element_type.clone())

    def get_element_struct(self) -> StructType | None:
        """Get element type as StructType if applicable.

        This is the key method for list comprehension resolution:
        it allows the resolver to determine what fields are available
        when iterating over the array elements.

        Returns:
            StructType if element_type is a struct, None otherwise
        """
        if isinstance(self.element_type, StructType):
            return self.element_type
        return None

    def __str__(self) -> str:
        return f"ARRAY<{self.element_type}>"


# =============================================================================
# Helper functions for type conversion
# =============================================================================


def python_type_to_data_type(python_type: type[Any] | None) -> DataType:
    """Convert a Python type to a DataType.

    This is a bridge for existing code that uses Python types.
    New code should use DataType directly.

    Args:
        python_type: Python type or None

    Returns:
        Corresponding DataType

    TODO: This function exists for backward compatibility.
          Eventually, all code should use DataType directly.
    """
    if python_type is None:
        return PrimitiveType.UNKNOWN
    if python_type is int:
        return PrimitiveType.INT
    if python_type is float:
        return PrimitiveType.DOUBLE
    if python_type is str:
        return PrimitiveType.STRING
    if python_type is bool:
        return PrimitiveType.BOOLEAN
    if python_type is list:
        # List without element type - this is a code smell
        # TODO: Callers should provide element type information
        return ArrayType(element_type=PrimitiveType.UNKNOWN)
    return PrimitiveType.UNKNOWN


def data_type_to_python_type(data_type: DataType | None) -> type[Any] | None:
    """Convert a DataType back to Python type.

    This is for backward compatibility with existing code.

    Args:
        data_type: DataType or None

    Returns:
        Corresponding Python type or None
    """
    if data_type is None:
        return None
    if isinstance(data_type, PrimitiveType):
        type_map = {
            PrimitiveKind.INT: int,
            PrimitiveKind.BIGINT: int,
            PrimitiveKind.FLOAT: float,
            PrimitiveKind.DOUBLE: float,
            PrimitiveKind.STRING: str,
            PrimitiveKind.BOOLEAN: bool,
        }
        return type_map.get(data_type.kind)
    if isinstance(data_type, ArrayType):
        return list
    if isinstance(data_type, StructType):
        return dict  # Structs map to dicts conceptually
    return None
