"""Schema definitions for the logical planner.

This module defines the Field and Schema classes that represent the output
schema of operators in the logical plan.

Design Philosophy (Authoritative Schema):
-----------------------------------------
Operators are AUTHORITATIVE about what they produce. The schema information
declared here is the source of truth that downstream components (ColumnResolver,
Renderer) MUST trust and use without inference or guessing.

See also: data_types.py for the type system.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Iterator, TYPE_CHECKING

if TYPE_CHECKING:
    from gsql2rsql.planner.data_types import DataType, ArrayType, StructType


@dataclass
class Field(ABC):
    """Represents a single alias (value column or entity) in a schema.

    This is the base class for all field types in an operator's output schema.
    Subclasses (ValueField, EntityField) provide specific field semantics.
    """

    field_alias: str

    @abstractmethod
    def clone(self) -> Field:
        """Create a deep copy of this field."""
        ...

    @abstractmethod
    def copy_from(self, other: Field) -> None:
        """Copy information from another field (except alias)."""
        ...


@dataclass
class ValueField(Field):
    """A field representing a single value (column).

    ValueField can hold either:
    - A Python type (legacy, for backward compatibility)
    - A DataType (authoritative, for structured types)

    For authoritative schema declarations (especially arrays/structs),
    use `structured_type` instead of `data_type`.

    Attributes:
        field_alias: The alias used to reference this field (e.g., 'path')
        field_name: The SQL column name (e.g., '_gsql2rsql_path')
        data_type: Legacy Python type (deprecated for new code)
        structured_type: Authoritative DataType (use this for arrays/structs)

    Example (authoritative path declaration):
        from gsql2rsql.planner.data_types import ArrayType, StructType, StructField, PrimitiveType

        path_field = ValueField(
            field_alias="path",
            field_name="_gsql2rsql_path",
            structured_type=ArrayType(
                element_type=StructType(
                    name="PathNode",
                    fields=(StructField("id", PrimitiveType.INT),)
                )
            )
        )
    """

    field_name: str = ""
    data_type: type[Any] | None = None
    # Authoritative structured type (preferred over data_type for arrays/structs)
    structured_type: "DataType | None" = None

    def clone(self) -> ValueField:
        return ValueField(
            field_alias=self.field_alias,
            field_name=self.field_name,
            data_type=self.data_type,
            structured_type=self.structured_type.clone() if self.structured_type else None,
        )

    def copy_from(self, other: Field) -> None:
        if isinstance(other, ValueField):
            self.field_name = other.field_name
            self.data_type = other.data_type
            self.structured_type = (
                other.structured_type.clone() if other.structured_type else None
            )

    def get_element_struct(self) -> "StructType | None":
        """Get the element struct type if this is an array of structs.

        This is the key method for resolving list comprehension variables.
        When processing [n IN array_field | n.prop], this method returns
        the StructType that describes what 'n' looks like.

        Returns:
            StructType if this field is an array of structs, None otherwise

        Example:
            # For path: ARRAY<STRUCT<id: INT, label: STRING>>
            element_struct = path_field.get_element_struct()
            # Returns StructType with fields 'id' and 'label'
        """
        if self.structured_type is None:
            return None
        # Import here to avoid circular imports
        from gsql2rsql.planner.data_types import ArrayType
        if isinstance(self.structured_type, ArrayType):
            return self.structured_type.get_element_struct()
        return None

    def is_array_type(self) -> bool:
        """Check if this field has an array type.

        Returns:
            True if structured_type is ArrayType
        """
        if self.structured_type is None:
            # Fallback to checking Python type
            return self.data_type is list
        from gsql2rsql.planner.data_types import ArrayType
        return isinstance(self.structured_type, ArrayType)

    def __str__(self) -> str:
        if self.structured_type:
            return f"{self.field_alias}: {self.field_name} ({self.structured_type})"
        type_name = self.data_type.__name__ if self.data_type else "?"
        return f"{self.field_alias}: {self.field_name} ({type_name})"


class EntityType(Enum):
    """Type of entity (node or relationship)."""

    NODE = auto()
    RELATIONSHIP = auto()


@dataclass
class EntityField(Field):
    """A field representing an entity (node or relationship)."""

    entity_name: str = ""
    entity_type: EntityType = EntityType.NODE
    bound_entity_name: str = ""
    bound_source_entity_name: str = ""
    bound_sink_entity_name: str = ""
    # For OR syntax ([:KNOWS|WORKS_AT]), stores resolved edge types
    # Empty list = single type (use bound_entity_name), non-empty = multiple types
    bound_edge_types: list[str] = field(default_factory=list)
    node_join_field: ValueField | None = None
    rel_source_join_field: ValueField | None = None
    rel_sink_join_field: ValueField | None = None
    encapsulated_fields: list[ValueField] = field(default_factory=list)
    _referenced_field_names: set[str] = field(default_factory=set)

    def clone(self) -> EntityField:
        return EntityField(
            field_alias=self.field_alias,
            entity_name=self.entity_name,
            entity_type=self.entity_type,
            bound_entity_name=self.bound_entity_name,
            bound_source_entity_name=self.bound_source_entity_name,
            bound_sink_entity_name=self.bound_sink_entity_name,
            bound_edge_types=list(self.bound_edge_types),
            node_join_field=self.node_join_field.clone() if self.node_join_field else None,
            rel_source_join_field=(
                self.rel_source_join_field.clone() if self.rel_source_join_field else None
            ),
            rel_sink_join_field=(
                self.rel_sink_join_field.clone() if self.rel_sink_join_field else None
            ),
            encapsulated_fields=[f.clone() for f in self.encapsulated_fields],
            _referenced_field_names=set(self._referenced_field_names),
        )

    def copy_from(self, other: Field) -> None:
        if isinstance(other, EntityField):
            self.entity_name = other.entity_name
            self.entity_type = other.entity_type
            self.bound_entity_name = other.bound_entity_name
            self.bound_source_entity_name = other.bound_source_entity_name
            self.bound_sink_entity_name = other.bound_sink_entity_name
            self.bound_edge_types = list(other.bound_edge_types)
            self.node_join_field = (
                other.node_join_field.clone() if other.node_join_field else None
            )
            self.rel_source_join_field = (
                other.rel_source_join_field.clone() if other.rel_source_join_field else None
            )
            self.rel_sink_join_field = (
                other.rel_sink_join_field.clone() if other.rel_sink_join_field else None
            )
            self.encapsulated_fields = [f.clone() for f in other.encapsulated_fields]
            self._referenced_field_names = set(other._referenced_field_names)

    @property
    def referenced_field_aliases(self) -> set[str]:
        """Get the set of referenced field names."""
        return self._referenced_field_names

    def add_reference_field_names(self, names: list[str] | None) -> None:
        """Add field names to the referenced set."""
        if names:
            self._referenced_field_names.update(names)

    def __str__(self) -> str:
        type_str = "Node" if self.entity_type == EntityType.NODE else "Rel"
        return f"{self.field_alias}: {self.entity_name} ({type_str})"


class Schema(list[Field]):
    """
    Schema representing the fields available at a point in the logical plan.

    This is essentially a list of Field objects with helper methods.
    """

    @property
    def fields(self) -> list[Field]:
        """Get all fields in the schema as a list."""
        return list(self)

    def add_field(self, field: Field) -> None:
        """Add a field to the schema."""
        self.append(field)

    def clone(self) -> Schema:
        """Create a deep copy of this schema."""
        return Schema([f.clone() for f in self])

    @classmethod
    def merge(cls, schema1: "Schema", schema2: "Schema") -> "Schema":
        """Merge two schemas into a new schema."""
        result = cls()
        for f in schema1:
            result.append(f.clone())
        for f in schema2:
            result.append(f.clone())
        return result

    def get_field(self, alias: str) -> Field | None:
        """Get a field by its alias."""
        for f in self:
            if f.field_alias == alias:
                return f
        return None

    def get_entity_fields(self) -> Iterator[EntityField]:
        """Get all entity fields."""
        for f in self:
            if isinstance(f, EntityField):
                yield f

    def get_value_fields(self) -> Iterator[ValueField]:
        """Get all value fields."""
        for f in self:
            if isinstance(f, ValueField):
                yield f

    def __str__(self) -> str:
        return f"Schema({', '.join(str(f) for f in self)})"

    def get_array_element_struct(self, alias: str) -> "StructType | None":
        """Get the element struct type for an array field.

        This is the key method for resolving lambda variable bindings in
        list comprehensions. Given [n IN array_expr | n.prop], this method
        returns the StructType that describes what 'n' looks like.

        AUTHORITATIVE DESIGN:
        ---------------------
        This method queries the schema for authoritative type information.
        If the field does not have a structured_type or is not an array
        of structs, it returns None (fail-fast, no guessing).

        Args:
            alias: The field alias (e.g., 'path' for nodes(path))

        Returns:
            StructType if the field is an array of structs, None otherwise

        Example:
            schema = operator.get_output_scope()
            # For [n IN nodes(path) | n.id]
            element_struct = schema.get_array_element_struct('path')
            if element_struct is None:
                raise Error("Cannot resolve: element type unknown")
            # element_struct has field 'id' -> resolve n.id
        """
        field = self.get_field(alias)
        if field is None:
            return None
        if isinstance(field, ValueField):
            return field.get_element_struct()
        return None

    def has_authoritative_type(self, alias: str) -> bool:
        """Check if a field has authoritative type information.

        A field has authoritative type info if it has a structured_type set.
        This is used to determine if schema-based resolution is possible.

        Args:
            alias: The field alias to check

        Returns:
            True if the field exists and has structured_type set
        """
        field = self.get_field(alias)
        if field is None:
            return False
        if isinstance(field, ValueField):
            return field.structured_type is not None
        # EntityField always has authoritative type info via encapsulated_fields
        if isinstance(field, EntityField):
            return True
        return False
