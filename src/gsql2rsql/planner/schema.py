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
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from gsql2rsql.planner.data_types import DataType, StructType


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

    def __str__(self) -> str:
        return f"Schema({', '.join(str(f) for f in self)})"
