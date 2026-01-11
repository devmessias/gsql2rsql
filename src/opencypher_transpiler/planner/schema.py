"""Schema definitions for the logical planner."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Iterator


@dataclass
class Field(ABC):
    """Represents a single alias (value column or entity) in a schema."""

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
    """A field representing a single value (column)."""

    field_name: str = ""
    data_type: type[Any] | None = None

    def clone(self) -> ValueField:
        return ValueField(
            field_alias=self.field_alias,
            field_name=self.field_name,
            data_type=self.data_type,
        )

    def copy_from(self, other: Field) -> None:
        if isinstance(other, ValueField):
            self.field_name = other.field_name
            self.data_type = other.data_type

    def __str__(self) -> str:
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
