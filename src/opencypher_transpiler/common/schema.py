"""Graph schema definitions for the transpiler."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class EntityProperty:
    """Represents a property of a node or edge in the graph schema."""

    property_name: str
    data_type: type[Any]  # Python type equivalent

    def __post_init__(self) -> None:
        """Validate the property after initialization."""
        if not self.property_name:
            raise ValueError("Property name cannot be empty")


@dataclass
class EntitySchema(ABC):
    """Base class for graph schema entities (nodes and edges)."""

    name: str
    properties: list[EntityProperty] = field(default_factory=list)

    @property
    @abstractmethod
    def id(self) -> str:
        """Unique identifier for this entity."""
        ...


@dataclass
class NodeSchema(EntitySchema):
    """Schema definition for a node type in the graph."""

    node_id_property: EntityProperty | None = None

    @property
    def id(self) -> str:
        """Return the node name as its identifier."""
        return self.name


@dataclass
class EdgeSchema(EntitySchema):
    """Schema definition for an edge type in the graph."""

    SEPARATOR: str = "@"

    source_node_id: str = ""
    sink_node_id: str = ""
    source_id_property: EntityProperty | None = None
    sink_id_property: EntityProperty | None = None

    @property
    def id(self) -> str:
        """Return a unique identifier combining source, verb, and sink."""
        return self.get_edge_id(self.name, self.source_node_id, self.sink_node_id)

    @classmethod
    def get_edge_id(cls, verb: str, source_node: str, sink_node: str) -> str:
        """Create a unique edge identifier from verb and connected nodes."""
        return f"{source_node}{cls.SEPARATOR}{verb}{cls.SEPARATOR}{sink_node}"


class IGraphSchemaProvider(ABC):
    """Interface for providing graph schema definitions."""

    @abstractmethod
    def get_node_definition(self, node_name: str) -> NodeSchema | None:
        """
        Return a NodeSchema for the given node name.

        Args:
            node_name: The name of the node type to look up.

        Returns:
            NodeSchema if found, None otherwise.
        """
        ...

    @abstractmethod
    def get_edge_definition(
        self, edge_verb: str, from_node_name: str, to_node_name: str
    ) -> EdgeSchema | None:
        """
        Return an EdgeSchema for the given edge verb and connected nodes.

        Args:
            edge_verb: The relationship type/verb.
            from_node_name: The source node type name.
            to_node_name: The target node type name.

        Returns:
            EdgeSchema if found, None otherwise.
        """
        ...


class SimpleGraphSchemaProvider(IGraphSchemaProvider):
    """A simple in-memory graph schema provider."""

    def __init__(self) -> None:
        self._nodes: dict[str, NodeSchema] = {}
        self._edges: dict[str, EdgeSchema] = {}

    def add_node(self, schema: NodeSchema) -> None:
        """Add a node schema to the provider."""
        self._nodes[schema.name] = schema

    def add_edge(self, schema: EdgeSchema) -> None:
        """Add an edge schema to the provider."""
        self._edges[schema.id] = schema

    def get_node_definition(self, node_name: str) -> NodeSchema | None:
        """Get a node schema by name."""
        return self._nodes.get(node_name)

    def get_edge_definition(
        self, edge_verb: str, from_node_name: str, to_node_name: str
    ) -> EdgeSchema | None:
        """Get an edge schema by verb and connected node names."""
        edge_id = EdgeSchema.get_edge_id(edge_verb, from_node_name, to_node_name)
        return self._edges.get(edge_id)
