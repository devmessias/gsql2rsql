"""Graph schema definitions for the transpiler."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

# Sentinel value for wildcard nodes (no-label support)
WILDCARD_NODE_TYPE = "__wildcard_node__"

# Sentinel value for wildcard edges (untyped edge support)
WILDCARD_EDGE_TYPE = "__wildcard_edge__"


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

    @abstractmethod
    def get_wildcard_node_definition(self) -> NodeSchema | None:
        """
        Return the wildcard node schema for no-label support.

        Returns:
            NodeSchema if wildcard support is enabled, None otherwise.
        """
        ...

    @abstractmethod
    def find_edges_by_verb(
        self,
        edge_verb: str,
        from_node_name: str | None = None,
        to_node_name: str | None = None,
    ) -> list[EdgeSchema]:
        """
        Find edges matching verb and optionally source/sink types.

        This method supports partial matching for no-label support.

        Args:
            edge_verb: The relationship type (required, exact match).
            from_node_name: Source node type (None/empty = match any).
            to_node_name: Target node type (None/empty = match any).

        Returns:
            List of matching EdgeSchema objects.
        """
        ...

    @abstractmethod
    def get_wildcard_edge_definition(self) -> EdgeSchema | None:
        """
        Return the wildcard edge schema for untyped edge support.

        Returns:
            EdgeSchema if wildcard edge support is enabled, None otherwise.
        """
        ...


class SimpleGraphSchemaProvider(IGraphSchemaProvider):
    """A simple in-memory graph schema provider."""

    def __init__(self) -> None:
        self._nodes: dict[str, NodeSchema] = {}
        self._edges: dict[str, EdgeSchema] = {}
        self._wildcard_node: NodeSchema | None = None
        self._wildcard_edge: EdgeSchema | None = None

    def add_node(self, schema: NodeSchema) -> None:
        """Add a node schema to the provider."""
        self._nodes[schema.name] = schema

    def add_edge(self, schema: EdgeSchema) -> None:
        """Add an edge schema to the provider."""
        self._edges[schema.id] = schema

    def set_wildcard_node(self, schema: NodeSchema) -> None:
        """Register a wildcard node schema for no-label support.

        WARNING: No-label support causes full table scans for unlabeled nodes.
        Use labels whenever possible for production queries.
        """
        self._wildcard_node = schema

    def set_wildcard_edge(self, schema: EdgeSchema) -> None:
        """Register a wildcard edge schema for untyped edge support.

        WARNING: Untyped edge support causes full table scans on the edges table.
        Specify edge types whenever possible for production queries.
        """
        self._wildcard_edge = schema

    def get_node_definition(self, node_name: str) -> NodeSchema | None:
        """Get a node schema by name."""
        return self._nodes.get(node_name)

    def get_edge_definition(
        self, edge_verb: str, from_node_name: str, to_node_name: str
    ) -> EdgeSchema | None:
        """Get an edge schema by verb and connected node names."""
        edge_id = EdgeSchema.get_edge_id(edge_verb, from_node_name, to_node_name)
        return self._edges.get(edge_id)

    def get_wildcard_node_definition(self) -> NodeSchema | None:
        """Get the wildcard node schema if enabled."""
        return self._wildcard_node

    def get_wildcard_edge_definition(self) -> EdgeSchema | None:
        """Get the wildcard edge schema if enabled."""
        return self._wildcard_edge

    def find_edges_by_verb(
        self,
        edge_verb: str,
        from_node_name: str | None = None,
        to_node_name: str | None = None,
    ) -> list[EdgeSchema]:
        """Find edges matching verb and optionally source/sink types.

        Args:
            edge_verb: Relationship type (required, exact match).
            from_node_name: Source type (None/empty = match any).
            to_node_name: Target type (None/empty = match any).
        """
        results = []
        for edge_schema in self._edges.values():
            if edge_schema.name != edge_verb:
                continue
            if from_node_name and edge_schema.source_node_id != from_node_name:
                continue
            if to_node_name and edge_schema.sink_node_id != to_node_name:
                continue
            results.append(edge_schema)
        return results
