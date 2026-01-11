"""SQL database schema provider interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from opencypher_transpiler.common.schema import (
    EdgeSchema,
    EntityProperty,
    IGraphSchemaProvider,
    NodeSchema,
)


@dataclass
class SQLTableDescriptor:
    """Describes how a graph entity maps to SQL table(s).

    Can be constructed in two ways:
    1. Direct: SQLTableDescriptor(table_or_view_name="Device", schema_name="dbo")
    2. Convenience: SQLTableDescriptor(
           entity_id="device",
           table_name="dbo.Device",
           node_id_columns=["id"]
       )

    When using the convenience form with table_name, the schema and table
    will be parsed automatically.
    """

    table_or_view_name: str = ""
    schema_name: str = "dbo"
    column_mappings: dict[str, str] = field(default_factory=dict)
    node_id_columns: list[str] = field(default_factory=list)
    entity_id: str | None = None
    table_name: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Parse table_name if provided and table_or_view_name is not set."""
        if self.table_name and not self.table_or_view_name:
            if "." in self.table_name:
                parts = self.table_name.split(".", 1)
                # Use object.__setattr__ since dataclass may be frozen
                object.__setattr__(self, "schema_name", parts[0])
                object.__setattr__(self, "table_or_view_name", parts[1])
            else:
                object.__setattr__(self, "table_or_view_name", self.table_name)

    @classmethod
    def from_table_name(
        cls,
        entity_id: str,
        table_name: str,
        node_id_columns: list[str] | None = None,
        column_mappings: dict[str, str] | None = None,
    ) -> "SQLTableDescriptor":
        """
        Create a SQLTableDescriptor from a fully qualified table name.

        Args:
            entity_id: The entity identifier
                (node name or edge id like "node1@verb@node2").
            table_name: Fully qualified table name
                (e.g., "dbo.Device" or "Device").
            node_id_columns: List of ID column names for the entity.
            column_mappings: Optional property to column mappings.

        Returns:
            A new SQLTableDescriptor instance.
        """
        if "." in table_name:
            parts = table_name.split(".", 1)
            schema_name = parts[0]
            table_or_view_name = parts[1]
        else:
            schema_name = "dbo"
            table_or_view_name = table_name

        return cls(
            table_or_view_name=table_or_view_name,
            schema_name=schema_name,
            column_mappings=column_mappings or {},
            node_id_columns=node_id_columns or [],
            entity_id=entity_id,
        )

    @property
    def full_table_name(self) -> str:
        """Get the fully qualified table name."""
        if self.schema_name:
            return f"[{self.schema_name}].[{self.table_or_view_name}]"
        return f"[{self.table_or_view_name}]"


class ISQLDBSchemaProvider(IGraphSchemaProvider, ABC):
    """
    Interface for SQL database schema providers.

    Extends IGraphSchemaProvider with SQL-specific mappings.
    """

    @abstractmethod
    def get_sql_table_descriptors(self, entity_name: str) -> SQLTableDescriptor | None:
        """
        Get the SQL table descriptor for a graph entity.

        Args:
            entity_name: The unique entity name (node name or edge id).

        Returns:
            SQLTableDescriptor if found, None otherwise.
        """
        ...


class SimpleSQLSchemaProvider(ISQLDBSchemaProvider):
    """A simple in-memory SQL schema provider."""

    def __init__(self) -> None:
        self._nodes: dict[str, NodeSchema] = {}
        self._edges: dict[str, EdgeSchema] = {}
        self._table_descriptors: dict[str, SQLTableDescriptor] = {}

    def add_node(
        self,
        schema: NodeSchema,
        table_descriptor: SQLTableDescriptor,
    ) -> None:
        """Add a node schema with its SQL table descriptor."""
        self._nodes[schema.name] = schema
        self._table_descriptors[schema.id] = table_descriptor

    def add_edge(
        self,
        schema: EdgeSchema,
        table_descriptor: SQLTableDescriptor,
    ) -> None:
        """Add an edge schema with its SQL table descriptor."""
        self._edges[schema.id] = schema
        self._table_descriptors[schema.id] = table_descriptor

    def get_node_definition(self, node_name: str) -> NodeSchema | None:
        """Get a node schema by name."""
        return self._nodes.get(node_name)

    def get_edge_definition(
        self, edge_verb: str, from_node_name: str, to_node_name: str
    ) -> EdgeSchema | None:
        """Get an edge schema by verb and connected node names."""
        edge_id = EdgeSchema.get_edge_id(edge_verb, from_node_name, to_node_name)
        return self._edges.get(edge_id)

    def get_sql_table_descriptors(self, entity_name: str) -> SQLTableDescriptor | None:
        """Get the SQL table descriptor for an entity."""
        return self._table_descriptors.get(entity_name)

    def add_table(self, descriptor: SQLTableDescriptor) -> None:
        """
        Add a table descriptor directly using its entity_id.

        This is a convenience method that automatically creates the appropriate
        NodeSchema or EdgeSchema based on the entity_id format.

        Args:
            descriptor: A SQLTableDescriptor with entity_id set.
                For nodes: entity_id should be the node name (e.g., "device").
                For edges: entity_id should be "from@verb@to"
                    (e.g., "device@belongsTo@tenant").

        Raises:
            ValueError: If descriptor.entity_id is not set.
        """
        if not descriptor.entity_id:
            raise ValueError(
                "descriptor.entity_id must be set when using add_table()"
            )

        entity_id = descriptor.entity_id

        # Check if this is an edge (format: "from@verb@to")
        if "@" in entity_id:
            parts = entity_id.split("@")
            if len(parts) == 3:
                from_node, verb, to_node = parts
                edge_schema = EdgeSchema(
                    name=verb,
                    source_node_id=from_node,
                    sink_node_id=to_node,
                    properties=[],
                )
                self._edges[edge_schema.id] = edge_schema
                self._table_descriptors[edge_schema.id] = descriptor
            else:
                raise ValueError(
                    f"Invalid edge entity_id format: {entity_id}. "
                    "Expected 'from@verb@to'."
                )
        else:
            # It's a node
            node_schema = NodeSchema(name=entity_id, properties=[])
            self._nodes[node_schema.name] = node_schema
            self._table_descriptors[node_schema.id] = descriptor
