"""SQL database schema provider interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from gsql2rsql.common.schema import (
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
    filter: str | None = None

    def __post_init__(self) -> None:
        """Parse table_name if provided and table_or_view_name is not set."""
        if self.table_name and not self.table_or_view_name:
            if "." in self.table_name:
                parts = self.table_name.rsplit(".", 1)
                # Use object.__setattr__ since dataclass may be frozen
                object.__setattr__(self, "schema_name", parts[0])
                object.__setattr__(self, "table_or_view_name", parts[1])
            else:
                object.__setattr__(self, "schema_name", "")
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
            parts = table_name.rsplit(".", 1)
            schema_name = parts[0]
            table_or_view_name = parts[1]
        else:
            schema_name = ""
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
        """Get the fully qualified table name for Databricks SQL.

        For Databricks SQL, table names can be:
        - Simple: table_name
        - Two-part: schema.table
        - Three-part: catalog.schema.table

        Table names are returned exactly as provided by the user.
        If backticks are needed, they should be included by the user when
        creating the SQLTableDescriptor.
        """
        table_name = self.table_or_view_name

        # If table_name already contains dots (e.g., catalog.schema.table),
        # use it directly without adding schema prefix
        if "." in table_name:
            return table_name

        # Skip 'dbo' prefix - it's a SQL Server convention, not Databricks
        if self.schema_name and self.schema_name.lower() != "dbo":
            return f"{self.schema_name}.{table_name}"

        return table_name


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

    @abstractmethod
    def get_wildcard_table_descriptor(self) -> SQLTableDescriptor | None:
        """
        Get the SQL table descriptor for wildcard nodes (no type filter).

        Returns:
            SQLTableDescriptor if wildcard support is enabled, None otherwise.
        """
        ...

    @abstractmethod
    def get_wildcard_edge_table_descriptor(self) -> SQLTableDescriptor | None:
        """
        Get the SQL table descriptor for wildcard edges (no type filter).

        Returns:
            SQLTableDescriptor if wildcard edge support is enabled, None otherwise.
        """
        ...

    def find_edge_by_verb(
        self, verb: str, target_node_name: str | None = None
    ) -> tuple[EdgeSchema, SQLTableDescriptor] | None:
        """
        Find an edge schema by verb (relationship name), optionally filtered by target node.

        This is useful when the source node type is unknown (e.g., in EXISTS patterns).

        Args:
            verb: The relationship type/verb (e.g., "ACTED_IN")
            target_node_name: Optional target node type to filter by

        Returns:
            Tuple of (EdgeSchema, SQLTableDescriptor) if found, None otherwise.
        """
        return None  # Default implementation - subclasses can override


class SimpleSQLSchemaProvider(ISQLDBSchemaProvider):
    """A simple in-memory SQL schema provider."""

    def __init__(self) -> None:
        self._nodes: dict[str, NodeSchema] = {}
        self._edges: dict[str, EdgeSchema] = {}
        self._table_descriptors: dict[str, SQLTableDescriptor] = {}
        self._wildcard_node: NodeSchema | None = None
        self._wildcard_table_desc: SQLTableDescriptor | None = None
        self._wildcard_edge: EdgeSchema | None = None
        self._wildcard_edge_table_desc: SQLTableDescriptor | None = None

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

    def set_wildcard_node(
        self,
        schema: NodeSchema,
        table_descriptor: SQLTableDescriptor,
    ) -> None:
        """Register wildcard node with its SQL descriptor.

        WARNING: No-label support causes full table scans for unlabeled nodes.
        Use labels whenever possible for production queries.

        Args:
            schema: Node schema for wildcard nodes.
            table_descriptor: SQL table descriptor with filter=None (no type filter).
        """
        self._wildcard_node = schema
        self._wildcard_table_desc = table_descriptor

    def set_wildcard_edge(
        self,
        schema: EdgeSchema,
        table_descriptor: SQLTableDescriptor,
    ) -> None:
        """Register wildcard edge with its SQL descriptor.

        WARNING: Untyped edge support causes full table scans on the edges table.
        Specify edge types whenever possible for production queries.

        Args:
            schema: Edge schema for wildcard edges.
            table_descriptor: SQL table descriptor with filter=None (no type filter).
        """
        self._wildcard_edge = schema
        self._wildcard_edge_table_desc = table_descriptor

    def enable_no_label_support(
        self,
        table_name: str,
        node_id_columns: list[str],
        properties: list[EntityProperty] | None = None,
    ) -> None:
        """Enable support for nodes without labels in MATCH patterns.

        This method configures the schema provider to handle queries like:
            MATCH (a)-[:REL]->(b:Label)
        where node 'a' has no label specified.

        WARNING: No-label support causes full table scans for unlabeled nodes.
        Use labels whenever possible for production queries.

        Args:
            table_name: The nodes table name (e.g., "catalog.schema.nodes").
            node_id_columns: List of node ID column names (e.g., ["node_id"]).
            properties: Optional list of node properties available on all nodes.

        Example:
            >>> provider = SimpleSQLSchemaProvider()
            >>> # Add node types...
            >>> provider.enable_no_label_support(
            ...     table_name="catalog.schema.nodes",
            ...     node_id_columns=["node_id"],
            ...     properties=[EntityProperty("name", str)],
            ... )
        """
        from gsql2rsql.common.schema import WILDCARD_NODE_TYPE

        # Create wildcard schema with provided properties
        wildcard_schema = NodeSchema(
            name=WILDCARD_NODE_TYPE,
            node_id_property=EntityProperty(
                property_name=node_id_columns[0] if node_id_columns else "id",
                data_type=str,
            ),
            properties=properties or [],
        )

        # Create table descriptor without type filter (matches all nodes)
        wildcard_desc = SQLTableDescriptor(
            table_name=table_name,
            node_id_columns=node_id_columns,
            filter=None,  # No type filter - matches all nodes
        )

        # Register the wildcard node
        self.set_wildcard_node(wildcard_schema, wildcard_desc)

    def get_wildcard_node_definition(self) -> NodeSchema | None:
        """Get the wildcard node schema if enabled."""
        return self._wildcard_node

    def get_wildcard_table_descriptor(self) -> SQLTableDescriptor | None:
        """Get SQL descriptor for wildcard nodes (no type filter)."""
        return self._wildcard_table_desc

    def get_wildcard_edge_definition(self) -> EdgeSchema | None:
        """Get the wildcard edge schema if enabled."""
        return self._wildcard_edge

    def get_wildcard_edge_table_descriptor(self) -> SQLTableDescriptor | None:
        """Get SQL descriptor for wildcard edges (no type filter)."""
        return self._wildcard_edge_table_desc

    def get_node_definition(self, node_name: str) -> NodeSchema | None:
        """Get a node schema by name."""
        return self._nodes.get(node_name)

    def get_edge_definition(
        self, edge_verb: str, from_node_name: str, to_node_name: str
    ) -> EdgeSchema | None:
        """Get an edge schema by verb and connected node names."""
        edge_id = EdgeSchema.get_edge_id(edge_verb, from_node_name, to_node_name)
        return self._edges.get(edge_id)

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

    def get_sql_table_descriptors(self, entity_name: str) -> SQLTableDescriptor | None:
        """Get the SQL table descriptor for an entity."""
        return self._table_descriptors.get(entity_name)

    def find_edge_by_verb(
        self, verb: str, target_node_name: str | None = None
    ) -> tuple[EdgeSchema, SQLTableDescriptor] | None:
        """Find an edge schema by verb, optionally filtered by target node."""
        for edge_id, edge_schema in self._edges.items():
            if edge_schema.name == verb:
                # Check target node if specified
                if target_node_name and edge_schema.sink_node_id != target_node_name:
                    continue
                # Found matching edge
                table_desc = self._table_descriptors.get(edge_id)
                if table_desc:
                    return (edge_schema, table_desc)
        return None

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
