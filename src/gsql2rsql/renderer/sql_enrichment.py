"""SQL Enrichment — pre-rendering resolution of SQL-specific metadata.

This module sits between the planner/resolver and the renderer emission phase.
It walks the resolved logical plan once, resolving all SQL-specific metadata
(table names, join columns, edge info) into an immutable side-channel
(``EnrichedPlanData``).  The renderer reads from this side-channel instead of
calling ``db_schema.*`` directly, making emission mechanical.

Architecture (see ADR-001, ADR-004 in decisions_log.md)::

    Planner (IGraphSchemaProvider)
        ↓  LogicalPlan with semantic info
    Optimizer (pass_manager.py)
        ↓  Optimized LogicalPlan
    Column Resolver
        ↓  ResolutionResult
    **SQL Enrichment** (this module)        ← runs inside render_plan()
        ↓  EnrichedPlanData (SQL table names, join columns, edge info)
    Renderer (reads EnrichedPlanData, emits SQL)

Design decisions:
    - **Side-channel, not mutation**: Operators stay semantic; SQL metadata is
      stored separately in frozen dataclasses keyed by operator_debug_id.
    - **Incremental migration**: Renderer falls back to ``db_schema`` for operator
      types not yet covered by enrichment.  Each migration adds an ``_enrich_*``
      method here and removes the corresponding ``db_schema.*`` call from the
      renderer.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from gsql2rsql.common.schema import WILDCARD_EDGE_TYPE, WILDCARD_NODE_TYPE
from gsql2rsql.planner.column_resolver import ResolutionResult
from gsql2rsql.planner.logical_plan import LogicalPlan
from gsql2rsql.planner.operators import (
    DataSourceOperator,
    JoinKeyPairType,
    JoinOperator,
    LogicalOperator,
    RecursiveTraversalOperator,
)
from gsql2rsql.planner.schema import EntityField, EntityType
from gsql2rsql.renderer.schema_provider import ISQLDBSchemaProvider, SQLTableDescriptor


# ---------------------------------------------------------------------------
# Enriched metadata dataclasses (immutable, produced by SQLEnrichmentPass)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EnrichedDataSource:
    """SQL-resolved metadata for a DataSourceOperator."""

    table_descriptor: SQLTableDescriptor
    type_filter_clause: str | None = None


@dataclass(frozen=True)
class EnrichedEdgeInfo:
    """SQL-resolved metadata for one edge type in a RecursiveTraversalOperator."""

    edge_type: str
    table_descriptor: SQLTableDescriptor
    source_column: str
    sink_column: str
    type_filter_clause: str | None = None
    properties: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class EnrichedJoinPair:
    """SQL-resolved column names for one join key pair."""

    left_column: str
    right_column: str
    original_pair_type: JoinKeyPairType


@dataclass(frozen=True)
class EnrichedPlanData:
    """Immutable SQL-resolved metadata for the entire plan.

    Produced by ``SQLEnrichmentPass``, consumed by the renderer.
    Keyed by ``operator_debug_id``.

    During incremental migration, a dict being empty for a given operator type
    means the renderer should fall back to its existing ``db_schema`` code path.
    """

    data_sources: dict[int, EnrichedDataSource] = field(default_factory=dict)
    join_pairs: dict[int, list[EnrichedJoinPair]] = field(default_factory=dict)
    edge_infos: dict[int, list[EnrichedEdgeInfo]] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Enrichment pass
# ---------------------------------------------------------------------------


class SQLEnrichmentPass:
    """Single-pass tree walker that resolves SQL metadata.

    Call ``.enrich(plan, resolution)`` to produce an ``EnrichedPlanData``
    instance.  The pass walks every operator exactly once (bottom-up) and
    delegates to type-specific ``_enrich_*`` methods.

    Currently a no-op skeleton — each ``_enrich_*`` method will be implemented
    as we migrate the corresponding renderer logic here.
    """

    def __init__(self, db_schema: ISQLDBSchemaProvider) -> None:
        self._db_schema = db_schema

    def enrich(
        self,
        plan: LogicalPlan,
        resolution: ResolutionResult,
    ) -> EnrichedPlanData:
        """Walk plan once, produce SQL-resolved metadata."""
        data_sources: dict[int, EnrichedDataSource] = {}
        join_pairs: dict[int, list[EnrichedJoinPair]] = {}
        edge_infos: dict[int, list[EnrichedEdgeInfo]] = {}

        visited: set[int] = set()
        for terminal in plan.terminal_operators:
            self._walk(
                terminal, visited, data_sources, join_pairs, edge_infos,
            )

        return EnrichedPlanData(
            data_sources=data_sources,
            join_pairs=join_pairs,
            edge_infos=edge_infos,
        )

    def _walk(
        self,
        op: LogicalOperator,
        visited: set[int],
        data_sources: dict[int, EnrichedDataSource],
        join_pairs: dict[int, list[EnrichedJoinPair]],
        edge_infos: dict[int, list[EnrichedEdgeInfo]],
    ) -> None:
        """Bottom-up walk: enrich children first, then current operator."""
        op_id = op.operator_debug_id
        if op_id in visited:
            return
        visited.add(op_id)

        for child in op.in_operators:
            self._walk(child, visited, data_sources, join_pairs, edge_infos)

        self._enrich_one(
            op, data_sources, join_pairs, edge_infos,
        )

    def _enrich_one(
        self,
        op: LogicalOperator,
        data_sources: dict[int, EnrichedDataSource],
        join_pairs: dict[int, list[EnrichedJoinPair]],
        edge_infos: dict[int, list[EnrichedEdgeInfo]],
    ) -> None:
        """Dispatch to type-specific enrichment.

        Skeleton — each branch will be implemented as we migrate renderer logic.
        """
        if isinstance(op, DataSourceOperator):
            self._enrich_data_source(op, data_sources)
        elif isinstance(op, JoinOperator):
            self._enrich_join(op, join_pairs)
        elif isinstance(op, RecursiveTraversalOperator):
            self._enrich_recursive(op, edge_infos)

    # ------------------------------------------------------------------
    # Type-specific enrichment methods (stubs for incremental migration)
    # ------------------------------------------------------------------

    def _enrich_data_source(
        self,
        op: DataSourceOperator,
        data_sources: dict[int, EnrichedDataSource],
    ) -> None:
        """Resolve SQL table name and type filter for a DataSourceOperator.

        Migrated from sql_renderer._render_data_source() lines 760-878.
        Resolves:
          - table_descriptor: SQL table from entity name (with wildcard fallback)
          - type_filter_clause: edge type filter (single or multi-edge OR)
        """
        if not op.output_schema:
            return

        entity_field = op.output_schema[0]
        if not isinstance(entity_field, EntityField):
            return

        # Resolve table descriptor (supports no-label nodes via wildcard)
        table_desc = self._resolve_table_descriptor(entity_field.bound_entity_name)
        if table_desc is None:
            return

        # Resolve type filter clause from edge types
        type_filter = self._resolve_type_filter(entity_field, table_desc)

        data_sources[op.operator_debug_id] = EnrichedDataSource(
            table_descriptor=table_desc,
            type_filter_clause=type_filter,
        )

    def _resolve_table_descriptor(
        self, entity_name: str
    ) -> SQLTableDescriptor | None:
        """Get table descriptor, falling back to wildcard for no-label nodes/edges."""
        if not entity_name or entity_name == WILDCARD_NODE_TYPE:
            return self._db_schema.get_wildcard_table_descriptor()
        if WILDCARD_EDGE_TYPE in entity_name:
            return self._db_schema.get_wildcard_edge_table_descriptor()
        return self._db_schema.get_sql_table_descriptors(entity_name)

    def _resolve_type_filter(
        self, entity_field: EntityField, table_desc: SQLTableDescriptor
    ) -> str | None:
        """Compute type filter clause for edge types.

        For multi-edge ([:KNOWS|WORKS_AT]), combines per-type filters with OR.
        For single-edge, uses the table descriptor's filter directly.
        """
        if len(entity_field.bound_edge_types) > 1:
            edge_filters: list[str] = []
            parts = entity_field.bound_entity_name.split("@")
            source_type = parts[0] if len(parts) >= 1 else None

            for edge_type in entity_field.bound_edge_types:
                edges = self._db_schema.find_edges_by_verb(
                    edge_type,
                    from_node_name=source_type,
                    to_node_name=None,
                )
                if edges:
                    edge_id = edges[0].id
                    type_desc = self._db_schema.get_sql_table_descriptors(edge_id)
                    if type_desc and type_desc.filter:
                        edge_filters.append(type_desc.filter)

            if edge_filters:
                return " OR ".join(f"({f})" for f in edge_filters)
            return None

        if table_desc.filter:
            return table_desc.filter

        return None

    def _enrich_join(
        self,
        op: JoinOperator,
        join_pairs: dict[int, list[EnrichedJoinPair]],
    ) -> None:
        """Resolve SQL column names for join key pairs.

        TODO: Migrate logic from sql_renderer._collect_join_key_columns() lines 397-486.
        """

    def _enrich_recursive(
        self,
        op: RecursiveTraversalOperator,
        edge_infos: dict[int, list[EnrichedEdgeInfo]],
    ) -> None:
        """Resolve edge table info for recursive traversal.

        TODO: Migrate logic from recursive_cte_renderer._resolve_edge_tables(),
        _get_edge_column_names(), _get_edge_table_name(), _get_edge_filter_clause().
        """
