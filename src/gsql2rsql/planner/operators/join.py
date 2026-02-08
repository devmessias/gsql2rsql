"""Join operators and related types."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto

from gsql2rsql.common.utils import fnv_hash
from gsql2rsql.planner.operators.base import BinaryLogicalOperator
from gsql2rsql.planner.schema import Schema


class JoinType(Enum):
    """Type of join operation."""

    CROSS = 0
    LEFT = 1
    INNER = 2


class JoinKeyPairType(Enum):
    """Type of join key pairing."""

    NONE = auto()
    SOURCE = auto()  # Node join to Relationship's SourceId
    SINK = auto()  # Node join to Relationship's SinkId
    EITHER = auto()  # Node can join either source or sink (legacy, for VLP)
    BOTH = auto()  # Node joins both source and sink
    NODE_ID = auto()  # Node to node join
    # For undirected single-hop with UNION ALL expansion:
    # - EITHER_AS_SOURCE: source-side node joins on source_key after UNION
    # - EITHER_AS_SINK: sink-side node joins on sink_key after UNION
    EITHER_AS_SOURCE = auto()
    EITHER_AS_SINK = auto()


@dataclass
class JoinKeyPair:
    """Structure designating how two entities should be joined.

    Attributes:
        node_alias: The alias of the node in the join.
        relationship_or_node_alias: The alias of the relationship or other node.
        pair_type: The type of join key pair (SOURCE, SINK, etc.).
        use_union_for_undirected: For undirected relationships (EITHER_AS_SOURCE,
            EITHER_AS_SINK), indicates whether the renderer should use UNION ALL
            expansion (True, default) or OR in JOIN conditions (False).
            This is a planner decision based on the edge access strategy.
    """

    node_alias: str
    relationship_or_node_alias: str
    pair_type: JoinKeyPairType = JoinKeyPairType.NONE
    use_union_for_undirected: bool = True

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JoinKeyPair):
            return False
        return (
            self.pair_type == other.pair_type
            and self.node_alias == other.node_alias
            and self.relationship_or_node_alias == other.relationship_or_node_alias
            and self.use_union_for_undirected == other.use_union_for_undirected
        )

    def __hash__(self) -> int:
        return fnv_hash(
            self.pair_type,
            self.node_alias,
            self.relationship_or_node_alias,
            self.use_union_for_undirected,
        )

    def __str__(self) -> str:
        return (
            f"JoinPair: Node={self.node_alias} "
            f"RelOrNode={self.relationship_or_node_alias} Type={self.pair_type.name}"
        )


@dataclass
class JoinOperator(BinaryLogicalOperator):
    """Operator to perform joins between data sources."""

    join_type: JoinType = JoinType.INNER
    join_pairs: list[JoinKeyPair] = field(default_factory=list)
    recursive_source_alias: str | None = None

    def propagate_data_types_for_in_schema(self) -> None:
        """Propagate data types from upstream operators to input schema."""
        if self.in_operator_left and self.in_operator_right:
            self.input_schema = Schema.merge(
                self.in_operator_left.output_schema,
                self.in_operator_right.output_schema,
            )

    def propagate_data_types_for_out_schema(self) -> None:
        """Propagate data types from input schema to output schema."""
        self.output_schema = Schema(self.input_schema.fields)

    @property
    def depth(self) -> int:
        left_depth = self.in_operator_left.depth if self.in_operator_left else 0
        right_depth = self.in_operator_right.depth if self.in_operator_right else 0
        return max(left_depth, right_depth) + 1

    def add_join_pair(self, pair: JoinKeyPair) -> None:
        """Add a join key pair."""
        if pair not in self.join_pairs:
            self.join_pairs.append(pair)

    def __str__(self) -> str:
        base = super().__str__()
        joins = ", ".join(str(p) for p in self.join_pairs)
        return f"{base}\n  JoinType: {self.join_type.name}\n  Joins: {joins}"
