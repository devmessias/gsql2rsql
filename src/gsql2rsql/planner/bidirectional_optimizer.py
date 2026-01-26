"""BFS Bidirectional Optimizer.

Detects variable-length path queries that can benefit from bidirectional BFS
and marks them for optimized rendering.

OPTIMIZATION CRITERIA:
1. Query has a RecursiveTraversalOperator (VLP)
2. Source node has EQUALITY filter on ID column
3. Target node has EQUALITY filter on ID column


SAFETY:
- Only applies to queries meeting ALL criteria
- Preserves exact semantics (same results as unidirectional)
- Can be disabled via feature flag

TWO IMPLEMENTATION MODES:
1. "recursive": Uses WITH RECURSIVE forward AS (...), backward AS (...)
   - Compact SQL
   - Dynamic depth
   - Good for unknown or large max_hops

2. "unrolling": Uses separate CTEs fwd0, fwd1, ..., bwd0, bwd1, ...
   - TRUE frontier behavior (each level only sees previous level)
   - Fixed depth at transpile time
   - Better memory for small fixed depth (max_hops <= 6)
   - SQL size grows O(depth^2)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from gsql2rsql.parser.ast import (
    QueryExpression,
    QueryExpressionBinary,
    QueryExpressionProperty,
    QueryExpressionValue,
)
from gsql2rsql.planner.operators import (
    LogicalOperator,
    RecursiveTraversalOperator,
)

if TYPE_CHECKING:
    from gsql2rsql.common.schema import IGraphSchemaProvider
    from gsql2rsql.planner.logical_plan import LogicalPlan


class BidirectionalBFSOptimizer:
    """Optimizer pass that detects and enables bidirectional BFS for eligible VLPs.

    This optimizer:
    1. Finds all RecursiveTraversalOperator nodes in the plan
    2. Checks if source AND target have equality filters on ID
    3. If yes, sets bidirectional_bfs_mode and computes depth split

    The RENDERER then uses these flags to generate optimized SQL.

    Attributes:
        _graph_schema: Graph schema provider for getting ID column names
        _mode: Bidirectional mode to use ("off", "recursive", "unrolling", "auto")
        _enabled: Whether optimization is enabled

    Example usage:
        optimizer = BidirectionalBFSOptimizer(schema, mode="auto")
        optimizer.optimize(plan)
    """

    def __init__(
        self,
        graph_schema: IGraphSchemaProvider | None = None,
        mode: Literal["off", "recursive", "unrolling", "auto"] = "auto",
        enabled: bool = True,
    ) -> None:
        """Initialize the bidirectional BFS optimizer.

        Args:
            graph_schema: Schema provider for node/edge definitions
            mode: Bidirectional mode:
                - "off": Disable bidirectional (no optimization)
                - "recursive": Use WITH RECURSIVE forward/backward
                - "unrolling": Use unrolled fwd0, fwd1, bwd0, bwd1 CTEs
                - "auto": Select based on max_hops (unrolling for <=6, recursive for >6)
            enabled: Whether to run this optimizer
        """
        self._graph_schema = graph_schema
        self._mode = mode
        self._enabled = enabled

    def optimize(self, plan: LogicalPlan) -> None:
        """Apply bidirectional optimization to eligible VLPs in the plan.

        Args:
            plan: The logical plan to optimize (modified in place)
        """
        if not self._enabled or self._mode == "off":
            return

        # Process each terminal operator's subtree
        for terminal_op in plan.terminal_operators:
            self._optimize_recursive(terminal_op)

    def _optimize_recursive(self, op: LogicalOperator) -> None:
        """Recursively process all operators in the plan tree."""
        if isinstance(op, RecursiveTraversalOperator):
            self._try_enable_bidirectional(op)

        # Process children
        for child in op.in_operators:
            self._optimize_recursive(child)

    def _try_enable_bidirectional(self, op: RecursiveTraversalOperator) -> None:
        """Check if this VLP operator can use bidirectional BFS and enable it.

        Criteria for bidirectional BFS:
        1. start_node_filter exists and contains equality on source ID
        2. sink_node_filter exists and contains equality on target ID

        Note: Both DIRECTED and UNDIRECTED traversals are supported.
        For undirected, the renderer generates UNION ALL to explore
        edges in both directions.

        Args:
            op: The RecursiveTraversalOperator to potentially optimize
        """
        # Get ID column names for source and target
        source_id_col = self._get_id_column(op.source_node_type, op.source_id_column)
        target_id_col = self._get_id_column(op.target_node_type, op.target_id_column)

        # Extract equality value from source filter
        source_value = self._extract_equality_value(
            op.start_node_filter,
            op.source_alias,
            source_id_col,
        )

        if source_value is None:
            return  # Source doesn't have equality filter on ID

        # Extract equality value from target filter
        target_value = self._extract_equality_value(
            op.sink_node_filter,
            op.target_alias,
            target_id_col,
        )

        if target_value is None:
            return  # Target doesn't have equality filter on ID

        # Both have equality filters - enable bidirectional!
        # Select mode based on configuration and query characteristics
        selected_mode = self._select_mode(op.max_hops)
        op.bidirectional_bfs_mode = selected_mode

        # Compute depth split (approximately half for each direction)
        total_depth = op.max_hops or 10  # Default to 10 if unbounded
        op.bidirectional_depth_forward = (total_depth + 1) // 2
        op.bidirectional_depth_backward = total_depth // 2

        # Store target value for backward CTE base case
        op.bidirectional_target_value = target_value

    def _select_mode(
        self,
        max_hops: int | None,
    ) -> Literal["recursive", "unrolling"]:
        """Select the bidirectional implementation mode.

        Decision tree:
        1. If mode is explicit ('recursive', 'unrolling') -> use it
        2. If 'auto':
           a. max_hops is None (unbounded) -> recursive
           b. max_hops <= 6 -> unrolling (manageable SQL size, true frontier)
           c. max_hops > 6 -> recursive (SQL would be too large)

        Args:
            max_hops: Maximum path length from the query

        Returns:
            Selected mode ("recursive" or "unrolling")
        """
        if self._mode in ("recursive", "unrolling"):
            return self._mode  # type: ignore[return-value]

        # Auto mode: select based on query characteristics
        if max_hops is None:
            # Unbounded depth - must use recursive
            return "recursive"
        elif max_hops <= 6:
            # Small fixed depth - unrolling gives true frontier behavior
            return "unrolling"
        else:
            # Large depth - recursive is more practical
            return "recursive"

    def _extract_equality_value(
        self,
        filter_expr: QueryExpression | None,
        alias: str,
        id_column: str,
    ) -> str | None:
        """Extract the literal value from an equality filter like 'a.id = "value"'.

        Handles:
        - Simple equality: a.id = 'value'
        - Reversed equality: 'value' = a.id
        - AND conjunctions: a.id = 'value' AND a.other = 'foo' (extracts first match)

        Returns None if:
        - No filter expression
        - Filter is not equality on the specified column
        - Filter has non-literal value
        - Filter uses OR (would need multiple values)

        Args:
            filter_expr: The filter expression to analyze
            alias: Variable alias to match (e.g., "a")
            id_column: ID column name to match (e.g., "node_id")

        Returns:
            The literal value as string, or None if not an equality filter
        """
        if filter_expr is None:
            return None

        # Handle AND conjunction: extract from any part
        if isinstance(filter_expr, QueryExpressionBinary):
            if filter_expr.operator and filter_expr.operator.name.name == "AND":
                # Try left side first
                left_val = self._extract_equality_value(
                    filter_expr.left_expression, alias, id_column
                )
                if left_val is not None:
                    return left_val
                # Try right side
                return self._extract_equality_value(
                    filter_expr.right_expression, alias, id_column
                )

            # Handle simple equality: a.id = 'value' or 'value' = a.id
            # Note: operator name is "EQ" not "=" in the AST
            if filter_expr.operator and filter_expr.operator.name.name in ("=", "EQ"):
                left = filter_expr.left_expression
                right = filter_expr.right_expression

                # Check if left is property access on the right alias/column
                if isinstance(left, QueryExpressionProperty):
                    if (
                        left.variable_name == alias
                        and left.property_name == id_column
                    ):
                        # Right should be a literal value
                        if isinstance(right, QueryExpressionValue):
                            return str(right.value)

                # Check reversed: 'value' = a.id
                if isinstance(right, QueryExpressionProperty):
                    if (
                        right.variable_name == alias
                        and right.property_name == id_column
                    ):
                        if isinstance(left, QueryExpressionValue):
                            return str(left.value)

        return None

    def _get_id_column(
        self,
        node_type: str | None,
        default: str,
    ) -> str:
        """Get the ID column name for a node type.

        Args:
            node_type: Node type name (may be None for unlabeled nodes)
            default: Default column name from the operator

        Returns:
            ID column name to use
        """
        if self._graph_schema and node_type:
            node_def = self._graph_schema.get_node_definition(node_type)
            if node_def and node_def.node_id_property:
                return node_def.node_id_property.property_name
        return default


def apply_bidirectional_optimization(
    plan: LogicalPlan,
    graph_schema: IGraphSchemaProvider | None = None,
    mode: Literal["off", "recursive", "unrolling", "auto"] = "auto",
) -> None:
    """Convenience function to apply bidirectional optimization to a plan.

    Args:
        plan: The logical plan to optimize (modified in place)
        graph_schema: Schema provider for node/edge definitions
        mode: Bidirectional mode (see BidirectionalBFSOptimizer for details)
    """
    optimizer = BidirectionalBFSOptimizer(
        graph_schema=graph_schema,
        mode=mode,
        enabled=mode != "off",
    )
    optimizer.optimize(plan)
