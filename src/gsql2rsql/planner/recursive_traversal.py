"""Recursive traversal (VLP) planning for variable-length paths.

This module handles the creation of RecursiveTraversalOperator for
variable-length path patterns like:
    MATCH path = (a)-[:TRANSFER*1..5]->(b)

It includes:
- Building the recursive CTE structure
- Extracting source/sink node filters for optimization
- Integration with PathExpressionAnalyzer for edge collection decisions
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from gsql2rsql.common.schema import EdgeAccessStrategy, IGraphSchemaProvider
from gsql2rsql.parser.ast import (
    Entity,
    MatchClause,
    NodeEntity,
    QueryExpression,
    QueryExpressionBinary,
    QueryExpressionProperty,
    RelationshipDirection,
    RelationshipEntity,
)
from gsql2rsql.planner.operators import (
    DataSourceOperator,
    JoinKeyPair,
    JoinKeyPairType,
    JoinOperator,
    JoinType,
    LogicalOperator,
    RecursiveTraversalOperator,
)
from gsql2rsql.planner.path_analyzer import (
    PathExpressionAnalyzer,
    remove_pushed_predicates,
)

if TYPE_CHECKING:
    from gsql2rsql.common.logging import ILoggable


def create_recursive_match_tree(
    match_clause: MatchClause,
    rel: RelationshipEntity,
    source_node: NodeEntity,
    target_node: NodeEntity,
    all_ops: list[LogicalOperator],
    return_exprs: list[QueryExpression] | None,
    graph_schema: IGraphSchemaProvider | None,
    logger: ILoggable | None = None,
) -> LogicalOperator:
    """Create logical tree for variable-length path traversal.

    This function uses PathExpressionAnalyzer to optimize the recursive CTE:

    1. EDGE COLLECTION OPTIMIZATION:
       Only collects edge properties (path_edges array) when relationships(path)
       is actually used. This avoids the overhead of building NAMED_STRUCT arrays
       when they're not needed (e.g., when only SIZE(path) is used).

    2. PREDICATE PUSHDOWN:
       Extracts edge predicates from ALL() expressions for pushdown
       into the CTE's WHERE clause, enabling early path elimination.

    Args:
        match_clause: The MATCH clause containing the variable-length pattern
        rel: The variable-length relationship entity (e.g., [:TRANSFER*2..4])
        source_node: The source node of the path
        target_node: The target node of the path
        all_ops: List to collect all created operators
        return_exprs: RETURN clause expressions for path usage analysis
        graph_schema: Graph schema provider for node/edge definitions
        logger: Optional logger for debugging
    """
    # Create data source for source node
    source_ds = DataSourceOperator(entity=source_node)
    all_ops.append(source_ds)

    # Parse edge types from entity_name
    # e.g., "KNOWS|FOLLOWS" -> ["KNOWS", "FOLLOWS"]
    edge_types: list[str] = []
    if rel.entity_name:
        edge_types = [
            t.strip() for t in rel.entity_name.split("|") if t.strip()
        ]

    # Extract source node filter from WHERE clause for optimization
    start_node_filter, remaining_where = extract_source_node_filter(
        match_clause.where_expression,
        source_node.alias,
    )

    # Extract sink (target) node filter for optimization
    # This filter will be applied in the recursive join WHERE clause
    sink_node_filter, remaining_where = extract_source_node_filter(
        remaining_where,
        target_node.alias,
    )

    # Update match_clause with remaining WHERE (without source/sink filters)
    match_clause.where_expression = remaining_where

    # =====================================================================
    # PATH USAGE ANALYSIS
    # =====================================================================
    # Use PathExpressionAnalyzer to determine if edge collection is needed.
    #
    # WHY: Collecting edge properties in the CTE is expensive. We only want
    # to do it when relationships(path) is actually used, for example in:
    #   - ALL(rel IN relationships(path) WHERE rel.amount > 1000)
    #   - REDUCE(sum = 0, r IN relationships(path) | sum + r.amount)
    #   - [r IN relationships(path) | r.timestamp]
    #
    # We DON'T need edge collection for:
    #   - SIZE(path)           -- Only uses path length
    #   - nodes(path)          -- Only uses node IDs (already in path array)
    #   - [n IN nodes(path)]   -- Uses nodes, not relationships
    #
    # This analysis examines both WHERE clause and RETURN expressions.
    # =====================================================================
    path_analyzer = PathExpressionAnalyzer()
    path_info = path_analyzer.analyze(
        path_variable=match_clause.path_variable,
        where_expr=match_clause.where_expression,
        return_exprs=return_exprs,
    )

    # Log analysis result for debugging
    if logger:
        logger.log(
            f"Path analysis for '{match_clause.path_variable}': "
            f"needs_edge_collection={path_info.needs_edge_collection}, "
            f"has_pushable_predicates={path_info.has_pushable_predicates}"
        )

    # =====================================================================
    # REMOVE REDUNDANT PREDICATES
    # =====================================================================
    # If we pushed predicates into the CTE, remove them from the WHERE
    # clause to avoid redundant FORALL evaluations.
    #
    # Example transformation:
    #   BEFORE: WHERE a.x > 1 AND ALL(r IN relationships(path) WHERE r.amt > 1000)
    #   AFTER:  WHERE a.x > 1
    #
    # The ALL predicate is now handled by the CTE's WHERE clause, so the
    # FORALL in the final query is redundant.
    # =====================================================================
    if path_info.pushed_all_expressions:
        match_clause.where_expression = remove_pushed_predicates(
            match_clause.where_expression,
            path_info.pushed_all_expressions,
        )
        if logger:
            logger.log(
                f"Removed {len(path_info.pushed_all_expressions)} pushed predicates "
                f"from WHERE clause"
            )

    # Get node ID columns from schema (supports wildcard for unlabeled nodes)
    source_id_col = "id"  # Default
    target_id_col = "id"  # Default

    if graph_schema:
        source_node_schema = graph_schema.get_node_definition(source_node.entity_name)
        if not source_node_schema and not source_node.entity_name:
            source_node_schema = graph_schema.get_wildcard_node_definition()

        target_node_schema = graph_schema.get_node_definition(target_node.entity_name)
        if not target_node_schema and not target_node.entity_name:
            target_node_schema = graph_schema.get_wildcard_node_definition()

        if source_node_schema and source_node_schema.node_id_property:
            source_id_col = source_node_schema.node_id_property.property_name

        if target_node_schema and target_node_schema.node_id_property:
            target_id_col = target_node_schema.node_id_property.property_name

    # Determine if we need internal UNION ALL for bidirectional traversal.
    # This is a planner decision based on:
    # - Direction is BOTH (undirected pattern like (a)-[:TYPE*]-(b))
    # - EdgeAccessStrategy is EDGE_LIST (edges stored as directed pairs)
    use_internal_union = (
        rel.direction == RelationshipDirection.BOTH
        and graph_schema is not None
        and graph_schema.get_edge_access_strategy() == EdgeAccessStrategy.EDGE_LIST
    )

    # Planner decision: swap source/sink columns for BACKWARD direction.
    swap_source_sink = rel.direction == RelationshipDirection.BACKWARD

    # Create recursive traversal operator with optimized settings
    recursive_op = RecursiveTraversalOperator(
        edge_types=edge_types,
        source_node_type=source_node.entity_name,
        target_node_type=target_node.entity_name,
        min_hops=rel.min_hops if rel.min_hops is not None else 1,
        max_hops=rel.max_hops,
        source_id_column=source_id_col,
        target_id_column=target_id_col,
        start_node_filter=start_node_filter,
        sink_node_filter=sink_node_filter,
        cte_name=f"paths_{rel.alias or 'r'}",
        source_alias=source_node.alias,
        target_alias=target_node.alias,
        path_variable=match_clause.path_variable,
        # Optimization: Only collect edges when relationships(path) is used
        collect_edges=path_info.needs_edge_collection,
        collect_nodes=path_info.needs_node_collection,
        # Predicate pushdown: Filter edges DURING CTE recursion
        edge_filter=path_info.combined_edge_predicate,
        edge_filter_lambda_var=path_info.edge_lambda_variable,
        # Direction for undirected traversal support
        direction=rel.direction,
        # Planner decision: use internal UNION for bidirectional traversal
        use_internal_union_for_bidirectional=use_internal_union,
        # Planner decision: swap source/sink for backward traversal
        swap_source_sink=swap_source_sink,
    )
    recursive_op.add_in_operator(source_ds)
    source_ds.add_out_operator(recursive_op)
    all_ops.append(recursive_op)

    # Create data source for target node
    target_ds = DataSourceOperator(entity=target_node)
    all_ops.append(target_ds)

    # Join recursive result with target node
    join_op = JoinOperator(join_type=JoinType.INNER)
    join_op.add_join_pair(
        JoinKeyPair(
            node_alias=target_node.alias,
            relationship_or_node_alias=recursive_op.cte_name,
            pair_type=JoinKeyPairType.SINK,
        )
    )
    join_op.set_in_operators(recursive_op, target_ds)
    all_ops.append(join_op)

    # Process remaining pattern parts after the VLP
    current_op = _process_remaining_pattern_parts(
        match_clause, rel, target_node, join_op, all_ops
    )

    return current_op


def _process_remaining_pattern_parts(
    match_clause: MatchClause,
    rel: RelationshipEntity,
    target_node: NodeEntity,
    current_op: LogicalOperator,
    all_ops: list[LogicalOperator],
) -> LogicalOperator:
    """Process pattern parts after the variable-length relationship.

    Handles patterns like: (a)-[:KNOWS*1..2]-(b)-[:HAS_LOAN]->(l)
    where we need to continue processing [:HAS_LOAN]->(l) after the recursive path.

    Args:
        match_clause: The full MATCH clause
        rel: The variable-length relationship
        target_node: The target node of the VLP
        current_op: Current operator (join with target)
        all_ops: List to collect created operators

    Returns:
        The final operator after processing remaining parts
    """
    # Find the index of the VLP relationship, then target is at vlp_idx + 1
    vlp_idx = match_clause.pattern_parts.index(rel)
    target_idx = vlp_idx + 1
    remaining_parts = match_clause.pattern_parts[target_idx + 1:]

    if not remaining_parts:
        return current_op

    # First pass: assign aliases and update entity names
    auto_alias_counter = 0
    prev_node: NodeEntity | None = target_node
    for entity in remaining_parts:
        if not entity.alias:
            auto_alias_counter += 1
            entity.alias = f"_anon{auto_alias_counter}"

        if isinstance(entity, NodeEntity):
            prev_node = entity
        elif isinstance(entity, RelationshipEntity) and prev_node:
            entity.left_entity_name = prev_node.entity_name

    # Second pass: update right_entity_name for relationships
    prev_entity: Entity | None = None
    for entity in remaining_parts:
        if isinstance(entity, NodeEntity) and prev_entity:
            if isinstance(prev_entity, RelationshipEntity):
                prev_entity.right_entity_name = entity.entity_name
        prev_entity = entity

    # Third pass: create operators and join them
    prev_node_alias = target_node.alias

    for entity in remaining_parts:
        ds_op = DataSourceOperator(entity=entity)
        all_ops.append(ds_op)

        # Determine join type and create join pairs
        new_join_op = JoinOperator(join_type=JoinType.INNER)

        if isinstance(entity, RelationshipEntity):
            # Join target node to relationship
            if prev_node_alias:
                pair_type = _determine_join_pair_type(entity)
                new_join_op.add_join_pair(JoinKeyPair(
                    node_alias=prev_node_alias,
                    relationship_or_node_alias=entity.alias,
                    pair_type=pair_type,
                ))
        elif isinstance(entity, NodeEntity):
            # Find the previous relationship to join with
            for prev_ent in remaining_parts:
                if isinstance(prev_ent, RelationshipEntity):
                    if prev_ent.right_entity_name == entity.entity_name:
                        pair_type = _determine_sink_join_type(prev_ent)
                        new_join_op.add_join_pair(
                            JoinKeyPair(
                                node_alias=entity.alias,
                                relationship_or_node_alias=prev_ent.alias,
                                pair_type=pair_type,
                            )
                        )
            prev_node_alias = entity.alias

        new_join_op.set_in_operators(current_op, ds_op)
        all_ops.append(new_join_op)
        current_op = new_join_op

    return current_op


def extract_source_node_filter(
    where_expr: QueryExpression | None,
    source_alias: str,
) -> tuple[QueryExpression | None, QueryExpression | None]:
    """Extract filters on a node variable from WHERE clause.

    This optimization pushes filters like `p.name = 'Alice'` into the
    recursive CTE's base case, dramatically reducing the number of paths
    explored.

    Args:
        where_expr: The WHERE clause expression
        source_alias: The node alias to extract filters for

    Returns:
        Tuple of (extracted_filter, remaining_expression)

    Examples:
        - WHERE p.name = 'Alice' -> (p.name = 'Alice', None)
        - WHERE p.name = 'Alice' AND f.age > 25 -> (p.name = 'Alice', f.age > 25)
        - WHERE f.age > 25 -> (None, f.age > 25)
    """
    if not where_expr:
        return None, None

    # Check if the entire expression references only the source node
    if _references_only_variable(where_expr, source_alias):
        return where_expr, None

    # Handle AND: split into source-only and other predicates
    if isinstance(where_expr, QueryExpressionBinary):
        if (where_expr.operator and
                where_expr.operator.name.name == "AND"):
            left = where_expr.left_expression
            right = where_expr.right_expression

            left_is_source = _references_only_variable(left, source_alias)
            right_is_source = _references_only_variable(right, source_alias)

            if left_is_source and right_is_source:
                # Both sides reference only source - return entire expression
                return where_expr, None
            elif left_is_source:
                # Left is source filter, right is remaining
                return left, right
            elif right_is_source:
                # Right is source filter, left is remaining
                return right, left

    return None, where_expr


def _references_only_variable(
    expr: QueryExpression,
    variable: str,
) -> bool:
    """Check if expression references only the specified variable.

    Returns True if ALL property references in the expression use the
    given variable name.

    Examples with variable='p':
        - p.name = 'Alice'              -> True
        - p.age > 30 AND p.active       -> True
        - p.name = f.name               -> False
        - f.age > 25                    -> False
        - 1 = 1                         -> False (no property references)
    """
    properties = _collect_property_references(expr)

    if not properties:
        return False

    return all(prop.variable_name == variable for prop in properties)


def _collect_property_references(
    expr: QueryExpression,
) -> list[QueryExpressionProperty]:
    """Collect all property references from an expression tree."""
    properties: list[QueryExpressionProperty] = []

    if isinstance(expr, QueryExpressionProperty):
        properties.append(expr)
    elif isinstance(expr, QueryExpressionBinary):
        properties.extend(_collect_property_references(expr.left_expression))
        properties.extend(_collect_property_references(expr.right_expression))
    elif hasattr(expr, 'children'):
        for child in expr.children:
            if isinstance(child, QueryExpression):
                properties.extend(_collect_property_references(child))

    return properties


def _determine_join_pair_type(rel: RelationshipEntity) -> JoinKeyPairType:
    """Determine the join pair type for a relationship's source node."""
    if rel.direction == RelationshipDirection.FORWARD:
        return JoinKeyPairType.SOURCE
    elif rel.direction == RelationshipDirection.BACKWARD:
        return JoinKeyPairType.SINK
    else:
        return JoinKeyPairType.EITHER_AS_SOURCE


def _determine_sink_join_type(rel: RelationshipEntity) -> JoinKeyPairType:
    """Determine the join pair type for a relationship's sink node."""
    if rel.direction == RelationshipDirection.FORWARD:
        return JoinKeyPairType.SINK
    elif rel.direction == RelationshipDirection.BACKWARD:
        return JoinKeyPairType.SOURCE
    else:
        return JoinKeyPairType.EITHER_AS_SINK
