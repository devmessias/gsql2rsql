"""Standard match tree building for fixed-length MATCH patterns.

This module handles the creation of logical operator trees for
standard (non-VLP) MATCH patterns like:
    MATCH (a:Person)-[:KNOWS]->(b:Person)
    MATCH (a)-[:R1]->(b), (a)-[:R2]->(c)

It includes:
- Creating DataSourceOperators for nodes and relationships
- Building JoinOperators to connect entities
- Converting inline property filters to WHERE predicates
- Processing partial queries (MATCH + WHERE + RETURN)
"""

from __future__ import annotations

from typing import Callable

from gsql2rsql.common.exceptions import TranspilerInternalErrorException
from gsql2rsql.parser.ast import (
    Entity,
    MatchClause,
    NodeEntity,
    PartialQueryNode,
    QueryExpression,
    QueryExpressionBinary,
    QueryExpressionMapLiteral,
    QueryExpressionProperty,
    QueryExpressionValue,
    RelationshipDirection,
    RelationshipEntity,
)
from gsql2rsql.parser.operators import (
    BinaryOperator,
    BinaryOperatorInfo,
    BinaryOperatorType,
)
from gsql2rsql.planner.operators import (
    DataSourceOperator,
    JoinKeyPair,
    JoinKeyPairType,
    JoinOperator,
    JoinType,
    LogicalOperator,
    ProjectionOperator,
    SelectionOperator,
    UnwindOperator,
)


def _extract_int_value(expr: QueryExpression | None) -> int | None:
    """Safely extract integer value from a QueryExpressionValue."""
    if isinstance(expr, QueryExpressionValue) and expr.value is not None:
        return int(expr.value)
    return None


def create_standard_match_tree(
    match_clause: MatchClause,
    all_ops: list[LogicalOperator],
) -> LogicalOperator:
    """Create standard logical tree for fixed-length MATCH clause.

    Note: Auto-alias assignment and inline property conversion should
    be done before calling this function.

    Args:
        match_clause: The MATCH clause to process
        all_ops: List to collect created operators

    Returns:
        The root operator of the match tree
    """
    # Create data source operators for each entity
    entity_ops: dict[str, DataSourceOperator] = {}
    prev_node: NodeEntity | None = None
    seen_node_aliases: set[str] = set()

    for entity in match_clause.pattern_parts:
        # Check if this entity alias was already seen (shared variable)
        if entity.alias in entity_ops:
            if isinstance(entity, NodeEntity):
                prev_node = entity
            continue

        ds_op = DataSourceOperator(entity=entity)
        entity_ops[entity.alias] = ds_op
        all_ops.append(ds_op)

        # Track nodes for relationship connections
        if isinstance(entity, NodeEntity):
            prev_node = entity
            seen_node_aliases.add(entity.alias)
        elif isinstance(entity, RelationshipEntity) and prev_node:
            entity.left_entity_name = prev_node.entity_name

    # Update right entity names for relationships
    prev_entity: Entity | None = None
    for entity in match_clause.pattern_parts:
        if isinstance(entity, NodeEntity) and prev_entity:
            if isinstance(prev_entity, RelationshipEntity):
                prev_entity.right_entity_name = entity.entity_name
        prev_entity = entity

    # Join all entities together
    current_op: LogicalOperator | None = None
    prev_node_alias: str | None = None
    joined_node_aliases: set[str] = set()

    for entity_idx, entity in enumerate(match_clause.pattern_parts):
        # Handle shared node variables
        if isinstance(entity, NodeEntity) and entity.alias in joined_node_aliases:
            prev_node_alias = entity.alias
            continue

        ds_op = entity_ops[entity.alias]

        if current_op is None:
            current_op = ds_op
            if isinstance(entity, NodeEntity):
                prev_node_alias = entity.alias
                joined_node_aliases.add(entity.alias)
        else:
            join_op = JoinOperator(join_type=JoinType.INNER)

            if isinstance(entity, RelationshipEntity):
                if prev_node_alias:
                    pair_type = determine_join_pair_type(entity)
                    join_op.add_join_pair(JoinKeyPair(
                        node_alias=prev_node_alias,
                        relationship_or_node_alias=entity.alias,
                        pair_type=pair_type,
                    ))
            elif isinstance(entity, NodeEntity):
                # Join node to its immediately preceding relationship
                if entity_idx > 0:
                    prev_ent = match_clause.pattern_parts[entity_idx - 1]
                    if isinstance(prev_ent, RelationshipEntity):
                        pair_type = determine_sink_join_type(prev_ent)
                        join_op.add_join_pair(
                            JoinKeyPair(
                                node_alias=entity.alias,
                                relationship_or_node_alias=prev_ent.alias,
                                pair_type=pair_type,
                            )
                        )
                prev_node_alias = entity.alias
                joined_node_aliases.add(entity.alias)

            join_op.set_in_operators(current_op, ds_op)
            all_ops.append(join_op)
            current_op = join_op

    if current_op is None:
        raise TranspilerInternalErrorException("Empty match clause")

    return current_op


def create_partial_query_tree(
    part: PartialQueryNode,
    all_ops: list[LogicalOperator],
    previous_op: LogicalOperator | None,
    create_match_tree_fn: Callable[
        [MatchClause, list[LogicalOperator], list[QueryExpression] | None],
        LogicalOperator
    ],
) -> LogicalOperator:
    """Create logical tree for a partial query (MATCH...RETURN).

    Args:
        part: The partial query node
        all_ops: List to collect created operators
        previous_op: Upstream operator (if any)
        create_match_tree_fn: Function to create match trees

    Returns:
        The root operator of the partial query tree
    """
    current_op = previous_op

    # Collect return expressions for path analysis optimization
    return_exprs = [
        ret.inner_expression for ret in part.return_body
    ] if part.return_body else []

    # Track node aliases seen across all match clauses for correlation
    seen_node_aliases: set[str] = set()

    # Process MATCH clauses
    for match_clause in part.match_clauses:
        match_op = create_match_tree_fn(match_clause, all_ops, return_exprs)

        if current_op is not None:
            join_type = JoinType.LEFT if match_clause.is_optional else JoinType.INNER
            join_op = JoinOperator(join_type=join_type)
            join_op.set_in_operators(current_op, match_op)

            # Add join pairs for shared node variables (correlation)
            for entity in match_clause.pattern_parts:
                if isinstance(entity, NodeEntity) and entity.alias in seen_node_aliases:
                    join_op.add_join_pair(JoinKeyPair(
                        node_alias=entity.alias,
                        relationship_or_node_alias=entity.alias,
                        pair_type=JoinKeyPairType.NODE_ID,
                    ))

            all_ops.append(join_op)
            current_op = join_op
        else:
            current_op = match_op

        # Track all node aliases from this match clause
        for entity in match_clause.pattern_parts:
            if isinstance(entity, NodeEntity) and entity.alias:
                seen_node_aliases.add(entity.alias)

        # Process WHERE clause from MatchClause
        if match_clause.where_expression and current_op:
            select_op = SelectionOperator(
                filter_expression=match_clause.where_expression
            )
            select_op.set_in_operator(current_op)
            all_ops.append(select_op)
            current_op = select_op

    # Process UNWIND clauses
    for unwind_clause in part.unwind_clauses:
        if current_op:
            unwind_op = UnwindOperator(
                list_expression=unwind_clause.list_expression,
                variable_name=unwind_clause.variable_name,
            )
            unwind_op.set_in_operator(current_op)
            all_ops.append(unwind_op)
            current_op = unwind_op

    # Process WHERE clause from PartialQueryNode (for compatibility)
    if part.where_expression and current_op:
        select_op = SelectionOperator(filter_expression=part.where_expression)
        select_op.set_in_operator(current_op)
        all_ops.append(select_op)
        current_op = select_op

    # Process RETURN clause
    if part.return_body and current_op:
        proj_op = ProjectionOperator(
            projections=[
                (ret.alias, ret.inner_expression) for ret in part.return_body
            ],
            is_distinct=part.is_distinct,
            order_by=[
                (item.expression, item.order.name == "DESC")
                for item in part.order_by
            ],
            limit=(
                _extract_int_value(part.limit_clause.limit_expression)
                if part.limit_clause else None
            ),
            skip=(
                _extract_int_value(part.limit_clause.skip_expression)
                if part.limit_clause and part.limit_clause.skip_expression else None
            ),
            having_expression=part.having_expression,
        )
        proj_op.set_in_operator(current_op)
        all_ops.append(proj_op)
        current_op = proj_op

    if current_op is None:
        raise TranspilerInternalErrorException("Empty partial query")

    return current_op


def convert_inline_properties_to_where(
    match_clause: MatchClause,
) -> QueryExpression | None:
    """Convert inline property filters to WHERE predicates.

    Extracts inline properties from all entities in the MATCH pattern
    and converts them to equality predicates combined with AND.

    IMPORTANT: Currently only supports LITERAL values (strings, numbers,
    booleans, null). Variable references (e.g., {id: variable}) are
    skipped to avoid issues with UNWIND variable scoping.

    Example:
        MATCH (a:Person {name: 'Alice', age: 30})-[r:KNOWS {since: 2020}]->(b)
        Produces:
        a.name = 'Alice' AND a.age = 30 AND r.since = 2020

    Args:
        match_clause: The MATCH clause containing pattern entities

    Returns:
        QueryExpression combining all inline property filters, or None
        if no inline properties are present
    """
    predicates: list[QueryExpression] = []

    for entity in match_clause.pattern_parts:
        # Only NodeEntity and RelationshipEntity have inline_properties
        if not isinstance(entity, (NodeEntity, RelationshipEntity)):
            continue

        inline_props = entity.inline_properties
        if not inline_props or not isinstance(inline_props, QueryExpressionMapLiteral):
            continue

        for prop_name, prop_value in inline_props.entries:
            # Skip variable references for now
            if isinstance(prop_value, QueryExpressionProperty):
                continue

            predicate = QueryExpressionBinary(
                left_expression=QueryExpressionProperty(
                    variable_name=entity.alias, property_name=prop_name
                ),
                right_expression=prop_value,
                operator=BinaryOperatorInfo(
                    BinaryOperator.EQ, BinaryOperatorType.COMPARISON
                ),
            )
            predicates.append(predicate)

    if not predicates:
        return None

    if len(predicates) == 1:
        return predicates[0]

    # Combine all predicates with AND
    and_op = BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL)
    result = predicates[0]
    for pred in predicates[1:]:
        result = QueryExpressionBinary(
            left_expression=result,
            right_expression=pred,
            operator=and_op,
        )

    return result


def determine_join_pair_type(rel: RelationshipEntity) -> JoinKeyPairType:
    """Determine the join pair type for a relationship's source node.

    For undirected relationships, returns EITHER_AS_SOURCE to signal that:
    1. This is an undirected relationship (needs UNION ALL expansion)
    2. This node is on the source side
    """
    if rel.direction == RelationshipDirection.FORWARD:
        return JoinKeyPairType.SOURCE
    elif rel.direction == RelationshipDirection.BACKWARD:
        return JoinKeyPairType.SINK
    else:
        return JoinKeyPairType.EITHER_AS_SOURCE


def determine_sink_join_type(rel: RelationshipEntity) -> JoinKeyPairType:
    """Determine the join pair type for a relationship's sink node.

    For undirected relationships, returns EITHER_AS_SINK to signal that:
    1. This is an undirected relationship (needs UNION ALL expansion)
    2. This node is on the sink side
    """
    if rel.direction == RelationshipDirection.FORWARD:
        return JoinKeyPairType.SINK
    elif rel.direction == RelationshipDirection.BACKWARD:
        return JoinKeyPairType.SOURCE
    else:
        return JoinKeyPairType.EITHER_AS_SINK
