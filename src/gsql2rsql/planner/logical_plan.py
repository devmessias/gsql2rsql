"""Logical plan creation from AST."""

from __future__ import annotations

from typing import Any

from gsql2rsql.common.exceptions import (
    TranspilerBindingException,
    TranspilerInternalErrorException,
)
from gsql2rsql.common.logging import ILoggable
from gsql2rsql.common.schema import IGraphSchemaProvider
from gsql2rsql.common.utils import change_indentation
from gsql2rsql.parser.ast import (
    Entity,
    InfixOperator,
    InfixQueryNode,
    MatchClause,
    NodeEntity,
    PartialQueryNode,
    QueryExpression,
    QueryExpressionBinary,
    QueryExpressionProperty,
    QueryExpressionValue,
    QueryExpressionWithAlias,
    QueryNode,
    RelationshipDirection,
    RelationshipEntity,
    SingleQueryNode,
    UnwindClause,
)
from gsql2rsql.planner.operators import (
    DataSourceOperator,
    IBindable,
    JoinKeyPair,
    JoinKeyPairType,
    JoinOperator,
    JoinType,
    LogicalOperator,
    ProjectionOperator,
    RecursiveTraversalOperator,
    SelectionOperator,
    SetOperationType,
    SetOperator,
    StartLogicalOperator,
    UnwindOperator,
)
from gsql2rsql.planner.schema import EntityField, EntityType, Schema


class LogicalPlan:
    """
    Creates a logical plan from an AST.

    The logical plan transforms the abstract syntax tree into a relational
    query logical plan similar to Relational Algebra.
    """

    def __init__(self, logger: ILoggable | None = None) -> None:
        self._logger = logger
        self._starting_operators: list[StartLogicalOperator] = []
        self._terminal_operators: list[LogicalOperator] = []

    @property
    def starting_operators(self) -> list[StartLogicalOperator]:
        """Return operators that are starting points of the logical plan."""
        return self._starting_operators

    @property
    def terminal_operators(self) -> list[LogicalOperator]:
        """Return operators that are terminals (representing output)."""
        return self._terminal_operators

    @classmethod
    def process_query_tree(
        cls,
        tree_root: QueryNode,
        graph_def: IGraphSchemaProvider,
        logger: ILoggable | None = None,
    ) -> LogicalPlan:
        """
        Create a LogicalPlan from a query AST.

        Args:
            tree_root: The root of the AST.
            graph_def: The graph schema provider.
            logger: Optional logger.

        Returns:
            A LogicalPlan instance.
        """
        planner = cls(logger)
        all_logical_ops: list[LogicalOperator] = []

        # Resolve entity names for nodes referenced without labels
        planner._resolve_entity_names(tree_root)

        # Create the logical tree from AST
        logical_root = planner._create_logical_tree(tree_root, all_logical_ops)

        # Collect starting and terminal operators
        planner._starting_operators = list(
            logical_root.get_all_upstream_operators(StartLogicalOperator)
        )
        planner._terminal_operators = [logical_root]

        # Assign debug IDs
        for i, op in enumerate(all_logical_ops, 1):
            op.operator_debug_id = i

        # Bind to graph schema
        for op in planner._starting_operators:
            if isinstance(op, IBindable):
                op.bind(graph_def)

        # Propagate data types
        planner._propagate_data_types()

        return planner

    def _resolve_entity_names(self, tree_node: QueryNode) -> None:
        """
        Resolve entity names for nodes that are referenced without labels.

        When a node is defined with a label in one MATCH clause (e.g., (d:device))
        and referenced without a label in another MATCH clause (e.g., (d)),
        we need to propagate the entity_name from the definition to the reference.
        """
        # Build a map of alias -> entity_name from all definitions
        alias_to_entity_name: dict[str, str] = {}

        def collect_from_node(node: QueryNode) -> None:
            if isinstance(node, SingleQueryNode):
                for part in node.parts:
                    for match_clause in part.match_clauses:
                        for entity in match_clause.pattern_parts:
                            if entity.alias and entity.entity_name:
                                # Record the entity name for this alias
                                if entity.alias not in alias_to_entity_name:
                                    alias_to_entity_name[entity.alias] = entity.entity_name
            elif isinstance(node, InfixQueryNode):
                collect_from_node(node.left_query)
                collect_from_node(node.right_query)

        def apply_to_node(node: QueryNode) -> None:
            if isinstance(node, SingleQueryNode):
                for part in node.parts:
                    for match_clause in part.match_clauses:
                        for entity in match_clause.pattern_parts:
                            if entity.alias and not entity.entity_name:
                                # Look up the entity name from a previous definition
                                if entity.alias in alias_to_entity_name:
                                    entity.entity_name = alias_to_entity_name[entity.alias]
            elif isinstance(node, InfixQueryNode):
                apply_to_node(node.left_query)
                apply_to_node(node.right_query)

        # First collect all alias -> entity_name mappings
        collect_from_node(tree_node)
        # Then apply them to nodes without entity_name
        apply_to_node(tree_node)

    def dump_graph(self) -> str:
        """Dump textual format of the logical plan."""
        lines: list[str] = []

        # Collect all operators grouped by depth
        all_ops: dict[int, list[LogicalOperator]] = {}
        for start_op in self._starting_operators:
            for op in start_op.get_all_downstream_operators(LogicalOperator):
                depth = op.depth
                if depth not in all_ops:
                    all_ops[depth] = []
                if op not in all_ops[depth]:
                    all_ops[depth].append(op)

        for depth in sorted(all_ops.keys()):
            lines.append(f"Level {depth}:")
            lines.append("-" * 70)
            for op in all_ops[depth]:
                in_ids = ",".join(str(o.operator_debug_id) for o in op.in_operators)
                out_ids = ",".join(str(o.operator_debug_id) for o in op.out_operators)
                lines.append(
                    f"OpId={op.operator_debug_id} Op={op.__class__.__name__}; "
                    f"InOpIds={in_ids}; OutOpIds={out_ids};"
                )
                lines.append(change_indentation(str(op), 1))
                lines.append("*")
            lines.append("-" * 70)

        return "\n".join(lines)

    def _create_logical_tree(
        self, tree_node: QueryNode, all_ops: list[LogicalOperator]
    ) -> LogicalOperator:
        """Create logical operator tree from a query AST node."""
        if isinstance(tree_node, SingleQueryNode):
            return self._create_single_query_tree(tree_node, all_ops)
        elif isinstance(tree_node, InfixQueryNode):
            return self._create_infix_query_tree(tree_node, all_ops)
        else:
            raise TranspilerInternalErrorException(
                f"Unknown query node type: {type(tree_node)}"
            )

    def _create_single_query_tree(
        self, query_node: SingleQueryNode, all_ops: list[LogicalOperator]
    ) -> LogicalOperator:
        """Create logical tree for a single query."""
        current_op: LogicalOperator | None = None

        for part in query_node.parts:
            part_op = self._create_partial_query_tree(part, all_ops, current_op)
            current_op = part_op

        if current_op is None:
            raise TranspilerInternalErrorException("Empty query")

        return current_op

    def _create_partial_query_tree(
        self,
        part: PartialQueryNode,
        all_ops: list[LogicalOperator],
        previous_op: LogicalOperator | None,
    ) -> LogicalOperator:
        """Create logical tree for a partial query (MATCH...RETURN)."""
        current_op = previous_op

        # Process MATCH clauses
        for match_clause in part.match_clauses:
            match_op = self._create_match_tree(match_clause, all_ops)

            if current_op is not None:
                # Join with previous result
                join_type = JoinType.LEFT if match_clause.is_optional else JoinType.INNER
                join_op = JoinOperator(join_type=join_type)
                join_op.set_in_operators(current_op, match_op)
                all_ops.append(join_op)
                current_op = join_op
            else:
                current_op = match_op

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
                    int(part.limit_clause.limit_expression.value)
                    if part.limit_clause
                    and hasattr(part.limit_clause.limit_expression, "value")
                    and part.limit_clause.limit_expression.value is not None
                    else None
                ),
                skip=(
                    int(part.limit_clause.skip_expression.value)
                    if part.limit_clause
                    and part.limit_clause.skip_expression
                    and hasattr(part.limit_clause.skip_expression, "value")
                    and part.limit_clause.skip_expression.value is not None
                    else None
                ),
                # HAVING expression from WITH ... WHERE on aggregated columns
                having_expression=part.having_expression,
            )
            proj_op.set_in_operator(current_op)
            all_ops.append(proj_op)
            current_op = proj_op

        if current_op is None:
            raise TranspilerInternalErrorException("Empty partial query")

        return current_op

    def _create_match_tree(
        self, match_clause: MatchClause, all_ops: list[LogicalOperator]
    ) -> LogicalOperator:
        """Create logical tree for a MATCH clause."""
        # Check for variable-length relationships
        var_length_rel = None
        source_node = None
        target_node = None

        for i, entity in enumerate(match_clause.pattern_parts):
            if isinstance(entity, RelationshipEntity):
                if entity.is_variable_length:
                    var_length_rel = entity
                    # Find source and target nodes
                    if i > 0:
                        prev = match_clause.pattern_parts[i - 1]
                        if isinstance(prev, NodeEntity):
                            source_node = prev
                    if i < len(match_clause.pattern_parts) - 1:
                        next_e = match_clause.pattern_parts[i + 1]
                        if isinstance(next_e, NodeEntity):
                            target_node = next_e

        # If we have a variable-length relationship, create recursive op
        if var_length_rel and source_node and target_node:
            return self._create_recursive_match_tree(
                match_clause,
                var_length_rel,
                source_node,
                target_node,
                all_ops,
            )

        # Standard match tree creation for fixed-length relationships
        return self._create_standard_match_tree(match_clause, all_ops)

    def _create_recursive_match_tree(
        self,
        match_clause: MatchClause,
        rel: RelationshipEntity,
        source_node: NodeEntity,
        target_node: NodeEntity,
        all_ops: list[LogicalOperator],
    ) -> LogicalOperator:
        """Create logical tree for variable-length path traversal."""
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
        start_node_filter, remaining_where = self._extract_source_node_filter(
            match_clause.where_expression,
            source_node.alias,
        )

        # Update match_clause with remaining WHERE (without source filter)
        match_clause.where_expression = remaining_where

        # Create recursive traversal operator
        # Pass path_variable to enable edge property collection for relationships(path)
        recursive_op = RecursiveTraversalOperator(
            edge_types=edge_types,
            source_node_type=source_node.entity_name,
            target_node_type=target_node.entity_name,
            min_hops=rel.min_hops if rel.min_hops is not None else 1,
            max_hops=rel.max_hops,
            source_id_column="id",  # Default, could be from schema
            target_id_column="id",  # Default, could be from schema
            start_node_filter=start_node_filter,
            cte_name=f"paths_{rel.alias or 'r'}",
            source_alias=source_node.alias,
            target_alias=target_node.alias,
            path_variable=match_clause.path_variable,  # Enable edge collection if path is named
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

        return join_op

    def _extract_source_node_filter(
        self,
        where_expr: QueryExpression | None,
        source_alias: str,
    ) -> tuple[str | None, QueryExpression | None]:
        """
        Extract simple equality filter on source node's id from WHERE clause.

        Returns (filter_value, remaining_expression).
        For example, WHERE p.id = 1 AND f.age > 25 returns ("1", f.age > 25).
        """
        if not where_expr:
            return None, None

        # Handle simple equality: p.id = value
        if isinstance(where_expr, QueryExpressionBinary):
            if self._is_source_id_equality(where_expr, source_alias):
                filter_val = self._extract_filter_value(where_expr)
                return filter_val, None

            # Handle AND: check if one side is source filter
            if (where_expr.operator and
                    where_expr.operator.name.name == "AND"):
                left = where_expr.left_expression
                right = where_expr.right_expression

                # Check left side
                if isinstance(left, QueryExpressionBinary):
                    if self._is_source_id_equality(left, source_alias):
                        filter_val = self._extract_filter_value(left)
                        return filter_val, right

                # Check right side
                if isinstance(right, QueryExpressionBinary):
                    if self._is_source_id_equality(right, source_alias):
                        filter_val = self._extract_filter_value(right)
                        return filter_val, left

        return None, where_expr

    def _is_source_id_equality(
        self,
        expr: QueryExpressionBinary,
        source_alias: str,
    ) -> bool:
        """Check if expression is source_alias.id = value."""
        if not expr.operator or expr.operator.name.name != "EQ":
            return False

        left = expr.left_expression
        right = expr.right_expression

        # Check left side is source.id
        if isinstance(left, QueryExpressionProperty):
            if (left.variable_name == source_alias and
                    left.property_name == "id"):
                return isinstance(right, QueryExpressionValue)

        # Check right side is source.id (reversed: value = source.id)
        if isinstance(right, QueryExpressionProperty):
            if (right.variable_name == source_alias and
                    right.property_name == "id"):
                return isinstance(left, QueryExpressionValue)

        return False

    def _extract_filter_value(self, expr: QueryExpressionBinary) -> str | None:
        """Extract the literal value from an equality expression."""
        left = expr.left_expression
        right = expr.right_expression

        if isinstance(right, QueryExpressionValue):
            return str(right.value)
        if isinstance(left, QueryExpressionValue):
            return str(left.value)
        return None

    def _create_standard_match_tree(
        self, match_clause: MatchClause, all_ops: list[LogicalOperator]
    ) -> LogicalOperator:
        """Create standard logical tree for fixed-length MATCH clause."""
        # Create data source operators for each entity
        # First, assign auto-generated aliases for entities without explicit aliases
        auto_alias_counter = 0
        for entity in match_clause.pattern_parts:
            if not entity.alias:
                auto_alias_counter += 1
                entity.alias = f"_anon{auto_alias_counter}"

        entity_ops: dict[str, DataSourceOperator] = {}
        prev_node: NodeEntity | None = None

        for entity in match_clause.pattern_parts:
            ds_op = DataSourceOperator(entity=entity)
            entity_ops[entity.alias] = ds_op
            all_ops.append(ds_op)

            # Track nodes for relationship connections
            if isinstance(entity, NodeEntity):
                prev_node = entity
            elif isinstance(entity, RelationshipEntity) and prev_node:
                # Update relationship's left entity name
                entity.left_entity_name = prev_node.entity_name

        # Now update right entity names for relationships
        prev_entity: Entity | None = None
        for entity in match_clause.pattern_parts:
            if isinstance(entity, NodeEntity) and prev_entity:
                if isinstance(prev_entity, RelationshipEntity):
                    prev_entity.right_entity_name = entity.entity_name
            prev_entity = entity

        # Join all entities together
        current_op: LogicalOperator | None = None
        prev_node_alias: str | None = None

        for entity in match_clause.pattern_parts:
            ds_op = entity_ops[entity.alias]

            if current_op is None:
                current_op = ds_op
                if isinstance(entity, NodeEntity):
                    prev_node_alias = entity.alias
            else:
                # Determine join type and create join pairs
                join_op = JoinOperator(join_type=JoinType.INNER)

                if isinstance(entity, RelationshipEntity):
                    # Join node to relationship
                    if prev_node_alias:
                        pair_type = self._determine_join_pair_type(entity)
                        join_op.add_join_pair(JoinKeyPair(
                            node_alias=prev_node_alias,
                            relationship_or_node_alias=entity.alias,
                            pair_type=pair_type,
                        ))
                elif isinstance(entity, NodeEntity):
                    # Find the previous relationship to join with
                    for prev_ent in match_clause.pattern_parts:
                        if isinstance(prev_ent, RelationshipEntity):
                            if prev_ent.right_entity_name == entity.entity_name:
                                pair_type = self._determine_sink_join_type(prev_ent)
                                join_op.add_join_pair(
                                    JoinKeyPair(
                                        node_alias=entity.alias,
                                        relationship_or_node_alias=prev_ent.alias,
                                        pair_type=pair_type,
                                    )
                                )
                    prev_node_alias = entity.alias

                join_op.set_in_operators(current_op, ds_op)
                all_ops.append(join_op)
                current_op = join_op

        if current_op is None:
            raise TranspilerInternalErrorException("Empty match clause")

        return current_op

    def _determine_join_pair_type(self, rel: RelationshipEntity) -> JoinKeyPairType:
        """Determine the join pair type for a relationship's source."""
        if rel.direction == RelationshipDirection.FORWARD:
            return JoinKeyPairType.SOURCE
        elif rel.direction == RelationshipDirection.BACKWARD:
            return JoinKeyPairType.SINK
        else:
            return JoinKeyPairType.EITHER

    def _determine_sink_join_type(self, rel: RelationshipEntity) -> JoinKeyPairType:
        """Determine the join pair type for a relationship's sink."""
        if rel.direction == RelationshipDirection.FORWARD:
            return JoinKeyPairType.SINK
        elif rel.direction == RelationshipDirection.BACKWARD:
            return JoinKeyPairType.SOURCE
        else:
            return JoinKeyPairType.EITHER

    def _create_infix_query_tree(
        self, query_node: InfixQueryNode, all_ops: list[LogicalOperator]
    ) -> LogicalOperator:
        """Create logical tree for a UNION query."""
        left_op = self._create_logical_tree(query_node.left_query, all_ops)
        right_op = self._create_logical_tree(query_node.right_query, all_ops)

        set_op_type = (
            SetOperationType.UNION_ALL
            if query_node.operator == InfixOperator.UNION_ALL
            else SetOperationType.UNION
        )

        set_op = SetOperator(set_operation=set_op_type)
        set_op.set_in_operators(left_op, right_op)
        all_ops.append(set_op)

        return set_op

    def _propagate_data_types(self) -> None:
        """Propagate data types through the logical plan."""
        # First propagate from starting operators downward
        for start_op in self._starting_operators:
            self._propagate_down(start_op)

    def _propagate_down(self, op: LogicalOperator) -> None:
        """Propagate data types down from an operator."""
        op.propagate_data_types_for_in_schema()
        op.propagate_data_types_for_out_schema()

        for out_op in op.out_operators:
            self._propagate_down(out_op)
