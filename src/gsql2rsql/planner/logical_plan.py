"""Logical plan creation from AST."""

from __future__ import annotations

from typing import Any

from gsql2rsql.common.exceptions import (
    TranspilerBindingException,
    TranspilerInternalErrorException,
    UnsupportedQueryPatternError,
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
    QueryExpressionAggregationFunction,
    QueryExpressionBinary,
    QueryExpressionFunction,
    QueryExpressionMapLiteral,
    QueryExpressionProperty,
    QueryExpressionValue,
    QueryExpressionWithAlias,
    QueryNode,
    RelationshipDirection,
    RelationshipEntity,
    SingleQueryNode,
    UnwindClause,
)
from gsql2rsql.parser.operators import (
    BinaryOperator,
    BinaryOperatorInfo,
    BinaryOperatorType,
)
from gsql2rsql.planner.operators import (
    AggregationBoundaryOperator,
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
from gsql2rsql.planner.path_analyzer import (
    PathExpressionAnalyzer,
    remove_pushed_predicates,
)
from gsql2rsql.planner.schema import EntityField, EntityType, Schema

# Import resolution types for optional column resolution
from gsql2rsql.planner.column_resolver import ColumnResolver, ResolutionResult
from gsql2rsql.planner.symbol_table import SymbolTable


class LogicalPlan:
    """
    Creates a logical plan from an AST.

    The logical plan transforms the abstract syntax tree into a relational
    query logical plan similar to Relational Algebra.

    After creation, call resolve() to perform column resolution which:
    - Builds a symbol table with all variable definitions
    - Validates all column references
    - Creates resolved references for use during rendering
    """

    def __init__(self, logger: ILoggable | None = None) -> None:
        self._logger = logger
        self._starting_operators: list[StartLogicalOperator] = []
        self._terminal_operators: list[LogicalOperator] = []
        self._resolution_result: ResolutionResult | None = None
        self._original_query: str = ""
        self._graph_schema: IGraphSchemaProvider | None = None

    @property
    def starting_operators(self) -> list[StartLogicalOperator]:
        """Return operators that are starting points of the logical plan."""
        return self._starting_operators

    @property
    def terminal_operators(self) -> list[LogicalOperator]:
        """Return operators that are terminals (representing output)."""
        return self._terminal_operators

    @property
    def resolution_result(self) -> ResolutionResult | None:
        """Return the column resolution result, if resolution was performed.

        Returns None if resolve() has not been called.
        """
        return self._resolution_result

    @property
    def is_resolved(self) -> bool:
        """Check if column resolution has been performed."""
        return self._resolution_result is not None

    @property
    def graph_schema(self) -> IGraphSchemaProvider | None:
        """Return the graph schema provider used to create this plan."""
        return self._graph_schema

    def resolve(self, original_query: str = "") -> ResolutionResult:
        """Perform column resolution on this plan.

        This validates all column references and creates ResolvedColumnRef
        objects for use during rendering. Should be called after process_query_tree.

        Args:
            original_query: The original Cypher query text (for error messages)

        Returns:
            ResolutionResult containing resolved references

        Raises:
            ColumnResolutionError: If any reference cannot be resolved
        """
        self._original_query = original_query
        resolver = ColumnResolver()
        self._resolution_result = resolver.resolve(self, original_query)
        return self._resolution_result

    def all_operators(self) -> list[LogicalOperator]:
        """Get all operators in the plan.

        Returns:
            List of all LogicalOperator instances in the plan
        """
        result: list[LogicalOperator] = []
        visited: set[int] = set()

        def visit(op: LogicalOperator) -> None:
            op_id = id(op)
            if op_id in visited:
                return
            visited.add(op_id)
            result.append(op)
            for out_op in op._out_operators:
                visit(out_op)

        for start_op in self._starting_operators:
            visit(start_op)

        return result

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
        planner._graph_schema = graph_def
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
        """Create logical tree for a single query.

        MATCH after aggregating WITH Support
        ====================================
        This method now supports queries where MATCH clauses follow a WITH clause
        that contains aggregation. When this pattern is detected:

        1. The aggregating WITH creates an AggregationBoundaryOperator
        2. This boundary materializes the aggregated result as a CTE
        3. Subsequent MATCHes join with the CTE using projected entity IDs

        Example:
            MATCH (a)-[:R1]->(b)
            WITH a, COUNT(b) AS cnt    -- Creates AggregationBoundaryOperator
            MATCH (a)-[:R2]->(c)       -- Joins with boundary on a.id
            RETURN a, cnt, COUNT(c)

        See: docs/development/TODO_MATCH_AFTER_AGGREGATING_WITH.md
        """
        current_op: LogicalOperator | None = None

        # Track whether we're past an aggregation boundary
        # and which variables were projected through it
        aggregation_boundary: AggregationBoundaryOperator | None = None

        for part_idx, part in enumerate(query_node.parts):
            # Check if this part creates an aggregation boundary
            # (has aggregation AND there's a subsequent part with MATCH)
            creates_boundary = self._part_creates_aggregation_boundary(
                query_node, part_idx
            )

            if aggregation_boundary is not None and part.match_clauses:
                # This is a MATCH after an aggregation boundary
                # Build the match tree and join it with the boundary
                current_op = self._create_match_after_boundary_tree(
                    part, aggregation_boundary, all_ops
                )
                # Reset boundary - only applies to immediately following part
                # (for chained boundaries, a new one will be created)
                aggregation_boundary = None
            elif creates_boundary:
                # This part creates an aggregation boundary
                # First, build the match tree for this part (without projections)
                match_op = self._create_match_tree_for_boundary(
                    part, all_ops, current_op
                )
                # Then create the aggregation boundary operator
                aggregation_boundary = self._create_aggregation_boundary(
                    part, match_op, all_ops
                )
                current_op = aggregation_boundary
            else:
                # Normal processing
                part_op = self._create_partial_query_tree(part, all_ops, current_op)
                current_op = part_op

        if current_op is None:
            raise TranspilerInternalErrorException("Empty query")

        return current_op

    def _create_match_tree_for_boundary(
        self,
        part: PartialQueryNode,
        all_ops: list[LogicalOperator],
        previous_op: LogicalOperator | None,
    ) -> LogicalOperator:
        """Create the match tree for a part that creates an aggregation boundary.

        This is similar to _create_partial_query_tree but stops before creating
        the projection - the projection will be handled by the boundary operator.

        Args:
            part: The partial query containing MATCH clauses
            all_ops: List to collect created operators
            previous_op: The upstream operator (if any)

        Returns:
            The operator representing the match tree (before aggregation)
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
            match_op = self._create_match_tree(match_clause, all_ops, return_exprs)

            if current_op is not None:
                # Join with previous result
                join_type = (
                    JoinType.LEFT if match_clause.is_optional else JoinType.INNER
                )
                join_op = JoinOperator(join_type=join_type)
                join_op.set_in_operators(current_op, match_op)

                # Add join pairs for shared node variables (correlation)
                for entity in match_clause.pattern_parts:
                    if (isinstance(entity, NodeEntity) and
                            entity.alias in seen_node_aliases):
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

        # NOTE: We do NOT create a ProjectionOperator here
        # The aggregation boundary will handle the projection

        if current_op is None:
            raise TranspilerInternalErrorException("Empty match tree for boundary")

        return current_op

    def _part_creates_aggregation_boundary(
        self, query_node: SingleQueryNode, part_idx: int
    ) -> bool:
        """Check if a partial query creates an aggregation boundary.

        A boundary is created when:
        1. The part has aggregation in its return_body (WITH clause)
        2. There is a subsequent part with MATCH clauses

        Args:
            query_node: The full query
            part_idx: Index of the part to check

        Returns:
            True if this part should create an aggregation boundary
        """
        part = query_node.parts[part_idx]

        # Must have aggregation
        if not self._has_aggregation_in_projections(part):
            return False

        # Must have a subsequent part with MATCH
        for subsequent_part in query_node.parts[part_idx + 1:]:
            if subsequent_part.match_clauses:
                return True

        return False

    def _create_aggregation_boundary(
        self,
        part: PartialQueryNode,
        input_op: LogicalOperator,
        all_ops: list[LogicalOperator],
    ) -> AggregationBoundaryOperator:
        """Create an AggregationBoundaryOperator from a WITH clause.

        This extracts group keys and aggregates from the WITH clause's
        return_body and creates a boundary operator that can be rendered
        as a CTE.

        Args:
            part: The partial query containing the aggregating WITH
            input_op: The upstream operator (match tree)
            all_ops: List to collect created operators

        Returns:
            The created AggregationBoundaryOperator
        """
        group_keys: list[tuple[str, QueryExpression]] = []
        aggregates: list[tuple[str, QueryExpression]] = []
        projected_variables: set[str] = set()

        # Separate group keys from aggregates
        for ret in part.return_body:
            alias = ret.alias
            expr = ret.inner_expression

            if self._has_aggregation_in_expression(expr):
                aggregates.append((alias, expr))
            else:
                group_keys.append((alias, expr))

                # Track projected entity variables for later joins
                # If the expression is just a variable reference (e.g., "a"),
                # we need it for joining subsequent MATCHes
                if isinstance(expr, QueryExpressionProperty):
                    if expr.property_name is None:
                        # Just a variable reference like "a" (not "a.name")
                        projected_variables.add(expr.variable_name)

        # Generate a unique CTE name
        cte_name = f"agg_boundary_{len([op for op in all_ops if isinstance(op, AggregationBoundaryOperator)]) + 1}"

        boundary_op = AggregationBoundaryOperator(
            group_keys=group_keys,
            aggregates=aggregates,
            having_filter=part.having_expression,
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
            cte_name=cte_name,
            projected_variables=projected_variables,
        )
        boundary_op.set_in_operator(input_op)
        all_ops.append(boundary_op)

        return boundary_op

    def _create_match_after_boundary_tree(
        self,
        part: PartialQueryNode,
        boundary: AggregationBoundaryOperator,
        all_ops: list[LogicalOperator],
    ) -> LogicalOperator:
        """Create logical tree for MATCH clauses after an aggregation boundary.

        This method:
        1. Builds the match tree for the new MATCH clauses
        2. Joins the match tree with the aggregation boundary
        3. Processes any WHERE/RETURN clauses

        Args:
            part: The partial query containing the MATCH after aggregation
            boundary: The aggregation boundary operator to join with
            all_ops: List to collect created operators

        Returns:
            The final operator for this partial query
        """
        current_op: LogicalOperator = boundary
        return_exprs = [
            ret.inner_expression for ret in part.return_body
        ] if part.return_body else []

        # Track node aliases for correlation detection
        seen_node_aliases: set[str] = set()
        # Include projected variables from boundary as already seen
        seen_node_aliases.update(boundary.projected_variables)

        for match_clause in part.match_clauses:
            match_op = self._create_match_tree(match_clause, all_ops, return_exprs)

            # Join the match tree with the current operator (boundary or previous match)
            join_type = JoinType.LEFT if match_clause.is_optional else JoinType.INNER
            join_op = JoinOperator(join_type=join_type)
            join_op.set_in_operators(current_op, match_op)

            # Add join pairs for shared variables with the boundary
            for entity in match_clause.pattern_parts:
                if isinstance(entity, NodeEntity) and entity.alias in boundary.projected_variables:
                    # This node references a variable from the boundary
                    # Need to join on the ID column
                    join_op.add_join_pair(JoinKeyPair(
                        node_alias=entity.alias,
                        relationship_or_node_alias=boundary.cte_name,
                        pair_type=JoinKeyPairType.NODE_ID,
                    ))
                elif isinstance(entity, NodeEntity) and entity.alias in seen_node_aliases:
                    # Shared variable within this part's matches
                    join_op.add_join_pair(JoinKeyPair(
                        node_alias=entity.alias,
                        relationship_or_node_alias=entity.alias,
                        pair_type=JoinKeyPairType.NODE_ID,
                    ))

            all_ops.append(join_op)
            current_op = join_op

            # Track aliases from this match
            for entity in match_clause.pattern_parts:
                if isinstance(entity, NodeEntity) and entity.alias:
                    seen_node_aliases.add(entity.alias)

            # Process WHERE from match clause
            if match_clause.where_expression:
                select_op = SelectionOperator(
                    filter_expression=match_clause.where_expression
                )
                select_op.set_in_operator(current_op)
                all_ops.append(select_op)
                current_op = select_op

        # Process UNWIND clauses
        for unwind_clause in part.unwind_clauses:
            unwind_op = UnwindOperator(
                list_expression=unwind_clause.list_expression,
                variable_name=unwind_clause.variable_name,
            )
            unwind_op.set_in_operator(current_op)
            all_ops.append(unwind_op)
            current_op = unwind_op

        # Process WHERE from PartialQueryNode
        if part.where_expression:
            select_op = SelectionOperator(filter_expression=part.where_expression)
            select_op.set_in_operator(current_op)
            all_ops.append(select_op)
            current_op = select_op

        # Process RETURN clause
        if part.return_body:
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
                having_expression=part.having_expression,
            )
            proj_op.set_in_operator(current_op)
            all_ops.append(proj_op)
            current_op = proj_op

        return current_op

    def _create_partial_query_tree(
        self,
        part: PartialQueryNode,
        all_ops: list[LogicalOperator],
        previous_op: LogicalOperator | None,
    ) -> LogicalOperator:
        """Create logical tree for a partial query (MATCH...RETURN)."""
        current_op = previous_op

        # Collect return expressions for path analysis optimization
        # These are needed to determine if relationships(path) is used in RETURN
        return_exprs = [
            ret.inner_expression for ret in part.return_body
        ] if part.return_body else []

        # Track node aliases seen across all match clauses for correlation
        seen_node_aliases: set[str] = set()

        # Process MATCH clauses
        for match_clause in part.match_clauses:
            match_op = self._create_match_tree(match_clause, all_ops, return_exprs)

            if current_op is not None:
                # Join with previous result
                join_type = JoinType.LEFT if match_clause.is_optional else JoinType.INNER
                join_op = JoinOperator(join_type=join_type)
                join_op.set_in_operators(current_op, match_op)

                # Add join pairs for shared node variables (correlation)
                # This is critical for OPTIONAL MATCH to work correctly
                for entity in match_clause.pattern_parts:
                    if isinstance(entity, NodeEntity) and entity.alias in seen_node_aliases:
                        # Found a shared variable - add NODE_ID join pair
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

    def _convert_inline_properties_to_where(
        self, match_clause: MatchClause
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

        # Extract inline properties from all entities (nodes and relationships)
        for entity in match_clause.pattern_parts:
            # Check if entity has inline_properties attribute
            if not hasattr(entity, "inline_properties"):
                continue

            inline_props = entity.inline_properties
            if not inline_props or not isinstance(
                inline_props, QueryExpressionMapLiteral
            ):
                continue

            # Convert each property to equality predicate: entity.prop = value
            for prop_name, prop_value in inline_props.entries:
                # LIMITATION: Skip variable references for now
                # Variable references in inline properties (e.g., {id: txId}
                # where txId comes from UNWIND) need special handling that
                # isn't implemented yet. Only convert literal values.
                if isinstance(prop_value, QueryExpressionProperty):
                    # This is a variable/property reference, not a literal
                    # Skip it to maintain backward compatibility with existing
                    # tests that rely on inline properties being ignored
                    # when they contain variables.
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

        # Combine all predicates with AND
        if len(predicates) == 1:
            return predicates[0]

        and_op = BinaryOperatorInfo(
            BinaryOperator.AND, BinaryOperatorType.LOGICAL
        )
        result = predicates[0]
        for pred in predicates[1:]:
            result = QueryExpressionBinary(
                left_expression=result,
                right_expression=pred,
                operator=and_op,
            )

        return result

    def _create_match_tree(
        self,
        match_clause: MatchClause,
        all_ops: list[LogicalOperator],
        return_exprs: list[QueryExpression] | None = None,
    ) -> LogicalOperator:
        """Create logical tree for a MATCH clause.

        Args:
            match_clause: The MATCH clause to process
            all_ops: List to collect all created operators
            return_exprs: RETURN clause expressions for path usage analysis.
                Used to determine if edge collection is needed in recursive CTEs.
        """
        # =====================================================================
        # AUTO-ALIAS ASSIGNMENT
        # =====================================================================
        # Assign auto-generated aliases for entities without explicit aliases
        # This must happen BEFORE inline property conversion so that anonymous
        # nodes have valid aliases for filter generation.
        # =====================================================================
        auto_alias_counter = 0
        for entity in match_clause.pattern_parts:
            if not entity.alias:
                auto_alias_counter += 1
                entity.alias = f"_anon{auto_alias_counter}"

        # =====================================================================
        # INLINE PROPERTY FILTERS → WHERE CONVERSION
        # =====================================================================
        # Convert inline property filters from pattern entities to WHERE clause
        # Example: (a:Person {name: 'Alice'}) → WHERE a.name = 'Alice'
        # This happens after auto-alias assignment so all entities have
        # valid aliases for filter generation.
        # =====================================================================
        inline_where = self._convert_inline_properties_to_where(match_clause)

        # Merge inline filters with existing WHERE clause using AND
        if inline_where:
            if match_clause.where_expression:
                # Combine: inline_filters AND existing_where
                and_op = BinaryOperatorInfo(
                    BinaryOperator.AND, BinaryOperatorType.LOGICAL
                )
                match_clause.where_expression = QueryExpressionBinary(
                    left_expression=inline_where,
                    right_expression=match_clause.where_expression,
                    operator=and_op,
                )
            else:
                # Use inline filters as the WHERE clause
                match_clause.where_expression = inline_where

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
                return_exprs,
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
        return_exprs: list[QueryExpression] | None = None,
    ) -> LogicalOperator:
        """Create logical tree for variable-length path traversal.

        This method uses PathExpressionAnalyzer to optimize the recursive CTE:

        1. EDGE COLLECTION OPTIMIZATION:
           Only collects edge properties (path_edges array) when relationships(path)
           is actually used. This avoids the overhead of building NAMED_STRUCT arrays
           when they're not needed (e.g., when only SIZE(path) is used).

        2. PREDICATE PUSHDOWN (future):
           Extracts edge predicates from ALL() expressions for potential pushdown
           into the CTE's WHERE clause, enabling early path elimination.

        Args:
            match_clause: The MATCH clause containing the variable-length pattern
            rel: The variable-length relationship entity (e.g., [:TRANSFER*2..4])
            source_node: The source node of the path
            target_node: The target node of the path
            all_ops: List to collect all created operators
            return_exprs: RETURN clause expressions for path usage analysis
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
        start_node_filter, remaining_where = self._extract_source_node_filter(
            match_clause.where_expression,
            source_node.alias,
        )

        # Extract sink (target) node filter for optimization
        # This filter will be applied in the recursive join WHERE clause
        sink_node_filter, remaining_where = self._extract_source_node_filter(
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
        if self._logger:
            self._logger.log(
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
            if self._logger:
                self._logger.log(
                    f"Removed {len(path_info.pushed_all_expressions)} pushed predicates "
                    f"from WHERE clause"
                )

        # Get node ID columns from schema
        source_node_schema = self._graph_schema.get_node_definition(source_node.entity_name)
        target_node_schema = self._graph_schema.get_node_definition(target_node.entity_name)

        source_id_col = "id"  # Default
        if source_node_schema and source_node_schema.node_id_property:
            source_id_col = source_node_schema.node_id_property.property_name

        target_id_col = "id"  # Default
        if target_node_schema and target_node_schema.node_id_property:
            target_id_col = target_node_schema.node_id_property.property_name

        # Create recursive traversal operator with optimized settings
        # collect_edges is now data-driven based on actual path usage
        #
        # ═══════════════════════════════════════════════════════════════════
        # PREDICATE PUSHDOWN INTEGRATION
        # ═══════════════════════════════════════════════════════════════════
        #
        # The PathExpressionAnalyzer extracts predicates from ALL() expressions
        # that can be pushed into the recursive CTE. For example:
        #
        #   WHERE ALL(rel IN relationships(path) WHERE rel.amount > 1000)
        #
        # Becomes:
        #   edge_filter: rel.amount > 1000
        #   edge_filter_lambda_var: "rel" (needed for rewriting to "e.amount")
        #
        # The renderer uses these to add WHERE clauses inside the CTE,
        # preventing invalid paths from being explored in the first place.
        #
        # See RecursiveTraversalOperator docstring for detailed optimization
        # diagrams showing the performance impact.
        # ═══════════════════════════════════════════════════════════════════
        recursive_op = RecursiveTraversalOperator(
            edge_types=edge_types,
            source_node_type=source_node.entity_name,
            target_node_type=target_node.entity_name,
            min_hops=rel.min_hops if rel.min_hops is not None else 1,
            max_hops=rel.max_hops,
            source_id_column=source_id_col,  # Read from schema
            target_id_column=target_id_col,  # Read from schema
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

        # Find index of target node to process any remaining pattern parts
        # This handles patterns like: (a)-[:KNOWS*1..2]-(b)-[:HAS_LOAN]->(l)
        # where we need to continue processing [:HAS_LOAN]->(l) after the recursive path
        current_op: LogicalOperator = join_op

        # Find the index of the VLP relationship, then target is at vlp_idx + 1
        # We can't use index(target_node) because for circular patterns like (a)-[*]->(a),
        # target_node might be the same object as source_node, causing index() to return
        # the wrong position (0 instead of 2)
        vlp_idx = match_clause.pattern_parts.index(rel)
        target_idx = vlp_idx + 1  # Target node is right after the VLP relationship
        remaining_parts = match_clause.pattern_parts[target_idx + 1:]

        if remaining_parts:
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
                        pair_type = self._determine_join_pair_type(entity)
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
                                pair_type = self._determine_sink_join_type(prev_ent)
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

    def _extract_source_node_filter(
        self,
        where_expr: QueryExpression | None,
        source_alias: str,
    ) -> tuple[QueryExpression | None, QueryExpression | None]:
        """
        Extract filters on the source node variable from WHERE clause.

        This optimization pushes filters like `p.name = 'Alice'` into the
        recursive CTE's base case, dramatically reducing the number of paths
        explored.

        Returns (source_filter, remaining_expression).

        Examples:
        - WHERE p.name = 'Alice' -> (p.name = 'Alice', None)
        - WHERE p.name = 'Alice' AND f.age > 25 -> (p.name = 'Alice', f.age > 25)
        - WHERE f.age > 25 -> (None, f.age > 25)

        The source_filter is applied to the source node JOIN in the CTE base case,
        limiting the starting points of the recursive traversal.
        """
        if not where_expr:
            return None, None

        # Check if the entire expression references only the source node
        if self._references_only_variable(where_expr, source_alias):
            return where_expr, None

        # Handle AND: split into source-only and other predicates
        if isinstance(where_expr, QueryExpressionBinary):
            if (where_expr.operator and
                    where_expr.operator.name.name == "AND"):
                left = where_expr.left_expression
                right = where_expr.right_expression

                left_is_source = self._references_only_variable(left, source_alias)
                right_is_source = self._references_only_variable(right, source_alias)

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
        self,
        expr: QueryExpression,
        variable: str,
    ) -> bool:
        """Check if expression references only the specified variable.

        Returns True if ALL property references in the expression use the
        given variable name. Used to identify filters that can be pushed
        into the CTE base case.

        Examples with variable='p':
        - p.name = 'Alice'              -> True (only references p)
        - p.age > 30 AND p.active       -> True (only references p)
        - p.name = f.name               -> False (references both p and f)
        - f.age > 25                    -> False (references f, not p)
        - 1 = 1                         -> False (no property references)
        """
        properties = self._collect_property_references(expr)

        if not properties:
            # No property references - can't push (e.g., literal comparison)
            return False

        # All properties must reference the specified variable
        return all(prop.variable_name == variable for prop in properties)

    def _collect_property_references(
        self,
        expr: QueryExpression,
    ) -> list[QueryExpressionProperty]:
        """Collect all property references from an expression tree."""
        properties: list[QueryExpressionProperty] = []

        if isinstance(expr, QueryExpressionProperty):
            properties.append(expr)
        elif isinstance(expr, QueryExpressionBinary):
            properties.extend(self._collect_property_references(expr.left_expression))
            properties.extend(self._collect_property_references(expr.right_expression))
        elif hasattr(expr, 'children'):
            for child in expr.children:
                if isinstance(child, QueryExpression):
                    properties.extend(self._collect_property_references(child))

        return properties

    def _create_standard_match_tree(
        self, match_clause: MatchClause, all_ops: list[LogicalOperator]
    ) -> LogicalOperator:
        """Create standard logical tree for fixed-length MATCH clause.

        Note: Auto-alias assignment is now done in _create_match_tree()
        before this method is called, so all entities have valid aliases.
        """
        # Create data source operators for each entity
        entity_ops: dict[str, DataSourceOperator] = {}
        prev_node: NodeEntity | None = None
        # Bug #2 Fix: Track node aliases that have already been seen
        # When the same variable appears in multiple pattern parts (e.g., (p)-[:A]->(x), (p)-[:B]->(y))
        # we need to add a join condition to correlate the two occurrences of 'p'
        seen_node_aliases: set[str] = set()

        for entity in match_clause.pattern_parts:
            # Check if this entity alias was already seen (shared variable)
            if entity.alias in entity_ops:
                # Reuse existing operator - don't create a duplicate
                # The join condition will be added when joining
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
        # Track which node aliases are already in the join tree
        joined_node_aliases: set[str] = set()

        for entity_idx, entity in enumerate(match_clause.pattern_parts):
            # Bug #2 Fix: Handle shared node variables (same alias in multiple pattern parts)
            if isinstance(entity, NodeEntity) and entity.alias in joined_node_aliases:
                # This node is already in the join tree - it's a shared variable
                # Just update prev_node_alias to use for subsequent relationships
                # The join condition will be added when we join the next relationship
                prev_node_alias = entity.alias
                continue

            ds_op = entity_ops[entity.alias]

            if current_op is None:
                current_op = ds_op
                if isinstance(entity, NodeEntity):
                    prev_node_alias = entity.alias
                    joined_node_aliases.add(entity.alias)
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
                    # Join node to its immediately preceding relationship in the pattern
                    # (not all relationships that match by entity name, which caused bugs
                    # with comma-separated patterns like (a1)-[:R]->(t1), (a2)-[:R]->(t2))
                    if entity_idx > 0:
                        prev_ent = match_clause.pattern_parts[entity_idx - 1]
                        if isinstance(prev_ent, RelationshipEntity):
                            pair_type = self._determine_sink_join_type(prev_ent)
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

    # =========================================================================
    # AGGREGATION DETECTION UTILITIES
    # =========================================================================

    def _has_aggregation_in_expression(self, expr: QueryExpression) -> bool:
        """Check if an expression contains an aggregation function.

        This is used to detect when a WITH clause creates an aggregation
        boundary that requires special handling for subsequent MATCH clauses.

        Args:
            expr: The expression to check for aggregation functions.

        Returns:
            True if the expression contains COUNT, SUM, AVG, etc.
        """
        if isinstance(expr, QueryExpressionAggregationFunction):
            return True
        if isinstance(expr, QueryExpressionBinary):
            left_has = (
                self._has_aggregation_in_expression(expr.left_expression)
                if expr.left_expression
                else False
            )
            right_has = (
                self._has_aggregation_in_expression(expr.right_expression)
                if expr.right_expression
                else False
            )
            return left_has or right_has
        if isinstance(expr, QueryExpressionFunction):
            return any(
                self._has_aggregation_in_expression(p) for p in expr.parameters
            )
        return False

    def _has_aggregation_in_projections(
        self, part: PartialQueryNode
    ) -> bool:
        """Check if a partial query's return body contains aggregations.

        Args:
            part: The partial query node to check.

        Returns:
            True if any projection in return_body contains an aggregation.
        """
        if not part.return_body:
            return False
        return any(
            self._has_aggregation_in_expression(ret.inner_expression)
            for ret in part.return_body
        )

    def _detect_match_after_aggregating_with(
        self, query_node: SingleQueryNode
    ) -> tuple[bool, int]:
        """Detect if query has MATCH clauses after a WITH that aggregates.

        This pattern requires special handling because:
        1. The aggregating WITH creates a materialization boundary
        2. Variables not projected are no longer visible
        3. Subsequent MATCHes must join with the aggregated result

        Args:
            query_node: The single query node to analyze.

        Returns:
            A tuple of (detected, boundary_index) where:
            - detected: True if the problematic pattern exists
            - boundary_index: Index of the first aggregating WITH that is
              followed by a MATCH clause (-1 if not detected)

        Example patterns detected:
            MATCH (a)-[:R1]->(b)
            WITH a, COUNT(b) AS cnt    -- Part 0: aggregation
            MATCH (a)-[:R2]->(c)       -- Part 1: MATCH after aggregation
            RETURN a, cnt, COUNT(c)    -- DETECTED at boundary_index=0
        """
        has_aggregating_with = False
        boundary_index = -1

        for i, part in enumerate(query_node.parts):
            # First check if we have a MATCH following a previous aggregation
            # This must be checked BEFORE updating has_aggregating_with
            if has_aggregating_with and part.match_clauses:
                return True, boundary_index

            # Then check if this part has aggregation in its projections
            # (which would create a boundary for subsequent parts)
            if self._has_aggregation_in_projections(part):
                has_aggregating_with = True
                boundary_index = i

        return False, -1

    def _raise_match_after_aggregation_error(
        self,
        query_node: SingleQueryNode,
        boundary_index: int,
    ) -> None:
        """Raise a helpful error for MATCH after aggregating WITH.

        This provides a clear error message explaining why the pattern
        is not supported and what the user can do.

        Args:
            query_node: The query that contains the unsupported pattern.
            boundary_index: Index of the aggregating WITH clause.
        """
        # Build a helpful error message
        agg_part = query_node.parts[boundary_index]
        agg_aliases = [ret.alias for ret in agg_part.return_body]

        msg = (
            "MATCH clause after aggregating WITH is not yet supported.\n\n"
            f"The WITH clause at position {boundary_index + 1} contains "
            f"aggregation and projects: {agg_aliases}\n\n"
            "This pattern requires the aggregated result to be materialized "
            "as a CTE before joining with subsequent MATCH patterns.\n\n"
            "Workaround: Split your query into multiple queries, using the "
            "aggregated result as input to the second query."
        )
        raise UnsupportedQueryPatternError(msg)
