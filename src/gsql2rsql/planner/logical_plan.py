"""Logical plan creation from AST."""

from __future__ import annotations

from gsql2rsql.common.exceptions import TranspilerInternalErrorException
from gsql2rsql.common.logging import ILoggable
from gsql2rsql.common.schema import IGraphSchemaProvider
from gsql2rsql.common.utils import change_indentation
from gsql2rsql.parser.ast import (
    InfixOperator,
    InfixQueryNode,
    MatchClause,
    NodeEntity,
    QueryExpression,
    QueryExpressionBinary,
    QueryExpressionExists,
    QueryExpressionFunction,
    QueryNode,
    RelationshipEntity,
    SingleQueryNode,
)
from gsql2rsql.parser.operators import (
    BinaryOperator,
    BinaryOperatorInfo,
    BinaryOperatorType,
)
from gsql2rsql.planner.aggregation_boundary import (
    create_aggregation_boundary,
    create_match_after_boundary_tree,
    create_match_tree_for_boundary,
    part_creates_aggregation_boundary,
)
from gsql2rsql.planner.column_resolver import ColumnResolver, ResolutionResult
from gsql2rsql.planner.match_tree import (
    convert_inline_properties_to_where,
    create_partial_query_tree,
    create_standard_match_tree,
)
from gsql2rsql.planner.operators import (
    AggregationBoundaryOperator,
    IBindable,
    LogicalOperator,
    SelectionOperator,
    SetOperationType,
    SetOperator,
    StartLogicalOperator,
)

# Import extracted modules
from gsql2rsql.planner.recursive_traversal import create_recursive_match_tree


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

        # Resolve EXISTS pattern direction context (SoC: planner decides, renderer uses)
        self._resolve_exists_patterns()

        return self._resolution_result

    def _resolve_exists_patterns(self) -> None:
        """Resolve direction context for all EXISTS expressions in the plan."""
        for op in self.all_operators():
            if isinstance(op, SelectionOperator) and op.filter_expression:
                self._resolve_exists_in_expression(op.filter_expression)

    def _resolve_exists_in_expression(self, expr: QueryExpression) -> None:
        """Recursively resolve EXISTS patterns in an expression tree."""
        if isinstance(expr, QueryExpressionExists):
            for entity in expr.pattern_entities:
                if isinstance(entity, RelationshipEntity):
                    expr.resolve_direction_context(entity.direction)
                    break
        elif isinstance(expr, QueryExpressionBinary):
            if expr.left_expression:
                self._resolve_exists_in_expression(expr.left_expression)
            if expr.right_expression:
                self._resolve_exists_in_expression(expr.right_expression)
        elif isinstance(expr, QueryExpressionFunction):
            for param in expr.parameters:
                self._resolve_exists_in_expression(param)

    def all_operators(self) -> list[LogicalOperator]:
        """Get all operators in the plan."""
        result: list[LogicalOperator] = []
        visited: set[int] = set()

        def visit(op: LogicalOperator) -> None:
            op_id = id(op)
            if op_id in visited:
                return
            visited.add(op_id)
            result.append(op)
            for out_op in op.out_operators:
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
        """Create a LogicalPlan from a query AST."""
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
        """Resolve entity names for nodes that are referenced without labels."""
        alias_to_entity_name: dict[str, str] = {}

        def collect_from_node(node: QueryNode) -> None:
            if isinstance(node, SingleQueryNode):
                for part in node.parts:
                    for match_clause in part.match_clauses:
                        for entity in match_clause.pattern_parts:
                            if entity.alias and entity.entity_name:
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
                                if entity.alias in alias_to_entity_name:
                                    entity.entity_name = alias_to_entity_name[entity.alias]
            elif isinstance(node, InfixQueryNode):
                apply_to_node(node.left_query)
                apply_to_node(node.right_query)

        collect_from_node(tree_node)
        apply_to_node(tree_node)

    def dump_graph(self) -> str:
        """Dump textual format of the logical plan."""
        lines: list[str] = []

        all_ops: dict[int, list[LogicalOperator]] = {}
        for start_op in self._starting_operators:
            for op in start_op.get_all_downstream_operators(LogicalOperator):  # type: ignore[type-abstract]
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
        aggregation_boundary: AggregationBoundaryOperator | None = None

        for part_idx, part in enumerate(query_node.parts):
            creates_boundary = part_creates_aggregation_boundary(
                query_node, part_idx
            )

            if aggregation_boundary is not None and part.match_clauses:
                current_op = create_match_after_boundary_tree(
                    part, aggregation_boundary, all_ops, self._create_match_tree
                )
                aggregation_boundary = None
            elif creates_boundary:
                match_op = create_match_tree_for_boundary(
                    part, all_ops, current_op, self._create_match_tree
                )
                aggregation_boundary = create_aggregation_boundary(
                    part, match_op, all_ops
                )
                current_op = aggregation_boundary
            else:
                part_op = create_partial_query_tree(
                    part, all_ops, current_op, self._create_match_tree
                )
                current_op = part_op

        if current_op is None:
            raise TranspilerInternalErrorException("Empty query")

        return current_op

    def _create_match_tree(
        self,
        match_clause: MatchClause,
        all_ops: list[LogicalOperator],
        return_exprs: list[QueryExpression] | None = None,
    ) -> LogicalOperator:
        """Create logical tree for a MATCH clause."""
        # Auto-alias assignment
        auto_alias_counter = 0
        for entity in match_clause.pattern_parts:
            if not entity.alias:
                auto_alias_counter += 1
                entity.alias = f"_anon{auto_alias_counter}"

        # Inline property filters → WHERE conversion
        inline_where = convert_inline_properties_to_where(match_clause)

        if inline_where:
            if match_clause.where_expression:
                and_op = BinaryOperatorInfo(
                    BinaryOperator.AND, BinaryOperatorType.LOGICAL
                )
                match_clause.where_expression = QueryExpressionBinary(
                    left_expression=inline_where,
                    right_expression=match_clause.where_expression,
                    operator=and_op,
                )
            else:
                match_clause.where_expression = inline_where

        # Check for variable-length relationships
        var_length_rel = None
        source_node = None
        target_node = None

        for i, entity in enumerate(match_clause.pattern_parts):
            if isinstance(entity, RelationshipEntity):
                if entity.is_variable_length:
                    var_length_rel = entity
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
            return create_recursive_match_tree(
                match_clause,
                var_length_rel,
                source_node,
                target_node,
                all_ops,
                return_exprs,
                self._graph_schema,
                self._logger,
            )

        # Standard match tree creation for fixed-length relationships
        return create_standard_match_tree(match_clause, all_ops)

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
        for start_op in self._starting_operators:
            self._propagate_down(start_op)

    def _propagate_down(self, op: LogicalOperator) -> None:
        """Propagate data types down from an operator."""
        op.propagate_data_types_for_in_schema()
        op.propagate_data_types_for_out_schema()

        for out_op in op.out_operators:
            self._propagate_down(out_op)
