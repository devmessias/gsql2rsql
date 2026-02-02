"""Dead Table Elimination Optimizer.

This module implements an optimization pass that removes unnecessary JOINs from
the logical plan when the joined table's columns are not used in the output.

Design Decision: Optimizer vs Planner
=====================================

We chose to implement this as an **Optimizer** (post-planning transformation)
rather than modifying the Planner for several reasons:

1. **Separation of Concerns**: The Planner's job is to faithfully translate
   Cypher semantics to a logical plan. Determining which columns are "dead"
   requires global analysis of RETURN/WHERE/ORDER BY clauses, which the Planner
   doesn't have at match-pattern construction time.

2. **Debuggability**: With an Optimizer, we can log/compare plans before and
   after transformation. If the Planner didn't create JOINs at all, there's
   nothing to compare.

3. **Feature Flag**: Easy to disable (`enabled=False`) for debugging or when
   correctness > performance (e.g., orphan edge detection).

4. **Follows Existing Pattern**: The project already has `SubqueryFlatteningOptimizer`
   and `SelectionPushdownOptimizer` as post-planning transformations.

Trade-offs Considered
=====================

1. **Performance vs Correctness (Orphan Edges)**
   - Optimized: `MATCH ()-[r]->() RETURN r` → `SELECT * FROM edges`
   - Original: `SELECT * FROM edges JOIN nodes ON ... JOIN nodes ON ...`
   - Difference: Original filters out edges pointing to non-existent nodes
   - Decision: Assume referential integrity (most graphs have it), provide flag

2. **Semi-join vs Full Elimination**
   - For typed nodes: `MATCH (a:Person)-[r]->(b) RETURN r`
   - Full elimination: Can't use `:Person` filter
   - Semi-join: `WHERE src IN (SELECT node_id FROM nodes WHERE type='Person')`
   - Decision: Use semi-join for typed nodes, full elimination for untyped

3. **Optimizer Overhead**
   - We create JOINs in Planner only to remove them in Optimizer
   - Trade-off: Small overhead for creation vs cleaner architecture
   - Decision: Accept overhead (plan creation is fast, execution is slow)

4. **Column Reference Analysis**
   - Must traverse all expressions (RETURN, WHERE, ORDER BY, aggregations)
   - Risk: Missing some reference → incorrectly remove needed JOIN
   - Decision: Conservative analysis - if unsure, keep JOIN

5. **Spark Optimizer Interaction**
   - Spark Catalyst can push projections but can't eliminate semantic JOINs
   - Our optimization is complementary, not redundant
   - Decision: Do both - our optimization + trust Spark for the rest

Usage
=====

Direct usage::

    optimizer = DeadTableEliminationOptimizer(enabled=True)
    optimizer.optimize(plan)
    print(optimizer.stats)  # See what was eliminated

Via optimize_plan::

    # In subquery_optimizer.py:
    def optimize_plan(..., dead_table_elimination: bool = True):
        if dead_table_elimination:
            DeadTableEliminationOptimizer().optimize(plan)
        ...

Via GraphContext::

    # In graph_context.py:
    graph = GraphContext(..., optimize_dead_tables=True)
    sql = graph.transpile("MATCH ()-[r]->() RETURN r")

Caveats and Special Cases
=========================

1. **OPTIONAL MATCH**: LEFT JOINs must NOT be eliminated (would change semantics)
2. **Aggregations**: `COUNT(DISTINCT a)` requires `a` even if not in RETURN
3. **ORDER BY**: `RETURN r ORDER BY a.name` requires JOIN for `a`
4. **Variable Length Paths**: Special handling via RecursiveTraversalOperator
5. **Bidirectional**: `(a)-[r]-(b)` uses UNION ALL internally, JOINs still can be dead

See Also
========
- new_bugs/002_dead_table_elimination.md: Full bug report with test cases
- planner/subquery_optimizer.py: Existing optimizer pattern to follow
- docs_help_dev/soc-violations.md: SoC principles this design respects
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from gsql2rsql.parser.ast import (
    NodeEntity,
    QueryExpression,
    QueryExpressionAggregationFunction,
    QueryExpressionBinary,
    QueryExpressionFunction,
    QueryExpressionProperty,
    RelationshipEntity,
)
from gsql2rsql.planner.operators import (
    DataSourceOperator,
    JoinOperator,
    JoinType,
    LogicalOperator,
    ProjectionOperator,
    SelectionOperator,
)
from gsql2rsql.planner.schema import EntityType

if TYPE_CHECKING:
    from gsql2rsql.planner.logical_plan import LogicalPlan


@dataclass
class DeadTableEliminationStats:
    """Statistics from dead table elimination optimization.

    Used for debugging, logging, and testing to verify the optimizer
    is working correctly.

    Attributes:
        joins_analyzed: Total JOINs inspected
        joins_removed: JOINs completely eliminated (both sides unused)
        joins_converted_to_semijoin: Full JOINs → semi-joins (typed but unused)
        nodes_eliminated: Node table references removed
        edges_preserved: Edge table references kept (should stay)
    """

    joins_analyzed: int = 0
    joins_removed: int = 0
    joins_converted_to_semijoin: int = 0
    nodes_eliminated: int = 0
    edges_preserved: int = 0


@dataclass
class UsedVariables:
    """Tracks which entity variables are used in the query.

    This is the result of column usage analysis. A variable is "used" if it
    appears in any of:
    - RETURN clause projections
    - WHERE clause predicates
    - ORDER BY expressions
    - Aggregation functions (COUNT, COLLECT, etc.)
    - HAVING clause

    Attributes:
        in_return: Variables directly in RETURN (e.g., `RETURN a` → 'a')
        in_property_access: Variables used for property access (e.g., `a.name` → 'a')
        in_filter: Variables in WHERE/HAVING (e.g., `WHERE a.age > 30` → 'a')
        in_order_by: Variables in ORDER BY (e.g., `ORDER BY a.name` → 'a')
        in_aggregation: Variables in aggregations (e.g., `COUNT(DISTINCT a)` → 'a')
        typed_entities: Variables with type filters (e.g., `(a:Person)` → 'a')
    """

    in_return: set[str] = field(default_factory=set)
    in_property_access: set[str] = field(default_factory=set)
    in_filter: set[str] = field(default_factory=set)
    in_order_by: set[str] = field(default_factory=set)
    in_aggregation: set[str] = field(default_factory=set)
    typed_entities: set[str] = field(default_factory=set)

    def is_used(self, variable: str) -> bool:
        """Check if a variable is used anywhere in the query.

        Returns True if the variable appears in any context that requires
        its columns to be available in the result.
        """
        return (
            variable in self.in_return
            or variable in self.in_property_access
            or variable in self.in_filter
            or variable in self.in_order_by
            or variable in self.in_aggregation
        )

    def needs_type_filter(self, variable: str) -> bool:
        """Check if variable has a type filter but is otherwise unused.

        This is the semi-join case: we need to filter by type but don't
        need the actual columns from the node table.
        """
        return variable in self.typed_entities and not self.is_used(variable)

    def all_used(self) -> set[str]:
        """Return all variables that are used anywhere."""
        return (
            self.in_return
            | self.in_property_access
            | self.in_filter
            | self.in_order_by
            | self.in_aggregation
        )


class DeadTableEliminationOptimizer:
    """Optimizer that removes JOINs with tables not contributing to output.

    This optimizer analyzes which entity variables (nodes/relationships) are
    actually used in the query output (RETURN, WHERE, ORDER BY, aggregations)
    and eliminates JOINs that only exist for pattern matching but whose
    columns are never accessed.

    Example Optimization
    --------------------

    Query: `MATCH (a)-[r]->(b) RETURN r`

    Before optimization:
        - Scan nodes (for a)
        - JOIN edges ON a.node_id = r.src
        - Scan nodes (for b)
        - JOIN ON r.dst = b.node_id
        - Project r

    After optimization:
        - Scan edges
        - Project r

    The JOINs with nodes table were eliminated because neither `a` nor `b`
    appear in RETURN, WHERE, or ORDER BY.

    Architectural Decision
    ----------------------

    Why Optimizer and not Planner?

    The Planner builds the logical plan by translating MATCH patterns literally.
    `MATCH (a)-[r]->(b)` semantically means "find edges where both endpoints
    exist in the nodes table". The Planner correctly creates JOINs for this.

    The Optimizer has access to the FULL query context (RETURN, WHERE, ORDER BY)
    and can determine that certain JOINs, while semantically correct, are
    unnecessary for computing the result. This is a **transformation**, not
    a **semantic interpretation**.

    Following SoC (Separation of Concerns):
    - Parser: Syntax → AST
    - Planner: Semantics → Logical Plan
    - Optimizer: Transformation → Better Logical Plan (this class)
    - Renderer: Logical Plan → SQL String

    If we put this in the Planner, it would need to know about RETURN/WHERE
    at MATCH time, violating the single-responsibility principle.

    Thread Safety
    -------------

    This class is NOT thread-safe. Each optimization should use a new instance:

        optimizer = DeadTableEliminationOptimizer()
        optimizer.optimize(plan)  # Modifies plan in-place

    Attributes:
        enabled: If False, optimize() is a no-op (useful for A/B testing)
        stats: Statistics about eliminations performed
    """

    def __init__(self, enabled: bool = True) -> None:
        """Initialize the optimizer.

        Args:
            enabled: If False, optimize() becomes a no-op. Useful for:
                    - A/B testing (compare SQL with/without elimination)
                    - Debugging (ensure JOINs are needed for correctness)
                    - Orphan edge detection (JOINs filter orphans)
        """
        self.enabled = enabled
        self.stats = DeadTableEliminationStats()

    def optimize(self, plan: LogicalPlan) -> None:
        """Apply dead table elimination to the logical plan.

        Modifies the plan IN-PLACE. After this call, the plan's operator
        graph may have fewer JoinOperators.

        Algorithm:
        1. Collect all used variables from RETURN/WHERE/ORDER BY/aggregations
        2. Identify typed entity variables (need semi-join, not elimination)
        3. For each JoinOperator:
           a. Determine which side(s) can be eliminated
           b. If node is used or typed: keep/convert to semi-join
           c. If node is unused and untyped: eliminate

        Args:
            plan: The logical plan to optimize. Modified in-place.
        """
        if not self.enabled:
            return

        # Reset stats for this optimization run
        self.stats = DeadTableEliminationStats()

        # Phase 1: Analyze column usage across the entire plan
        used_vars = self._collect_used_variables(plan)

        # Phase 2: Collect typed entities (need semi-join, not full elimination)
        self._collect_typed_entities(plan, used_vars)

        # Phase 3: Transform JOINs based on usage analysis
        self._eliminate_unused_joins(plan, used_vars)

    def _collect_used_variables(self, plan: LogicalPlan) -> UsedVariables:
        """Analyze the plan to determine which variables are actually used.

        Traverses all operators looking for variable references in:
        - ProjectionOperator.projections (RETURN clause)
        - SelectionOperator.filter_expression (WHERE clause)
        - ProjectionOperator.order_by (ORDER BY clause)
        - ProjectionOperator.having_expression (HAVING clause)
        - Aggregation function parameters (COUNT, COLLECT, etc.)

        Returns:
            UsedVariables instance with all usage information.
        """
        used = UsedVariables()

        for op in plan.all_operators():
            if isinstance(op, ProjectionOperator):
                # Analyze RETURN projections
                for alias, expr in op.projections:
                    self._collect_from_expression(expr, used, "return")

                # Analyze ORDER BY
                for expr, _ in op.order_by:
                    self._collect_from_expression(expr, used, "order_by")

                # Analyze HAVING
                if op.having_expression:
                    self._collect_from_expression(op.having_expression, used, "filter")

                # Analyze WHERE (filter_expression, from flattened Selection)
                if op.filter_expression:
                    self._collect_from_expression(op.filter_expression, used, "filter")

            elif isinstance(op, SelectionOperator):
                # Analyze WHERE (before flattening)
                if op.filter_expression:
                    self._collect_from_expression(op.filter_expression, used, "filter")

        return used

    def _collect_from_expression(
        self,
        expr: QueryExpression,
        used: UsedVariables,
        context: str,
    ) -> None:
        """Recursively extract variable references from an expression.

        In this project, variable references are represented as:
        - QueryExpressionProperty with property_name=None → bare variable (RETURN a)
        - QueryExpressionProperty with property_name set → property access (a.name)

        Args:
            expr: The expression to analyze
            used: UsedVariables to populate
            context: One of "return", "filter", "order_by" for categorization
        """
        if isinstance(expr, QueryExpressionProperty):
            # Both bare variables (a) and property access (a.name) use this class
            var_name = expr.variable_name

            if expr.property_name:
                # Property access: a.name → 'a' is used for property access
                used.in_property_access.add(var_name)
            # else: bare variable reference (RETURN a)

            # Add to context-specific set
            if context == "return":
                used.in_return.add(var_name)
            elif context == "filter":
                used.in_filter.add(var_name)
            elif context == "order_by":
                used.in_order_by.add(var_name)

        elif isinstance(expr, QueryExpressionAggregationFunction):
            # Aggregation functions like COUNT(DISTINCT a), COLLECT(a.name)
            if expr.inner_expression:
                # Mark as used in aggregation context
                if isinstance(expr.inner_expression, QueryExpressionProperty):
                    used.in_aggregation.add(expr.inner_expression.variable_name)
                # Recurse into inner expression
                self._collect_from_expression(expr.inner_expression, used, context)

        elif isinstance(expr, QueryExpressionFunction):
            # Regular function call: collect parameters
            # Check if it's a function that uses variables directly
            func_name = ""
            if expr.function and expr.function.name:
                func_name = expr.function.name.upper()

            for param in expr.parameters:
                # Recurse into parameters
                self._collect_from_expression(param, used, context)

        elif isinstance(expr, QueryExpressionBinary):
            # Binary expression: recurse into both sides
            if expr.left_expression:
                self._collect_from_expression(expr.left_expression, used, context)
            if expr.right_expression:
                self._collect_from_expression(expr.right_expression, used, context)

    def _collect_typed_entities(
        self, plan: LogicalPlan, used: UsedVariables
    ) -> None:
        """Identify entity variables that have type filters.

        For queries like `MATCH (a:Person)-[r]->(b) RETURN r`:
        - `a` has type :Person
        - `a` is NOT used in RETURN
        - But we still need to filter edges by `a`'s type

        These become semi-join candidates instead of full elimination.

        Args:
            plan: The logical plan to analyze
            used: UsedVariables to update with typed entities
        """
        for op in plan.all_operators():
            if isinstance(op, DataSourceOperator) and op.entity:
                entity = op.entity
                if isinstance(entity, NodeEntity):
                    # Check if node has a type filter
                    if entity.entity_name:  # Has a label like :Person
                        if entity.alias:
                            used.typed_entities.add(entity.alias)
                elif isinstance(entity, RelationshipEntity):
                    # Relationships can also have types
                    if entity.entity_name:  # Has a type like :KNOWS
                        if entity.alias:
                            used.typed_entities.add(entity.alias)

    def _eliminate_unused_joins(
        self, plan: LogicalPlan, used: UsedVariables
    ) -> None:
        """Transform the plan to eliminate or simplify unused JOINs.

        For each JoinOperator in the plan:
        1. Check if the node/entity on each side is used
        2. If unused and untyped: eliminate the JOIN entirely
        3. If unused but typed: convert to semi-join (TODO: future work)
        4. If used: keep the JOIN as-is

        This method modifies the plan's operator graph IN-PLACE.

        Args:
            plan: The logical plan to transform
            used: Usage analysis from _collect_used_variables
        """
        # Find all JoinOperators
        join_ops = [
            op for op in plan.all_operators()
            if isinstance(op, JoinOperator)
        ]

        for join_op in join_ops:
            self.stats.joins_analyzed += 1

            # Check if this is an OPTIONAL MATCH (LEFT JOIN) - never eliminate
            if join_op.join_type == JoinType.LEFT:
                continue

            # Analyze left and right sides
            left_entity = self._get_entity_from_subtree(join_op.in_operator_left)
            right_entity = self._get_entity_from_subtree(join_op.in_operator_right)

            left_var = left_entity.alias if left_entity else None
            right_var = right_entity.alias if right_entity else None

            # Check if entities are used
            left_used = left_var and used.is_used(left_var)
            right_used = right_var and used.is_used(right_var)

            # Check if entities need type filter (semi-join case)
            left_needs_type = left_var and used.needs_type_filter(left_var)
            right_needs_type = right_var and used.needs_type_filter(right_var)

            # Decision logic
            if not left_used and not right_used:
                # Both sides unused - check if we can eliminate
                if left_needs_type or right_needs_type:
                    # Need semi-join for type filter (TODO: implement)
                    # For now, keep the JOIN
                    self.stats.joins_converted_to_semijoin += 1
                else:
                    # Can eliminate entirely
                    # This is the main optimization case
                    self._eliminate_join(plan, join_op, used)
                    self.stats.joins_removed += 1

            elif not left_used and not left_needs_type:
                # Left side unused and no type filter - could simplify
                # For now, we keep the JOIN (safe conservative approach)
                # TODO: Implement single-side elimination
                pass

            elif not right_used and not right_needs_type:
                # Right side unused and no type filter - could simplify
                # For now, we keep the JOIN (safe conservative approach)
                # TODO: Implement single-side elimination
                pass

    def _get_entity_from_subtree(
        self, op: LogicalOperator | None
    ) -> NodeEntity | RelationshipEntity | None:
        """Extract the entity from a DataSourceOperator subtree.

        Traverses up the operator tree to find the DataSourceOperator
        that feeds into this JOIN side.

        Args:
            op: The operator at the root of the subtree

        Returns:
            The entity (NodeEntity or RelationshipEntity) if found, None otherwise.
        """
        if op is None:
            return None

        if isinstance(op, DataSourceOperator):
            return op.entity  # type: ignore[return-value]

        # Traverse children (for non-source operators)
        for child in op.in_operators:
            result = self._get_entity_from_subtree(child)
            if result:
                return result

        return None

    def _eliminate_join(
        self,
        plan: LogicalPlan,
        join_op: JoinOperator,
        used: UsedVariables,
    ) -> None:
        """Eliminate a JOIN by rewiring the operator graph.

        This is the core elimination logic. When a JOIN is eliminated:
        1. The JOIN's output is connected directly to the non-eliminated side
        2. The eliminated DataSourceOperator is orphaned
        3. The plan's starting_operators list is updated

        Preconditions:
        - Both sides of the JOIN are unused (verified by caller)
        - The JOIN is an INNER JOIN (LEFT JOINs are never eliminated)

        Args:
            plan: The logical plan being modified
            join_op: The JoinOperator to eliminate
            used: Usage analysis for decision making

        Note:
            This implementation currently handles the simple case where
            the entire JOIN can be bypassed. More complex cases (keeping
            one side, semi-join conversion) are marked as TODOs.
        """
        # For the initial implementation, we focus on the case where
        # edges are the only thing needed (most common optimization case)
        #
        # MATCH (a)-[r]->(b) RETURN r
        #   - Both `a` and `b` are unused
        #   - The edges table is the only necessary data source
        #   - We can bypass both node JOINs

        # Find the edge DataSourceOperator (if any)
        edge_source = self._find_edge_source_in_subtree(join_op)

        if edge_source is None:
            # No edge source found - this might be a node-only query
            # or a complex pattern we don't handle yet
            return

        # Check if edge's entity is used
        edge_entity = edge_source.entity
        if not edge_entity or not edge_entity.alias:
            return

        if not used.is_used(edge_entity.alias):
            # Even the edge isn't used - unusual case, skip
            return

        # Rewire: Connect JOIN's outputs to edge source
        for out_op in list(join_op.graph_out_operators):
            # Replace JOIN with edge source in the output's inputs
            if join_op in out_op.graph_in_operators:
                idx = out_op.graph_in_operators.index(join_op)
                out_op.graph_in_operators[idx] = edge_source

            # Add output to edge source's outputs
            if out_op not in edge_source.graph_out_operators:
                edge_source.graph_out_operators.append(out_op)

        # Orphan the JOIN and its node sources
        join_op.graph_in_operators = []
        join_op.graph_out_operators = []

        # Update plan's starting operators
        self._update_starting_operators(plan)

        self.stats.nodes_eliminated += 2  # Both source and target nodes
        self.stats.edges_preserved += 1

    def _find_edge_source_in_subtree(
        self, op: LogicalOperator | None
    ) -> DataSourceOperator | None:
        """Find a DataSourceOperator for edges in the operator subtree.

        Args:
            op: Root of subtree to search

        Returns:
            DataSourceOperator with a RelationshipEntity, or None if not found.
        """
        if op is None:
            return None

        if isinstance(op, DataSourceOperator):
            if op.entity and isinstance(op.entity, RelationshipEntity):
                return op

        # Search recursively
        for child in op.in_operators:
            result = self._find_edge_source_in_subtree(child)
            if result:
                return result

        return None

    def _update_starting_operators(self, plan: LogicalPlan) -> None:
        """Update plan's starting_operators list after graph modifications.

        Removes orphaned operators and ensures consistency.

        Args:
            plan: The plan to update
        """
        # Collect all reachable operators from terminal operators
        reachable: set[int] = set()

        def mark_reachable(op: LogicalOperator) -> None:
            op_id = id(op)
            if op_id in reachable:
                return
            reachable.add(op_id)
            for in_op in op.in_operators:
                mark_reachable(in_op)

        for terminal in plan.terminal_operators:
            mark_reachable(terminal)

        # Filter starting operators to only those reachable
        plan.starting_operators = [
            op for op in plan.starting_operators
            if id(op) in reachable
        ]
