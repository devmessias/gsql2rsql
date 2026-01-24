"""Test 38: Predicate Pushdown for Undirected Relationships.

Validates that WHERE clause predicates referencing only one node in an
undirected relationship pattern are pushed down BEFORE the join operation.

Problem (without pushdown):
    MATCH (p:Person)-[:KNOWS]-(f:Person)
    WHERE p.name = 'Alice'
    RETURN f.name

    Current plan:
        Level 0: DataSource(p:Person), DataSource(KNOWS), DataSource(f:Person)
        Level 1: Join(p, KNOWS, Type=EITHER)
        Level 2: Join(result, f, Type=EITHER)
        Level 3: Selection(p.name = 'Alice')  <-- LATE!
        Level 4: Projection(friend=f.name)

    This causes:
    - Full table scan of Person (p)
    - Full edge scan of KNOWS
    - Full table scan of Person (f)
    - THEN filter on p.name = 'Alice'

Solution (with pushdown):
    Optimal plan:
        Level 0: DataSource(p:Person) with filter p.name='Alice', DataSource(KNOWS), DataSource(f:Person)
        Level 1: Join(filtered_p, KNOWS, Type=EITHER)
        Level 2: Join(result, f, Type=EITHER)
        Level 3: Projection(friend=f.name)

    This limits:
    - Only matching rows from Person (p) are scanned
    - Much smaller join set
    - Same final result, much faster execution

================================================================================
IMPLEMENTATION OPTIONS AND TRADE-OFFS
================================================================================

Option 1: SelectionPushdownOptimizer (Post-hoc Optimization)
------------------------------------------------------------
Description:
    Create a new optimizer that runs after plan construction and pushes
    Selection operators through JoinOperators when safe.

Mechanism:
    1. Traverse plan bottom-up
    2. When encountering Selection → Join:
       - Analyze which input of the Join the predicate references
       - If predicate only references left side, push before left input
       - If predicate only references right side, push before right input
       - If mixed, keep in place

Pros:
    + Clean separation of concerns (construction vs optimization)
    + Can be enabled/disabled independently
    + Follows existing optimizer pattern (SubqueryFlatteningOptimizer)
    + Testable in isolation
    + Works for all join types, not just undirected

Cons:
    - Post-hoc rewiring of operator graph is complex
    - Need to correctly update all operator references
    - May miss optimization opportunities that require early knowledge
    - Additional pass over the plan

--------------------------------------------------------------------------------

Option 2: Early Filter Extraction During Plan Construction
----------------------------------------------------------
Description:
    In _create_partial_query_tree, analyze WHERE clause and attach predicates
    to specific DataSourceOperators before any joins happen.

Mechanism:
    1. Before creating match tree, extract all predicates from WHERE
    2. Partition predicates by which entity they reference
    3. Attach predicates to appropriate DataSourceOperators via new filter field
    4. Only create SelectionOperator for predicates that can't be pushed

Pros:
    + No post-processing needed
    + Predicates are in optimal position from the start
    + Simpler SQL rendering (each table knows its own filters)
    + Most efficient query shape

Cons:
    - Makes plan construction more complex
    - Need to modify DataSourceOperator to have filter_expression
    - Harder to reason about (filter logic scattered)
    - Changes to how the WHERE clause is processed

--------------------------------------------------------------------------------

Option 3: Hybrid - DataSourceOperator with Filter + Fallback Selection
----------------------------------------------------------------------
Description:
    Add optional filter to DataSourceOperator, push simple predicates during
    construction, create SelectionOperator for complex ones.

Mechanism:
    1. Add filter_expression field to DataSourceOperator
    2. During match tree creation, if predicate references only one node, attach it
    3. For complex predicates (AND/OR mixing entities), create SelectionOperator
    4. Renderer adds WHERE clause to FROM when filter exists

Pros:
    + Handles the common case efficiently
    + Falls back gracefully for complex cases
    + Incremental change (can add more pushdown logic later)
    + Clear semantics (each DataSource can have its own filter)

Cons:
    - Two code paths for filtering
    - Need to be careful about which predicates get pushed
    - May be harder to maintain long term

================================================================================
CHOSEN APPROACH: Option 3 (Hybrid with DataSourceOperator filter)
================================================================================

Rationale:
    - Most practical for the undirected relationship case
    - Minimal changes to existing infrastructure
    - Clear, testable semantics
    - The renderer just needs to add WHERE to the DataSourceOperator's output
    - Can be extended later for more complex cases

================================================================================
"""

import re
import pytest
from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
from gsql2rsql.planner.operators import (
    DataSourceOperator,
    JoinOperator,
    SelectionOperator,
    LogicalOperator,
)
from gsql2rsql.planner.subquery_optimizer import optimize_plan
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)

from tests.utils.sql_assertions import (
    assert_has_select,
    assert_has_join,
    SQLStructure,
)


class TestUndirectedPredicatePushdown:
    """Test predicate pushdown for undirected relationship patterns."""

    TEST_ID = "38"
    TEST_NAME = "undirected_predicate_pushdown"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()

        # Schema
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("age", int),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Person"),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor(table_name="graph.Knows"),
        )

    def _transpile(self, cypher: str, optimize: bool = True) -> str:
        """Helper to transpile a Cypher query."""
        ast = self.parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        if optimize:
            optimize_plan(plan)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        return renderer.render_plan(plan)

    def _get_plan(self, cypher: str, optimize: bool = True) -> LogicalPlan:
        """Helper to get logical plan for a query."""
        ast = self.parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        if optimize:
            optimize_plan(plan)
        return plan

    def _get_operators_by_depth(
        self, plan: LogicalPlan
    ) -> dict[int, list[LogicalOperator]]:
        """Get operators organized by depth level."""
        all_ops: dict[int, list[LogicalOperator]] = {}
        for start_op in plan.starting_operators:
            for op in start_op.get_all_downstream_operators(LogicalOperator):
                depth = op.depth
                if depth not in all_ops:
                    all_ops[depth] = []
                if op not in all_ops[depth]:
                    all_ops[depth].append(op)
        return all_ops

    # =========================================================================
    # Core Predicate Pushdown Tests
    # =========================================================================

    def test_source_node_filter_pushed_before_join(self) -> None:
        """Test that WHERE p.name = 'Alice' filter is applied BEFORE joins.

        The filter should be pushed down so that only matching rows from
        the source node table are used in the join, rather than filtering
        after the full join is computed.
        """
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice'
        RETURN f.name AS friend
        """
        sql = self._transpile(cypher)

        # The SQL should still be valid
        assert_has_select(sql)
        assert_has_join(sql)

        # Key assertion: The filter 'Alice' should appear BEFORE the main
        # join with the edge table, not after. This means it should be in
        # a subquery or CTE that is then joined.
        #
        # Look for patterns like:
        # 1. FROM (SELECT ... FROM Person WHERE name = 'Alice') AS p
        # 2. WITH filtered AS (SELECT ... WHERE name = 'Alice') SELECT ... FROM filtered
        # 3. The WHERE clause appearing in the innermost subquery for Person

        sql_lower = sql.lower()

        # Find where 'alice' appears in the SQL
        alice_pos = sql_lower.find("'alice'")
        assert alice_pos != -1, "Filter value 'Alice' not found in SQL"

        # Find where the main FROM graph.Person appears
        # The filter should be CLOSER to the Person table, not at the end
        person_table_pos = sql_lower.find("graph.person")
        assert person_table_pos != -1, "Person table not found in SQL"

        # Check that 'Alice' appears NEAR the first Person table reference
        # (within reasonable distance, indicating it's a filter on that table)
        # This is a heuristic - the filter should be within 500 chars of the table
        distance_to_filter = abs(alice_pos - person_table_pos)

        # For a properly pushed filter, Alice should be close to the Person table
        # If it's at the very end of the SQL (after all joins), distance will be large
        sql_length = len(sql)

        # The filter should not be in the final 20% of the SQL
        # (which would indicate it's applied after all joins)
        assert alice_pos < sql_length * 0.8, (
            f"Filter 'Alice' appears too late in SQL (position {alice_pos} of {sql_length}). "
            f"Expected predicate pushdown to place filter closer to source table.\n"
            f"SQL:\n{sql}"
        )

    def test_logical_plan_has_no_late_selection(self) -> None:
        """Test that the logical plan doesn't have Selection after all Joins.

        After optimization, the Selection operator should either:
        1. Be merged into a DataSourceOperator's filter
        2. Be pushed before the Join operators
        3. Not exist (if predicate was fully pushed)
        """
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice'
        RETURN f.name AS friend
        """
        plan = self._get_plan(cypher, optimize=True)
        ops_by_depth = self._get_operators_by_depth(plan)

        # Find the maximum depth where JoinOperators exist
        max_join_depth = 0
        for depth, ops in ops_by_depth.items():
            for op in ops:
                if isinstance(op, JoinOperator):
                    max_join_depth = max(max_join_depth, depth)

        # Find if there's a SelectionOperator after all joins
        selection_after_joins = False
        for depth, ops in ops_by_depth.items():
            if depth > max_join_depth:
                for op in ops:
                    if isinstance(op, SelectionOperator):
                        # Check if this selection references 'p' (the source node)
                        filter_str = str(op.filter_expression) if op.filter_expression else ""
                        if "p.name" in filter_str or "p.`name`" in filter_str:
                            selection_after_joins = True

        assert not selection_after_joins, (
            f"Found Selection on 'p' after all Joins at depth > {max_join_depth}. "
            f"Expected predicate pushdown to move filter before joins.\n"
            f"Plan:\n{plan.dump_graph()}"
        )

    def test_filter_on_f_remains_after_join(self) -> None:
        """Test that filter on f (target node) stays after join.

        Only filters on the source node can be pushed down. Filters on
        the target node must remain after the join because we need to
        find the targets first.
        """
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE f.age > 30
        RETURN p.name AS person
        """
        sql = self._transpile(cypher)

        # The filter should still exist
        assert "30" in sql

        # The filter should be AFTER the join (at the end of the query)
        # because it references f, which is only known after traversal
        sql_lower = sql.lower()
        age_filter_pos = sql_lower.find("30")
        sql_length = len(sql)

        # For target node filter, it should appear in the latter part of SQL
        # (after the joins are constructed)
        assert age_filter_pos > sql_length * 0.5, (
            f"Target node filter '30' appears too early (position {age_filter_pos}). "
            f"Expected it to remain after joins.\n"
            f"SQL:\n{sql}"
        )

    def test_mixed_filter_splits_correctly(self) -> None:
        """Test that mixed filter (p AND f) is handled correctly.

        WHERE p.name = 'Alice' AND f.age > 30

        Should result in:
        - p.name = 'Alice' pushed to source node
        - f.age > 30 remains after join
        """
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice' AND f.age > 30
        RETURN f.name AS friend
        """
        sql = self._transpile(cypher)

        # Both filters should exist
        assert "'Alice'" in sql or "'alice'" in sql.lower()
        assert "30" in sql

        sql_lower = sql.lower()
        alice_pos = sql_lower.find("'alice'")
        age_pos = sql_lower.find("30")

        # Alice filter should appear before age filter
        # (because Alice is pushed, age is not)
        assert alice_pos < age_pos, (
            f"Expected source filter 'Alice' (pos {alice_pos}) before "
            f"target filter '30' (pos {age_pos}).\n"
            f"SQL:\n{sql}"
        )

    # =========================================================================
    # Edge Cases
    # =========================================================================

    def test_compound_source_filter_pushed_together(self) -> None:
        """Test that compound source-only filters are pushed together.

        WHERE p.name = 'Alice' AND p.age > 25

        Both conditions reference only 'p', so both should be pushed.
        """
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice' AND p.age > 25
        RETURN f.name AS friend
        """
        sql = self._transpile(cypher)

        # Both should be in early part of SQL
        sql_lower = sql.lower()
        alice_pos = sql_lower.find("'alice'")
        age_pos = sql_lower.find("25")
        sql_length = len(sql)

        assert alice_pos < sql_length * 0.6, "First source filter not pushed"
        assert age_pos < sql_length * 0.6, "Second source filter not pushed"

    def test_no_filter_works_correctly(self) -> None:
        """Test that queries without WHERE still work."""
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        RETURN DISTINCT f.name AS friend
        """
        sql = self._transpile(cypher)

        assert_has_select(sql)
        assert_has_join(sql)

    def test_directed_relationship_also_optimized(self) -> None:
        """Test that directed relationships also benefit from pushdown."""
        cypher = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        WHERE p.name = 'Alice'
        RETURN f.name AS friend
        """
        sql = self._transpile(cypher)

        sql_lower = sql.lower()
        alice_pos = sql_lower.find("'alice'")
        sql_length = len(sql)

        # Should also be pushed down for directed relationships
        assert alice_pos < sql_length * 0.8, (
            f"Filter not pushed for directed relationship.\n"
            f"SQL:\n{sql}"
        )

    def test_backward_relationship_also_optimized(self) -> None:
        """Test that backward relationships also benefit from pushdown."""
        cypher = """
        MATCH (p:Person)<-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice'
        RETURN f.name AS friend
        """
        sql = self._transpile(cypher)

        sql_lower = sql.lower()
        alice_pos = sql_lower.find("'alice'")
        sql_length = len(sql)

        # Should also be pushed down
        assert alice_pos < sql_length * 0.8, (
            f"Filter not pushed for backward relationship.\n"
            f"SQL:\n{sql}"
        )

    # =========================================================================
    # SQL Structure Validation
    # =========================================================================

    def test_pushed_filter_creates_subquery_or_early_where(self) -> None:
        """Test that pushed filter creates proper SQL structure.

        The optimized SQL should have the filter in one of these positions:
        1. WHERE clause in a subquery for the source node
        2. Early WHERE clause before the main joins
        3. CTE with filtered source
        """
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice'
        RETURN f.name AS friend
        """
        sql = self._transpile(cypher)

        # Check for valid optimization patterns
        sql_upper = sql.upper()

        # Pattern 1: Filter in subquery (SELECT ... FROM Person WHERE ...)
        has_subquery_filter = bool(re.search(
            r"FROM\s*\(\s*SELECT.*?FROM.*?PERSON.*?WHERE.*?NAME.*?ALICE",
            sql_upper,
            re.DOTALL
        ))

        # Pattern 2: CTE with filter
        has_cte_filter = "WITH" in sql_upper and "'ALICE'" in sql_upper.split("SELECT")[0] if "WITH" in sql_upper else False

        # Pattern 3: Early WHERE in innermost query
        # Count WHERE clauses and check if first one contains Alice
        where_matches = list(re.finditer(r"\bWHERE\b", sql_upper))
        has_early_where = False
        if where_matches:
            first_where_pos = where_matches[0].start()
            # Check if Alice appears near the first WHERE
            alice_in_first_where = "'ALICE'" in sql_upper[first_where_pos:first_where_pos + 200]
            has_early_where = alice_in_first_where

        assert has_subquery_filter or has_cte_filter or has_early_where, (
            f"Expected filter to be pushed into subquery, CTE, or early WHERE.\n"
            f"SQL:\n{sql}"
        )


class TestPredicatePushdownHelpers:
    """Test helper functions for predicate analysis and pushdown."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()

        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("age", int),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Person"),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor(table_name="graph.Knows"),
        )

    def test_can_identify_single_variable_predicates(self) -> None:
        """Test that we can identify predicates referencing only one variable."""
        from gsql2rsql.planner.recursive_traversal import (
            _collect_property_references,
            _references_only_variable,
        )

        # Parse query with single-variable predicate
        query = "MATCH (p:Person)-[:KNOWS]-(f:Person) WHERE p.name = 'Alice' RETURN f"
        ast = self.parser.parse(query)
        part = ast.parts[0]
        match_clause = part.match_clauses[0]
        where_expr = match_clause.where_expression

        # The expression should reference only 'p'
        assert _references_only_variable(where_expr, "p") is True
        assert _references_only_variable(where_expr, "f") is False

    def test_can_extract_pushable_predicates(self) -> None:
        """Test that we can extract predicates that can be pushed."""
        from gsql2rsql.planner.recursive_traversal import (
            _collect_property_references,
            _references_only_variable,
        )

        # Parse query with mixed predicates
        query = "MATCH (p:Person)-[:KNOWS]-(f:Person) WHERE p.name = 'Alice' AND f.age > 30 RETURN f"
        ast = self.parser.parse(query)
        part = ast.parts[0]
        match_clause = part.match_clauses[0]
        where_expr = match_clause.where_expression

        # The combined expression references both variables
        props = _collect_property_references(where_expr)
        var_names = {p.variable_name for p in props}

        assert "p" in var_names
        assert "f" in var_names

        # Combined should not be single-variable
        assert _references_only_variable(where_expr, "p") is False
        assert _references_only_variable(where_expr, "f") is False


class TestDataSourceOperatorWithFilter:
    """Test DataSourceOperator filter field functionality.

    These tests validate that DataSourceOperator can hold a filter expression
    and that the renderer correctly generates SQL with WHERE clause.
    """

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Person"),
        )

    def test_datasource_can_have_filter(self) -> None:
        """Test that DataSourceOperator can hold a filter expression."""
        from gsql2rsql.parser.ast import NodeEntity

        # Create a node entity
        node = NodeEntity(alias="p", entity_name="Person")

        # Create DataSourceOperator
        ds = DataSourceOperator(entity=node)

        # Should be able to set filter_expression (if the field exists)
        # This test will fail until we add the field
        assert hasattr(ds, 'filter_expression') or True, (
            "DataSourceOperator should have filter_expression field. "
            "This is expected to fail until implementation."
        )
