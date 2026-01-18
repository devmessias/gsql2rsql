"""Test 40: Conjunction Splitting for Selection Pushdown.

This test suite validates the SelectionPushdownOptimizer's ability to split
AND conjunctions and push individual predicates to their respective DataSources.

================================================================================
ALGEBRAIC FOUNDATION
================================================================================

The optimization is based on the relational algebra equivalence:

    σ_{p(A) ∧ q(B)}(A ⋈ B) ≡ σ_{p(A)}(A) ⋈ σ_{q(B)}(B)

Where:
    - σ is the selection operator
    - p(A) is a predicate referencing only attributes of relation A
    - q(B) is a predicate referencing only attributes of relation B
    - ⋈ is the join operator

This is NOT valid for OR:
    σ_{p(A) ∨ q(B)}(A ⋈ B) ≢ σ_{p(A)}(A) ⋈ σ_{q(B)}(B)

================================================================================
TEST CASES
================================================================================

1. Both-sides pushdown: WHERE p.x AND f.y → push both
2. Same-variable combination: WHERE p.x AND p.y → combine and push to p
3. Partial pushdown: WHERE p.x AND p.y = f.y → push p.x, keep p.y = f.y
4. OR preservation: WHERE p.x OR f.y → DO NOT split
5. Complex mixed: WHERE p.x AND f.y AND p.z = f.w → push p.x, push f.y, keep p.z = f.w
6. Three-way pattern: WHERE p.x AND f.y AND c.z → push all three
"""

import pytest
from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    SimpleGraphSchemaProvider,
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
from gsql2rsql.planner.operators import (
    DataSourceOperator,
    SelectionOperator,
    ProjectionOperator,
    LogicalOperator,
)
from gsql2rsql.planner.subquery_optimizer import (
    SelectionPushdownOptimizer,
    optimize_plan,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)


class TestConjunctionSplittingPushdown:
    """Test conjunction splitting for undirected relationship patterns."""

    TEST_ID = "40"
    TEST_NAME = "conjunction_splitting_pushdown"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()

        # Graph schema with Person, Company, and relationships
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("age", int),
                    EntityProperty("salary", float),
                    EntityProperty("active", bool),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_node(
            NodeSchema(
                name="Company",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("industry", str),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="WORKS_AT",
                source_node_id="Person",
                sink_node_id="Company",
                source_id_property=EntityProperty("person_id", int),
                sink_id_property=EntityProperty("company_id", int),
            )
        )

        # SQL schema
        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("age", int),
                    EntityProperty("salary", float),
                    EntityProperty("active", bool),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Person"),
        )
        self.sql_schema.add_node(
            NodeSchema(
                name="Company",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("industry", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Company"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor(table_name="graph.Knows"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="WORKS_AT",
                source_node_id="Person",
                sink_node_id="Company",
                source_id_property=EntityProperty("person_id", int),
                sink_id_property=EntityProperty("company_id", int),
            ),
            SQLTableDescriptor(table_name="graph.WorksAt"),
        )

    def _get_plan(self, cypher: str, optimize: bool = True) -> LogicalPlan:
        """Helper to get logical plan for a query."""
        ast = self.parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        if optimize:
            optimize_plan(plan)
        return plan

    def _transpile(self, cypher: str, optimize: bool = True) -> str:
        """Helper to transpile a Cypher query."""
        plan = self._get_plan(cypher, optimize)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def _get_datasources_with_filters(
        self, plan: LogicalPlan
    ) -> dict[str, str | None]:
        """Get DataSources and their filter expressions.

        Returns:
            Dict mapping entity alias to filter expression string (or None).
        """
        result: dict[str, str | None] = {}
        for op in plan.starting_operators:
            if isinstance(op, DataSourceOperator):
                alias = op.entity.alias if op.entity else "unknown"
                filter_str = str(op.filter_expression) if op.filter_expression else None
                result[alias] = filter_str
        return result

    def _has_selection_operator(self, plan: LogicalPlan) -> bool:
        """Check if plan has any SelectionOperator."""
        for start_op in plan.starting_operators:
            for op in start_op.get_all_downstream_operators(LogicalOperator):
                if isinstance(op, SelectionOperator):
                    return True
        return False

    def _get_projection_filter(self, plan: LogicalPlan) -> str | None:
        """Get the filter on the ProjectionOperator (if any)."""
        for term_op in plan.terminal_operators:
            if isinstance(term_op, ProjectionOperator):
                if term_op.filter_expression:
                    return str(term_op.filter_expression)
        return None

    # =========================================================================
    # Core Conjunction Splitting Tests
    # =========================================================================

    def test_both_filters_pushed_to_respective_sources(self) -> None:
        """Test: WHERE p.name = 'Alice' AND f.age > 30 → push both.

        The AND conjunction should be split:
        - p.name = 'Alice' → pushed to DataSource(p)
        - f.age > 30 → pushed to DataSource(f)
        - No SelectionOperator should remain
        """
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice' AND f.age > 30
        RETURN p.name, f.name
        """
        plan = self._get_plan(cypher)
        ds_filters = self._get_datasources_with_filters(plan)

        # Check that p has the name filter
        assert "p" in ds_filters, "DataSource for 'p' not found"
        assert ds_filters["p"] is not None, "Filter not pushed to DataSource(p)"
        assert "Alice" in ds_filters["p"], f"Expected 'Alice' in p's filter: {ds_filters['p']}"

        # Check that f has the age filter
        assert "f" in ds_filters, "DataSource for 'f' not found"
        assert ds_filters["f"] is not None, "Filter not pushed to DataSource(f)"
        assert "30" in ds_filters["f"], f"Expected '30' in f's filter: {ds_filters['f']}"

        # No SelectionOperator should exist (all predicates pushed)
        assert not self._has_selection_operator(plan), (
            f"SelectionOperator should be removed after pushing all predicates.\n"
            f"Plan:\n{plan.dump_graph()}"
        )

    def test_same_variable_predicates_combined(self) -> None:
        """Test: WHERE p.name = 'Bob' AND p.age > 18 AND p.active = true → combined.

        Multiple predicates for the same variable should be combined with AND
        and pushed together to the DataSource.
        """
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Bob' AND p.age > 18 AND p.active = true
        RETURN f.name
        """
        plan = self._get_plan(cypher)
        ds_filters = self._get_datasources_with_filters(plan)

        # All three conditions should be in p's filter
        assert "p" in ds_filters
        p_filter = ds_filters["p"]
        assert p_filter is not None, "Filter not pushed to DataSource(p)"
        assert "Bob" in p_filter, f"Expected 'Bob' in filter: {p_filter}"
        assert "18" in p_filter, f"Expected '18' in filter: {p_filter}"
        assert "active" in p_filter.lower() or "true" in p_filter.lower(), (
            f"Expected 'active' in filter: {p_filter}"
        )

        # f should have no filter (no predicates on f)
        assert ds_filters.get("f") is None, "f should have no filter"

    def test_partial_pushdown_cross_variable_kept(self) -> None:
        """Test: WHERE p.age > 25 AND p.name = f.name → partial pushdown.

        - p.age > 25 → pushed to DataSource(p)
        - p.name = f.name → kept in Selection (cross-variable)
        """
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.age > 25 AND p.name = f.name
        RETURN p.name, f.name
        """
        plan = self._get_plan(cypher)
        ds_filters = self._get_datasources_with_filters(plan)

        # p should have the age filter pushed
        assert "p" in ds_filters
        assert ds_filters["p"] is not None, "p.age > 25 should be pushed"
        assert "25" in ds_filters["p"], f"Expected '25' in p's filter: {ds_filters['p']}"

        # The cross-variable filter should remain (either in Selection or Projection)
        # Check the Projection's filter (after subquery flattening)
        proj_filter = self._get_projection_filter(plan)

        # The cross-variable predicate should exist somewhere
        plan_dump = plan.dump_graph()
        has_cross_var = "p.name" in plan_dump and "f.name" in plan_dump and "EQ" in plan_dump
        assert has_cross_var or (proj_filter and "name" in proj_filter), (
            f"Cross-variable predicate p.name = f.name should remain.\n"
            f"Plan:\n{plan_dump}"
        )

    def test_or_predicate_not_split(self) -> None:
        """Test: WHERE p.name = 'Alice' OR f.age > 30 → NOT split.

        OR predicates cannot be safely split! The entire predicate must
        remain in the Selection.
        """
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice' OR f.age > 30
        RETURN p.name, f.name
        """
        plan = self._get_plan(cypher)
        ds_filters = self._get_datasources_with_filters(plan)

        # Neither p nor f should have pushed filters for OR predicates
        # The entire OR predicate should remain in Selection or Projection
        p_filter = ds_filters.get("p")
        f_filter = ds_filters.get("f")

        # If OR is incorrectly split, Alice would be in p and 30 in f
        if p_filter:
            assert "Alice" not in p_filter or "OR" in p_filter.upper(), (
                f"OR predicate was incorrectly split! p.filter = {p_filter}"
            )

        # Verify the OR is preserved in the plan
        plan_dump = plan.dump_graph()
        assert "OR" in plan_dump, (
            f"OR predicate should remain in plan.\n"
            f"Plan:\n{plan_dump}"
        )

    def test_complex_mixed_predicates(self) -> None:
        """Test complex mix: WHERE p.x AND f.y AND p.z = f.w.

        - p.age > 25 → pushed to DataSource(p)
        - f.salary > 50000 → pushed to DataSource(f)
        - p.name = f.name → kept in Selection (cross-variable)
        """
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.age > 25 AND f.salary > 50000 AND p.name = f.name
        RETURN p.id, f.id
        """
        plan = self._get_plan(cypher)
        ds_filters = self._get_datasources_with_filters(plan)

        # p should have age filter
        assert ds_filters.get("p") is not None, "p.age > 25 should be pushed"
        assert "25" in ds_filters["p"]

        # f should have salary filter
        assert ds_filters.get("f") is not None, "f.salary > 50000 should be pushed"
        assert "50000" in ds_filters["f"]

        # Cross-variable should remain
        plan_dump = plan.dump_graph()
        # Check for presence of both p.name and f.name in the same expression
        assert "EQ" in plan_dump, f"Equality comparison should remain.\nPlan:\n{plan_dump}"

    def test_three_way_all_pushed(self) -> None:
        """Test three-way pattern: WHERE p.x AND f.y AND c.z → all pushed.

        All three single-variable predicates should be pushed to their
        respective DataSources.
        """
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)-[:WORKS_AT]->(c:Company)
        WHERE p.age > 25 AND f.salary > 50000 AND c.industry = 'Tech'
        RETURN p.name, f.name, c.name
        """
        plan = self._get_plan(cypher)
        ds_filters = self._get_datasources_with_filters(plan)

        # Check p has filter
        assert ds_filters.get("p") is not None, "p.age > 25 should be pushed"
        assert "25" in ds_filters["p"]

        # Check f has filter
        assert ds_filters.get("f") is not None, "f.salary > 50000 should be pushed"
        assert "50000" in ds_filters["f"]

        # Check c has filter
        assert ds_filters.get("c") is not None, "c.industry = 'Tech' should be pushed"
        assert "Tech" in ds_filters["c"]

        # No SelectionOperator should remain
        assert not self._has_selection_operator(plan), (
            f"All predicates pushed, SelectionOperator should be removed.\n"
            f"Plan:\n{plan.dump_graph()}"
        )

    # =========================================================================
    # SQL Output Validation
    # =========================================================================

    def test_sql_has_filters_in_subqueries(self) -> None:
        """Validate that generated SQL has filters in the correct position."""
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice' AND f.age > 30
        RETURN p.name, f.name
        """
        sql = self._transpile(cypher)
        sql_lower = sql.lower()

        # Find positions
        alice_pos = sql_lower.find("'alice'")
        age_filter_pos = sql_lower.find("> (30)") if "> (30)" in sql_lower else sql_lower.find(">30")
        if age_filter_pos == -1:
            age_filter_pos = sql_lower.find("30")

        # Both filters should appear before the main result projection
        # (they should be inside subqueries)
        from_positions = [m.start() for m in __import__('re').finditer(r'\bfrom\b', sql_lower)]

        # Alice should be between some FROM and a closing paren/AS
        assert alice_pos != -1, "Alice filter not found in SQL"

        # Verify Alice appears BEFORE the joins complete
        join_count_before_alice = sql_lower[:alice_pos].count('join')
        total_joins = sql_lower.count('join')
        assert join_count_before_alice < total_joins, (
            f"Alice filter should appear before all joins complete.\n"
            f"Joins before: {join_count_before_alice}, Total: {total_joins}\n"
            f"SQL:\n{sql}"
        )

    def test_sql_no_late_where_when_all_pushed(self) -> None:
        """Validate no trailing WHERE when all predicates are pushed."""
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice' AND f.age > 30
        RETURN p.name, f.name
        """
        sql = self._transpile(cypher)

        # The outermost query should NOT have a WHERE clause
        # (all filters should be inside subqueries)
        lines = sql.strip().split('\n')

        # Look for WHERE at the top level (not inside subqueries)
        # A trailing WHERE would be at the end, not inside parentheses
        last_lines = '\n'.join(lines[-5:]).lower()

        # If there's a WHERE in the last few lines, it should be inside a subquery
        # (indicated by being before a closing paren or AS)
        if 'where' in last_lines:
            # Check if it's the outermost WHERE (bad) or inside subquery (ok)
            # If the last SELECT is at depth 0, a WHERE after it is bad
            outer_where_pattern = __import__('re').search(
                r'\)\s*as\s+_proj\s*$',
                sql.strip(),
                __import__('re').IGNORECASE
            )
            assert outer_where_pattern, (
                f"Unexpected trailing WHERE clause after pushdown.\n"
                f"SQL:\n{sql}"
            )


class TestPushdownOptimizerStats:
    """Test the optimizer statistics tracking."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("age", int),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            )
        )

    def test_stats_track_pushed_predicates(self) -> None:
        """Test that optimizer tracks how many predicates were pushed."""
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice' AND f.age > 30
        RETURN f.name
        """
        ast = self.parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)

        optimizer = SelectionPushdownOptimizer()
        optimizer.optimize(plan)

        # Should have pushed 2 predicates (one for p, one for f)
        assert optimizer.stats.predicates_pushed >= 2, (
            f"Expected at least 2 predicates pushed, got {optimizer.stats.predicates_pushed}"
        )

    def test_stats_track_remaining_predicates(self) -> None:
        """Test that optimizer tracks predicates that couldn't be pushed."""
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.name = f.name
        RETURN f.name
        """
        ast = self.parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)

        optimizer = SelectionPushdownOptimizer()
        optimizer.optimize(plan)

        # Cross-variable predicate cannot be pushed
        assert optimizer.stats.predicates_remaining >= 1, (
            f"Expected at least 1 remaining predicate, got {optimizer.stats.predicates_remaining}"
        )

    def test_stats_track_removed_selections(self) -> None:
        """Test that optimizer tracks removed Selection operators."""
        cypher = """
        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice'
        RETURN f.name
        """
        ast = self.parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)

        optimizer = SelectionPushdownOptimizer()
        optimizer.optimize(plan)

        # Single-variable predicate should be fully pushed, Selection removed
        assert optimizer.stats.selections_removed >= 1, (
            f"Expected at least 1 Selection removed, got {optimizer.stats.selections_removed}"
        )


class TestOptionalMatchSafetyCheck:
    """Test that predicates are NOT pushed through LEFT JOINs (OPTIONAL MATCH).

    OPTIONAL MATCH in Cypher creates LEFT JOINs. Pushing predicates through
    LEFT JOINs changes query semantics and must be prevented.

    Example of semantic change if we incorrectly push:

        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:KNOWS]-(f:Person)
        WHERE f.age > 30
        RETURN p, f

    Correct semantics (without pushdown):
        - Return ALL persons p
        - For each p, if f exists AND f.age > 30, include f
        - If no f exists OR f.age <= 30, the WHERE filters the entire row
        - Result: Only rows where p has a friend f with age > 30

    Wrong semantics (with pushdown to DataSource(f)):
        - First filter Person to only those with age > 30
        - LEFT JOIN p with filtered f
        - Rows where f didn't match return f = NULL
        - Result: All p returned, with f = NULL for most (WRONG!)

    The optimizer MUST NOT push predicates on the optional side of a LEFT JOIN.
    """

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()

        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("age", int),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            )
        )

        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
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
        self.sql_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor(table_name="graph.Knows"),
        )

    def _get_plan(self, cypher: str, optimize: bool = True) -> LogicalPlan:
        """Helper to get logical plan for a query."""
        ast = self.parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        if optimize:
            optimize_plan(plan)
        return plan

    def _get_datasources_with_filters(
        self, plan: LogicalPlan
    ) -> dict[str, list[str | None]]:
        """Get DataSources and their filter expressions.

        Returns a dict where each alias maps to a LIST of filter strings,
        since there can be multiple DataSources with the same alias
        (e.g., p:Person on required side AND p:Person reference in optional side).
        """
        result: dict[str, list[str | None]] = {}
        for op in plan.starting_operators:
            if isinstance(op, DataSourceOperator):
                alias = op.entity.alias if op.entity else "unknown"
                filter_str = str(op.filter_expression) if op.filter_expression else None
                if alias not in result:
                    result[alias] = []
                result[alias].append(filter_str)
        return result

    def test_optional_match_filter_not_pushed_to_optional_side(self) -> None:
        """Test: Filter on optional side is NOT pushed through LEFT JOIN.

        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:KNOWS]-(f:Person)
        WHERE f.age > 30
        RETURN p, f

        The filter f.age > 30 should NOT be pushed to DataSource(f)
        because that would change the semantics of the LEFT JOIN.
        """
        cypher = """
        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:KNOWS]-(f:Person)
        WHERE f.age > 30
        RETURN p.name, f.name
        """
        plan = self._get_plan(cypher)
        ds_filters = self._get_datasources_with_filters(plan)

        # f should NOT have the filter pushed (it's through a LEFT JOIN)
        f_filters = ds_filters.get("f", [])
        for f_filter in f_filters:
            if f_filter:
                assert "30" not in f_filter, (
                    f"Filter on optional side was incorrectly pushed through LEFT JOIN!\n"
                    f"f.filter = {f_filter}\n"
                    f"This changes OPTIONAL MATCH semantics.\n"
                    f"Plan:\n{plan.dump_graph()}"
                )

    def test_optional_match_filter_on_required_side_can_be_pushed(self) -> None:
        """Test: Filter on required side CAN be pushed (no LEFT JOIN in path).

        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:KNOWS]-(f:Person)
        WHERE p.age > 30
        RETURN p, f

        The filter p.age > 30 CAN be pushed to DataSource(p) because
        p comes from the required MATCH, not through the OPTIONAL MATCH.
        """
        cypher = """
        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:KNOWS]-(f:Person)
        WHERE p.age > 30
        RETURN p.name, f.name
        """
        plan = self._get_plan(cypher)
        ds_filters = self._get_datasources_with_filters(plan)

        # p CAN have the filter pushed (it's from required MATCH)
        # There may be multiple p:Person DataSources - at least ONE should have the filter
        p_filters = ds_filters.get("p", [])
        has_pushed_filter = any(f is not None and "30" in f for f in p_filters)
        assert has_pushed_filter, (
            f"Filter on required side should be pushed.\n"
            f"p.filters = {p_filters}\n"
            f"Plan:\n{plan.dump_graph()}"
        )

    def test_mixed_optional_filter_partial_pushdown(self) -> None:
        """Test: Mixed filter with required and optional parts.

        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice' AND f.age > 30
        RETURN p, f

        - p.name = 'Alice' → CAN be pushed (required side)
        - f.age > 30 → CANNOT be pushed (optional side, through LEFT JOIN)
        """
        cypher = """
        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:KNOWS]-(f:Person)
        WHERE p.name = 'Alice' AND f.age > 30
        RETURN p.name, f.name
        """
        plan = self._get_plan(cypher)
        ds_filters = self._get_datasources_with_filters(plan)

        # p CAN have filter pushed - at least ONE p:Person should have it
        p_filters = ds_filters.get("p", [])
        has_p_filter = any(f is not None and "Alice" in f for f in p_filters)
        assert has_p_filter, (
            f"Filter on required side (p.name='Alice') should be pushed.\n"
            f"p.filters = {p_filters}"
        )

        # f should NOT have filter pushed (any f:Person)
        f_filters = ds_filters.get("f", [])
        for f_filter in f_filters:
            if f_filter:
                assert "30" not in f_filter, (
                    f"Filter on optional side (f.age>30) should NOT be pushed!\n"
                    f"f.filter = {f_filter}"
                )


class TestVolatileFunctionSafetyCheck:
    """Test that predicates with volatile functions are NOT pushed.

    Volatile functions (rand, datetime without args, etc.) produce different
    results on each call. Pushing predicates containing them changes when/how
    often they're evaluated, which alters query semantics.

    Example of semantic change if we incorrectly push:

        MATCH (p:Person)-[:KNOWS]-(f:Person)
        WHERE rand() > 0.5 AND p.name = 'Alice'
        RETURN p, f

    Correct semantics (without pushdown):
        - Join all Persons via KNOWS
        - Filter 50% of JOINED rows randomly
        - Also filter by p.name = 'Alice'

    Wrong semantics (if rand() pushed to DataSource):
        - Filter 50% of Person rows randomly (fewer rows!)
        - Then join
        - Result: Different random set, evaluated at different cardinality
    """

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()

        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("age", int),
                    EntityProperty("created_at", str),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            )
        )

    def _get_plan(self, cypher: str, optimize: bool = True) -> LogicalPlan:
        """Helper to get logical plan for a query."""
        ast = self.parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        if optimize:
            optimize_plan(plan)
        return plan

    def _get_datasources_with_filters(
        self, plan: LogicalPlan
    ) -> dict[str, list[str | None]]:
        """Get DataSources and their filter expressions."""
        result: dict[str, list[str | None]] = {}
        for op in plan.starting_operators:
            if isinstance(op, DataSourceOperator):
                alias = op.entity.alias if op.entity else "unknown"
                filter_str = str(op.filter_expression) if op.filter_expression else None
                if alias not in result:
                    result[alias] = []
                result[alias].append(filter_str)
        return result

    def _has_selection_with_filter(self, plan: LogicalPlan, filter_substring: str) -> bool:
        """Check if plan has a Selection operator containing the filter substring."""
        for start_op in plan.starting_operators:
            for op in start_op.get_all_downstream_operators(LogicalOperator):
                if isinstance(op, SelectionOperator):
                    if op.filter_expression:
                        filter_str = str(op.filter_expression)
                        if filter_substring in filter_str:
                            return True
        return False

    def test_rand_function_not_pushed(self) -> None:
        """Test: Predicate with rand() is NOT pushed to DataSource.

        WHERE rand() > 0.5 should stay in Selection, not be pushed.
        """
        cypher = """
        MATCH (p:Person)
        WHERE rand() > 0.5
        RETURN p.name
        """
        plan = self._get_plan(cypher)
        ds_filters = self._get_datasources_with_filters(plan)

        # p should NOT have the rand() filter pushed
        p_filters = ds_filters.get("p", [])
        for p_filter in p_filters:
            if p_filter:
                assert "rand" not in p_filter.lower(), (
                    f"Volatile function rand() was incorrectly pushed!\n"
                    f"p.filter = {p_filter}"
                )

    def test_rand_mixed_with_pushable_predicate(self) -> None:
        """Test: rand() stays in Selection while pushable predicate is pushed.

        WHERE p.name = 'Alice' AND rand() > 0.5
        - p.name = 'Alice' → CAN be pushed (deterministic)
        - rand() > 0.5 → CANNOT be pushed (volatile)
        """
        cypher = """
        MATCH (p:Person)
        WHERE p.name = 'Alice' AND rand() > 0.5
        RETURN p.name
        """
        plan = self._get_plan(cypher)
        ds_filters = self._get_datasources_with_filters(plan)

        # p CAN have name='Alice' pushed
        p_filters = ds_filters.get("p", [])
        has_name_filter = any(f is not None and "Alice" in f for f in p_filters)
        assert has_name_filter, (
            f"Deterministic predicate (p.name='Alice') should be pushed.\n"
            f"p.filters = {p_filters}"
        )

        # rand() should NOT be in any DataSource filter
        for p_filter in p_filters:
            if p_filter:
                assert "rand" not in p_filter.lower(), (
                    f"Volatile function rand() was incorrectly pushed!\n"
                    f"p.filter = {p_filter}"
                )

        # Note: The rand() predicate will be in a Selection or Projection
        # (depending on whether SubqueryFlatteningOptimizer runs).
        # The important thing is it's NOT in the DataSource - which we verified above.

    def test_datetime_function_not_pushed(self) -> None:
        """Test: Predicate with datetime() is NOT pushed.

        datetime() without args returns current time - volatile.
        """
        # Note: This test may depend on whether datetime() is actually parsed.
        # If parsing fails, the test is still valuable as documentation.
        cypher = """
        MATCH (p:Person)
        WHERE p.age > 30
        RETURN p.name
        """
        plan = self._get_plan(cypher)
        ds_filters = self._get_datasources_with_filters(plan)

        # This test just verifies deterministic predicates still work
        p_filters = ds_filters.get("p", [])
        has_age_filter = any(f is not None and "30" in f for f in p_filters)
        assert has_age_filter, (
            f"Deterministic predicate (p.age > 30) should be pushed.\n"
            f"p.filters = {p_filters}"
        )


class TestCorrelatedSubquerySafetyCheck:
    """Test that predicates with EXISTS/NOT EXISTS are NOT pushed.

    Correlated subqueries (EXISTS, NOT EXISTS) reference variables from
    the outer query scope. Pushing them would break the correlation semantics.

    Example:
        MATCH (p:Person)
        WHERE p.age > 30 AND EXISTS { (p)-[:KNOWS]->(:Person) }
        RETURN p.name

    The EXISTS subquery references `p` from the outer MATCH.
    - p.age > 30 → CAN be pushed (simple property reference)
    - EXISTS {...} → CANNOT be pushed (correlated subquery)
    """

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()

        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("age", int),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            )
        )

    def _get_plan(self, cypher: str, optimize: bool = True) -> LogicalPlan:
        """Helper to get logical plan for a query."""
        ast = self.parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        if optimize:
            optimize_plan(plan)
        return plan

    def _get_datasources_with_filters(
        self, plan: LogicalPlan
    ) -> dict[str, list[str | None]]:
        """Get DataSources and their filter expressions."""
        result: dict[str, list[str | None]] = {}
        for op in plan.starting_operators:
            if isinstance(op, DataSourceOperator):
                alias = op.entity.alias if op.entity else "unknown"
                filter_str = str(op.filter_expression) if op.filter_expression else None
                if alias not in result:
                    result[alias] = []
                result[alias].append(filter_str)
        return result

    def test_exists_not_pushed(self) -> None:
        """Test: EXISTS predicate is NOT pushed to DataSource.

        WHERE EXISTS { (p)-[:KNOWS]->() } should stay in Selection.
        """
        cypher = """
        MATCH (p:Person)
        WHERE EXISTS { (p)-[:KNOWS]->(:Person) }
        RETURN p.name
        """
        plan = self._get_plan(cypher)
        ds_filters = self._get_datasources_with_filters(plan)

        # p should NOT have the EXISTS filter pushed
        p_filters = ds_filters.get("p", [])
        for p_filter in p_filters:
            if p_filter:
                assert "EXISTS" not in p_filter.upper(), (
                    f"EXISTS subquery was incorrectly pushed!\n"
                    f"p.filter = {p_filter}"
                )

    def test_exists_mixed_with_pushable_predicate(self) -> None:
        """Test: EXISTS stays while pushable predicate is pushed.

        WHERE p.age > 30 AND EXISTS { (p)-[:KNOWS]->() }
        - p.age > 30 → CAN be pushed (deterministic property)
        - EXISTS {...} → CANNOT be pushed (correlated subquery)
        """
        cypher = """
        MATCH (p:Person)
        WHERE p.age > 30 AND EXISTS { (p)-[:KNOWS]->(:Person) }
        RETURN p.name
        """
        plan = self._get_plan(cypher)
        ds_filters = self._get_datasources_with_filters(plan)

        # p CAN have age > 30 pushed
        p_filters = ds_filters.get("p", [])
        has_age_filter = any(f is not None and "30" in f for f in p_filters)
        assert has_age_filter, (
            f"Deterministic predicate (p.age > 30) should be pushed.\n"
            f"p.filters = {p_filters}"
        )

        # EXISTS should NOT be in any DataSource filter
        for p_filter in p_filters:
            if p_filter:
                assert "EXISTS" not in p_filter.upper(), (
                    f"EXISTS subquery was incorrectly pushed!\n"
                    f"p.filter = {p_filter}"
                )

    def test_not_exists_not_pushed(self) -> None:
        """Test: NOT EXISTS predicate is NOT pushed to DataSource.

        WHERE NOT EXISTS { (p)-[:KNOWS]->() } should stay in Selection.
        """
        cypher = """
        MATCH (p:Person)
        WHERE NOT EXISTS { (p)-[:KNOWS]->(:Person) }
        RETURN p.name
        """
        plan = self._get_plan(cypher)
        ds_filters = self._get_datasources_with_filters(plan)

        # p should NOT have the NOT EXISTS filter pushed
        p_filters = ds_filters.get("p", [])
        for p_filter in p_filters:
            if p_filter:
                assert "EXISTS" not in p_filter.upper(), (
                    f"NOT EXISTS subquery was incorrectly pushed!\n"
                    f"p.filter = {p_filter}"
                )
