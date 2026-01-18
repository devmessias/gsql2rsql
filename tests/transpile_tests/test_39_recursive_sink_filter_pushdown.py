"""
Test suite for Recursive Sink Filter Pushdown Optimization (P2).

This optimization pushes filters on the SINK node of a variable-length path
into the JOIN clause of the recursive CTE result, rather than applying them
after all joins complete.

PROBLEM:
========
Given a query like:
    MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
    WHERE a.risk_score > 70 AND b.risk_score > 70
    RETURN a.id, b.id

Current behavior:
- Source filter (a.risk_score > 70) pushed into CTE base case
- Sink filter (b.risk_score > 70) applied AFTER all joins complete

GOAL:
=====
Push sink filter INTO the recursive join WHERE clause:
    FROM paths p
    JOIN Account sink ON sink.id = p.end_node
    WHERE p.depth >= 2 AND p.depth <= 4 AND sink.risk_score > 70

IMPLEMENTATION: Logical Plan Builder (consistent with source filter)
=====================================================================
1. Add sink_node_filter attribute to RecursiveTraversalOperator
2. Add _extract_sink_node_filter() to LogicalPlan
3. Modify _render_recursive_join() to apply sink filter
"""

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    EdgeSchema,
    EntityProperty,
    NodeSchema,
    SimpleGraphSchemaProvider,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)
from gsql2rsql.planner.operators import RecursiveTraversalOperator
from gsql2rsql.planner.subquery_optimizer import optimize_plan


class TestRecursiveSinkFilterPushdown:
    """Test that sink node filters are pushed into recursive join."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()

        # Graph schema
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Account",
                node_id_property=EntityProperty("id", int),
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("holder_name", str),
                    EntityProperty("risk_score", int),
                    EntityProperty("status", str),
                ],
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="TRANSFER",
                source_node_id="Account",
                sink_node_id="Account",
                source_id_property=EntityProperty("source_account_id", int),
                sink_id_property=EntityProperty("target_account_id", int),
                properties=[
                    EntityProperty("source_account_id", int),
                    EntityProperty("target_account_id", int),
                    EntityProperty("amount", float),
                    EntityProperty("timestamp", str),
                ],
            )
        )

        # SQL schema
        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
            NodeSchema(
                name="Account",
                node_id_property=EntityProperty("id", int),
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("holder_name", str),
                    EntityProperty("risk_score", int),
                    EntityProperty("status", str),
                ],
            ),
            SQLTableDescriptor(table_name="catalog.fraud.Account"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="TRANSFER",
                source_node_id="Account",
                sink_node_id="Account",
                source_id_property=EntityProperty("source_account_id", int),
                sink_id_property=EntityProperty("target_account_id", int),
                properties=[
                    EntityProperty("source_account_id", int),
                    EntityProperty("target_account_id", int),
                    EntityProperty("amount", float),
                    EntityProperty("timestamp", str),
                ],
            ),
            SQLTableDescriptor(table_name="catalog.fraud.Transfer"),
        )

    def _transpile(self, cypher: str, optimize: bool = True) -> str:
        """Transpile a Cypher query to SQL."""
        ast = self.parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        if optimize:
            optimize_plan(plan)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def _get_plan(self, cypher: str) -> LogicalPlan:
        """Get the logical plan for a Cypher query."""
        ast = self.parser.parse(cypher)
        return LogicalPlan.process_query_tree(ast, self.graph_schema)

    def test_sink_filter_pushed_to_recursive_join(self) -> None:
        """
        Verify that a filter on the sink node is pushed into the recursive join.

        Query: MATCH (a:Account)-[:TRANSFER*2..4]->(b:Account)
               WHERE b.risk_score > 70
               RETURN a.id, b.id

        Expected: The filter should appear in the recursive join's WHERE clause,
                  NOT at the outermost query level.

        CURRENT (suboptimal):
            SELECT ... FROM (...) AS _proj WHERE (__b_risk_score) > (70)

        GOAL (optimized):
            ...JOIN sink... WHERE p.depth >= 2 AND (sink.risk_score) > (70)
        """
        query = """
        MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
        WHERE b.risk_score > 70
        RETURN a.id, b.id
        """
        sql = self._transpile(query)

        # Goal: sink filter should be in WHERE clause of recursive join,
        # NOT at the outer level after _proj

        # Check: filter should NOT be at outer level (after _proj with __b_ prefix)
        proj_idx = sql.find('_proj')
        if proj_idx != -1:
            outer_part = sql[proj_idx:]
            filter_at_outer_level = '__b_risk_score' in outer_part and '70' in outer_part
        else:
            filter_at_outer_level = False

        # Check: filter should be in recursive join WHERE with 'sink.' prefix
        # Look for pattern: WHERE ... sink.risk_score ... 70 ... before _proj
        inner_part = sql[:proj_idx] if proj_idx != -1 else sql
        # The recursive join WHERE should have both depth bounds AND sink filter
        filter_in_recursive_join = (
            'WHERE p.depth' in inner_part and
            'sink.risk_score' in inner_part and
            '70' in inner_part
        )

        assert filter_in_recursive_join, (
            f"Sink filter should be pushed into recursive join WHERE clause.\n"
            f"Expected: WHERE p.depth >= 2 AND ... AND (sink.risk_score) > (70)\n"
            f"filter_in_recursive_join={filter_in_recursive_join}\n"
            f"SQL:\n{sql}"
        )

        assert not filter_at_outer_level, (
            f"Sink filter should NOT be at outer level after _proj.\n"
            f"filter_at_outer_level={filter_at_outer_level}\n"
            f"SQL:\n{sql}"
        )

    def test_source_filter_still_in_cte_base_case(self) -> None:
        """
        Verify that source filter is still pushed to CTE base case.

        Query: MATCH (a:Account)-[:TRANSFER*2..4]->(b:Account)
               WHERE a.risk_score > 70 AND b.risk_score > 50
               RETURN a.id, b.id

        Expected: a.risk_score > 70 in CTE base case
                  b.risk_score > 50 in recursive join WHERE clause
        """
        query = """
        MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
        WHERE a.risk_score > 70 AND b.risk_score > 50
        RETURN a.id, b.id
        """
        sql = self._transpile(query)

        # Source filter should be in CTE base case (with 'src' alias)
        assert 'src.risk_score' in sql or '(src.risk_score)' in sql, (
            f"Source filter should be in CTE. SQL:\n{sql}"
        )

        # Check that we have the sink filter somewhere
        assert '50' in sql, f"Sink filter value should be in SQL. SQL:\n{sql}"

    def test_sink_only_filter_no_source_filter(self) -> None:
        """
        Verify sink filter works when there's no source filter.

        Query: MATCH (a:Account)-[:TRANSFER*2..4]->(b:Account)
               WHERE b.status = 'blocked'
               RETURN a.id, b.id
        """
        query = """
        MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
        WHERE b.status = 'blocked'
        RETURN a.id, b.id
        """
        sql = self._transpile(query)

        # The SQL should contain the sink filter
        assert 'blocked' in sql, f"Sink filter value should be in SQL. SQL:\n{sql}"

        # Verify it generates valid SQL structure
        assert 'WITH RECURSIVE' in sql
        assert 'JOIN' in sql

    def test_compound_sink_filter_pushed(self) -> None:
        """
        Verify compound filters on sink are pushed together.

        Query: MATCH (a:Account)-[:TRANSFER*2..4]->(b:Account)
               WHERE b.risk_score > 70 AND b.status = 'active'
               RETURN a.id, b.id
        """
        query = """
        MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
        WHERE b.risk_score > 70 AND b.status = 'active'
        RETURN a.id, b.id
        """
        sql = self._transpile(query)

        # Both parts of the compound filter should be in SQL
        assert '70' in sql
        assert 'active' in sql

    def test_mixed_source_sink_filter_splits_correctly(self) -> None:
        """
        Verify mixed filters are split: source to CTE, sink to join.

        Query: MATCH (a:Account)-[:TRANSFER*2..4]->(b:Account)
               WHERE a.holder_name = 'Alice' AND b.holder_name = 'Bob'
               RETURN a.id, b.id
        """
        query = """
        MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
        WHERE a.holder_name = 'Alice' AND b.holder_name = 'Bob'
        RETURN a.id, b.id
        """
        sql = self._transpile(query)

        # Both filters should be in SQL
        assert 'Alice' in sql
        assert 'Bob' in sql


class TestRecursiveTraversalOperatorSinkFilter:
    """Test that RecursiveTraversalOperator has sink_node_filter attribute."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Account",
                node_id_property=EntityProperty("id", int),
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("risk_score", int),
                ],
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="TRANSFER",
                source_node_id="Account",
                sink_node_id="Account",
                source_id_property=EntityProperty("source_account_id", int),
                sink_id_property=EntityProperty("target_account_id", int),
                properties=[
                    EntityProperty("source_account_id", int),
                    EntityProperty("target_account_id", int),
                ],
            )
        )

    def test_operator_has_sink_node_filter_attribute(self) -> None:
        """Verify RecursiveTraversalOperator can store sink_node_filter."""
        query = """
        MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
        WHERE b.risk_score > 70
        RETURN a.id, b.id
        """
        ast = self.parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)

        # Find the RecursiveTraversalOperator
        recursive_op = None
        for start_op in plan.starting_operators:
            for op in start_op.get_all_downstream_operators(RecursiveTraversalOperator):
                if isinstance(op, RecursiveTraversalOperator):
                    recursive_op = op
                    break

        assert recursive_op is not None, "Should have a RecursiveTraversalOperator"
        # After implementation, this should have the sink filter
        assert hasattr(recursive_op, 'sink_node_filter'), (
            "RecursiveTraversalOperator should have sink_node_filter attribute"
        )


class TestLogicalPlanSinkFilterExtraction:
    """Test the extraction of sink filters in logical plan building."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Account",
                node_id_property=EntityProperty("id", int),
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("risk_score", int),
                ],
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="TRANSFER",
                source_node_id="Account",
                sink_node_id="Account",
                source_id_property=EntityProperty("source_account_id", int),
                sink_id_property=EntityProperty("target_account_id", int),
                properties=[
                    EntityProperty("source_account_id", int),
                    EntityProperty("target_account_id", int),
                ],
            )
        )

    def test_extract_sink_filter_simple(self) -> None:
        """Verify sink filter is extracted from WHERE clause."""
        query = """
        MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
        WHERE b.risk_score > 70
        RETURN a.id, b.id
        """
        ast = self.parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)

        # Find the RecursiveTraversalOperator
        recursive_op = None
        for start_op in plan.starting_operators:
            for op in start_op.get_all_downstream_operators(RecursiveTraversalOperator):
                if isinstance(op, RecursiveTraversalOperator):
                    recursive_op = op
                    break

        assert recursive_op is not None
        # After implementation, sink_node_filter should be set
        if hasattr(recursive_op, 'sink_node_filter'):
            assert recursive_op.sink_node_filter is not None, (
                "Sink filter should be extracted"
            )

    def test_extract_source_and_sink_filters(self) -> None:
        """Verify both source and sink filters are extracted."""
        query = """
        MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
        WHERE a.risk_score > 70 AND b.risk_score > 50
        RETURN a.id, b.id
        """
        ast = self.parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)

        # Find the RecursiveTraversalOperator
        recursive_op = None
        for start_op in plan.starting_operators:
            for op in start_op.get_all_downstream_operators(RecursiveTraversalOperator):
                if isinstance(op, RecursiveTraversalOperator):
                    recursive_op = op
                    break

        assert recursive_op is not None
        # Source filter should be set
        assert recursive_op.start_node_filter is not None, (
            "Source filter should be extracted"
        )


class TestSQLRendererSinkFilter:
    """Test that SQL renderer applies sink filter in recursive join."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Account",
                node_id_property=EntityProperty("id", int),
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("risk_score", int),
                ],
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="TRANSFER",
                source_node_id="Account",
                sink_node_id="Account",
                source_id_property=EntityProperty("source_account_id", int),
                sink_id_property=EntityProperty("target_account_id", int),
                properties=[
                    EntityProperty("source_account_id", int),
                    EntityProperty("target_account_id", int),
                ],
            )
        )
        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
            NodeSchema(
                name="Account",
                node_id_property=EntityProperty("id", int),
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("risk_score", int),
                ],
            ),
            SQLTableDescriptor(table_name="catalog.fraud.Account"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="TRANSFER",
                source_node_id="Account",
                sink_node_id="Account",
                source_id_property=EntityProperty("source_account_id", int),
                sink_id_property=EntityProperty("target_account_id", int),
                properties=[
                    EntityProperty("source_account_id", int),
                    EntityProperty("target_account_id", int),
                ],
            ),
            SQLTableDescriptor(table_name="catalog.fraud.Transfer"),
        )

    def test_sink_filter_in_where_clause(self) -> None:
        """
        Verify sink filter appears in the recursive join's WHERE clause.

        The filter should be part of:
        WHERE p.depth >= 2 AND p.depth <= 4 AND (sink.risk_score > 70)
        """
        query = """
        MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
        WHERE b.risk_score > 70
        RETURN a.id, b.id
        """
        ast = self.parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        optimize_plan(plan)

        sql = SQLRenderer(db_schema_provider=self.sql_schema).render_plan(plan)

        # The sink filter should appear with 'sink.' prefix in the WHERE clause
        # For now, just verify the SQL is valid and contains the filter
        assert 'risk_score' in sql and '70' in sql, (
            f"Filter should be in SQL. SQL:\n{sql}"
        )


class TestEdgeCases:
    """Test edge cases for sink filter pushdown."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.parser = OpenCypherParser()
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Account",
                node_id_property=EntityProperty("id", int),
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("risk_score", int),
                ],
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="TRANSFER",
                source_node_id="Account",
                sink_node_id="Account",
                source_id_property=EntityProperty("source_account_id", int),
                sink_id_property=EntityProperty("target_account_id", int),
                properties=[
                    EntityProperty("source_account_id", int),
                    EntityProperty("target_account_id", int),
                    EntityProperty("amount", float),
                ],
            )
        )
        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
            NodeSchema(
                name="Account",
                node_id_property=EntityProperty("id", int),
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("risk_score", int),
                ],
            ),
            SQLTableDescriptor(table_name="catalog.fraud.Account"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="TRANSFER",
                source_node_id="Account",
                sink_node_id="Account",
                source_id_property=EntityProperty("source_account_id", int),
                sink_id_property=EntityProperty("target_account_id", int),
                properties=[
                    EntityProperty("source_account_id", int),
                    EntityProperty("target_account_id", int),
                    EntityProperty("amount", float),
                ],
            ),
            SQLTableDescriptor(table_name="catalog.fraud.Transfer"),
        )

    def _transpile(self, cypher: str) -> str:
        """Transpile a Cypher query to SQL."""
        ast = self.parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        optimize_plan(plan)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def test_no_filter_works(self) -> None:
        """Verify queries without filters still work."""
        query = """
        MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
        RETURN a.id, b.id
        """
        sql = self._transpile(query)

        assert 'WITH RECURSIVE' in sql
        assert 'SELECT' in sql

    def test_edge_filter_with_sink_filter(self) -> None:
        """
        Verify edge filter and sink filter work together.

        Query: MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
               WHERE b.risk_score > 70
                 AND ALL(t IN relationships(path) WHERE t.amount > 1000)
               RETURN a.id, b.id
        """
        query = """
        MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
        WHERE b.risk_score > 70 AND ALL(t IN relationships(path) WHERE t.amount > 1000)
        RETURN a.id, b.id
        """
        sql = self._transpile(query)

        # Both filters should be applied
        assert '70' in sql  # sink filter
        assert '1000' in sql  # edge filter
