"""Test 44: MATCH after aggregating WITH pattern support.

Validates that the transpiler correctly supports queries where
MATCH clauses follow a WITH clause that contains aggregation functions.

This feature creates an AggregationBoundaryOperator that materializes
the aggregated result as a CTE, allowing subsequent MATCHes to join with it.
"""

import pytest

from gsql2rsql import OpenCypherParser, LogicalPlan
from gsql2rsql.common.schema import (
    SimpleGraphSchemaProvider,
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
from gsql2rsql.planner.operators import (
    AggregationBoundaryOperator,
    JoinOperator,
    ProjectionOperator,
)
from gsql2rsql.renderer.sql_renderer import SQLRenderer
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)


class TestMatchAfterAggregatingWith:
    """Test MATCH after aggregating WITH pattern support."""

    TEST_ID = "44"
    TEST_NAME = "match_after_aggregating_with"

    def setup_method(self) -> None:
        """Set up test fixtures."""
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
        self.graph_schema.add_node(
            NodeSchema(
                name="City",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
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
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="LIVES_IN",
                source_node_id="Person",
                sink_node_id="City",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="WORKS_AT",
                source_node_id="Person",
                sink_node_id="Company",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            )
        )

        # Set up SQL schema for rendering tests
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
            SQLTableDescriptor.from_table_name(
                "Person", "graph.Person", node_id_columns=["id"]
            ),
        )
        self.sql_schema.add_node(
            NodeSchema(
                name="City",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor.from_table_name(
                "City", "graph.City", node_id_columns=["id"]
            ),
        )
        self.sql_schema.add_node(
            NodeSchema(
                name="Company",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor.from_table_name(
                "Company", "graph.Company", node_id_columns=["id"]
            ),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="LIVES_IN",
                source_node_id="Person",
                sink_node_id="City",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor.from_table_name(
                "LIVES_IN:Person->City", "graph.Lives_In"
            ),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="WORKS_AT",
                source_node_id="Person",
                sink_node_id="Company",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor.from_table_name(
                "WORKS_AT:Person->Company", "graph.Works_At"
            ),
        )

    def _parse(self, cypher: str):
        """Parse a Cypher query to AST."""
        parser = OpenCypherParser()
        return parser.parse(cypher)

    def test_simple_match_after_count_creates_boundary(self) -> None:
        """Test that MATCH after COUNT creates an AggregationBoundaryOperator."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COUNT(p) AS population
        MATCH (c)<-[:LIVES_IN]-(worker:Person)-[:WORKS_AT]->(company:Company)
        RETURN c.name, population, COUNT(company) AS employers
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        # Verify plan was created successfully
        assert plan is not None
        assert len(plan.terminal_operators) > 0

        # Find the AggregationBoundaryOperator in the plan
        boundary_ops = list(
            plan.terminal_operators[0].get_all_upstream_operators(
                AggregationBoundaryOperator
            )
        )
        assert len(boundary_ops) == 1

        boundary = boundary_ops[0]
        assert boundary.cte_name == "agg_boundary_1"
        assert len(boundary.group_keys) == 1  # 'c'
        assert len(boundary.aggregates) == 1  # 'population'
        assert "c" in boundary.projected_variables

    def test_match_after_sum_creates_boundary(self) -> None:
        """Test that MATCH after SUM creates an AggregationBoundaryOperator."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, SUM(p.age) AS total_age
        MATCH (c)<-[:LIVES_IN]-(other:Person)
        RETURN c.name, total_age, COUNT(other)
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        assert plan is not None
        boundary_ops = list(
            plan.terminal_operators[0].get_all_upstream_operators(
                AggregationBoundaryOperator
            )
        )
        assert len(boundary_ops) == 1

    def test_match_after_avg_creates_boundary(self) -> None:
        """Test that MATCH after AVG creates an AggregationBoundaryOperator."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, AVG(p.age) AS avg_age
        MATCH (c)<-[:LIVES_IN]-(other:Person)
        RETURN c.name, avg_age
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        assert plan is not None
        boundary_ops = list(
            plan.terminal_operators[0].get_all_upstream_operators(
                AggregationBoundaryOperator
            )
        )
        assert len(boundary_ops) == 1

    def test_match_after_collect_creates_boundary(self) -> None:
        """Test that MATCH after COLLECT creates an AggregationBoundaryOperator."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COLLECT(p.name) AS residents
        MATCH (c)<-[:LIVES_IN]-(other:Person)
        RETURN c.name, residents
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        assert plan is not None
        boundary_ops = list(
            plan.terminal_operators[0].get_all_upstream_operators(
                AggregationBoundaryOperator
            )
        )
        assert len(boundary_ops) == 1

    def test_no_aggregation_with_match_no_boundary(self) -> None:
        """Test that MATCH after non-aggregating WITH has no boundary."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH p, c
        MATCH (p)-[:WORKS_AT]->(company:Company)
        RETURN p.name, c.name, company.name
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        assert plan is not None
        # Should not have an AggregationBoundaryOperator
        boundary_ops = list(
            plan.terminal_operators[0].get_all_upstream_operators(
                AggregationBoundaryOperator
            )
        )
        assert len(boundary_ops) == 0

    def test_aggregation_without_subsequent_match_no_boundary(self) -> None:
        """Test that aggregation followed by RETURN (no MATCH) has no boundary."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COUNT(p) AS population
        WHERE population > 100
        RETURN c.name, population
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        assert plan is not None
        # Should not have an AggregationBoundaryOperator (no MATCH after)
        boundary_ops = list(
            plan.terminal_operators[0].get_all_upstream_operators(
                AggregationBoundaryOperator
            )
        )
        assert len(boundary_ops) == 0

    def test_multiple_aggregations_creates_boundary(self) -> None:
        """Test that multiple aggregation functions in WITH create a boundary."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COUNT(p) AS pop, AVG(p.age) AS avg_age
        MATCH (c)<-[:LIVES_IN]-(other:Person)
        RETURN c.name, pop, avg_age
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        assert plan is not None
        boundary_ops = list(
            plan.terminal_operators[0].get_all_upstream_operators(
                AggregationBoundaryOperator
            )
        )
        assert len(boundary_ops) == 1

        boundary = boundary_ops[0]
        assert len(boundary.aggregates) == 2  # pop and avg_age

    def test_aggregation_in_expression_creates_boundary(self) -> None:
        """Test that aggregation in a larger expression creates a boundary."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COUNT(p) * 2 AS doubled_pop
        MATCH (c)<-[:LIVES_IN]-(other:Person)
        RETURN c.name, doubled_pop
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        assert plan is not None
        boundary_ops = list(
            plan.terminal_operators[0].get_all_upstream_operators(
                AggregationBoundaryOperator
            )
        )
        assert len(boundary_ops) == 1

    def test_nested_aggregation_in_function_creates_boundary(self) -> None:
        """Test that aggregation nested in a function call creates a boundary."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COALESCE(COUNT(p), 0) AS pop
        MATCH (c)<-[:LIVES_IN]-(other:Person)
        RETURN c.name, pop
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        assert plan is not None
        boundary_ops = list(
            plan.terminal_operators[0].get_all_upstream_operators(
                AggregationBoundaryOperator
            )
        )
        assert len(boundary_ops) == 1

    def test_boundary_has_correct_projected_variables(self) -> None:
        """Test that boundary tracks projected entity variables correctly."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COUNT(p) AS population
        MATCH (c)<-[:LIVES_IN]-(other:Person)
        RETURN c.name, population
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        boundary_ops = list(
            plan.terminal_operators[0].get_all_upstream_operators(
                AggregationBoundaryOperator
            )
        )
        boundary = boundary_ops[0]

        # 'c' should be in projected_variables for join correlation
        assert "c" in boundary.projected_variables
        # 'p' should NOT be (it was aggregated over)
        assert "p" not in boundary.projected_variables


class TestMatchAfterAggregatingWithSQL:
    """Test SQL rendering for MATCH after aggregating WITH."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_node(
            NodeSchema(
                name="City",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="LIVES_IN",
                source_node_id="Person",
                sink_node_id="City",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            )
        )

        # Set up SQL schema
        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor.from_table_name(
                "Person", "graph.Person", node_id_columns=["id"]
            ),
        )
        self.sql_schema.add_node(
            NodeSchema(
                name="City",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor.from_table_name(
                "City", "graph.City", node_id_columns=["id"]
            ),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="LIVES_IN",
                source_node_id="Person",
                sink_node_id="City",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor.from_table_name(
                "LIVES_IN:Person->City", "graph.Lives_In"
            ),
        )

    def _parse(self, cypher: str):
        """Parse a Cypher query to AST."""
        parser = OpenCypherParser()
        return parser.parse(cypher)

    def test_sql_contains_cte(self) -> None:
        """Test that generated SQL contains a WITH clause for the CTE."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COUNT(p) AS population
        MATCH (c)<-[:LIVES_IN]-(other:Person)
        RETURN c.name AS city, population, COUNT(other) AS total
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        renderer = SQLRenderer(self.sql_schema)
        sql = renderer.render_plan(plan)

        # Should use WITH (not WITH RECURSIVE)
        assert "WITH\n" in sql or sql.startswith("WITH\n")
        assert "WITH RECURSIVE" not in sql

        # Should have CTE name
        assert "agg_boundary_1 AS" in sql

        # Should have GROUP BY
        assert "GROUP BY" in sql

        # Should reference the CTE in the main query
        assert "FROM agg_boundary_1" in sql

    def test_sql_join_condition_on_entity_id(self) -> None:
        """Test that the join condition correctly links CTE to new MATCH."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COUNT(p) AS population
        MATCH (c)<-[:LIVES_IN]-(other:Person)
        RETURN c.name, population
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        renderer = SQLRenderer(self.sql_schema)
        sql = renderer.render_plan(plan)

        # The join should be on the city ID
        # The CTE projects 'c' (the city ID), and the new MATCH has '_gsql2rsql_c_id'
        assert "`c` = " in sql or "= _right._gsql2rsql_c_id" in sql

    def test_sql_having_clause_for_where_on_aggregation(self) -> None:
        """Test that WHERE on aggregated column becomes HAVING."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c, COUNT(p) AS population
        WHERE population > 100
        MATCH (c)<-[:LIVES_IN]-(other:Person)
        RETURN c.name, population
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        renderer = SQLRenderer(self.sql_schema)
        sql = renderer.render_plan(plan)

        # The filter on population should be a HAVING clause in the CTE
        assert "HAVING" in sql
        assert "population" in sql.lower() or "100" in sql


class TestMatchAfterAggregatingWithEdgeCases:
    """Test edge cases for MATCH after aggregating WITH."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Node",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("value", int),
                ],
                node_id_property=EntityProperty("id", int),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="REL",
                source_node_id="Node",
                sink_node_id="Node",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            )
        )

    def _parse(self, cypher: str):
        """Parse a Cypher query to AST."""
        parser = OpenCypherParser()
        return parser.parse(cypher)

    def test_simple_query_no_with(self) -> None:
        """Test that simple queries without WITH work fine."""
        cypher = """
        MATCH (a:Node)-[:REL]->(b:Node)
        RETURN a.id, b.id, COUNT(*)
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)
        assert plan is not None

    def test_with_where_on_aggregation_no_match(self) -> None:
        """Test WITH ... WHERE on aggregated column without subsequent MATCH."""
        cypher = """
        MATCH (a:Node)-[:REL]->(b:Node)
        WITH a, COUNT(b) AS cnt
        WHERE cnt > 5
        RETURN a.id, cnt
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)
        assert plan is not None

        # No boundary needed - no MATCH after aggregation
        boundary_ops = list(
            plan.terminal_operators[0].get_all_upstream_operators(
                AggregationBoundaryOperator
            )
        )
        assert len(boundary_ops) == 0

    def test_count_star_creates_boundary(self) -> None:
        """Test that COUNT(*) followed by MATCH creates a boundary."""
        cypher = """
        MATCH (a:Node)-[:REL]->(b:Node)
        WITH a, COUNT(*) AS cnt
        MATCH (a)-[:REL]->(c:Node)
        RETURN a.id, cnt, c.id
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)

        assert plan is not None
        boundary_ops = list(
            plan.terminal_operators[0].get_all_upstream_operators(
                AggregationBoundaryOperator
            )
        )
        assert len(boundary_ops) == 1

    def test_distinct_without_aggregation_no_boundary(self) -> None:
        """Test that DISTINCT without aggregation doesn't create a boundary."""
        cypher = """
        MATCH (a:Node)-[:REL]->(b:Node)
        WITH DISTINCT a
        MATCH (a)-[:REL]->(c:Node)
        RETURN a.id, c.id
        """
        ast = self._parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)
        assert plan is not None

        # DISTINCT alone doesn't create an aggregation boundary
        boundary_ops = list(
            plan.terminal_operators[0].get_all_upstream_operators(
                AggregationBoundaryOperator
            )
        )
        assert len(boundary_ops) == 0
