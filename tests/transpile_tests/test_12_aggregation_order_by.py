"""Test 12: Aggregation with ORDER BY.

Validates that aggregation queries can be ordered by aggregated values
or grouping columns with proper ASC/DESC modifiers.
"""

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    SimpleGraphSchemaProvider,
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)

from tests.utils.sql_test_utils import assert_sql_equal, load_expected_sql
from tests.utils.sql_assertions import (
    assert_has_from_table,
    assert_has_join,
    assert_has_group_by,
    assert_has_order_by,
    assert_has_aggregation,
    assert_has_limit_offset,
    assert_no_cartesian_join,
)


class TestAggregationOrderBy:
    """Test aggregation with ORDER BY semantics."""

    TEST_ID = "12"
    TEST_NAME = "aggregation_order_by"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Graph schema
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
        self.graph_schema.add_edge(
            EdgeSchema(
                name="LIVES_IN",
                source_node_id="Person",
                sink_node_id="City",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
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
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Person"),
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
            SQLTableDescriptor(table_name="graph.City"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="LIVES_IN",
                source_node_id="Person",
                sink_node_id="City",
                source_id_property=EntityProperty("source_id", int),
                sink_id_property=EntityProperty("target_id", int),
            ),
            SQLTableDescriptor(table_name="graph.LivesIn"),
        )

    def _transpile(self, cypher: str) -> str:
        """Helper to transpile a Cypher query."""
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def test_golden_file_match(self) -> None:
        """Test that transpiled SQL matches golden file."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, COUNT(p) AS population
        ORDER BY population DESC
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql-12"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_structural_has_order_by(self) -> None:
        """Test SQL has ORDER BY clause."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, COUNT(p) AS population
        ORDER BY population DESC
        """
        sql = self._transpile(cypher)
        assert_has_order_by(sql)

    def test_structural_has_desc(self) -> None:
        """Test SQL has DESC keyword."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, COUNT(p) AS population
        ORDER BY population DESC
        """
        sql = self._transpile(cypher)
        sql_upper = sql.upper()
        assert "DESC" in sql_upper, "Should have DESC keyword"

    def test_structural_order_by_asc(self) -> None:
        """Test ORDER BY ascending."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, COUNT(p) AS population
        ORDER BY city ASC
        """
        sql = self._transpile(cypher)
        assert_has_order_by(sql)
        sql_upper = sql.upper()
        assert "ASC" in sql_upper, "Should have ASC keyword"

    def test_structural_multiple_order_by(self) -> None:
        """Test ORDER BY with multiple columns."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, COUNT(p) AS pop, AVG(p.age) AS avgAge
        ORDER BY pop DESC, avgAge ASC
        """
        sql = self._transpile(cypher)
        assert_has_order_by(sql)
        sql_upper = sql.upper()
        assert "DESC" in sql_upper, "Should have DESC keyword"
        assert "ASC" in sql_upper, "Should have ASC keyword"

    def test_structural_order_by_with_limit(self) -> None:
        """Test ORDER BY combined with LIMIT."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, COUNT(p) AS population
        ORDER BY population DESC
        LIMIT 10
        """
        sql = self._transpile(cypher)
        assert_has_order_by(sql)
        assert_has_limit_offset(sql, limit=10)

    def test_structural_no_cartesian_joins(self) -> None:
        """Test that no cartesian joins are generated."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, COUNT(p) AS population
        ORDER BY population DESC
        """
        sql = self._transpile(cypher)
        assert_no_cartesian_join(sql)
