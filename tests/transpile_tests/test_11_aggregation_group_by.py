"""Test 11: Aggregation with GROUP BY.

Validates that aggregation functions with non-aggregated expressions
generate proper GROUP BY clauses in the SQL output.
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
    assert_has_aggregation,
    assert_no_cartesian_join,
)


class TestAggregationGroupBy:
    """Test aggregation with GROUP BY semantics."""

    TEST_ID = "11"
    TEST_NAME = "aggregation_group_by"

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
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def test_golden_file_match(self) -> None:
        """Test that transpiled SQL matches golden file."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, COUNT(p) AS population
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql-11"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_structural_has_group_by(self) -> None:
        """Test SQL has GROUP BY clause."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, COUNT(p) AS population
        """
        sql = self._transpile(cypher)
        assert_has_group_by(sql)

    def test_structural_has_count_aggregation(self) -> None:
        """Test SQL has COUNT aggregation."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, COUNT(p) AS population
        """
        sql = self._transpile(cypher)
        assert_has_aggregation(sql, function="COUNT")

    def test_structural_has_joins(self) -> None:
        """Test SQL has proper joins."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, COUNT(p) AS population
        """
        sql = self._transpile(cypher)
        assert_has_join(sql)

    def test_structural_no_cartesian_joins(self) -> None:
        """Test that no cartesian joins are generated."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, COUNT(p) AS population
        """
        sql = self._transpile(cypher)
        assert_no_cartesian_join(sql)

    def test_structural_references_both_tables(self) -> None:
        """Test SQL references both Person and City tables."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, COUNT(p) AS population
        """
        sql = self._transpile(cypher)
        assert_has_from_table(sql, "Person")
        assert_has_from_table(sql, "City")

    def test_multiple_aggregations(self) -> None:
        """Test multiple aggregation functions."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, COUNT(p) AS pop, AVG(p.age) AS avgAge
        """
        sql = self._transpile(cypher)

        assert_has_group_by(sql)
        assert_has_aggregation(sql, function="COUNT")
        assert_has_aggregation(sql, function="AVG")
        assert_no_cartesian_join(sql)

    def test_sum_aggregation(self) -> None:
        """Test SUM aggregation function."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, SUM(p.age) AS totalAge
        """
        sql = self._transpile(cypher)

        assert_has_group_by(sql)
        assert_has_aggregation(sql, function="SUM")

    def test_min_max_aggregation(self) -> None:
        """Test MIN and MAX aggregation functions."""
        cypher = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        RETURN c.name AS city, MIN(p.age) AS youngest, MAX(p.age) AS oldest
        """
        sql = self._transpile(cypher)

        assert_has_group_by(sql)
        assert_has_aggregation(sql, function="MIN")
        assert_has_aggregation(sql, function="MAX")
