"""Test 14: Collect aggregation.

Validates that COLLECT() aggregation function correctly generates
COLLECT_LIST() in Databricks SQL and groups values into arrays.
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


class TestCollectAggregation:
    """Test COLLECT aggregation function."""

    TEST_ID = "14"
    TEST_NAME = "collect_aggregation"

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
        MATCH (c:City)<-[:LIVES_IN]-(p:Person)
        RETURN c.name AS city, COLLECT(p.name) AS residents
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql-14"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_structural_has_group_by(self) -> None:
        """Test SQL has GROUP BY clause."""
        cypher = """
        MATCH (c:City)<-[:LIVES_IN]-(p:Person)
        RETURN c.name AS city, COLLECT(p.name) AS residents
        """
        sql = self._transpile(cypher)
        assert_has_group_by(sql)

    def test_structural_has_collect(self) -> None:
        """Test SQL has COLLECT_LIST aggregation."""
        cypher = """
        MATCH (c:City)<-[:LIVES_IN]-(p:Person)
        RETURN c.name AS city, COLLECT(p.name) AS residents
        """
        sql = self._transpile(cypher)

        # Should have COLLECT_LIST or collect_list
        sql_upper = sql.upper()
        assert "COLLECT_LIST" in sql_upper, "Should have COLLECT_LIST function"

    def test_structural_has_joins(self) -> None:
        """Test SQL has proper joins."""
        cypher = """
        MATCH (c:City)<-[:LIVES_IN]-(p:Person)
        RETURN c.name AS city, COLLECT(p.name) AS residents
        """
        sql = self._transpile(cypher)
        assert_has_join(sql)

    def test_structural_no_cartesian_joins(self) -> None:
        """Test that no cartesian joins are generated."""
        cypher = """
        MATCH (c:City)<-[:LIVES_IN]-(p:Person)
        RETURN c.name AS city, COLLECT(p.name) AS residents
        """
        sql = self._transpile(cypher)
        assert_no_cartesian_join(sql)

    def test_collect_with_count(self) -> None:
        """Test COLLECT combined with COUNT."""
        cypher = """
        MATCH (c:City)<-[:LIVES_IN]-(p:Person)
        RETURN c.name AS city, COLLECT(p.name) AS residents, COUNT(p) AS count
        """
        sql = self._transpile(cypher)

        assert_has_group_by(sql)
        sql_upper = sql.upper()
        assert "COLLECT_LIST" in sql_upper, "Should have COLLECT_LIST"
        assert_has_aggregation(sql, function="COUNT")

    def test_collect_ids(self) -> None:
        """Test collecting IDs instead of names."""
        cypher = """
        MATCH (c:City)<-[:LIVES_IN]-(p:Person)
        RETURN c.name AS city, COLLECT(p.id) AS residentIds
        """
        sql = self._transpile(cypher)

        assert_has_group_by(sql)
        sql_upper = sql.upper()
        assert "COLLECT_LIST" in sql_upper, "Should have COLLECT_LIST"
