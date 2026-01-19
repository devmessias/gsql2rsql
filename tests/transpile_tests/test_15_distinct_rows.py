"""Test 15: DISTINCT rows.

Validates that DISTINCT keyword correctly generates SELECT DISTINCT
in Databricks SQL to remove duplicate result rows.
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
    assert_has_distinct,
    assert_has_order_by,
    assert_has_limit_offset,
    assert_no_cartesian_join,
)


class TestDistinctRows:
    """Test DISTINCT keyword for deduplication."""

    TEST_ID = "15"
    TEST_NAME = "distinct_rows"

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
        self.graph_schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
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
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        RETURN DISTINCT f.name
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql-15"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_structural_has_distinct(self) -> None:
        """Test SQL has DISTINCT keyword."""
        cypher = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        RETURN DISTINCT f.name
        """
        sql = self._transpile(cypher)
        assert_has_distinct(sql)

    def test_structural_has_joins(self) -> None:
        """Test SQL has proper joins."""
        cypher = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        RETURN DISTINCT f.name
        """
        sql = self._transpile(cypher)
        assert_has_join(sql)

    def test_structural_no_cartesian_joins(self) -> None:
        """Test that no cartesian joins are generated."""
        cypher = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        RETURN DISTINCT f.name
        """
        sql = self._transpile(cypher)
        assert_no_cartesian_join(sql)

    def test_distinct_multiple_columns(self) -> None:
        """Test DISTINCT with multiple columns."""
        cypher = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        RETURN DISTINCT f.name, f.age
        """
        sql = self._transpile(cypher)

        assert_has_distinct(sql)
        # Should project both columns
        sql_lower = sql.lower()
        assert "name" in sql_lower
        assert "age" in sql_lower

    def test_distinct_with_order_by(self) -> None:
        """Test DISTINCT combined with ORDER BY."""
        cypher = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        RETURN DISTINCT f.name
        ORDER BY f.name ASC
        """
        sql = self._transpile(cypher)

        assert_has_distinct(sql)
        assert_has_order_by(sql)

    def test_distinct_with_limit(self) -> None:
        """Test DISTINCT combined with LIMIT."""
        cypher = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        RETURN DISTINCT f.name
        LIMIT 10
        """
        sql = self._transpile(cypher)

        assert_has_distinct(sql)
        assert_has_limit_offset(sql, limit=10)
