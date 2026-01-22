"""Test 02: Node lookup with property filter.

Validates that MATCH (p:Person) WHERE p.name = 'Alice' RETURN p
correctly transpiles to SQL with WHERE clause.
"""

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    NodeSchema,
    EntityProperty,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)

from tests.utils.sql_test_utils import (
    assert_sql_equal,
    load_expected_sql,
)
from tests.utils.sql_assertions import (
    assert_has_select,
    assert_has_from_table,
    assert_has_where,
    assert_no_join,
    SQLStructure,
)


class TestNodeWithPropertyFilter:
    """Test node lookup with WHERE property filter."""

    TEST_ID = "02"
    TEST_NAME = "node_with_property_filter"

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
            SQLTableDescriptor(
                table_name="dbo.Person",
                node_id_columns=["id"],
            ),
        )

    def _transpile(self, cypher: str) -> str:
        """Helper to transpile a Cypher query."""
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        return renderer.render_plan(plan)

    def test_golden_file_match(self) -> None:
        """Test that transpiled SQL matches golden file."""
        cypher = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p"
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No golden file found. Create: "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql"
            )

        assert_sql_equal(
            expected_sql,
            actual_sql,
            self.TEST_ID,
            self.TEST_NAME,
        )

    def test_structural_has_where(self) -> None:
        """Test that SQL contains WHERE clause."""
        cypher = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p"
        sql = self._transpile(cypher)

        assert_has_where(sql)

    def test_structural_where_has_filter_value(self) -> None:
        """Test that WHERE clause contains the filter value."""
        cypher = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p"
        sql = self._transpile(cypher)

        # Should contain the filter value
        assert "'Alice'" in sql, "Expected filter value 'Alice' in WHERE clause"

    def test_structural_has_equality_operator(self) -> None:
        """Test that filter uses equality comparison."""
        cypher = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p"
        sql = self._transpile(cypher)

        structure = SQLStructure(raw_sql=sql)
        assert structure.has_where
        assert "=" in sql, "Expected equality operator in WHERE clause"

    def test_structural_no_joins(self) -> None:
        """Test that property filter doesn't introduce joins."""
        cypher = "MATCH (p:Person) WHERE p.name = 'Bob' RETURN p"
        sql = self._transpile(cypher)

        assert_no_join(sql)

    def test_structural_from_table(self) -> None:
        """Test that SQL references correct table."""
        cypher = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p"
        sql = self._transpile(cypher)

        assert_has_from_table(sql, "Person")

    def test_structural_select(self) -> None:
        """Test that SQL has SELECT statement."""
        cypher = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p"
        sql = self._transpile(cypher)

        assert_has_select(sql)
