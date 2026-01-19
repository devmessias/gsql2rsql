"""Test 01: Simple node lookup by label.

Validates that MATCH (p:Person) RETURN p correctly transpiles to SQL.
"""

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    SimpleGraphSchemaProvider,
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
    assert_no_join,
    SQLStructure,
)


class TestSimpleNodeLookup:
    """Test simple node lookup by label without filters."""

    TEST_ID = "01"
    TEST_NAME = "simple_node_lookup"

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

        # SQL schema
        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
            NodeSchema(
                name="Person",
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
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def test_golden_file_match(self) -> None:
        """Test that transpiled SQL matches golden file."""
        cypher = "MATCH (p:Person) RETURN p"
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

    def test_structural_select(self) -> None:
        """Test that SQL has valid SELECT structure."""
        cypher = "MATCH (p:Person) RETURN p"
        sql = self._transpile(cypher)

        assert_has_select(sql)

    def test_structural_from_table(self) -> None:
        """Test that SQL references correct table."""
        cypher = "MATCH (p:Person) RETURN p"
        sql = self._transpile(cypher)

        assert_has_from_table(sql, "Person")

    def test_structural_no_joins(self) -> None:
        """Test that simple lookup doesn't generate joins."""
        cypher = "MATCH (p:Person) RETURN p"
        sql = self._transpile(cypher)

        assert_no_join(sql)

    def test_structural_no_where(self) -> None:
        """Test that there's no WHERE clause for unfiltered query."""
        cypher = "MATCH (p:Person) RETURN p"
        sql = self._transpile(cypher)

        structure = SQLStructure(raw_sql=sql)
        assert not structure.has_where, "Unexpected WHERE clause"

    def test_structural_no_recursion(self) -> None:
        """Test that simple lookup doesn't use WITH RECURSIVE."""
        cypher = "MATCH (p:Person) RETURN p"
        sql = self._transpile(cypher)

        structure = SQLStructure(raw_sql=sql)
        assert not structure.has_recursive_cte, "Unexpected WITH RECURSIVE"

    def test_projects_node_properties(self) -> None:
        """Test that node properties are projected."""
        cypher = "MATCH (p:Person) RETURN p"
        sql = self._transpile(cypher)

        # Should have internal field names for projected properties
        assert "_gsql2rsql_p_id" in sql, "Expected id property projection"
        assert "_gsql2rsql_p_name" in sql, "Expected name property projection"
