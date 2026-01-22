"""Test 03: Property projection with aliases.

Validates that RETURN p.name AS personName, p.id AS personId
correctly transpiles to SQL with proper column aliasing.
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
    assert_no_join,
    assert_projected_columns,
)


class TestPropertyProjectionAliases:
    """Test projecting specific properties with aliases."""

    TEST_ID = "03"
    TEST_NAME = "property_projection_aliases"

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
        cypher = "MATCH (p:Person) RETURN p.name AS personName, p.id AS personId"
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

    def test_structural_has_aliases(self) -> None:
        """Test that SQL contains the requested aliases."""
        cypher = "MATCH (p:Person) RETURN p.name AS personName, p.id AS personId"
        sql = self._transpile(cypher)

        assert_projected_columns(sql, ["personName", "personId"])

    def test_structural_uses_as_keyword(self) -> None:
        """Test that SQL uses AS keyword for aliasing."""
        cypher = "MATCH (p:Person) RETURN p.name AS personName, p.id AS personId"
        sql = self._transpile(cypher)

        # Should use AS keyword (case insensitive)
        assert " AS " in sql or " as " in sql, "Expected AS keyword for aliasing"

    def test_structural_has_select(self) -> None:
        """Test that SQL has SELECT statement."""
        cypher = "MATCH (p:Person) RETURN p.name AS personName"
        sql = self._transpile(cypher)

        assert_has_select(sql)

    def test_structural_from_table(self) -> None:
        """Test that SQL references correct table."""
        cypher = "MATCH (p:Person) RETURN p.name AS personName"
        sql = self._transpile(cypher)

        assert_has_from_table(sql, "Person")

    def test_structural_no_joins(self) -> None:
        """Test that simple projection doesn't generate joins."""
        cypher = "MATCH (p:Person) RETURN p.name AS personName"
        sql = self._transpile(cypher)

        assert_no_join(sql)

    def test_projects_requested_properties(self) -> None:
        """Test that only requested properties appear in output."""
        cypher = "MATCH (p:Person) RETURN p.name"
        sql = self._transpile(cypher)

        # Should reference name
        assert "name" in sql.lower(), "Expected name property in projection"
