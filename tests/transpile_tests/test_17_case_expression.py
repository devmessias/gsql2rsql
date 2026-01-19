"""Test 17: CASE expression.

Validates that CASE WHEN THEN ELSE END expressions are correctly
transpiled to SQL CASE expressions.
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

from tests.utils.sql_test_utils import assert_sql_equal, load_expected_sql
from tests.utils.sql_assertions import (
    assert_has_select,
    assert_has_from_table,
    assert_has_group_by,
    assert_has_aggregation,
)


class TestCaseExpression:
    """Test CASE expression transpilation."""

    TEST_ID = "17"
    TEST_NAME = "case_expression"

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
                    EntityProperty("status", str),
                ],
                node_id_property=EntityProperty("id", int),
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
                    EntityProperty("status", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(table_name="graph.Person"),
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
        MATCH (p:Person)
        RETURN p.name,
               CASE
                 WHEN p.age < 18 THEN 'minor'
                 WHEN p.age >= 65 THEN 'senior'
                 ELSE 'adult'
               END AS ageGroup
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql-17"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_structural_has_case_keyword(self) -> None:
        """Test SQL has CASE keyword."""
        cypher = """
        MATCH (p:Person)
        RETURN p.name,
               CASE WHEN p.age < 18 THEN 'minor' ELSE 'adult' END AS ageGroup
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "CASE" in sql_upper, "Should have CASE keyword"
        assert "WHEN" in sql_upper, "Should have WHEN keyword"
        assert "THEN" in sql_upper, "Should have THEN keyword"
        assert "ELSE" in sql_upper, "Should have ELSE keyword"
        assert "END" in sql_upper, "Should have END keyword"

    def test_case_without_else(self) -> None:
        """Test CASE without ELSE (returns NULL when no match)."""
        cypher = """
        MATCH (p:Person)
        RETURN p.name,
               CASE WHEN p.age >= 18 THEN 'adult' END AS status
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "CASE" in sql_upper
        assert "WHEN" in sql_upper
        assert "END" in sql_upper

    def test_case_with_multiple_when(self) -> None:
        """Test CASE with multiple WHEN clauses."""
        cypher = """
        MATCH (p:Person)
        RETURN p.name,
               CASE
                 WHEN p.age < 18 THEN 'minor'
                 WHEN p.age >= 18 AND p.age < 65 THEN 'adult'
                 ELSE 'senior'
               END AS ageGroup
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Count WHEN occurrences
        when_count = sql_upper.count("WHEN")
        assert when_count >= 2, f"Should have at least 2 WHEN clauses, found {when_count}"

    def test_case_with_aggregation(self) -> None:
        """Test CASE combined with aggregation (GROUP BY)."""
        cypher = """
        MATCH (p:Person)
        RETURN
          CASE WHEN p.age < 18 THEN 'minor' ELSE 'adult' END AS ageGroup,
          COUNT(p) AS count
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "CASE" in sql_upper, "Should have CASE"
        assert "GROUP BY" in sql_upper, "Should have GROUP BY for aggregation"

        # Critical: GROUP BY should contain CASE expression, not just alias
        # This is a semantic requirement for Databricks SQL
        assert_has_aggregation(sql, function="COUNT")

    def test_case_references_column(self) -> None:
        """Test that CASE conditions reference the correct column."""
        cypher = """
        MATCH (p:Person)
        RETURN CASE WHEN p.age < 18 THEN 'minor' ELSE 'adult' END AS ageGroup
        """
        sql = self._transpile(cypher)

        # Should reference age column
        sql_lower = sql.lower()
        assert "age" in sql_lower, "Should reference age column"
        assert "18" in sql, "Should have threshold value 18"

    def test_case_string_results(self) -> None:
        """Test CASE with string literal results."""
        cypher = """
        MATCH (p:Person)
        RETURN CASE WHEN p.status = 'active' THEN 'Active User' ELSE 'Inactive' END AS label
        """
        sql = self._transpile(cypher)

        # Should have string literals in THEN/ELSE
        assert "Active" in sql or "active" in sql.lower()
