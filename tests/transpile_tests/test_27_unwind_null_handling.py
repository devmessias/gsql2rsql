"""Test 27: UNWIND NULL and empty array handling.

Validates UNWIND behavior with NULL and empty arrays, and patterns
for handling these cases.

OpenCypher UNWIND semantics:
- UNWIND NULL AS x → produces 0 rows (row is dropped)
- UNWIND [] AS x → produces 0 rows (row is dropped)

Databricks SQL mappings:
- EXPLODE(NULL) → drops the row (matches OpenCypher)
- EXPLODE([]) → drops the row (matches OpenCypher)
- EXPLODE_OUTER(NULL) → preserves row with NULL value (extension)
- EXPLODE_OUTER([]) → preserves row with NULL value (extension)

Patterns for NULL handling:
- COALESCE(array, [default]) → provide default for NULL arrays
- CASE WHEN array IS NULL THEN ... → explicit NULL handling

Fraud Use Cases:
- Handle accounts with no transactions gracefully
- Process optional risk factor arrays
- Default behavior for missing data
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


class TestUnwindNullHandling:
    """Test UNWIND NULL/empty array handling."""

    TEST_ID = "27"
    TEST_NAME = "unwind_null_handling"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("name", str),
                    EntityProperty("tags", list),
                    EntityProperty("optionalFlags", list),
                ],
                node_id_property=EntityProperty("id", str),
            )
        )

        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("name", str),
                    EntityProperty("tags", list),
                    EntityProperty("optionalFlags", list),
                ],
                node_id_property=EntityProperty("id", str),
            ),
            SQLTableDescriptor(table_name="graph.Account"),
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
        MATCH (a:Account)
        UNWIND COALESCE(a.tags, ['no_tags']) AS tag
        RETURN a.id, tag
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql ID=27 NAME=unwind_null_handling"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_default_uses_explode_not_outer(self) -> None:
        """Test that default UNWIND uses EXPLODE (drops NULL rows).

        OpenCypher semantics: UNWIND NULL produces 0 rows.
        EXPLODE matches this behavior.
        """
        cypher = """
        MATCH (a:Account)
        UNWIND a.tags AS tag
        RETURN a.id, tag
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE(" in sql_upper, "Should use EXPLODE"
        assert "EXPLODE_OUTER" not in sql_upper, \
            "Should NOT use EXPLODE_OUTER by default"

    def test_coalesce_provides_default_for_null(self) -> None:
        """Test COALESCE pattern for NULL array handling.

        Pattern: UNWIND COALESCE(array, [default]) AS x
        This ensures at least one row is produced even if array is NULL.
        """
        cypher = """
        MATCH (a:Account)
        UNWIND COALESCE(a.optionalFlags, ['none']) AS flag
        RETURN a.id, flag
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "COALESCE" in sql_upper, \
            "Should preserve COALESCE for NULL handling"

    def test_coalesce_with_empty_default(self) -> None:
        """Test COALESCE with empty array as default.

        Pattern: UNWIND COALESCE(array, []) AS x
        If array is NULL, use empty array (still produces 0 rows).
        """
        cypher = """
        MATCH (a:Account)
        UNWIND COALESCE(a.tags, []) AS tag
        RETURN a.id, tag
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "COALESCE" in sql_upper

    def test_unwind_empty_literal_array(self) -> None:
        """Test UNWIND of empty literal array.

        UNWIND [] AS x → produces 0 rows
        """
        cypher = """
        MATCH (a:Account)
        UNWIND [] AS item
        RETURN a.id, item
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper

    def test_unwind_null_literal(self) -> None:
        """Test UNWIND of NULL literal.

        UNWIND NULL AS x → produces 0 rows
        """
        cypher = """
        MATCH (a:Account)
        UNWIND NULL AS item
        RETURN a.id, item
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper

    def test_fraud_optional_risk_factors(self) -> None:
        """Test fraud scenario: handle optional risk factor arrays.

        Some accounts may not have risk factors assigned.
        Use COALESCE to provide a default.
        """
        cypher = """
        MATCH (a:Account)
        UNWIND COALESCE(a.optionalFlags, ['unassessed']) AS flag
        RETURN a.id, flag
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "EXPLODE" in sql_upper
        assert "COALESCE" in sql_upper

    def test_no_lateral_in_null_handling(self) -> None:
        """Test that NULL handling patterns don't use deprecated LATERAL."""
        cypher = """
        MATCH (a:Account)
        UNWIND COALESCE(a.tags, ['default']) AS tag
        RETURN a.id, tag
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "LATERAL" not in sql_upper, \
            "Should NOT use deprecated LATERAL keyword"
