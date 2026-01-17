"""Test 20: COALESCE function.

Validates that COALESCE function is correctly transpiled to SQL.

COALESCE returns the first non-null value from its argument list.
"""

import pytest

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


class TestCoalesce:
    """Test COALESCE function transpilation."""

    TEST_ID = "20"
    TEST_NAME = "coalesce"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Graph schema with nullable properties
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                    EntityProperty("nickname", str),
                    EntityProperty("alias", str),
                    EntityProperty("age", int),
                    EntityProperty("city", str),
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
                    EntityProperty("nickname", str),
                    EntityProperty("alias", str),
                    EntityProperty("age", int),
                    EntityProperty("city", str),
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
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def test_golden_file_match(self) -> None:
        """Test that transpiled SQL matches golden file."""
        cypher = """
        MATCH (p:Person)
        RETURN COALESCE(p.nickname, p.name) AS displayName
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql-20"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_structural_has_coalesce(self) -> None:
        """Test SQL has COALESCE function."""
        cypher = """
        MATCH (p:Person)
        RETURN COALESCE(p.nickname, p.name) AS displayName
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "COALESCE" in sql_upper, "Should have COALESCE function"

    def test_coalesce_with_literal_default(self) -> None:
        """Test COALESCE with literal string as default value."""
        cypher = """
        MATCH (p:Person)
        RETURN COALESCE(p.nickname, 'Unknown') AS displayName
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "COALESCE" in sql_upper, "Should have COALESCE function"
        assert "'Unknown'" in sql or "'UNKNOWN'" in sql_upper, "Should have literal default"

    def test_coalesce_multiple_arguments(self) -> None:
        """Test COALESCE with multiple fallback arguments."""
        cypher = """
        MATCH (p:Person)
        RETURN COALESCE(p.nickname, p.alias, p.name, 'Anonymous') AS displayName
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "COALESCE" in sql_upper, "Should have COALESCE function"
        # Should reference all properties
        assert "__p_nickname" in sql.lower() or "p_nickname" in sql.lower()
        assert "__p_alias" in sql.lower() or "p_alias" in sql.lower()
        assert "__p_name" in sql.lower() or "p_name" in sql.lower()

    def test_coalesce_in_where_clause(self) -> None:
        """Test COALESCE in WHERE filter."""
        cypher = """
        MATCH (p:Person)
        WHERE COALESCE(p.age, 0) > 18
        RETURN p.name
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "COALESCE" in sql_upper, "Should have COALESCE in WHERE"
        assert "WHERE" in sql_upper, "Should have WHERE clause"
        # May have parentheses around values: "> (18)" or "> 18"
        assert "> 18" in sql or ">18" in sql or "> (18)" in sql, "Should have comparison"

    def test_coalesce_with_aggregation(self) -> None:
        """Test COALESCE with GROUP BY aggregation."""
        cypher = """
        MATCH (p:Person)
        RETURN COALESCE(p.city, 'Unknown') AS city, COUNT(p) AS count
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert "COALESCE" in sql_upper, "Should have COALESCE"
        assert "COUNT" in sql_upper, "Should have COUNT aggregation"
        assert "GROUP BY" in sql_upper, "Should have GROUP BY"

    def test_coalesce_no_cartesian_join(self) -> None:
        """Test COALESCE does NOT produce cartesian join."""
        cypher = """
        MATCH (p:Person)
        RETURN COALESCE(p.nickname, p.name) AS displayName
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should NOT have ON TRUE (cartesian join)
        assert "ON TRUE" not in sql_upper, "Should not have cartesian join"
        # Should NOT have CROSS JOIN
        assert "CROSS JOIN" not in sql_upper, "Should not have CROSS JOIN"
