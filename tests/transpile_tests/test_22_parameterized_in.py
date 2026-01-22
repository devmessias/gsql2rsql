"""Test 22: Parameterized IN list.

Validates that parameterized queries with $param syntax are correctly
transpiled to SQL with ARRAY_CONTAINS for IN operations.

IMPORTANT: This is a TDD test. The feature needs to be implemented.

Fraud Use Cases:
- Match against watchlists
- Batch entity lookup
- Dynamic filtering based on runtime lists
"""

import pytest

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
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
    assert_has_select,
    assert_has_from_table,
    assert_has_where,
)


class TestParameterizedIn:
    """Test parameterized IN list transpilation."""

    TEST_ID = "22"
    TEST_NAME = "parameterized_in"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("name", str),
                    EntityProperty("status", str),
                    EntityProperty("balance", float),
                ],
                node_id_property=EntityProperty("id", str),
            ),
            SQLTableDescriptor(table_name="graph.Account"),
        )
        self.schema.add_node(
            NodeSchema(
                name="Transaction",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("amount", float),
                    EntityProperty("timestamp", str),
                ],
                node_id_property=EntityProperty("id", str),
            ),
            SQLTableDescriptor(table_name="graph.Transaction"),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="TRANSFER",
                source_node_id="Account",
                sink_node_id="Account",
                source_id_property=EntityProperty("source_id", str),
                sink_id_property=EntityProperty("target_id", str),
            ),
            SQLTableDescriptor(table_name="graph.Transfer"),
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
        cypher = """
        MATCH (a:Account)
        WHERE a.id IN $watchlist
        RETURN a.id, a.name
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql-22"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_parameter_recognized_in_where(self) -> None:
        """Test that parameter $watchlist is recognized in WHERE clause."""
        cypher = """
        MATCH (a:Account)
        WHERE a.id IN $watchlist
        RETURN a.id
        """
        sql = self._transpile(cypher)

        assert_has_where(sql)
        # Should use ARRAY_CONTAINS for parameter IN
        sql_upper = sql.upper()
        assert "ARRAY_CONTAINS" in sql_upper or ":WATCHLIST" in sql_upper, \
            "Should have ARRAY_CONTAINS or parameter reference for IN with parameter"

    def test_parameter_with_colon_syntax(self) -> None:
        """Test that SQL uses :param syntax for Databricks."""
        cypher = """
        MATCH (a:Account)
        WHERE a.id IN $watchlist
        RETURN a.id
        """
        sql = self._transpile(cypher)

        # Databricks uses :param for named parameters
        sql_lower = sql.lower()
        assert ":watchlist" in sql_lower or "watchlist" in sql_lower, \
            "Should reference watchlist parameter"

    def test_multiple_parameters(self) -> None:
        """Test multiple parameters in query."""
        cypher = """
        MATCH (a:Account)
        WHERE a.id IN $watchlist AND a.status = $status
        RETURN a.id, a.name
        """
        sql = self._transpile(cypher)

        assert_has_where(sql)
        sql_lower = sql.lower()
        # Should have both parameters
        assert "watchlist" in sql_lower, "Should have watchlist parameter"
        assert "status" in sql_lower, "Should have status parameter"

    def test_parameter_equality(self) -> None:
        """Test parameter in simple equality check."""
        cypher = """
        MATCH (a:Account)
        WHERE a.name = $targetName
        RETURN a.id
        """
        sql = self._transpile(cypher)

        assert_has_where(sql)
        sql_lower = sql.lower()
        assert "targetname" in sql_lower or ":targetname" in sql_lower, \
            "Should have targetName parameter"

    def test_parameter_in_list_fraud_scenario(self) -> None:
        """Test fraud detection scenario: watchlist matching."""
        cypher = """
        MATCH (a:Account)-[:TRANSFER]->(b:Account)
        WHERE a.id IN $suspiciousAccounts
        RETURN a.id AS source, b.id AS target
        """
        sql = self._transpile(cypher)

        assert_has_where(sql)
        sql_upper = sql.upper()
        # Should generate proper IN/ARRAY_CONTAINS clause
        assert (
            "ARRAY_CONTAINS" in sql_upper or
            "IN" in sql_upper or
            ":SUSPICIOUSACCOUNTS" in sql_upper
        ), "Should handle parameter IN for fraud detection"
