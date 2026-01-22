"""Test 23: HAVING-like aggregation (WITH ... WHERE on aggregated columns).

Validates that Cypher's WITH ... WHERE pattern on aggregated columns
is correctly transpiled to SQL HAVING clause.

IMPORTANT: This is a TDD test. The feature needs to be implemented.

Fraud Use Cases:
- Identify accounts with abnormally high transaction counts
- Filter entities exceeding risk thresholds
- Detect structuring (many transactions just below reporting limits)
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
    assert_has_group_by,
    assert_has_having,
    assert_has_aggregation,
)


class TestHavingAggregation:
    """Test HAVING-like aggregation transpilation."""

    TEST_ID = "23"
    TEST_NAME = "having_aggregation"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("name", str),
                    EntityProperty("risk_score", float),
                ],
                node_id_property=EntityProperty("id", str),
            ),
            SQLTableDescriptor(table_name="graph.Account"),
        )
        self.schema.add_edge(
            EdgeSchema(
                name="TRANSFER",
                source_node_id="Account",
                sink_node_id="Account",
                properties=[
                    EntityProperty("amount", float),
                    EntityProperty("timestamp", str),
                ],
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
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, COUNT(b) AS transferCount, SUM(t.amount) AS totalAmount
        WHERE transferCount > 100 AND totalAmount > 1000000
        RETURN a.id, transferCount, totalAmount
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql-23"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_structural_has_having(self) -> None:
        """Test SQL has HAVING clause."""
        cypher = """
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, COUNT(b) AS transferCount
        WHERE transferCount > 100
        RETURN a.id, transferCount
        """
        sql = self._transpile(cypher)

        assert_has_having(sql)

    def test_structural_has_group_by(self) -> None:
        """Test SQL has GROUP BY clause."""
        cypher = """
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, COUNT(b) AS transferCount
        WHERE transferCount > 100
        RETURN a.id, transferCount
        """
        sql = self._transpile(cypher)

        assert_has_group_by(sql)

    def test_having_with_count(self) -> None:
        """Test HAVING with COUNT aggregation."""
        cypher = """
        MATCH (a:Account)-[:TRANSFER]->(b:Account)
        WITH a, COUNT(b) AS cnt
        WHERE cnt > 10
        RETURN a.id, cnt
        """
        sql = self._transpile(cypher)

        assert_has_having(sql)
        assert_has_aggregation(sql, function="COUNT")

    def test_having_with_sum(self) -> None:
        """Test HAVING with SUM aggregation."""
        cypher = """
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, SUM(t.amount) AS total
        WHERE total > 10000
        RETURN a.id, total
        """
        sql = self._transpile(cypher)

        assert_has_having(sql)
        assert_has_aggregation(sql, function="SUM")

    def test_having_multiple_conditions(self) -> None:
        """Test HAVING with multiple aggregation conditions."""
        cypher = """
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, COUNT(b) AS cnt, SUM(t.amount) AS total
        WHERE cnt > 5 AND total > 10000
        RETURN a.id, cnt, total
        """
        sql = self._transpile(cypher)

        assert_has_having(sql)
        # Should have both COUNT and SUM in HAVING
        sql_upper = sql.upper()
        assert "COUNT" in sql_upper
        assert "SUM" in sql_upper

    def test_fraud_high_transaction_count(self) -> None:
        """Test fraud detection: accounts with abnormally high transaction counts."""
        cypher = """
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, COUNT(t) AS txCount, SUM(t.amount) AS totalSent
        WHERE txCount > 100
        RETURN a.id AS suspiciousAccount, txCount, totalSent
        ORDER BY txCount DESC
        """
        sql = self._transpile(cypher)

        assert_has_having(sql)
        assert_has_group_by(sql)

    def test_fraud_structuring_detection(self) -> None:
        """Test fraud detection: structuring (transactions just below threshold)."""
        cypher = """
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, COUNT(t) AS txCount, AVG(t.amount) AS avgAmount
        WHERE txCount > 10 AND avgAmount < 10000 AND avgAmount > 9000
        RETURN a.id, txCount, avgAmount
        """
        sql = self._transpile(cypher)

        assert_has_having(sql)
        # Should have AVG in the query
        sql_upper = sql.upper()
        assert "AVG" in sql_upper or "AVERAGE" in sql_upper
