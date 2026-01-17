"""Test 24: ALL / ANY predicate.

Validates that Cypher's ALL() and ANY() list predicates are correctly
transpiled to Databricks SQL using FILTER() and SIZE() functions.

Cypher to Databricks mappings:
- ALL(x IN list WHERE pred) -> SIZE(FILTER(list, x -> NOT pred)) = 0
- ANY(x IN list WHERE pred) -> SIZE(FILTER(list, x -> pred)) > 0
- NONE(x IN list WHERE pred) -> SIZE(FILTER(list, x -> pred)) = 0
- SINGLE(x IN list WHERE pred) -> SIZE(FILTER(list, x -> pred)) = 1

Fraud Use Cases:
- Verify all transactions in batch meet compliance rules
- Check if any transaction in window is flagged
- Validate all counterparties are verified
"""

import pytest

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
    assert_has_select,
    assert_has_from_table,
    assert_has_where,
    assert_has_group_by,
)


class TestAllAnyPredicate:
    """Test ALL/ANY predicate transpilation."""

    TEST_ID = "24"
    TEST_NAME = "all_any_predicate"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Graph schema for fraud detection scenario
        self.graph_schema = SimpleGraphSchemaProvider()
        self.graph_schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("name", str),
                    EntityProperty("verified", bool),
                ],
                node_id_property=EntityProperty("id", str),
            )
        )
        self.graph_schema.add_edge(
            EdgeSchema(
                name="TRANSFER",
                source_node_id="Account",
                sink_node_id="Account",
                properties=[
                    EntityProperty("amount", float),
                    EntityProperty("flagged", bool),
                    EntityProperty("timestamp", str),
                ],
                source_id_property=EntityProperty("source_id", str),
                sink_id_property=EntityProperty("target_id", str),
            )
        )

        # SQL schema
        self.sql_schema = SimpleSQLSchemaProvider()
        self.sql_schema.add_node(
            NodeSchema(
                name="Account",
                properties=[
                    EntityProperty("id", str),
                    EntityProperty("name", str),
                    EntityProperty("verified", bool),
                ],
                node_id_property=EntityProperty("id", str),
            ),
            SQLTableDescriptor(table_name="graph.Account"),
        )
        self.sql_schema.add_edge(
            EdgeSchema(
                name="TRANSFER",
                source_node_id="Account",
                sink_node_id="Account",
                properties=[
                    EntityProperty("amount", float),
                    EntityProperty("flagged", bool),
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
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def test_golden_file_match(self) -> None:
        """Test that transpiled SQL matches golden file."""
        cypher = """
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, COLLECT(t.amount) AS amounts
        WHERE ALL(x IN amounts WHERE x > 1000)
        RETURN a.id
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR

            raise AssertionError(
                f"No expected SQL file found at "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql. "
                "Create it first using: make dump-sql-24"
            )

        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_all_predicate_uses_filter(self) -> None:
        """Test ALL predicate translates to FILTER + SIZE = 0.

        ALL(x IN list WHERE cond) means "no element fails the condition"
        Databricks 17.x optimization: FORALL(list, x -> cond)
        Legacy pattern: SIZE(FILTER(list, x -> NOT cond)) = 0
        """
        cypher = """
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, COLLECT(t.amount) AS amounts
        WHERE ALL(x IN amounts WHERE x > 1000)
        RETURN a.id
        """
        sql = self._transpile(cypher)

        # ALL uses FORALL HOF (Databricks 17.x) or FILTER with NOT condition
        sql_upper = sql.upper()
        assert (
            "FORALL" in sql_upper or
            ("FILTER" in sql_upper and "SIZE" in sql_upper)
        ), "ALL predicate should use FORALL or FILTER with SIZE = 0"

    def test_any_predicate_uses_filter(self) -> None:
        """Test ANY predicate translates to FILTER + SIZE."""
        cypher = """
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, COLLECT(t.flagged) AS flags
        WHERE ANY(f IN flags WHERE f = true)
        RETURN a.id
        """
        sql = self._transpile(cypher)

        # ANY should use FILTER function with SIZE check
        sql_upper = sql.upper()
        assert (
            ("FILTER" in sql_upper and "SIZE" in sql_upper) or
            "ARRAY_CONTAINS" in sql_upper or  # Alternative for simple true check
            "EXISTS" in sql_upper
        ), "ANY predicate should use FILTER+SIZE or ARRAY_CONTAINS"

    def test_none_predicate(self) -> None:
        """Test NONE predicate translates to NOT EXISTS or FILTER + SIZE = 0.

        Databricks 17.x optimization: NOT EXISTS(list, x -> cond)
        Or for equality: NOT ARRAY_CONTAINS(list, value)
        """
        cypher = """
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, COLLECT(t.flagged) AS flags
        WHERE NONE(f IN flags WHERE f = true)
        RETURN a.id
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # NONE uses NOT EXISTS, NOT ARRAY_CONTAINS, or FILTER with SIZE = 0
        assert (
            "NOT EXISTS" in sql_upper or
            "NOT ARRAY_CONTAINS" in sql_upper or
            ("FILTER" in sql_upper and "SIZE" in sql_upper)
        ), "NONE predicate should use NOT EXISTS or FILTER with SIZE = 0"

    def test_single_predicate(self) -> None:
        """Test SINGLE predicate translates to FILTER + SIZE = 1."""
        cypher = """
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, COLLECT(t.flagged) AS flags
        WHERE SINGLE(f IN flags WHERE f = true)
        RETURN a.id
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # SINGLE should check SIZE = 1
        assert "FILTER" in sql_upper or "SIZE" in sql_upper, \
            "SINGLE predicate should use FILTER with SIZE = 1"

    def test_all_with_threshold(self) -> None:
        """Test ALL predicate with threshold comparison.

        Databricks 17.x: FORALL(list, x -> cond)
        """
        cypher = """
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, COLLECT(t.amount) AS amounts
        WHERE ALL(amt IN amounts WHERE amt > 10000)
        RETURN a.id AS compliantAccount
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert (
            "FORALL" in sql_upper or
            ("FILTER" in sql_upper and "SIZE" in sql_upper)
        )
        assert "10000" in sql

    def test_any_with_boolean(self) -> None:
        """Test ANY predicate with boolean value check."""
        cypher = """
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, COLLECT(t.flagged) AS flags
        WHERE ANY(f IN flags WHERE f = true)
        RETURN a.id AS suspiciousAccount
        """
        sql = self._transpile(cypher)

        # Should have filter or array_contains for boolean check
        sql_upper = sql.upper()
        assert (
            "FILTER" in sql_upper or
            "ARRAY_CONTAINS" in sql_upper or
            "EXISTS" in sql_upper
        )

    def test_fraud_all_verified_counterparties(self) -> None:
        """Test fraud scenario: verify all counterparties are verified.

        Databricks 17.x: FORALL(list, x -> cond)
        """
        cypher = """
        MATCH (a:Account)-[:TRANSFER]->(b:Account)
        WITH a, COLLECT(b.verified) AS counterpartyStatus
        WHERE ALL(v IN counterpartyStatus WHERE v = true)
        RETURN a.id AS trustedSender
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert (
            "FORALL" in sql_upper or
            ("FILTER" in sql_upper and "SIZE" in sql_upper)
        ), "Should use FORALL or FILTER + SIZE for ALL verification check"

    def test_fraud_any_flagged_transaction(self) -> None:
        """Test fraud scenario: check if any transaction is flagged.

        Databricks 17.x: EXISTS(list, x -> cond) or ARRAY_CONTAINS for equality
        """
        cypher = """
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, COLLECT(t.flagged) AS txFlags
        WHERE ANY(f IN txFlags WHERE f = true)
        RETURN a.id AS needsReview
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        assert (
            "EXISTS" in sql_upper or
            "FILTER" in sql_upper or
            "ARRAY_CONTAINS" in sql_upper
        ), "Should use EXISTS, FILTER, or ARRAY_CONTAINS for ANY flagged check"

    def test_combined_all_and_any(self) -> None:
        """Test combining ALL and ANY predicates.

        Databricks 17.x: FORALL for ALL, EXISTS for ANY
        """
        cypher = """
        MATCH (a:Account)-[t:TRANSFER]->(b:Account)
        WITH a, COLLECT(t.amount) AS amounts, COLLECT(t.flagged) AS flags
        WHERE ALL(amt IN amounts WHERE amt < 100000)
          AND ANY(f IN flags WHERE f = true)
        RETURN a.id
        """
        sql = self._transpile(cypher)

        sql_upper = sql.upper()
        # Should have FORALL/EXISTS or FILTER/SIZE for predicates
        assert (
            ("FORALL" in sql_upper and "EXISTS" in sql_upper) or
            ("FILTER" in sql_upper and "SIZE" in sql_upper)
        ), "Should have FORALL/EXISTS or FILTER/SIZE for predicates"
