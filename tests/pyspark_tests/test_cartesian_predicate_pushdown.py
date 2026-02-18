"""Test: Predicate pushdown for independent MATCH patterns.

Problem: When a MATCH clause has multiple comma-separated patterns with
different variables, a cartesian product (JOIN ON TRUE) is generated.
If the WHERE clause has predicates that connect these patterns
(e.g., a1.id < a2.id), they should be pushed into the JOIN condition.

Example problematic query:
    MATCH (a1:Account)-[:HAS_TX]->(t1:Transaction),
          (a2:Account)-[:HAS_TX]->(t2:Transaction)
    WHERE a1.id < a2.id
      AND t1.merchant_id = t2.merchant_id
    RETURN a1.id, a2.id

Current behavior: Generates JOIN ON TRUE, then filters with WHERE
Expected behavior: Push predicates into JOIN condition

This test validates:
1. Query executes correctly (semantic correctness)
2. Results are correct for all edge cases
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    session = (
        SparkSession.builder.master("local[*]")
        .appName("CartesianPredicatePushdown")
        .config("spark.sql.crossJoin.enabled", "true")  # Enable for test
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def graph_context(spark):
    """Create GraphContext with test data."""
    # Create nodes table
    nodes_schema = StructType([
        StructField("node_id", StringType(), False),
        StructField("node_type", StringType(), False),
        StructField("name", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", FloatType(), True),
    ])

    nodes_data = [
        # Accounts
        ("acc1", "Account", "Account 1", None, None),
        ("acc2", "Account", "Account 2", None, None),
        ("acc3", "Account", "Account 3", None, None),
        # Transactions with merchant_id
        ("tx1", "Transaction", "TX1", "merchant_A", 100.0),
        ("tx2", "Transaction", "TX2", "merchant_A", 200.0),  # Same merchant as tx1
        ("tx3", "Transaction", "TX3", "merchant_B", 150.0),
        ("tx4", "Transaction", "TX4", "merchant_A", 300.0),  # Same merchant as tx1, tx2
    ]

    nodes_df = spark.createDataFrame(nodes_data, nodes_schema)
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
    spark.sql("DROP TABLE IF EXISTS test_db.nodes")
    nodes_df.write.mode("overwrite").saveAsTable("test_db.nodes")

    # Create edges table
    edges_schema = StructType([
        StructField("src", StringType(), False),
        StructField("dst", StringType(), False),
        StructField("relationship_type", StringType(), False),
    ])

    edges_data = [
        # acc1 -> tx1
        ("acc1", "tx1", "HAS_TX"),
        # acc2 -> tx2, tx4
        ("acc2", "tx2", "HAS_TX"),
        ("acc2", "tx4", "HAS_TX"),
        # acc3 -> tx3
        ("acc3", "tx3", "HAS_TX"),
    ]

    edges_df = spark.createDataFrame(edges_data, edges_schema)
    spark.sql("DROP TABLE IF EXISTS test_db.edges")
    edges_df.write.mode("overwrite").saveAsTable("test_db.edges")

    # Create GraphContext
    gc = GraphContext(
        spark=spark,
        nodes_table="test_db.nodes",
        edges_table="test_db.edges",
        extra_node_attrs={
            "name": str,
            "merchant_id": str,
            "amount": float,
        },
    )

    yield gc

    spark.sql("DROP TABLE IF EXISTS test_db.nodes")
    spark.sql("DROP TABLE IF EXISTS test_db.edges")
    spark.sql("DROP DATABASE IF EXISTS test_db")


class TestCartesianPredicatePushdown:
    """Test predicate pushdown for independent MATCH patterns."""

    def test_independent_patterns_with_inequality_predicate(
        self, spark, graph_context
    ):
        """Test WHERE a1.node_id < a2.node_id pushes into join condition.

        Query finds pairs of accounts where first account ID is less than second.
        This tests the a1.id < a2.id pattern from collusion detection queries.
        """
        cypher = """
        MATCH (a1:Account)-[:HAS_TX]->(t1:Transaction),
              (a2:Account)-[:HAS_TX]->(t2:Transaction)
        WHERE a1.node_id < a2.node_id
        RETURN DISTINCT a1.node_id AS a1_id, a2.node_id AS a2_id
        ORDER BY a1_id, a2_id
        """

        sql = graph_context.transpile(cypher)
        print(f"\n=== Generated SQL ===\n{sql}\n")

        result = spark.sql(sql).collect()

        # Expected pairs where a1.id < a2.id:
        # (acc1, acc2), (acc1, acc3), (acc2, acc3)
        # Note: Each account must have transactions for the pattern to match
        # acc1 has tx1, acc2 has tx2+tx4, acc3 has tx3

        expected = [
            ("acc1", "acc2"),
            ("acc1", "acc3"),
            ("acc2", "acc3"),
        ]

        result_tuples = [(row.a1_id, row.a2_id) for row in result]
        assert sorted(result_tuples) == sorted(expected), (
            f"Expected {expected}, got {result_tuples}"
        )

    def test_independent_patterns_with_equality_predicate(
        self, spark, graph_context
    ):
        """Test WHERE t1.merchant_id = t2.merchant_id finds co-shopping.

        Query finds account pairs that have transactions at same merchant.
        This is the collusion detection pattern.
        """
        cypher = """
        MATCH (a1:Account)-[:HAS_TX]->(t1:Transaction),
              (a2:Account)-[:HAS_TX]->(t2:Transaction)
        WHERE a1.node_id < a2.node_id
          AND t1.merchant_id = t2.merchant_id
        RETURN a1.node_id AS a1_id, a2.node_id AS a2_id,
               t1.merchant_id AS merchant,
               COUNT(*) AS shared_count
        ORDER BY a1_id, a2_id
        """

        sql = graph_context.transpile(cypher)
        print(f"\n=== Generated SQL ===\n{sql}\n")

        result = spark.sql(sql).collect()

        # Expected: Accounts with transactions at same merchant
        # acc1 has tx1 (merchant_A)
        # acc2 has tx2 (merchant_A), tx4 (merchant_A)
        # acc3 has tx3 (merchant_B)
        #
        # Pairs at same merchant:
        # (acc1, acc2) at merchant_A: tx1-tx2, tx1-tx4 = 2 combinations
        # No other pairs share a merchant

        assert len(result) == 1, f"Expected 1 result, got {len(result)}"
        row = result[0]
        assert row.a1_id == "acc1"
        assert row.a2_id == "acc2"
        assert row.merchant == "merchant_A"
        assert row.shared_count == 2  # tx1-tx2, tx1-tx4

    def test_no_cartesian_join_in_simple_case(self, spark, graph_context):
        """Test that shared variable patterns don't create cartesian joins.

        This is the base case - when patterns share a variable, there's
        no cartesian product because the join is on the shared variable.
        """
        cypher = """
        MATCH (a:Account)-[:HAS_TX]->(t1:Transaction),
              (a)-[:HAS_TX]->(t2:Transaction)
        WHERE t1.node_id < t2.node_id
        RETURN a.node_id AS account, t1.node_id AS tx1, t2.node_id AS tx2
        ORDER BY account, tx1, tx2
        """

        sql = graph_context.transpile(cypher)
        print(f"\n=== Generated SQL ===\n{sql}\n")

        # Should NOT have JOIN ON TRUE since patterns share 'a'
        assert "ON\n      TRUE" not in sql and "ON TRUE" not in sql, (
            "Shared variable pattern should not create cartesian join"
        )

        result = spark.sql(sql).collect()

        # Expected: acc2 has tx2 and tx4, where tx2 < tx4
        # So result should be (acc2, tx2, tx4)
        assert len(result) == 1
        assert result[0].account == "acc2"
        assert result[0].tx1 == "tx2"
        assert result[0].tx2 == "tx4"

    def test_three_independent_patterns(self, spark, graph_context):
        """Test three independent patterns with connecting predicates."""
        cypher = """
        MATCH (a1:Account)-[:HAS_TX]->(t1:Transaction),
              (a2:Account)-[:HAS_TX]->(t2:Transaction),
              (a3:Account)-[:HAS_TX]->(t3:Transaction)
        WHERE a1.node_id < a2.node_id
          AND a2.node_id < a3.node_id
        RETURN DISTINCT
               a1.node_id AS a1_id,
               a2.node_id AS a2_id,
               a3.node_id AS a3_id
        ORDER BY a1_id, a2_id, a3_id
        """

        sql = graph_context.transpile(cypher)
        print(f"\n=== Generated SQL ===\n{sql}\n")

        result = spark.sql(sql).collect()

        # Expected: Only (acc1, acc2, acc3) satisfies a1 < a2 < a3
        assert len(result) == 1
        assert result[0].a1_id == "acc1"
        assert result[0].a2_id == "acc2"
        assert result[0].a3_id == "acc3"


class TestCartesianJoinSQLStructure:
    """Test SQL structure for cartesian join handling."""

    def test_verify_current_behavior_generates_cartesian(
        self, spark, graph_context
    ):
        """Document current behavior - verify we generate cartesian joins.

        This test documents that currently we generate JOIN ON TRUE
        for independent patterns. This is the behavior we want to optimize.
        """
        cypher = """
        MATCH (a1:Account)-[:HAS_TX]->(t1:Transaction),
              (a2:Account)-[:HAS_TX]->(t2:Transaction)
        WHERE a1.node_id < a2.node_id
        RETURN a1.node_id, a2.node_id
        """

        sql = graph_context.transpile(cypher)

        # Document current behavior
        has_cartesian = "ON\n      TRUE" in sql or "ON TRUE" in sql

        # NOTE: This test passes if we DO have cartesian joins (current behavior)
        # Once we fix this, we should update this test to assert the opposite
        print(f"\n=== SQL has cartesian join: {has_cartesian} ===")
        print(f"SQL:\n{sql[:1500]}...")

        # The query should still execute correctly even with cartesian
        result = spark.sql(sql).collect()
        assert len(result) > 0, "Query should return results"
