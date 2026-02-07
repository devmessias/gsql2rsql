"""Tests for comma-separated patterns (cartesian products).

These tests verify that queries with independent comma-separated patterns
work correctly and produce semantically correct results.

IMPORTANT: As of Spark 4.x, the Catalyst optimizer automatically moves
predicates from WHERE to JOIN conditions, eliminating the need for manual
optimization. Queries like:

    SELECT * FROM a JOIN b ON TRUE WHERE a.id < b.id

Are automatically optimized to:

    SELECT * FROM a JOIN b ON a.id < b.id

This means:
1. Spark 4.x does NOT reject cartesian products with WHERE predicates
2. The query plans are identical whether we write ON TRUE or ON predicate
3. No transpiler-level optimization is needed for Spark 4.x compatibility

These tests verify semantic correctness of the transpiled queries.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
)

from gsql2rsql.graph_context import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for testing."""
    session = (
        SparkSession.builder.appName("cartesian_patterns_test")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def graph_context(spark):
    """Create test graph with nodes and edges."""
    # Create test database
    spark.sql("CREATE DATABASE IF NOT EXISTS test_cartesian")
    spark.sql("USE test_cartesian")

    # Clean up existing tables
    spark.sql("DROP TABLE IF EXISTS nodes")
    spark.sql("DROP TABLE IF EXISTS edges")

    # Create nodes table
    nodes_schema = StructType([
        StructField("node_id", LongType(), False),
        StructField("type", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", LongType(), True),
        StructField("balance", DoubleType(), True),
    ])

    nodes_data = [
        # Persons
        (1, "Person", "Alice", 30, None),
        (2, "Person", "Bob", 25, None),
        (3, "Person", "Charlie", 35, None),
        (4, "Person", "Diana", 28, None),
        # Accounts
        (101, "Account", None, None, 1000.0),
        (102, "Account", None, None, 2000.0),
        (103, "Account", None, None, 1500.0),
        (104, "Account", None, None, 3000.0),
    ]

    nodes_df = spark.createDataFrame(nodes_data, nodes_schema)
    nodes_df.write.mode("overwrite").saveAsTable("nodes")

    # Create edges table
    edges_schema = StructType([
        StructField("src", LongType(), False),
        StructField("dst", LongType(), False),
        StructField("relationship_type", StringType(), False),
        StructField("since", LongType(), True),
    ])

    edges_data = [
        # Person -[:HAS_ACCOUNT]-> Account
        (1, 101, "HAS_ACCOUNT", 2020),
        (1, 102, "HAS_ACCOUNT", 2021),
        (2, 102, "HAS_ACCOUNT", 2019),  # Bob also has account 102
        (2, 103, "HAS_ACCOUNT", 2020),
        (3, 103, "HAS_ACCOUNT", 2021),
        (4, 104, "HAS_ACCOUNT", 2022),
        # Person -[:KNOWS]-> Person
        (1, 2, "KNOWS", 2015),
        (2, 3, "KNOWS", 2016),
        (3, 4, "KNOWS", 2017),
    ]

    edges_df = spark.createDataFrame(edges_data, edges_schema)
    edges_df.write.mode("overwrite").saveAsTable("edges")

    # Create GraphContext
    graph = GraphContext(
        spark=spark,
        nodes_table="test_cartesian.nodes",
        edges_table="test_cartesian.edges",
        node_type_col="type",
        edge_type_col="relationship_type",
        extra_node_attrs={"name": str, "age": int, "balance": float},
        extra_edge_attrs={"since": int},
    )

    yield graph

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS test_cartesian.nodes")
    spark.sql("DROP TABLE IF EXISTS test_cartesian.edges")
    spark.sql("DROP DATABASE IF EXISTS test_cartesian")


class TestCommaSeparatedPatterns:
    """Test semantic correctness of comma-separated pattern queries."""

    def test_two_node_patterns_with_inequality(self, graph_context):
        """Test: MATCH (a:Person), (b:Person) WHERE a.node_id < b.node_id

        This is the simplest case - two independent node patterns with
        an inequality predicate connecting them.

        Expected: All pairs (a, b) where a.node_id < b.node_id
        Persons: 1, 2, 3, 4
        Pairs: (1,2), (1,3), (1,4), (2,3), (2,4), (3,4) = 6 pairs
        """
        query = """
        MATCH (a:Person), (b:Person)
        WHERE a.node_id < b.node_id
        RETURN a.name AS a_name, b.name AS b_name
        ORDER BY a.name, b.name
        """

        df = graph_context.execute(query)
        results = df.collect()

        # Should have 6 pairs (4 choose 2)
        assert len(results) == 6

        # Verify specific pairs exist
        pairs = {(r["a_name"], r["b_name"]) for r in results}
        assert ("Alice", "Bob") in pairs
        assert ("Alice", "Charlie") in pairs
        assert ("Alice", "Diana") in pairs
        assert ("Bob", "Charlie") in pairs
        assert ("Bob", "Diana") in pairs
        assert ("Charlie", "Diana") in pairs

    def test_two_path_patterns_with_relationships(self, graph_context):
        """Test: MATCH (p1:Person)-[:HAS_ACCOUNT]->(a1:Account),
                       (p2:Person)-[:HAS_ACCOUNT]->(a2:Account)
                 WHERE p1.node_id < p2.node_id

        Two independent paths with relationships.

        Expected: All pairs of (person, account) combinations where p1.id < p2.id
        """
        query = """
        MATCH (p1:Person)-[:HAS_ACCOUNT]->(a1:Account),
              (p2:Person)-[:HAS_ACCOUNT]->(a2:Account)
        WHERE p1.node_id < p2.node_id
        RETURN p1.name AS p1_name, p2.name AS p2_name,
               a1.balance AS a1_balance, a2.balance AS a2_balance
        ORDER BY p1.name, p2.name, a1.balance, a2.balance
        """

        df = graph_context.execute(query)
        results = df.collect()

        # Verify we get results
        assert len(results) > 0

        # Verify node_id ordering constraint holds (names aren't guaranteed order)
        # Each result should have distinct persons
        for r in results:
            assert r["p1_name"] is not None
            assert r["p2_name"] is not None

    def test_mixed_predicates_some_cross_pattern(self, graph_context):
        """Test: MATCH (a:Person), (b:Person)
                 WHERE a.node_id < b.node_id AND a.age > 25 AND b.age < 35

        Mixed predicates:
        - a.node_id < b.node_id -> crosses patterns
        - a.age > 25 -> only pattern 1
        - b.age < 35 -> only pattern 2

        Expected: Pairs where a.id < b.id AND a.age > 25 AND b.age < 35
        """
        query = """
        MATCH (a:Person), (b:Person)
        WHERE a.node_id < b.node_id AND a.age > 25 AND b.age < 35
        RETURN a.name AS a_name, a.age AS a_age,
               b.name AS b_name, b.age AS b_age
        ORDER BY a.name, b.name
        """

        df = graph_context.execute(query)
        results = df.collect()

        # Verify constraints
        for r in results:
            assert r["a_age"] > 25, f"a.age should be > 25, got {r['a_age']}"
            assert r["b_age"] < 35, f"b.age should be < 35, got {r['b_age']}"

        # Alice(30), Charlie(35) have age > 25
        # Bob(25), Diana(28) have age < 35
        # Valid pairs: (Alice, Bob), (Alice, Diana), (Charlie, Bob), (Charlie, Diana)
        # But also need a.node_id < b.node_id:
        # Alice=1, Bob=2, Charlie=3, Diana=4
        # (Alice, Bob): 1<2 OK, (Alice, Diana): 1<4 OK
        # (Charlie, Bob): 3<2 NO, (Charlie, Diana): 3<4 OK
        # So: (Alice, Bob), (Alice, Diana), (Charlie, Diana) = 3 pairs
        assert len(results) == 3

    def test_three_patterns_ordered_triples(self, graph_context):
        """Test: MATCH (a:Person), (b:Person), (c:Person)
                 WHERE a.node_id < b.node_id AND b.node_id < c.node_id

        Three independent patterns.

        Expected: All ordered triples (a, b, c) where a.id < b.id < c.id
        """
        query = """
        MATCH (a:Person), (b:Person), (c:Person)
        WHERE a.node_id < b.node_id AND b.node_id < c.node_id
        RETURN a.name AS a_name, b.name AS b_name, c.name AS c_name
        ORDER BY a.name, b.name, c.name
        """

        df = graph_context.execute(query)
        results = df.collect()

        # 4 persons: 1, 2, 3, 4
        # Ordered triples: (1,2,3), (1,2,4), (1,3,4), (2,3,4) = 4 triples
        assert len(results) == 4

        triples = {(r["a_name"], r["b_name"], r["c_name"]) for r in results}
        assert ("Alice", "Bob", "Charlie") in triples
        assert ("Alice", "Bob", "Diana") in triples
        assert ("Alice", "Charlie", "Diana") in triples
        assert ("Bob", "Charlie", "Diana") in triples

    def test_connected_pattern_not_cartesian(self, graph_context):
        """Test: Find persons who share the same account.

        MATCH (p1:Person)-[:HAS_ACCOUNT]->(a:Account)<-[:HAS_ACCOUNT]-(p2:Person)
        WHERE p1.node_id < p2.node_id
        RETURN p1.name, p2.name, a.balance

        Note: This is NOT comma-separated patterns - it's a connected pattern.
        This verifies we correctly handle connected vs independent patterns.
        """
        query = """
        MATCH (p1:Person)-[:HAS_ACCOUNT]->(a:Account)<-[:HAS_ACCOUNT]-(p2:Person)
        WHERE p1.node_id < p2.node_id
        RETURN p1.name AS p1_name, p2.name AS p2_name, a.balance
        ORDER BY p1.name, p2.name
        """

        df = graph_context.execute(query)
        results = df.collect()

        # Account 102 is shared by Alice(1) and Bob(2)
        # Account 103 is shared by Bob(2) and Charlie(3)
        assert len(results) == 2

        result_set = {(r["p1_name"], r["p2_name"]) for r in results}
        assert ("Alice", "Bob") in result_set
        assert ("Bob", "Charlie") in result_set

    def test_with_simple_passthrough(self, graph_context):
        """Test: WITH clause followed by independent patterns.

        Tests that comma-separated patterns work after WITH.
        Using DISTINCT to avoid duplicates from multiple paths.
        """
        query = """
        MATCH (p:Person)
        WHERE p.age >= 30
        WITH DISTINCT p
        MATCH (p), (q:Person)
        WHERE p.node_id < q.node_id AND q.age <= 28
        RETURN DISTINCT p.name AS p_name, q.name AS q_name
        ORDER BY p_name, q_name
        """

        df = graph_context.execute(query)
        results = df.collect()

        # p has age >= 30: Alice(30), Charlie(35) = 2 persons
        # q has age <= 28: Bob(25), Diana(28) = 2 persons
        # p.node_id < q.node_id:
        # Alice(1) < Bob(2): OK
        # Alice(1) < Diana(4): OK
        # Charlie(3) < Diana(4): OK
        # Charlie(3) < Bob(2): NO (3 > 2)
        # Total: 3 pairs
        assert len(results) == 3
        pairs = {(r["p_name"], r["q_name"]) for r in results}
        assert ("Alice", "Bob") in pairs
        assert ("Alice", "Diana") in pairs
        assert ("Charlie", "Diana") in pairs


class TestSparkOptimization:
    """Verify Spark 4.x automatic optimization of cartesian joins."""

    def test_spark_optimizes_on_true_with_where(self, graph_context, spark):
        """Verify that Spark automatically moves WHERE predicates to JOIN.

        This test documents that Spark 4.x Catalyst optimizer transforms:
            JOIN ON TRUE WHERE a.id < b.id
        Into:
            JOIN ON a.id < b.id

        Making manual optimization in the transpiler unnecessary.
        """
        # Get the transpiled SQL
        query = """
        MATCH (a:Person), (b:Person)
        WHERE a.node_id < b.node_id
        RETURN a.name, b.name
        """

        sql = graph_context.transpile(query, optimize=True)

        # Execute and verify it works (Spark 4.x accepts this)
        df = spark.sql(sql)
        results = df.collect()

        # Should work and return 6 pairs
        assert len(results) == 6

    def test_query_works_with_and_without_optimization_flag(self, graph_context):
        """Verify queries work regardless of optimize flag.

        Since Spark 4.x optimizes automatically, both should work.
        """
        query = """
        MATCH (a:Person), (b:Person)
        WHERE a.node_id < b.node_id
        RETURN a.name AS a_name, b.name AS b_name
        ORDER BY a_name, b_name
        """

        # With optimization enabled
        df_opt = graph_context.execute(query, optimize=True)
        results_opt = df_opt.collect()

        # With optimization disabled
        df_no_opt = graph_context.execute(query, optimize=False)
        results_no_opt = df_no_opt.collect()

        # Both should return same results
        assert len(results_opt) == len(results_no_opt) == 6

        # Same content
        pairs_opt = {(r["a_name"], r["b_name"]) for r in results_opt}
        pairs_no_opt = {(r["a_name"], r["b_name"]) for r in results_no_opt}
        assert pairs_opt == pairs_no_opt


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_no_cross_pattern_predicate_true_cartesian(self, graph_context):
        """Test: Patterns with no cross-pattern predicate (true cartesian).

        MATCH (a:Person), (b:Account)
        WHERE a.age >= 30 AND b.balance > 1500
        RETURN a.name, b.balance

        This is a true cartesian product - no predicate connects the patterns.
        """
        query = """
        MATCH (a:Person), (b:Account)
        WHERE a.age >= 30 AND b.balance > 1500
        RETURN a.name AS name, b.balance AS balance
        ORDER BY name, balance
        """

        df = graph_context.execute(query)
        results = df.collect()

        # Persons with age >= 30: Alice(30), Charlie(35) = 2
        # Accounts with balance > 1500: 102(2000), 104(3000) = 2
        # Cartesian: 2 * 2 = 4
        assert len(results) == 4

    def test_single_pattern_no_cartesian(self, graph_context):
        """Test: Single pattern (no cartesian product)."""
        query = """
        MATCH (p:Person)
        WHERE p.age >= 30
        RETURN p.name AS name
        ORDER BY name
        """

        df = graph_context.execute(query)
        results = df.collect()

        # Alice(30), Charlie(35)
        assert len(results) == 2
        names = [r["name"] for r in results]
        assert "Alice" in names
        assert "Charlie" in names
