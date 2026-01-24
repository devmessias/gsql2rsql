"""Test: Validate the fraud detection example from docs/index.md.

This test ensures the main documentation example actually works correctly
with real data and produces the expected results.

Test Graph (fraud network):
    12345 (Person, risk=0.1) --TRANSACTION--> 1001 (Person, risk=0.5)
    1001 --TRANSACTION--> 1002 (Person, risk=0.9)  <- HIGH RISK
    1002 --TRANSACTION--> 1003 (Person, risk=0.95) <- HIGH RISK
    1003 --TRANSACTION--> 1004 (Person, risk=0.85) <- HIGH RISK

    Also some branches:
    12345 --TRANSACTION--> 2001 (Person, risk=0.3)
    2001 --TRANSACTION--> 2002 (Person, risk=0.92)  <- HIGH RISK

    And a longer path (depth 4):
    12345 --TRANSACTION--> 3001 --TRANSACTION--> 3002 --TRANSACTION--> 3003 --TRANSACTION--> 3004 (risk=0.99)

Expected results for query:
    MATCH path = (origin:Person {id: 12345})-[:TRANSACTION*1..4]->(dest:Person)
    WHERE dest.risk_score > 0.8
    RETURN dest.id, dest.name, dest.risk_score, length(path) AS depth
    ORDER BY depth, dest.risk_score DESC
    LIMIT 100

Should return:
    depth=2: 1002 (0.9), 2002 (0.92)
    depth=3: 1003 (0.95)
    depth=4: 1004 (0.85), 3004 (0.99)
"""

import pytest
from pyspark.sql import SparkSession
from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    spark = (
        SparkSession.builder
        .appName("DocsExample_FraudTest")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def fraud_graph_data(spark):
    """Create test graph for fraud detection example."""
    # Nodes: id, name, risk_score, type
    nodes_data = [
        # Origin
        (12345, "Alice", 0.1, "Person"),
        # Path 1: 12345 -> 1001 -> 1002 -> 1003 -> 1004
        (1001, "Bob", 0.5, "Person"),
        (1002, "Charlie", 0.9, "Person"),      # HIGH RISK
        (1003, "David", 0.95, "Person"),       # HIGH RISK
        (1004, "Eve", 0.85, "Person"),         # HIGH RISK
        # Path 2: 12345 -> 2001 -> 2002
        (2001, "Frank", 0.3, "Person"),
        (2002, "Grace", 0.92, "Person"),       # HIGH RISK
        # Path 3: 12345 -> 3001 -> 3002 -> 3003 -> 3004 (depth 4)
        (3001, "Henry", 0.2, "Person"),
        (3002, "Ivy", 0.4, "Person"),
        (3003, "Jack", 0.6, "Person"),
        (3004, "Karen", 0.99, "Person"),       # HIGH RISK
        # Some low-risk nodes (should NOT appear in results)
        (4001, "Leo", 0.1, "Person"),
        (4002, "Mia", 0.2, "Person"),
    ]
    nodes_df = spark.createDataFrame(
        nodes_data,
        ["id", "name", "risk_score", "type"]
    )
    nodes_df.createOrReplaceTempView("nodes")

    # Edges: src, dst, amount, timestamp, relationship_type
    edges_data = [
        # Path 1
        (12345, 1001, 1000.0, "2024-01-01", "TRANSACTION"),
        (1001, 1002, 2000.0, "2024-01-02", "TRANSACTION"),
        (1002, 1003, 3000.0, "2024-01-03", "TRANSACTION"),
        (1003, 1004, 4000.0, "2024-01-04", "TRANSACTION"),
        # Path 2
        (12345, 2001, 500.0, "2024-01-01", "TRANSACTION"),
        (2001, 2002, 1500.0, "2024-01-02", "TRANSACTION"),
        # Path 3 (depth 4)
        (12345, 3001, 100.0, "2024-01-01", "TRANSACTION"),
        (3001, 3002, 200.0, "2024-01-02", "TRANSACTION"),
        (3002, 3003, 300.0, "2024-01-03", "TRANSACTION"),
        (3003, 3004, 400.0, "2024-01-04", "TRANSACTION"),
        # Low-risk path (should not appear)
        (12345, 4001, 50.0, "2024-01-01", "TRANSACTION"),
        (4001, 4002, 60.0, "2024-01-02", "TRANSACTION"),
    ]
    edges_df = spark.createDataFrame(
        edges_data,
        ["src", "dst", "amount", "timestamp", "relationship_type"]
    )
    edges_df.createOrReplaceTempView("edges")

    print("\n=== Fraud Test Graph ===")
    print("\nNodes:")
    nodes_df.show(truncate=False)
    print("\nEdges:")
    edges_df.show(truncate=False)

    return {"nodes": nodes_df, "edges": edges_df}


@pytest.fixture(scope="module")
def graph_context(spark, fraud_graph_data):
    """Create GraphContext for fraud detection."""
    graph = GraphContext(
        spark=spark,
        nodes_table="nodes",
        edges_table="edges",
        node_type_col="type",
        node_id_col="id",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={"name": str, "risk_score": float},
        extra_edge_attrs={"amount": float, "timestamp": str},
    )
    graph.set_types(
        node_types=["Person"],
        edge_types=["TRANSACTION"],
    )
    return graph


class TestDocsExampleFraudQuery:
    """Test the exact query from docs/index.md."""

    def test_fraud_query_transpiles(self, graph_context):
        """Verify the query from docs transpiles without error."""
        query = """
        MATCH path = (origin:Person {id: 12345})-[:TRANSACTION*1..4]->(dest:Person)
        WHERE dest.risk_score > 0.8
        RETURN dest.id, dest.name, dest.risk_score, length(path) AS depth
        ORDER BY depth, dest.risk_score DESC
        LIMIT 100
        """

        sql = graph_context.transpile(query)
        print(f"\n=== Generated SQL ===\n{sql}")

        # Basic structure checks
        assert "WITH RECURSIVE" in sql, "Should use recursive CTE"
        assert "paths_" in sql, "Should have paths CTE"
        assert "risk_score" in sql, "Should reference risk_score"
        assert "LIMIT 100" in sql, "Should have LIMIT"

    def test_fraud_query_executes_correctly(self, spark, graph_context):
        """Execute the docs query and verify results."""
        query = """
        MATCH path = (origin:Person {id: 12345})-[:TRANSACTION*1..4]->(dest:Person)
        WHERE dest.risk_score > 0.8
        RETURN dest.id, dest.name, dest.risk_score, length(path) AS depth
        ORDER BY depth, dest.risk_score DESC
        LIMIT 100
        """

        sql = graph_context.transpile(query)
        print(f"\n=== Generated SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        print("\n=== Query Results ===")
        for row in rows:
            print(f"  id={row['id']}, name={row['name']}, "
                  f"risk_score={row['risk_score']}, depth={row['depth']}")

        # Should have exactly 5 high-risk destinations
        # depth=2: 1002 (0.9), 2002 (0.92)
        # depth=3: 1003 (0.95)
        # depth=4: 1004 (0.85), 3004 (0.99)
        assert len(rows) == 5, f"Expected 5 results, got {len(rows)}"

        # Convert to dict for easier assertions
        results = {row['id']: row for row in rows}

        # Verify all high-risk nodes are found
        assert 1002 in results, "Should find Charlie (id=1002, risk=0.9)"
        assert 2002 in results, "Should find Grace (id=2002, risk=0.92)"
        assert 1003 in results, "Should find David (id=1003, risk=0.95)"
        assert 1004 in results, "Should find Eve (id=1004, risk=0.85)"
        assert 3004 in results, "Should find Karen (id=3004, risk=0.99)"

        # Verify depths
        assert results[1002]['depth'] == 2, "Charlie should be at depth 2"
        assert results[2002]['depth'] == 2, "Grace should be at depth 2"
        assert results[1003]['depth'] == 3, "David should be at depth 3"
        assert results[1004]['depth'] == 4, "Eve should be at depth 4"
        assert results[3004]['depth'] == 4, "Karen should be at depth 4"

        # Verify risk scores
        assert results[1002]['risk_score'] == 0.9
        assert results[2002]['risk_score'] == 0.92
        assert results[1003]['risk_score'] == 0.95
        assert results[1004]['risk_score'] == 0.85
        assert results[3004]['risk_score'] == 0.99

        # Verify names
        assert results[1002]['name'] == "Charlie"
        assert results[2002]['name'] == "Grace"
        assert results[1003]['name'] == "David"
        assert results[1004]['name'] == "Eve"
        assert results[3004]['name'] == "Karen"

        # Verify low-risk nodes are NOT in results
        assert 1001 not in results, "Bob (risk=0.5) should NOT be in results"
        assert 2001 not in results, "Frank (risk=0.3) should NOT be in results"
        assert 4001 not in results, "Leo (risk=0.1) should NOT be in results"
        assert 4002 not in results, "Mia (risk=0.2) should NOT be in results"

        print("\n✅ All assertions passed!")

    def test_fraud_query_ordering(self, spark, graph_context):
        """Verify results are ordered by depth ASC, risk_score DESC."""
        query = """
        MATCH path = (origin:Person {id: 12345})-[:TRANSACTION*1..4]->(dest:Person)
        WHERE dest.risk_score > 0.8
        RETURN dest.id, dest.name, dest.risk_score, length(path) AS depth
        ORDER BY depth, dest.risk_score DESC
        LIMIT 100
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Check ordering
        # depth=2 should come first, then depth=3, then depth=4
        # Within same depth, higher risk_score should come first

        depths = [row['depth'] for row in rows]
        assert depths == sorted(depths), f"Depths should be ascending: {depths}"

        # Group by depth and check risk_score ordering
        depth_2_rows = [r for r in rows if r['depth'] == 2]
        depth_4_rows = [r for r in rows if r['depth'] == 4]

        if len(depth_2_rows) == 2:
            # 2002 (0.92) should come before 1002 (0.9) at depth 2
            assert depth_2_rows[0]['id'] == 2002, "Grace (0.92) should be first at depth 2"
            assert depth_2_rows[1]['id'] == 1002, "Charlie (0.9) should be second at depth 2"

        if len(depth_4_rows) == 2:
            # 3004 (0.99) should come before 1004 (0.85) at depth 4
            assert depth_4_rows[0]['id'] == 3004, "Karen (0.99) should be first at depth 4"
            assert depth_4_rows[1]['id'] == 1004, "Eve (0.85) should be second at depth 4"

        print("\n✅ Ordering is correct!")

    def test_fraud_query_limit_works(self, spark, graph_context):
        """Verify LIMIT is applied correctly."""
        query = """
        MATCH path = (origin:Person {id: 12345})-[:TRANSACTION*1..4]->(dest:Person)
        WHERE dest.risk_score > 0.8
        RETURN dest.id, dest.name, dest.risk_score, length(path) AS depth
        ORDER BY depth, dest.risk_score DESC
        LIMIT 2
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 2, f"LIMIT 2 should return exactly 2 rows, got {len(rows)}"

        # Should be the first 2 by ordering (depth=2, highest risk)
        # 2002 (Grace, 0.92) and 1002 (Charlie, 0.9)
        ids = {row['id'] for row in rows}
        assert 2002 in ids, "First result should be Grace (id=2002)"
        assert 1002 in ids, "Second result should be Charlie (id=1002)"

        print(f"\n✅ LIMIT 2 works correctly: {[r['name'] for r in rows]}")


class TestDocsExampleEdgeCases:
    """Test edge cases for the fraud detection query."""

    def test_no_results_for_nonexistent_origin(self, spark, graph_context):
        """Query with non-existent origin should return empty."""
        query = """
        MATCH path = (origin:Person {id: 99999})-[:TRANSACTION*1..4]->(dest:Person)
        WHERE dest.risk_score > 0.8
        RETURN dest.id, length(path) AS depth
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 0, f"Non-existent origin should return 0 rows, got {len(rows)}"
        print("\n✅ Non-existent origin returns empty result")

    def test_high_risk_threshold_filters_all(self, spark, graph_context):
        """Very high risk threshold should return empty."""
        query = """
        MATCH path = (origin:Person {id: 12345})-[:TRANSACTION*1..4]->(dest:Person)
        WHERE dest.risk_score > 0.999
        RETURN dest.id, length(path) AS depth
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 0, f"risk_score > 0.999 should return 0 rows, got {len(rows)}"
        print("\n✅ High threshold filters all results")

    def test_depth_1_only(self, spark, graph_context):
        """Query with depth 1 only should find direct neighbors."""
        query = """
        MATCH path = (origin:Person {id: 12345})-[:TRANSACTION*1..1]->(dest:Person)
        RETURN dest.id, dest.name, length(path) AS depth
        ORDER BY dest.id
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        print("\n=== Depth 1 Results ===")
        for row in rows:
            print(f"  id={row['id']}, name={row['name']}, depth={row['depth']}")

        # Direct neighbors of 12345: 1001, 2001, 3001, 4001
        assert len(rows) == 4, f"Should have 4 direct neighbors, got {len(rows)}"

        ids = {row['id'] for row in rows}
        assert ids == {1001, 2001, 3001, 4001}, f"Wrong neighbors: {ids}"

        # All should be depth 1
        for row in rows:
            assert row['depth'] == 1, f"All should be depth 1, got {row['depth']}"

        print("\n✅ Depth 1 query works correctly")
