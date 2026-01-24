"""Test: Bug where src == dst in directed queries.

This test uses multiple node types and edge types to reproduce
the bug seen in production.

Test Graph:
    Alice (Person) ---sent---> Bob (Person)
    Carol (Person) ---sent---> Dave (Company)
    Eve (Company) ---paid---> Frank (Person)

Expected: src != dst for all rows
Bug: src == dst
"""

import pytest
from pyspark.sql import SparkSession
from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    spark = (
        SparkSession.builder
        .appName("SrcDstBug_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph_data(spark):
    """Create test graph with multiple node and edge types."""
    nodes_data = [
        ("Alice", "Person"),
        ("Bob", "Person"),
        ("Carol", "Person"),
        ("Dave", "Company"),
        ("Eve", "Company"),
        ("Frank", "Person"),
    ]
    nodes_df = spark.createDataFrame(nodes_data, ["node_id", "type"])
    nodes_df.createOrReplaceTempView("nodes")

    edges_data = [
        ("Alice", "Bob", "sent"),
        ("Carol", "Dave", "sent"),
        ("Eve", "Frank", "paid"),
    ]
    edges_df = spark.createDataFrame(edges_data, ["src", "dst", "relationship_type"])
    edges_df.createOrReplaceTempView("edges")

    print("\n=== Nodes ===")
    nodes_df.show()
    print("\n=== Edges ===")
    edges_df.show()

    return {"nodes": nodes_df, "edges": edges_df}


@pytest.fixture(scope="module")
def graph_context(spark, graph_data):
    """Create GraphContext with multiple types."""
    graph = GraphContext(
        spark=spark,
        nodes_table="nodes",
        edges_table="edges",
        node_type_col="type",
        node_id_col="node_id",
    )
    graph.set_types(
        node_types=["Person", "Company"],
        edge_types=["sent", "paid"],
    )
    return graph


class TestSrcDstBug:
    """Test that src != dst in directed queries."""

    def test_directed_src_not_equal_dst(self, spark, graph_context):
        """In directed query, src should NOT equal dst.

        Query: (origem)-[:sent]->(destino)

        Expected results:
            src=Alice, dst=Bob
            src=Carol, dst=Dave

        Bug symptom: src == dst for all rows
        """
        query = """
        MATCH (origem)-[:sent]->(destino)
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY src
        """

        sql = graph_context.transpile(query)
        print(f"\n=== Generated SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        print("\n=== Results ===")
        for row in rows:
            print(f"  src={row['src']}, dst={row['dst']}")

        # Verify we have results
        assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"

        # THE MAIN ASSERTION: src should NOT equal dst
        for row in rows:
            assert row["src"] != row["dst"], (
                f"BUG: src ({row['src']}) == dst ({row['dst']}). "
                "In a directed edge, source and destination must be different!"
            )

        # Verify exact expected values
        results = [(row["src"], row["dst"]) for row in rows]
        expected = [("Alice", "Bob"), ("Carol", "Dave")]
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"\n✅ Test passed: {results}")

    def test_directed_paid_edge(self, spark, graph_context):
        """Test with paid edge type."""
        query = """
        MATCH (origem)-[:paid]->(destino)
        RETURN origem.node_id AS src, destino.node_id AS dst
        """

        sql = graph_context.transpile(query)
        print(f"\n=== Generated SQL (paid) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        print("\n=== Results ===")
        for row in rows:
            print(f"  src={row['src']}, dst={row['dst']}")

        assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"

        row = rows[0]
        assert row["src"] != row["dst"], (
            f"BUG: src ({row['src']}) == dst ({row['dst']})"
        )
        assert row["src"] == "Eve", f"Expected src=Eve, got {row['src']}"
        assert row["dst"] == "Frank", f"Expected dst=Frank, got {row['dst']}"

        print(f"\n✅ Test passed: src={row['src']}, dst={row['dst']}")

    def test_untyped_edge_all_types(self, spark, graph_context):
        """Test untyped edge (matches all edge types)."""
        query = """
        MATCH (origem)-[]->(destino)
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY src
        """

        sql = graph_context.transpile(query)
        print(f"\n=== Generated SQL (untyped) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        print("\n=== Results ===")
        for row in rows:
            print(f"  src={row['src']}, dst={row['dst']}")

        # Should have 3 edges total
        assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"

        # All should have src != dst
        for row in rows:
            assert row["src"] != row["dst"], (
                f"BUG: src ({row['src']}) == dst ({row['dst']})"
            )

        results = {(row["src"], row["dst"]) for row in rows}
        expected = {("Alice", "Bob"), ("Carol", "Dave"), ("Eve", "Frank")}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"\n✅ Test passed: {results}")
