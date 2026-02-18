"""PySpark tests for P1a: Self-loop deduplication in undirected UNION ALL.

When an undirected relationship query uses UNION ALL (forward + reverse),
a self-loop edge (X)-[:KNOWS]->(X) matches BOTH branches, causing it to
appear twice. The reverse branch should exclude self-loops.

Test Graph:
    Alice ---KNOWS---> Bob
    Alice ---KNOWS---> Alice  (self-loop)
"""

import pytest
from pyspark.sql import SparkSession

from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .appName("SelfLoopDedup_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW loop_nodes AS
        SELECT * FROM VALUES
            ('Alice', 'Person'),
            ('Bob',   'Person')
        AS t(node_id, node_type)
    """)
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW loop_edges AS
        SELECT * FROM VALUES
            ('Alice', 'Bob',   'KNOWS'),
            ('Alice', 'Alice', 'KNOWS')
        AS t(src, dst, relationship_type)
    """)
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph(spark):
    ctx = GraphContext(
        spark=spark,
        nodes_table="loop_nodes",
        edges_table="loop_edges",
        node_id_col="node_id",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        edge_src_col="src",
        edge_dst_col="dst",
    )
    ctx.set_types(
        node_types=["Person"],
        edge_types=["KNOWS"],
    )
    return ctx


class TestSelfLoopDedup:
    """Tests for self-loop deduplication in undirected queries."""

    def test_undirected_no_self_loop_dup(self, spark, graph):
        """Undirected query: self-loop should appear once, not twice.

        Alice -[:KNOWS]- b WHERE a='Alice':
          forward: Alice->Bob, Alice->Alice
          reverse: Bob->Alice (ok), Alice->Alice (DUPLICATE!)

        Expected: {Bob, Alice} with Alice appearing exactly once.
        """
        query = """
        MATCH (a:Person)-[:KNOWS]-(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL (undirected) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        dst_values = [row["dst"] for row in rows]

        # Alice should appear exactly once (from the self-loop, forward only)
        alice_count = dst_values.count("Alice")
        assert alice_count == 1, (
            f"Self-loop Alice should appear once, got {alice_count} times. "
            f"Full results: {dst_values}"
        )

        # Total: Alice (once) + Bob (once from forward Alice->Bob)
        assert set(dst_values) == {"Alice", "Bob"}, (
            f"Expected {{Alice, Bob}}, got {dst_values}"
        )

    def test_directed_self_loop_unaffected(self, spark, graph):
        """Directed query: no UNION ALL, self-loop appears once naturally."""
        query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL (directed) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}
        expected = {"Bob", "Alice"}
        assert results == expected, f"Expected {expected}, got {results}"
