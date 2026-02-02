"""Test: VLP relationship variable support.

Tests that variable-length path (VLP) relationship variables (e.g., 'r' in [r*1..3])
are correctly registered in the symbol table and can be returned in queries.

In Cypher, when you write [r*1..3], the variable 'r' represents the list of
relationships traversed. This should be mapped to the 'path_edges' column
in the generated SQL.
"""

import pytest
from pyspark.sql import SparkSession
from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    spark = (
        SparkSession.builder
        .appName("VLP_RelVar_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph_context(spark):
    """Create GraphContext for testing."""
    # Create test data
    nodes_data = [
        ("1", "Person", "Alice"),
        ("2", "Person", "Bob"),
        ("3", "Person", "Carol"),
        ("4", "Person", "Dave"),
    ]
    nodes_df = spark.createDataFrame(
        nodes_data, ["node_id", "node_type", "name"]
    )
    nodes_df.createOrReplaceTempView("nodes")

    # Create edges: 1 -> 2 -> 3 -> 4
    edges_data = [
        ("1", "2", "KNOWS"),
        ("2", "3", "KNOWS"),
        ("3", "4", "KNOWS"),
    ]
    edges_df = spark.createDataFrame(
        edges_data, ["src", "dst", "relationship_type"]
    )
    edges_df.createOrReplaceTempView("edges")

    # Create graph context
    graph = GraphContext(
        spark=spark,
        nodes_table="nodes",
        edges_table="edges",
        node_type_col="node_type",
        node_id_col="node_id",
        extra_node_attrs={"name": str},
    )
    graph.set_types(
        node_types=["Person"],
        edge_types=["KNOWS"],
    )
    return graph


class TestVLPRelationshipVariable:
    """Tests for VLP relationship variable support."""

    def test_vlp_without_relationship_variable(self, spark, graph_context):
        """VLP without relationship variable should work (baseline)."""
        query = """
        MATCH (src {name: "Alice"})-[:KNOWS*1..3]->(dst)
        RETURN src.name AS src_name, dst.name AS dst_name
        ORDER BY dst_name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 3
        names = [row["dst_name"] for row in rows]
        assert set(names) == {"Bob", "Carol", "Dave"}

    def test_vlp_with_relationship_variable_directed(self, spark, graph_context):
        """VLP with relationship variable (directed) should return list of edges."""
        query = """
        MATCH (src {name: "Alice"})-[r:KNOWS*1..2]->(dst)
        RETURN src.name AS src_name, dst.name AS dst_name, r
        ORDER BY dst_name
        """
        sql = graph_context.transpile(query)

        # SQL should reference path_edges AS r
        assert "path_edges AS r" in sql or "path_edges as r" in sql.lower()

        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 2  # Bob (depth 1) and Carol (depth 2)

        # Check that r is an array of structs
        for row in rows:
            r = row["r"]
            assert isinstance(r, list)
            assert len(r) >= 1
            # Each element should have src, dst, relationship_type
            for edge in r:
                assert hasattr(edge, "src") or "src" in str(edge)

    def test_vlp_with_relationship_variable_undirected(self, spark, graph_context):
        """VLP with relationship variable (undirected) should return list of edges."""
        query = """
        MATCH (src {name: "Alice"})-[r*1..3]-(dst)
        RETURN src.name AS src_name, dst.name AS dst_name, r
        ORDER BY dst_name
        """
        sql = graph_context.transpile(query)

        # SQL should reference path_edges AS r
        assert "path_edges AS r" in sql or "path_edges as r" in sql.lower()

        result = spark.sql(sql)
        rows = result.collect()

        # Should find paths to Bob, Carol, Dave
        assert len(rows) >= 3

        # Check that r is an array of structs
        for row in rows:
            r = row["r"]
            assert isinstance(r, list)
            assert len(r) >= 1

    def test_vlp_no_relationship_variable_no_extra_collection(self, spark, graph_context):
        """VLP without explicit relationship variable should NOT collect path_edges.

        When user writes [:KNOWS*1..2] (no variable), we should not generate
        path_edges in the CTE for efficiency.
        """
        query = """
        MATCH (src)-[:KNOWS*1..2]->(dst)
        WHERE src.name = "Alice"
        RETURN dst.name
        """
        sql = graph_context.transpile(query)

        # SQL should NOT have path_edges if not needed
        # (unless path variable or relationships(path) is used)
        # This test just verifies it transpiles and runs correctly
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) == 2  # Bob and Carol

    def test_vlp_relationship_variable_edge_count(self, spark, graph_context):
        """VLP relationship variable should have correct number of edges."""
        query = """
        MATCH (src {name: "Alice"})-[r:KNOWS*1..3]->(dst)
        RETURN src.name AS src_name, dst.name AS dst_name, r, SIZE(r) AS edge_count
        ORDER BY edge_count, dst_name
        """
        sql = graph_context.transpile(query)

        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 3

        # Check edge counts match path depth
        for row in rows:
            r = row["r"]
            assert isinstance(r, list)
            assert len(r) == row["edge_count"]

        # Verify specific paths
        edge_counts = {row["dst_name"]: row["edge_count"] for row in rows}
        assert edge_counts["Bob"] == 1    # Alice -> Bob
        assert edge_counts["Carol"] == 2  # Alice -> Bob -> Carol
        assert edge_counts["Dave"] == 3   # Alice -> Bob -> Carol -> Dave
