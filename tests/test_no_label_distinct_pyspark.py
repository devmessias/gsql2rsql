"""Test: No-label node queries with and without DISTINCT.

Tests for verifying transpiler correctness when:
1. Source node has no label: (a)-[:REL]->(b:Label)
2. Destination node has no label: (a:Label)-[:REL]->(b)
3. Both nodes have no labels: (a)-[:REL]->(b)
4. With and without DISTINCT on returned columns
5. Directed vs undirected traversals

Test Graph:
    Alice ---KNOWS---> Bob ---KNOWS---> Carol
      |                 ^
      v                 |
    Dave ---KNOWS---> Eve ---DEFRAUDED---> Frank
                        |
                        v
                      Grace (DEFRAUDED)

Edges (directed):
  Alice -> Bob (KNOWS)
  Alice -> Dave (KNOWS)
  Bob -> Carol (KNOWS)
  Dave -> Eve (KNOWS)
  Eve -> Bob (KNOWS)
  Eve -> Frank (DEFRAUDED)
  Eve -> Grace (DEFRAUDED)
"""

import pytest
from pyspark.sql import SparkSession
from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    spark = (
        SparkSession.builder
        .appName("NoLabel_Distinct_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph_data(spark):
    """Create test graph data with multiple edge types."""
    nodes_data = [
        ("Alice", "Person", 25),
        ("Bob", "Person", 30),
        ("Carol", "Person", 28),
        ("Dave", "Person", 35),
        ("Eve", "Person", 27),
        ("Frank", "Person", 40),
        ("Grace", "Person", 22),
    ]
    nodes_df = spark.createDataFrame(
        nodes_data, ["node_id", "node_type", "age"]
    )
    nodes_df.createOrReplaceTempView("nodes")

    edges_data = [
        ("Alice", "Bob", "KNOWS"),
        ("Alice", "Dave", "KNOWS"),
        ("Bob", "Carol", "KNOWS"),
        ("Dave", "Eve", "KNOWS"),
        ("Eve", "Bob", "KNOWS"),
        ("Eve", "Frank", "DEFRAUDED"),
        ("Eve", "Grace", "DEFRAUDED"),
    ]
    edges_df = spark.createDataFrame(
        edges_data, ["src", "dst", "relationship_type"]
    )
    edges_df.createOrReplaceTempView("edges")

    return {"nodes": nodes_df, "edges": edges_df}


@pytest.fixture(scope="module")
def graph_context(spark, graph_data):
    """Create GraphContext for the test graph."""
    graph = GraphContext(
        spark=spark,
        nodes_table="nodes",
        edges_table="edges",
        node_type_col="node_type",
        node_id_col="node_id",
        extra_node_attrs={"age": int},
    )
    graph.set_types(
        node_types=["Person"],
        edge_types=["KNOWS", "DEFRAUDED"],
    )
    return graph


class TestNoLabelSourceDirected:
    """Tests for (a)-[:REL]->(b:Label) - no label on source."""

    def test_no_label_source_all_edges(self, spark, graph_context):
        """All edges without filtering source by label."""
        query = """
        MATCH (origem)-[:KNOWS]->(destino:Person)
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY src, dst
        """

        sql = graph_context.transpile(query)
        print(f"\n=== SQL for no-label source (all edges) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        results = {(row["src"], row["dst"]) for row in rows}
        # All KNOWS edges - source has no label filter
        expected = {
            ("Alice", "Bob"),
            ("Alice", "Dave"),
            ("Bob", "Carol"),
            ("Dave", "Eve"),
            ("Eve", "Bob"),
        }
        assert results == expected, f"Expected {expected}, got {results}"
        print(f"✅ No-label source (all edges): {len(results)} rows")

    def test_no_label_source_distinct_destinations(self, spark, graph_context):
        """Get DISTINCT destinations without filtering source by label."""
        query = """
        MATCH (origem)-[:KNOWS]->(destino:Person)
        RETURN DISTINCT destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        print(f"\n=== SQL for DISTINCT destinations ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        results = [row["dst"] for row in rows]
        # Unique destinations: Bob (from Alice, Eve), Carol (from Bob),
        # Dave (from Alice), Eve (from Dave)
        expected = ["Bob", "Carol", "Dave", "Eve"]
        assert results == expected, f"Expected {expected}, got {results}"
        print(f"✅ DISTINCT destinations: {results}")

    def test_no_label_source_without_distinct(self, spark, graph_context):
        """Get all destinations (with duplicates) without DISTINCT."""
        query = """
        MATCH (origem)-[:KNOWS]->(destino:Person)
        RETURN destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = [row["dst"] for row in rows]
        # Bob appears twice (from Alice, from Eve)
        expected = ["Bob", "Bob", "Carol", "Dave", "Eve"]
        assert results == expected, f"Expected {expected}, got {results}"
        print(f"✅ Without DISTINCT: {results}")


class TestNoLabelDestinationDirected:
    """Tests for (a:Label)-[:REL]->(b) - no label on destination."""

    def test_no_label_destination_all_edges(self, spark, graph_context):
        """All edges without filtering destination by label."""
        query = """
        MATCH (origem:Person)-[:KNOWS]->(destino)
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY src, dst
        """

        sql = graph_context.transpile(query)
        print(f"\n=== SQL for no-label destination (all edges) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        results = {(row["src"], row["dst"]) for row in rows}
        expected = {
            ("Alice", "Bob"),
            ("Alice", "Dave"),
            ("Bob", "Carol"),
            ("Dave", "Eve"),
            ("Eve", "Bob"),
        }
        assert results == expected, f"Expected {expected}, got {results}"
        print(f"✅ No-label destination (all edges): {len(results)} rows")

    def test_no_label_destination_distinct_sources(self, spark, graph_context):
        """Get DISTINCT sources with label, destination without label."""
        query = """
        MATCH (origem:Person)-[:KNOWS]->(destino)
        RETURN DISTINCT origem.node_id AS src
        ORDER BY src
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = [row["src"] for row in rows]
        # Unique sources: Alice, Bob, Dave, Eve
        expected = ["Alice", "Bob", "Dave", "Eve"]
        assert results == expected, f"Expected {expected}, got {results}"
        print(f"✅ DISTINCT sources: {results}")


class TestNoLabelBothDirected:
    """Tests for (a)-[:REL]->(b) - no labels on either node."""

    def test_no_label_both_all_edges(self, spark, graph_context):
        """All KNOWS edges without any label filters."""
        query = """
        MATCH (origem)-[:KNOWS]->(destino)
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY src, dst
        """

        sql = graph_context.transpile(query)
        print(f"\n=== SQL for no-label both (all edges) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        results = {(row["src"], row["dst"]) for row in rows}
        expected = {
            ("Alice", "Bob"),
            ("Alice", "Dave"),
            ("Bob", "Carol"),
            ("Dave", "Eve"),
            ("Eve", "Bob"),
        }
        assert results == expected, f"Expected {expected}, got {results}"
        print(f"✅ No-label both (all edges): {len(results)} rows")

    def test_no_label_both_distinct_all(self, spark, graph_context):
        """Get DISTINCT src-dst pairs (should be same as without DISTINCT for unique edges)."""
        query = """
        MATCH (origem)-[:KNOWS]->(destino)
        RETURN DISTINCT origem.node_id AS src, destino.node_id AS dst
        ORDER BY src, dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = [(row["src"], row["dst"]) for row in rows]
        expected = [
            ("Alice", "Bob"),
            ("Alice", "Dave"),
            ("Bob", "Carol"),
            ("Dave", "Eve"),
            ("Eve", "Bob"),
        ]
        assert results == expected, f"Expected {expected}, got {results}"
        print(f"✅ DISTINCT src-dst pairs: {len(results)} rows")

    def test_no_label_both_distinct_sources_only(self, spark, graph_context):
        """Get only DISTINCT source nodes."""
        query = """
        MATCH (origem)-[:KNOWS]->(destino)
        RETURN DISTINCT origem.node_id AS src
        ORDER BY src
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = [row["src"] for row in rows]
        expected = ["Alice", "Bob", "Dave", "Eve"]
        assert results == expected, f"Expected {expected}, got {results}"
        print(f"✅ DISTINCT sources only: {results}")

    def test_no_label_both_distinct_destinations_only(self, spark, graph_context):
        """Get only DISTINCT destination nodes."""
        query = """
        MATCH (origem)-[:KNOWS]->(destino)
        RETURN DISTINCT destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = [row["dst"] for row in rows]
        expected = ["Bob", "Carol", "Dave", "Eve"]
        assert results == expected, f"Expected {expected}, got {results}"
        print(f"✅ DISTINCT destinations only: {results}")


class TestNoLabelUndirected:
    """Tests for undirected traversals without labels."""

    def test_no_label_both_undirected(self, spark, graph_context):
        """(a)-[:KNOWS]-(b) - undirected without labels."""
        query = """
        MATCH (origem)-[:KNOWS]-(destino)
        WHERE origem.node_id = 'Bob'
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        print(f"\n=== SQL for no-label undirected from Bob ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        # Bob's neighbors undirected: Alice->Bob, Bob->Carol, Eve->Bob
        expected = {"Alice", "Carol", "Eve"}
        assert results == expected, f"Expected {expected}, got {results}"
        print(f"✅ No-label undirected from Bob: {results}")

    def test_no_label_undirected_distinct(self, spark, graph_context):
        """Get DISTINCT neighbors in undirected traversal."""
        query = """
        MATCH (origem)-[:KNOWS]-(destino)
        RETURN DISTINCT origem.node_id AS src, destino.node_id AS dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        count = result.count()

        # Each edge appears twice in undirected (once from each endpoint)
        # 5 KNOWS edges * 2 = 10 pairs
        assert count == 10, f"Expected 10 pairs, got {count}"
        print(f"✅ No-label undirected DISTINCT pairs: {count}")

    def test_no_label_undirected_all_neighbors(self, spark, graph_context):
        """Get all undirected edges (with duplicates due to bidirection)."""
        query = """
        MATCH (origem)-[:KNOWS]-(destino)
        RETURN origem.node_id AS src, destino.node_id AS dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        count = result.count()

        # 5 KNOWS edges * 2 directions = 10 rows
        assert count == 10, f"Expected 10 rows, got {count}"
        print(f"✅ No-label undirected all rows: {count}")


class TestMixedLabelCombinations:
    """Tests for various label combinations."""

    def test_source_label_destination_no_label_defrauded(self, spark, graph_context):
        """(a:Person)-[:DEFRAUDED]->(b) - source with label, destination without."""
        query = """
        MATCH (origem:Person)-[:DEFRAUDED]->(destino)
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = {(row["src"], row["dst"]) for row in rows}
        expected = {("Eve", "Frank"), ("Eve", "Grace")}
        assert results == expected, f"Expected {expected}, got {results}"
        print(f"✅ Source label, no destination label: {results}")

    def test_no_label_source_destination_label_defrauded(self, spark, graph_context):
        """(a)-[:DEFRAUDED]->(b:Person) - source without label, destination with."""
        query = """
        MATCH (origem)-[:DEFRAUDED]->(destino:Person)
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = {(row["src"], row["dst"]) for row in rows}
        expected = {("Eve", "Frank"), ("Eve", "Grace")}
        assert results == expected, f"Expected {expected}, got {results}"
        print(f"✅ No source label, destination label: {results}")


class TestDistinctWithFilters:
    """Tests for DISTINCT with WHERE filters."""

    def test_distinct_with_where_filter_on_source(self, spark, graph_context):
        """DISTINCT with WHERE filter on source node."""
        query = """
        MATCH (origem)-[:KNOWS]->(destino)
        WHERE origem.age > 25
        RETURN DISTINCT origem.node_id AS src
        ORDER BY src
        """

        sql = graph_context.transpile(query)
        print(f"\n=== SQL for DISTINCT with WHERE ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        results = [row["src"] for row in rows]
        # Sources with age > 25: Bob (30), Dave (35), Eve (27)
        expected = ["Bob", "Dave", "Eve"]
        assert results == expected, f"Expected {expected}, got {results}"
        print(f"✅ DISTINCT with WHERE age > 25: {results}")

    def test_distinct_with_where_filter_on_destination(self, spark, graph_context):
        """DISTINCT with WHERE filter on destination node."""
        query = """
        MATCH (origem)-[:KNOWS]->(destino)
        WHERE destino.age >= 30
        RETURN DISTINCT destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = [row["dst"] for row in rows]
        # Destinations with age >= 30: Bob (30), Dave (35)
        expected = ["Bob", "Dave"]
        assert results == expected, f"Expected {expected}, got {results}"
        print(f"✅ DISTINCT destinations with age >= 30: {results}")


class TestCountWithNoLabel:
    """Tests for COUNT operations with no-label queries."""

    def test_count_all_edges_no_labels(self, spark, graph_context):
        """COUNT all edges without labels."""
        query = """
        MATCH (origem)-[:KNOWS]->(destino)
        RETURN COUNT(*) AS total
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        count = rows[0]["total"]
        assert count == 5, f"Expected 5 KNOWS edges, got {count}"
        print(f"✅ COUNT all KNOWS edges: {count}")

    def test_count_distinct_sources_no_labels(self, spark, graph_context):
        """COUNT DISTINCT sources without labels."""
        query = """
        MATCH (origem)-[:KNOWS]->(destino)
        RETURN COUNT(DISTINCT origem.node_id) AS unique_sources
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        count = rows[0]["unique_sources"]
        # Unique sources: Alice, Bob, Dave, Eve
        assert count == 4, f"Expected 4 unique sources, got {count}"
        print(f"✅ COUNT DISTINCT sources: {count}")

    def test_count_distinct_destinations_no_labels(self, spark, graph_context):
        """COUNT DISTINCT destinations without labels."""
        query = """
        MATCH (origem)-[:KNOWS]->(destino)
        RETURN COUNT(DISTINCT destino.node_id) AS unique_destinations
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        count = rows[0]["unique_destinations"]
        # Unique destinations: Bob, Carol, Dave, Eve
        assert count == 4, f"Expected 4 unique destinations, got {count}"
        print(f"✅ COUNT DISTINCT destinations: {count}")


class TestCompareWithAndWithoutLabels:
    """Compare results with and without labels to verify consistency."""

    def test_same_results_with_and_without_labels(self, spark, graph_context):
        """Verify that queries with all nodes being Person produce same results."""
        # With labels
        query_with_labels = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        RETURN a.node_id AS src, b.node_id AS dst
        ORDER BY src, dst
        """

        # Without labels (should be same since all nodes are Person)
        query_without_labels = """
        MATCH (a)-[:KNOWS]->(b)
        RETURN a.node_id AS src, b.node_id AS dst
        ORDER BY src, dst
        """

        sql_with = graph_context.transpile(query_with_labels)
        sql_without = graph_context.transpile(query_without_labels)

        result_with = spark.sql(sql_with).collect()
        result_without = spark.sql(sql_without).collect()

        results_with = [(r["src"], r["dst"]) for r in result_with]
        results_without = [(r["src"], r["dst"]) for r in result_without]

        assert results_with == results_without, (
            f"Results with labels {results_with} != without labels {results_without}"
        )
        print(f"✅ With/without labels produce same results: {len(results_with)} rows")

    def test_distinct_count_matches(self, spark, graph_context):
        """Verify DISTINCT counts match with and without labels."""
        # With labels
        query_with = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        RETURN COUNT(DISTINCT a.node_id) AS cnt
        """

        # Without labels
        query_without = """
        MATCH (a)-[:KNOWS]->(b)
        RETURN COUNT(DISTINCT a.node_id) AS cnt
        """

        cnt_with = spark.sql(graph_context.transpile(query_with)).collect()[0]["cnt"]
        cnt_without = spark.sql(graph_context.transpile(query_without)).collect()[0]["cnt"]

        assert cnt_with == cnt_without, f"Count with {cnt_with} != without {cnt_without}"
        print(f"✅ DISTINCT counts match: {cnt_with}")
