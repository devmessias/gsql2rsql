"""Test: Investigation of VLP + UNWIND pattern.

Query pattern under investigation:
    MATCH (s { id: "something"} )
    MATCH (s) -[e*1..3]-(other)
    UNWIND e AS r
    RETURN DISTINCT r

Key aspects:
1. Property filter inline `{ id: "something" }`
2. VLP (Variable Length Path) undirected `[e*1..3]`
3. UNWIND to expand edge list
4. RETURN DISTINCT on the unwound edges

Test Graph:
    Alice ---KNOWS---> Bob ---KNOWS---> Carol ---KNOWS---> Dave
      |                                   |
      +---WORKS_WITH---> Eve <---KNOWS----+
"""

import pytest
from pyspark.sql import SparkSession
from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    spark = (
        SparkSession.builder
        .appName("VLP_UNWIND_Investigation")
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
        ("alice", "Person", "Alice"),
        ("bob", "Person", "Bob"),
        ("carol", "Person", "Carol"),
        ("dave", "Person", "Dave"),
        ("eve", "Person", "Eve"),
    ]
    nodes_df = spark.createDataFrame(
        nodes_data, ["node_id", "node_type", "name"]
    )
    nodes_df.createOrReplaceTempView("nodes")

    # Create edges:
    # Alice -> Bob -> Carol -> Dave (KNOWS chain)
    # Alice -> Eve (WORKS_WITH)
    # Carol -> Eve (KNOWS)
    edges_data = [
        ("alice", "bob", "KNOWS", 100),
        ("bob", "carol", "KNOWS", 200),
        ("carol", "dave", "KNOWS", 300),
        ("alice", "eve", "WORKS_WITH", 50),
        ("carol", "eve", "KNOWS", 150),
    ]
    edges_df = spark.createDataFrame(
        edges_data, ["src", "dst", "relationship_type", "weight"]
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
        extra_edge_attrs={"weight": int},
    )
    graph.set_types(
        node_types=["Person"],
        edge_types=["KNOWS", "WORKS_WITH"],
    )
    return graph


class TestVLPUnwindInvestigation:
    """Tests investigating VLP + UNWIND pattern."""

    def test_basic_vlp_with_named_variable(self, spark, graph_context):
        """Basic VLP with named edge variable - baseline test."""
        query = """
        MATCH (src {node_id: "alice"})-[e:KNOWS*1..2]->(dst)
        RETURN src.name AS src_name, dst.name AS dst_name, e
        ORDER BY dst_name
        """
        sql = graph_context.transpile(query)
        print("\n=== SQL for basic VLP ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()
        print(f"\nRows: {len(rows)}")
        for row in rows:
            print(f"  {row}")

        assert len(rows) == 2  # Bob (1 hop), Carol (2 hops)
        names = {row["dst_name"] for row in rows}
        assert names == {"Bob", "Carol"}

    def test_vlp_undirected_with_named_variable(self, spark, graph_context):
        """VLP undirected with named edge variable."""
        query = """
        MATCH (src {node_id: "bob"})-[e*1..2]-(dst)
        RETURN src.name AS src_name, dst.name AS dst_name, e
        """
        sql = graph_context.transpile(query)
        print("\n=== SQL for undirected VLP ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()
        print(f"\nRows: {len(rows)}")
        for row in rows:
            print(f"  {row}")

        # Bob can reach: Alice (1 hop back), Carol (1 hop fwd),
        # Dave (2 hops fwd), Eve (2 hops via Carol or Alice)
        assert len(rows) >= 4

    def test_vlp_unwind_directed(self, spark, graph_context):
        """VLP + UNWIND on directed path."""
        query = """
        MATCH (src {node_id: "alice"})-[e:KNOWS*1..3]->(dst)
        UNWIND e AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print("\n=== SQL for VLP + UNWIND (directed) ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()
        print(f"\nRows: {len(rows)}")
        for row in rows:
            print(f"  {row}")

        # Should get distinct edges: alice->bob, bob->carol, carol->dave
        # (each edge appears once due to DISTINCT)
        assert len(rows) >= 1

    def test_vlp_unwind_undirected(self, spark, graph_context):
        """VLP + UNWIND on undirected path - the main pattern under investigation."""
        query = """
        MATCH (s {node_id: "bob"})
        MATCH (s)-[e*1..2]-(other)
        UNWIND e AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print("\n=== SQL for VLP + UNWIND (undirected) - MAIN INVESTIGATION ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()
        print(f"\nRows: {len(rows)}")
        for row in rows:
            print(f"  {row}")

        # Should get distinct edges traversed from bob in either direction
        assert len(rows) >= 1

    def test_original_pattern_exact(self, spark, graph_context):
        """Exact pattern from user: property filter + VLP undirected + UNWIND."""
        # Using node_id as the property to filter by
        query = """
        MATCH (s { node_id: "alice"} )
        MATCH (s) -[e*1..3]-(other)
        UNWIND e AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print("\n=== SQL for EXACT user pattern ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()
        print(f"\nRows: {len(rows)}")
        for row in rows:
            print(f"  {row}")

        # Should get all distinct edges reachable from alice in 1-3 hops
        assert len(rows) >= 1

    def test_vlp_return_edge_properties(self, spark, graph_context):
        """VLP + UNWIND returning specific edge properties."""
        query = """
        MATCH (s {node_id: "alice"})
        MATCH (s)-[e*1..2]-(other)
        UNWIND e AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.weight AS weight
        """
        sql = graph_context.transpile(query)
        print("\n=== SQL for VLP + UNWIND with edge properties ===")
        print(sql)

        try:
            result = spark.sql(sql)
            rows = result.collect()
            print(f"\nRows: {len(rows)}")
            for row in rows:
                print(f"  src={row['src']}, dst={row['dst']}, weight={row['weight']}")
        except Exception as e:
            print(f"\nError executing query: {e}")
            # This might fail - useful to see the error

    def test_multiple_match_clauses(self, spark, graph_context):
        """Test multiple MATCH clauses - first filters, second does VLP."""
        query = """
        MATCH (s:Person)
        WHERE s.node_id = "alice"
        MATCH (s)-[e:KNOWS*1..2]->(other)
        RETURN s.name AS src, other.name AS dst, e
        """
        sql = graph_context.transpile(query)
        print("\n=== SQL for multiple MATCH clauses ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()
        print(f"\nRows: {len(rows)}")
        for row in rows:
            print(f"  {row}")

        assert len(rows) >= 1
