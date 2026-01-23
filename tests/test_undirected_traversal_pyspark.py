"""Test: Undirected Traversal on Directed Graph via OpenCypher VLP in PySpark.

Creates a directed graph and traverses it as undirected (both directions).

Test Graph (directed edges):

       A -----> B -----> C
       |        ^
       v        |
       D -----> E -----> F

Directed Edges:
  A -> B, A -> D
  B -> C
  D -> E
  E -> B, E -> F

Expected Undirected Reachability from A:
  - Depth 1: B, D (direct neighbors in any direction)
  - Depth 2: C (via B), E (via D or B)
  - Depth 3: F (via E), and we can reach back to other nodes

The test validates that undirected traversal can follow edges backwards.
"""

import pytest
from pyspark.sql import SparkSession
from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    spark = (
        SparkSession.builder
        .appName("Undirected_Traversal_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph_data(spark):
    """Create directed graph data.

    Graph structure:
           A -----> B -----> C
           |        ^
           v        |
           D -----> E -----> F
    """
    nodes_data = [
        ("A", "Node", 0),
        ("B", "Node", 1),
        ("C", "Node", 2),
        ("D", "Node", 1),
        ("E", "Node", 2),
        ("F", "Node", 3),
    ]
    nodes_df = spark.createDataFrame(
        nodes_data, ["node_id", "node_type", "depth_from_a"]
    )
    nodes_df.createOrReplaceTempView("nodes")

    # Directed edges
    edges_data = [
        ("A", "B", "CONNECTS"),  # A -> B
        ("A", "D", "CONNECTS"),  # A -> D
        ("B", "C", "CONNECTS"),  # B -> C
        ("D", "E", "CONNECTS"),  # D -> E
        ("E", "B", "CONNECTS"),  # E -> B (creates a cycle in undirected view)
        ("E", "F", "CONNECTS"),  # E -> F
    ]
    edges_df = spark.createDataFrame(
        edges_data, ["src", "dst", "relationship_type"]
    )
    edges_df.createOrReplaceTempView("edges")

    return {"nodes": nodes_df, "edges": edges_df}


@pytest.fixture(scope="module")
def graph_context(spark, graph_data):
    """Create GraphContext for the directed graph."""
    graph = GraphContext(
        spark=spark,
        nodes_table="nodes",
        edges_table="edges",
        node_type_col="node_type",
        node_id_col="node_id",
        extra_node_attrs={"depth_from_a": int},
    )
    graph.set_types(
        node_types=["Node"],
        edge_types=["CONNECTS"],
    )
    return graph


class TestUndirectedTraversal:
    """Tests for undirected traversal on directed graph."""

    def test_directed_from_a_depth_1(self, spark, graph_context):
        """Directed traversal depth 1 from A: only A -> B, A -> D."""
        query = """
        MATCH (start)-[:CONNECTS*1..1]->(neighbor)
        WHERE start.node_id = 'A'
        RETURN start.node_id AS start, neighbor.node_id AS end_node
        ORDER BY end_node
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL for directed depth 1 from A ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        neighbors = {row["end_node"] for row in rows}
        assert neighbors == {"B", "D"}, f"Directed from A should reach {{B, D}}, got {neighbors}"

        print(f"✅ Directed depth 1 from A: {neighbors}")

    def test_undirected_from_b_depth_1(self, spark, graph_context):
        """Undirected traversal depth 1 from B: A, C, E.

        Directed edges involving B:
          - A -> B (incoming to B)
          - B -> C (outgoing from B)
          - E -> B (incoming to B)

        Undirected should find: A, C, E
        """
        query = """
        MATCH (start)-[:CONNECTS*1..1]-(neighbor)
        WHERE start.node_id = 'B'
        RETURN start.node_id AS start, neighbor.node_id AS end_node
        ORDER BY end_node
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL for undirected depth 1 from B ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        neighbors = {row["end_node"] for row in rows}
        # Undirected from B should reach:
        # - A (via A -> B, backwards)
        # - C (via B -> C, forwards)
        # - E (via E -> B, backwards)
        expected = {"A", "C", "E"}
        assert neighbors == expected, f"Undirected from B should reach {expected}, got {neighbors}"

        print(f"✅ Undirected depth 1 from B: {neighbors}")

    def test_undirected_from_c_depth_1(self, spark, graph_context):
        """Undirected traversal depth 1 from C: only B (via B -> C backwards).

        C has only one edge: B -> C
        Undirected should find: B
        """
        query = """
        MATCH (start)-[:CONNECTS*1..1]-(neighbor)
        WHERE start.node_id = 'C'
        RETURN start.node_id AS start, neighbor.node_id AS end_node
        ORDER BY end_node
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL for undirected depth 1 from C ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        neighbors = {row["end_node"] for row in rows}
        expected = {"B"}
        assert neighbors == expected, f"Undirected from C should reach {expected}, got {neighbors}"

        print(f"✅ Undirected depth 1 from C: {neighbors}")

    def test_undirected_from_a_depth_2(self, spark, graph_context):
        """Undirected traversal depth 1..2 from A.

        Depth 1: B, D
        Depth 2:
          - From B: A (back), C, E
          - From D: A (back), E

        Unique at depth 2 (excluding A): C, E
        """
        query = """
        MATCH path = (start)-[:CONNECTS*1..2]-(neighbor)
        WHERE start.node_id = 'A'
        RETURN DISTINCT neighbor.node_id AS end_node, length(path) AS depth
        ORDER BY depth, end_node
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL for undirected depth 2 from A ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        depth_1 = {row["end_node"] for row in rows if row["depth"] == 1}
        depth_2 = {row["end_node"] for row in rows if row["depth"] == 2}

        assert depth_1 == {"B", "D"}, f"Depth 1 should be {{B, D}}, got {depth_1}"
        # Depth 2: C (from B), E (from B or D), possibly A again (cycle)
        assert "C" in depth_2, f"C should be reachable at depth 2, got {depth_2}"
        assert "E" in depth_2, f"E should be reachable at depth 2, got {depth_2}"

        print(f"✅ Undirected depth 1 from A: {depth_1}")
        print(f"✅ Undirected depth 2 from A: {depth_2}")

    def test_undirected_from_f_reaches_all(self, spark, graph_context):
        """F should reach all nodes via undirected traversal.

        F is at the "end" of the directed graph, but undirected
        traversal should allow reaching back to all nodes.

        Path: F <- E <- D <- A -> B -> C
              F <- E -> B (shortcut via E -> B edge)
        """
        query = """
        MATCH (start)-[:CONNECTS*1..4]-(neighbor)
        WHERE start.node_id = 'F'
        RETURN DISTINCT neighbor.node_id AS reachable
        ORDER BY reachable
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL for undirected traversal from F ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        reachable = {row["reachable"] for row in rows}
        expected = {"A", "B", "C", "D", "E"}  # All nodes except F itself
        assert reachable == expected, f"From F should reach all nodes {expected}, got {reachable}"

        print(f"✅ Undirected from F reaches: {reachable}")

    def test_compare_directed_vs_undirected(self, spark, graph_context):
        """Compare directed vs undirected traversal from same node.

        From E:
        - Directed (->): B, F
        - Undirected (-): B, D, F (D via D -> E backwards)
        """
        # Directed query
        directed_query = """
        MATCH (start)-[:CONNECTS*1..1]->(neighbor)
        WHERE start.node_id = 'E'
        RETURN neighbor.node_id AS end_node
        ORDER BY end_node
        """

        # Undirected query
        undirected_query = """
        MATCH (start)-[:CONNECTS*1..1]-(neighbor)
        WHERE start.node_id = 'E'
        RETURN neighbor.node_id AS end_node
        ORDER BY end_node
        """

        directed_sql = graph_context.transpile(directed_query)
        undirected_sql = graph_context.transpile(undirected_query)

        print("\n=== Directed SQL from E ===")
        print(directed_sql)
        print("\n=== Undirected SQL from E ===")
        print(undirected_sql)

        directed_result = spark.sql(directed_sql)
        undirected_result = spark.sql(undirected_sql)

        directed_neighbors = {row["end_node"] for row in directed_result.collect()}
        undirected_neighbors = {row["end_node"] for row in undirected_result.collect()}

        # Directed from E: only outgoing edges E -> B, E -> F
        assert directed_neighbors == {"B", "F"}, f"Directed from E should be {{B, F}}, got {directed_neighbors}"

        # Undirected from E: B, F (outgoing) + D (via D -> E incoming)
        assert undirected_neighbors == {"B", "D", "F"}, f"Undirected from E should be {{B, D, F}}, got {undirected_neighbors}"

        # Undirected should always be a superset of directed
        assert directed_neighbors.issubset(undirected_neighbors), \
            "Directed neighbors should be subset of undirected neighbors"

        print(f"✅ Directed from E: {directed_neighbors}")
        print(f"✅ Undirected from E: {undirected_neighbors}")
        print(f"✅ Additional nodes via undirected: {undirected_neighbors - directed_neighbors}")

    def test_undirected_path_nodes(self, spark, graph_context):
        """Verify actual path through undirected traversal.

        Path from C to D should exist via undirected:
        C <- B <- A -> D (following edges backwards then forwards)
        """
        query = """
        MATCH path = (start)-[:CONNECTS*1..3]-(end_node)
        WHERE start.node_id = 'C' AND end_node.node_id = 'D'
        RETURN nodes(path) AS path_nodes, length(path) AS path_length
        ORDER BY path_length
        LIMIT 1
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL for path C to D ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) > 0, "Should find path from C to D via undirected traversal"

        path = rows[0]["path_nodes"]
        path_length = rows[0]["path_length"]

        assert path[0] == "C", f"Path should start with C, got {path}"
        assert path[-1] == "D", f"Path should end with D, got {path}"
        assert path_length == 3, f"Shortest path C-B-A-D should have length 3, got {path_length}"

        print(f"✅ Path from C to D: {path} (length {path_length})")


class TestUndirectedSubgraphValidation:
    """Validate undirected subgraph extraction using DataFrame comparison."""

    def test_extract_undirected_subgraph_from_a(self, spark, graph_context, graph_data):
        """Extract all nodes reachable from A via undirected traversal and validate."""
        query = """
        MATCH (start)-[:CONNECTS*1..5]-(reachable)
        WHERE start.node_id = 'A'
        RETURN DISTINCT reachable.node_id AS node_id
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)

        # All nodes should be reachable from A via undirected
        reachable_nodes = {row["node_id"] for row in result.collect()}
        all_nodes = {row["node_id"] for row in graph_data["nodes"].collect()}
        expected = all_nodes - {"A"}  # All except starting node

        assert reachable_nodes == expected, \
            f"All nodes should be reachable from A, missing: {expected - reachable_nodes}"

        print(f"✅ Subgraph from A contains all nodes: {reachable_nodes}")

    def test_extract_edges_in_undirected_subgraph(self, spark, graph_context, graph_data):
        """Extract all edges traversed in undirected manner and validate against DataFrame."""
        # For depth 1 undirected from each node, we should see all edges (both directions)
        query = """
        MATCH (a)-[:CONNECTS*1..1]-(b)
        RETURN DISTINCT
            CASE WHEN a.node_id < b.node_id THEN a.node_id ELSE b.node_id END AS node1,
            CASE WHEN a.node_id < b.node_id THEN b.node_id ELSE a.node_id END AS node2
        ORDER BY node1, node2
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL for edge extraction ===")
        print(sql)

        result = spark.sql(sql)
        edges_found = {(row["node1"], row["node2"]) for row in result.collect()}

        # Build expected undirected edges from the directed edge list
        edges_df = graph_data["edges"]
        directed_edges = [(row["src"], row["dst"]) for row in edges_df.collect()]

        # Convert to undirected (canonical form: smaller id first)
        expected_undirected = set()
        for src, dst in directed_edges:
            edge = (min(src, dst), max(src, dst))
            expected_undirected.add(edge)

        assert edges_found == expected_undirected, \
            f"Edges mismatch. Found: {edges_found}, Expected: {expected_undirected}"

        print(f"✅ All undirected edges found: {edges_found}")

    def test_connected_component_via_undirected(self, spark, graph_context):
        """Verify entire graph is one connected component via undirected traversal.

        Start from any node and should reach all others.
        """
        # Start from node F (furthest in directed sense)
        query = """
        MATCH (start)-[:CONNECTS*1..10]-(other)
        WHERE start.node_id = 'F'
        RETURN DISTINCT other.node_id AS node_id
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)

        reachable = {row["node_id"] for row in result.collect()}
        expected = {"A", "B", "C", "D", "E"}  # All except F

        assert reachable == expected, \
            f"Graph should be connected. From F should reach {expected}, got {reachable}"

        print(f"✅ Graph is connected: F reaches {reachable}")
