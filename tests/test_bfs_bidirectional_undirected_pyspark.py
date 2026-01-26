"""TDD Tests: Bidirectional BFS for Undirected Traversals.

This test file verifies that bidirectional BFS optimization works correctly
with UNDIRECTED variable-length path queries (using `-` instead of `->`).

Test Graph (directed edges, but traversed undirected):

       A -----> B -----> C
       |        ^
       v        |
       D -----> E -----> F

Edges (directed):
  A -> B, A -> D
  B -> C
  D -> E
  E -> B, E -> F

When traversed UNDIRECTED, edges can be followed in both directions.
For example, from C we can reach B (following B->C backwards).

The bidirectional BFS for undirected should:
1. Forward CTE: explore from source in BOTH edge directions
2. Backward CTE: explore from target in BOTH edge directions
3. Meet in the middle

Expected behavior:
- Results should be IDENTICAL to unidirectional undirected BFS
- Path counts must match exactly
- Actual paths must match exactly
"""

import pytest
from pyspark.sql import SparkSession

from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    spark = (
        SparkSession.builder.appName("BidirectionalUndirectedTest")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph_data(spark):
    """Create directed graph data that will be traversed undirected.

    Graph structure:
           A -----> B -----> C
           |        ^
           v        |
           D -----> E -----> F

    This graph has known paths when traversed undirected:
    - C to D: C-B-A-D (length 3) or C-B-E-D (length 3)
    - F to A: F-E-D-A (length 3) or F-E-B-A (length 3)
    - C to F: C-B-E-F (length 3)
    """
    nodes_data = [
        ("A", "Node", 0),
        ("B", "Node", 1),
        ("C", "Node", 2),
        ("D", "Node", 1),
        ("E", "Node", 2),
        ("F", "Node", 3),
    ]
    nodes_df = spark.createDataFrame(nodes_data, ["node_id", "node_type", "depth_from_a"])
    nodes_df.createOrReplaceTempView("nodes")

    # Directed edges
    edges_data = [
        ("A", "B", "CONNECTS"),  # A -> B
        ("A", "D", "CONNECTS"),  # A -> D
        ("B", "C", "CONNECTS"),  # B -> C
        ("D", "E", "CONNECTS"),  # D -> E
        ("E", "B", "CONNECTS"),  # E -> B (creates connection between branches)
        ("E", "F", "CONNECTS"),  # E -> F
    ]
    edges_df = spark.createDataFrame(edges_data, ["src", "dst", "relationship_type"])
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


class TestBidirectionalUndirectedCorrectness:
    """Tests that verify bidirectional BFS works correctly with undirected traversal."""

    def test_undirected_bidirectional_count_matches_unidirectional(
        self, spark, graph_context
    ):
        """
        Path count must be IDENTICAL between bidirectional and unidirectional.

        Query: Find all paths from C to D (undirected, length 1..4)
        Both modes should find the same number of paths.

        Note: Only tests recursive mode. Unrolling mode for undirected is
        not yet implemented.
        """
        query = """
        MATCH (start)-[:CONNECTS*1..4]-(end_node)
        WHERE start.node_id = 'C' AND end_node.node_id = 'D'
        RETURN count(*) AS path_count
        """

        # Unidirectional (baseline)
        sql_off = graph_context.transpile(query, bidirectional_mode="off")
        count_off = spark.sql(sql_off).collect()[0]["path_count"]

        # Bidirectional recursive
        sql_recursive = graph_context.transpile(query, bidirectional_mode="recursive")
        count_recursive = spark.sql(sql_recursive).collect()[0]["path_count"]

        print(f"\n=== Path counts C to D (undirected) ===")
        print(f"Unidirectional: {count_off}")
        print(f"Bidirectional recursive: {count_recursive}")

        assert count_off > 0, "Should find at least one path from C to D"
        assert count_recursive == count_off, (
            f"Recursive count {count_recursive} != unidirectional {count_off}"
        )

    def test_undirected_bidirectional_paths_match_unidirectional(
        self, spark, graph_context
    ):
        """
        Actual paths must be IDENTICAL between bidirectional and unidirectional.

        Query: Find all paths from C to D (undirected, length 1..4)
        Compare actual path node sequences.

        Note: Only tests recursive mode. Unrolling mode for undirected is
        not yet implemented.
        """
        query = """
        MATCH path = (start)-[:CONNECTS*1..4]-(end_node)
        WHERE start.node_id = 'C' AND end_node.node_id = 'D'
        RETURN nodes(path) AS path_nodes
        ORDER BY size(path_nodes), path_nodes
        """

        # Unidirectional (baseline)
        sql_off = graph_context.transpile(query, bidirectional_mode="off")
        paths_off = sorted(
            [tuple(r["path_nodes"]) for r in spark.sql(sql_off).collect()]
        )

        # Bidirectional recursive
        sql_recursive = graph_context.transpile(query, bidirectional_mode="recursive")
        paths_recursive = sorted(
            [tuple(r["path_nodes"]) for r in spark.sql(sql_recursive).collect()]
        )

        print(f"\n=== Paths C to D (undirected) ===")
        print(f"Unidirectional paths: {paths_off}")
        print(f"Bidirectional recursive paths: {paths_recursive}")

        assert len(paths_off) > 0, "Should find at least one path"
        assert paths_recursive == paths_off, (
            f"Recursive paths differ:\n{paths_recursive}\n!=\n{paths_off}"
        )

    def test_undirected_bidirectional_f_to_a(self, spark, graph_context):
        """
        Test path from F to A (undirected).

        F is at the "end" of directed graph, but undirected allows reaching A.
        Possible paths: F-E-D-A, F-E-B-A
        """
        query = """
        MATCH path = (start)-[:CONNECTS*1..4]-(end_node)
        WHERE start.node_id = 'F' AND end_node.node_id = 'A'
        RETURN nodes(path) AS path_nodes, length(path) AS path_length
        ORDER BY path_length, path_nodes
        """

        # Unidirectional (baseline)
        sql_off = graph_context.transpile(query, bidirectional_mode="off")
        result_off = spark.sql(sql_off).collect()
        paths_off = [(tuple(r["path_nodes"]), r["path_length"]) for r in result_off]

        # Bidirectional recursive
        sql_recursive = graph_context.transpile(query, bidirectional_mode="recursive")
        result_recursive = spark.sql(sql_recursive).collect()
        paths_recursive = [
            (tuple(r["path_nodes"]), r["path_length"]) for r in result_recursive
        ]

        print(f"\n=== Paths F to A (undirected) ===")
        print(f"Unidirectional: {paths_off}")
        print(f"Bidirectional: {paths_recursive}")

        assert len(paths_off) > 0, "Should find path from F to A"
        assert sorted(paths_recursive) == sorted(paths_off), (
            f"Paths differ:\n{paths_recursive}\n!=\n{paths_off}"
        )

    def test_undirected_bidirectional_shortest_path(self, spark, graph_context):
        """
        Verify shortest path is found correctly with bidirectional.

        Query: Shortest path from C to F (undirected)
        Expected: C-B-E-F (length 3)
        """
        query = """
        MATCH path = (start)-[:CONNECTS*1..5]-(end_node)
        WHERE start.node_id = 'C' AND end_node.node_id = 'F'
        RETURN nodes(path) AS path_nodes, length(path) AS path_length
        ORDER BY path_length
        LIMIT 1
        """

        # Unidirectional (baseline)
        sql_off = graph_context.transpile(query, bidirectional_mode="off")
        result_off = spark.sql(sql_off).collect()

        # Bidirectional recursive
        sql_recursive = graph_context.transpile(query, bidirectional_mode="recursive")
        result_recursive = spark.sql(sql_recursive).collect()

        assert len(result_off) == 1, "Should find shortest path"
        assert len(result_recursive) == 1, "Bidirectional should find shortest path"

        path_off = result_off[0]["path_nodes"]
        length_off = result_off[0]["path_length"]
        path_recursive = result_recursive[0]["path_nodes"]
        length_recursive = result_recursive[0]["path_length"]

        print(f"\n=== Shortest path C to F (undirected) ===")
        print(f"Unidirectional: {path_off} (length {length_off})")
        print(f"Bidirectional: {path_recursive} (length {length_recursive})")

        assert length_recursive == length_off, (
            f"Shortest path length differs: {length_recursive} != {length_off}"
        )
        # Path nodes should match (same shortest path)
        assert path_recursive == path_off, (
            f"Shortest path differs: {path_recursive} != {path_off}"
        )


class TestBidirectionalUndirectedSQLStructure:
    """Tests that verify SQL structure for undirected bidirectional."""

    def test_sql_has_separate_forward_backward_ctes(self, graph_context):
        """
        SQL should have SEPARATE forward_paths and backward_paths CTEs
        for undirected queries when bidirectional is enabled.

        This is different from the "Forward direction" / "Backward direction"
        comments inside a single CTE (which is undirected edge traversal).

        Bidirectional BFS structure:
        - forward_paths_X CTE: explores from SOURCE
        - backward_paths_X CTE: explores from TARGET
        - Final JOIN between them
        """
        query = """
        MATCH (start)-[:CONNECTS*1..4]-(end_node)
        WHERE start.node_id = 'C' AND end_node.node_id = 'D'
        RETURN count(*) AS cnt
        """

        sql = graph_context.transpile(query, bidirectional_mode="recursive")
        sql_lower = sql.lower()

        print(f"\n=== SQL for undirected bidirectional ===")
        print(sql)

        # Should have SEPARATE forward_paths and backward_paths CTEs
        # (not just comments inside a single CTE)
        has_forward_cte = "forward_paths" in sql_lower
        has_backward_cte = "backward_paths" in sql_lower

        assert has_forward_cte, (
            "Expected forward_paths CTE for bidirectional BFS.\n"
            "Current implementation may be falling back to unidirectional."
        )
        assert has_backward_cte, (
            "Expected backward_paths CTE for bidirectional BFS.\n"
            "Current implementation may be falling back to unidirectional."
        )

    def test_sql_has_join_between_directions(self, graph_context):
        """
        Bidirectional BFS should JOIN forward and backward CTEs at meeting point.
        """
        query = """
        MATCH (start)-[:CONNECTS*1..4]-(end_node)
        WHERE start.node_id = 'C' AND end_node.node_id = 'D'
        RETURN count(*) AS cnt
        """

        sql = graph_context.transpile(query, bidirectional_mode="recursive")
        sql_lower = sql.lower()

        print(f"\n=== SQL structure check for JOIN ===")
        print(sql)

        # Bidirectional should have a JOIN between forward and backward
        # Look for pattern like: "forward_paths ... JOIN ... backward_paths"
        has_forward_cte = "forward_paths" in sql_lower
        has_backward_cte = "backward_paths" in sql_lower
        has_join = "join" in sql_lower

        # If we have separate CTEs, they should be joined
        if has_forward_cte and has_backward_cte:
            assert has_join, "Expected JOIN between forward and backward CTEs"
        else:
            # If no separate CTEs, the optimization is not applied
            pytest.fail(
                "Bidirectional BFS not applied to undirected query. "
                "Expected forward_paths and backward_paths CTEs."
            )


class TestBidirectionalUndirectedRandomGraphs:
    """Tests with random graphs to catch edge cases."""

    @pytest.mark.parametrize("seed", [42, 123, 456])
    def test_random_graph_undirected_bidirectional_matches(self, spark, seed):
        """
        Test on random graphs that bidirectional produces same results as unidirectional.

        Note: We exclude self-loops (src == dst) because they are handled differently
        by bidirectional BFS cycle detection. This is a known limitation.
        """
        import random

        random.seed(seed)

        # Generate random graph (excluding self-loops)
        num_nodes = 15
        num_edges = 30
        nodes = [f"n{i}" for i in range(num_nodes)]

        nodes_data = [(n, "Node") for n in nodes]
        # Generate edges, excluding self-loops
        edges_data = []
        while len(edges_data) < num_edges:
            src = random.choice(nodes)
            dst = random.choice(nodes)
            if src != dst:  # Exclude self-loops
                edges_data.append((src, dst, "E"))

        nodes_df = spark.createDataFrame(nodes_data, ["node_id", "node_type"])
        edges_df = spark.createDataFrame(edges_data, ["src", "dst", "relationship_type"])

        nodes_df.createOrReplaceTempView(f"random_nodes_{seed}")
        edges_df.createOrReplaceTempView(f"random_edges_{seed}")

        ctx = GraphContext(
            spark=spark,
            nodes_table=f"random_nodes_{seed}",
            edges_table=f"random_edges_{seed}",
            node_type_col="node_type",
            node_id_col="node_id",
        )
        ctx.set_types(node_types=["Node"], edge_types=["E"])

        # Pick random source and target
        source = random.choice(nodes)
        target = random.choice(nodes)
        while target == source:
            target = random.choice(nodes)

        # Compare unique paths (bidirectional uses DISTINCT, so compare sets)
        query_paths = f"""
        MATCH path = (a)-[:E*1..4]-(b)
        WHERE a.node_id = '{source}' AND b.node_id = '{target}'
        RETURN nodes(path) AS path_nodes
        """

        sql_off = ctx.transpile(query_paths, bidirectional_mode="off")
        sql_recursive = ctx.transpile(query_paths, bidirectional_mode="recursive")

        paths_off = set(
            tuple(r["path_nodes"]) for r in spark.sql(sql_off).collect()
        )
        paths_recursive = set(
            tuple(r["path_nodes"]) for r in spark.sql(sql_recursive).collect()
        )

        print(f"\n=== Random graph seed={seed}, {source} to {target} ===")
        print(f"Unidirectional unique paths: {len(paths_off)}")
        print(f"Bidirectional unique paths: {len(paths_recursive)}")

        assert paths_recursive == paths_off, (
            f"Seed {seed}: paths differ.\n"
            f"Missing in bidir: {paths_off - paths_recursive}\n"
            f"Extra in bidir: {paths_recursive - paths_off}"
        )


class TestBidirectionalUndirectedEdgeCases:
    """Edge cases for undirected bidirectional BFS."""

    def test_no_path_returns_empty(self, spark, graph_context):
        """When no path exists, both modes should return empty."""
        # Create isolated node
        spark.sql(
            "SELECT 'ISOLATED' as node_id, 'Node' as node_type, -1 as depth_from_a"
        ).createOrReplaceTempView("isolated_node")

        # Add isolated node to the view
        spark.sql("""
            SELECT * FROM nodes
            UNION ALL
            SELECT * FROM isolated_node
        """).createOrReplaceTempView("nodes_with_isolated")

        ctx = GraphContext(
            spark=spark,
            nodes_table="nodes_with_isolated",
            edges_table="edges",
            node_type_col="node_type",
            node_id_col="node_id",
        )
        ctx.set_types(node_types=["Node"], edge_types=["CONNECTS"])

        query = """
        MATCH (start)-[:CONNECTS*1..5]-(end_node)
        WHERE start.node_id = 'A' AND end_node.node_id = 'ISOLATED'
        RETURN count(*) AS path_count
        """

        sql_off = ctx.transpile(query, bidirectional_mode="off")
        sql_recursive = ctx.transpile(query, bidirectional_mode="recursive")

        count_off = spark.sql(sql_off).collect()[0]["path_count"]
        count_recursive = spark.sql(sql_recursive).collect()[0]["path_count"]

        assert count_off == 0, "No path should exist to isolated node"
        assert count_recursive == 0, "Bidirectional should also find no path"

    def test_source_equals_target_undirected(self, spark, graph_context):
        """
        When source = target, behavior should be consistent.

        For undirected with min_hops=1, we might find cycles back to start.
        """
        query = """
        MATCH path = (start)-[:CONNECTS*2..4]-(end_node)
        WHERE start.node_id = 'A' AND end_node.node_id = 'A'
        RETURN count(*) AS path_count
        """

        sql_off = graph_context.transpile(query, bidirectional_mode="off")
        sql_recursive = graph_context.transpile(query, bidirectional_mode="recursive")

        count_off = spark.sql(sql_off).collect()[0]["path_count"]
        count_recursive = spark.sql(sql_recursive).collect()[0]["path_count"]

        print(f"\n=== Cycle back to A (undirected) ===")
        print(f"Unidirectional: {count_off}")
        print(f"Bidirectional: {count_recursive}")

        # Both should find the same cycles (if any)
        assert count_recursive == count_off, (
            f"Cycle count differs: {count_recursive} != {count_off}"
        )

    def test_min_hops_greater_than_one(self, spark, graph_context):
        """Test with min_hops > 1 for undirected bidirectional."""
        query = """
        MATCH path = (start)-[:CONNECTS*2..4]-(end_node)
        WHERE start.node_id = 'C' AND end_node.node_id = 'D'
        RETURN nodes(path) AS path_nodes, length(path) AS path_length
        ORDER BY path_length
        """

        sql_off = graph_context.transpile(query, bidirectional_mode="off")
        sql_recursive = graph_context.transpile(query, bidirectional_mode="recursive")

        result_off = spark.sql(sql_off).collect()
        result_recursive = spark.sql(sql_recursive).collect()

        paths_off = sorted([(tuple(r["path_nodes"]), r["path_length"]) for r in result_off])
        paths_recursive = sorted(
            [(tuple(r["path_nodes"]), r["path_length"]) for r in result_recursive]
        )

        print(f"\n=== Paths with min_hops=2 ===")
        print(f"Unidirectional: {paths_off}")
        print(f"Bidirectional: {paths_recursive}")

        # All paths should have length >= 2
        for path, length in paths_off:
            assert length >= 2, f"Path {path} has length {length} < 2"

        assert paths_recursive == paths_off, (
            f"Paths differ with min_hops=2:\n{paths_recursive}\n!=\n{paths_off}"
        )
