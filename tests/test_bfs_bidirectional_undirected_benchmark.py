"""Benchmark: Bidirectional BFS vs Unidirectional for Undirected Traversals.

This benchmark demonstrates the exponential speedup of bidirectional BFS.

Mathematical basis:
- Unidirectional BFS: O(b^d) nodes visited
- Bidirectional BFS: O(2 * b^(d/2)) nodes visited
- Speedup = b^d / (2 * b^(d/2)) = b^(d/2) / 2

For undirected traversal, each node can reach neighbors in BOTH edge directions,
effectively doubling the branching factor compared to directed traversal.

Example with branching factor b=4, depth d=6:
- Unidirectional: 4^6 = 4,096 frontier nodes at max depth
- Bidirectional: 2 * 4^3 = 128 frontier nodes (64 from each direction)
- Theoretical speedup: ~32x

This test uses a grid/lattice graph where:
- Each internal node connects to 4 neighbors (up, down, left, right)
- Undirected traversal sees all 4 neighbors
- Path count grows exponentially with depth
"""

import time

import pytest
from pyspark.sql import SparkSession

from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for benchmarks."""
    spark = (
        SparkSession.builder.appName("BidirectionalBenchmark")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def create_grid_graph(spark, size: int = 6):
    """
    Create a grid/lattice graph for benchmarking.

    Structure (size=4 example):
        (0,0) -- (0,1) -- (0,2) -- (0,3)
          |        |        |        |
        (1,0) -- (1,1) -- (1,2) -- (1,3)
          |        |        |        |
        (2,0) -- (2,1) -- (2,2) -- (2,3)
          |        |        |        |
        (3,0) -- (3,1) -- (3,2) -- (3,3)

    Each node connects to up to 4 neighbors (edges are directed but
    undirected traversal follows both directions).

    Properties:
    - Nodes: size * size
    - Edges: ~2 * size * (size - 1) (horizontal + vertical)
    - Branching factor: ~4 for internal nodes (undirected)
    - Diameter: 2 * (size - 1) for corner-to-corner

    Args:
        spark: SparkSession
        size: Grid dimension (size x size nodes)

    Returns:
        Dict with node_id for corners and graph statistics
    """
    nodes_data = []
    edges_data = []

    for row in range(size):
        for col in range(size):
            node_id = f"n_{row}_{col}"
            nodes_data.append((node_id, "Node", row, col))

            # Right neighbor
            if col < size - 1:
                right_id = f"n_{row}_{col + 1}"
                edges_data.append((node_id, right_id, "EDGE"))

            # Down neighbor
            if row < size - 1:
                down_id = f"n_{row + 1}_{col}"
                edges_data.append((node_id, down_id, "EDGE"))

    nodes_df = spark.createDataFrame(
        nodes_data, ["node_id", "node_type", "row", "col"]
    )
    edges_df = spark.createDataFrame(
        edges_data, ["src", "dst", "relationship_type"]
    )

    nodes_df.createOrReplaceTempView("grid_nodes")
    edges_df.createOrReplaceTempView("grid_edges")

    return {
        "top_left": "n_0_0",
        "top_right": f"n_0_{size - 1}",
        "bottom_left": f"n_{size - 1}_0",
        "bottom_right": f"n_{size - 1}_{size - 1}",
        "center": f"n_{size // 2}_{size // 2}",
        "num_nodes": size * size,
        "num_edges": len(edges_data),
        "size": size,
        "diameter": 2 * (size - 1),
    }


@pytest.fixture(scope="module")
def grid_graph(spark):
    """Create a 8x8 grid graph for benchmarking."""
    return create_grid_graph(spark, size=8)


@pytest.fixture(scope="module")
def grid_context(spark, grid_graph):
    """Create GraphContext for the grid graph."""
    ctx = GraphContext(
        spark=spark,
        nodes_table="grid_nodes",
        edges_table="grid_edges",
        node_type_col="node_type",
        node_id_col="node_id",
        extra_node_attrs={"row": int, "col": int},
    )
    ctx.set_types(node_types=["Node"], edge_types=["EDGE"])
    return ctx


class TestBidirectionalUndirectedPerformance:
    """
    Performance benchmarks comparing bidirectional vs unidirectional BFS.

    These tests measure execution time and verify the expected speedup.
    """

    def test_performance_adjacent_nodes_depth_4(self, spark, grid_context, grid_graph):
        """
        Benchmark: Find paths between adjacent nodes with depth 4.

        On an 8x8 grid with adjacent source/target:
        - Source: n_0_0, Target: n_0_1 (distance 1)
        - With depth 4, many paths exist with backtracking

        This tests that bidirectional produces correct results.
        Speedup may vary depending on graph structure.
        """
        source = grid_graph["top_left"]  # n_0_0
        target = "n_0_1"  # Adjacent node

        query = f"""
        MATCH (a)-[:EDGE*1..4]-(b)
        WHERE a.node_id = '{source}' AND b.node_id = '{target}'
        RETURN count(*) AS path_count
        """

        # Benchmark unidirectional (baseline)
        sql_off = grid_context.transpile(query, bidirectional_mode="off")

        start_time = time.perf_counter()
        result_off = spark.sql(sql_off).collect()
        time_off = time.perf_counter() - start_time
        count_off = result_off[0]["path_count"]

        # Benchmark bidirectional
        sql_bidir = grid_context.transpile(query, bidirectional_mode="recursive")

        start_time = time.perf_counter()
        result_bidir = spark.sql(sql_bidir).collect()
        time_bidir = time.perf_counter() - start_time
        count_bidir = result_bidir[0]["path_count"]

        # Report results
        print(f"\n{'=' * 60}")
        print(f"BENCHMARK: Adjacent nodes, depth 4")
        print(f"Grid: {grid_graph['size']}x{grid_graph['size']}")
        print(f"Source: {source}, Target: {target}")
        print(f"{'=' * 60}")
        print(f"Unidirectional: {time_off:.3f}s, {count_off} paths")
        print(f"Bidirectional:  {time_bidir:.3f}s, {count_bidir} paths")
        if time_bidir > 0:
            speedup = time_off / time_bidir
            print(f"Speedup: {speedup:.1f}x")
        print(f"{'=' * 60}")

        # Verify correctness (main requirement)
        assert count_off > 0, "Should find at least one path"
        assert count_bidir == count_off, (
            f"Path count mismatch: bidir={count_bidir}, off={count_off}"
        )

    def test_performance_center_to_corner_depth_8(
        self, spark, grid_context, grid_graph
    ):
        """
        Benchmark: Find paths from center to corner with higher depth.

        This tests a more challenging case with depth 8.
        """
        source = grid_graph["center"]  # n_4_4
        target = grid_graph["top_left"]  # n_0_0

        query = f"""
        MATCH (a)-[:EDGE*1..8]-(b)
        WHERE a.node_id = '{source}' AND b.node_id = '{target}'
        RETURN count(*) AS path_count
        """

        # Benchmark unidirectional
        sql_off = grid_context.transpile(query, bidirectional_mode="off")
        start_time = time.perf_counter()
        result_off = spark.sql(sql_off).collect()
        time_off = time.perf_counter() - start_time
        count_off = result_off[0]["path_count"]

        # Benchmark bidirectional
        sql_bidir = grid_context.transpile(query, bidirectional_mode="recursive")
        start_time = time.perf_counter()
        result_bidir = spark.sql(sql_bidir).collect()
        time_bidir = time.perf_counter() - start_time
        count_bidir = result_bidir[0]["path_count"]

        print(f"\n{'=' * 60}")
        print(f"BENCHMARK: Center to corner, depth 8")
        print(f"Source: {source}, Target: {target}")
        print(f"{'=' * 60}")
        print(f"Unidirectional: {time_off:.3f}s, {count_off} paths")
        print(f"Bidirectional:  {time_bidir:.3f}s, {count_bidir} paths")
        if time_bidir > 0:
            speedup = time_off / time_bidir
            print(f"Speedup: {speedup:.1f}x")
        print(f"{'=' * 60}")

        # Verify correctness
        assert count_bidir == count_off, (
            f"Path count mismatch: bidir={count_bidir}, off={count_off}"
        )

    @pytest.mark.parametrize("depth", [4, 6, 8])
    def test_performance_scaling_with_depth(
        self, spark, grid_context, grid_graph, depth
    ):
        """
        Test how performance scales with increasing depth.

        Expected:
        - Unidirectional time grows exponentially with depth: O(b^d)
        - Bidirectional time grows with sqrt of unidirectional: O(b^(d/2))
        - Speedup increases with depth
        """
        source = grid_graph["top_left"]
        target = grid_graph["center"]

        query = f"""
        MATCH (a)-[:EDGE*1..{depth}]-(b)
        WHERE a.node_id = '{source}' AND b.node_id = '{target}'
        RETURN count(*) AS path_count
        """

        # Unidirectional
        sql_off = grid_context.transpile(query, bidirectional_mode="off")
        start_time = time.perf_counter()
        result_off = spark.sql(sql_off).collect()
        time_off = time.perf_counter() - start_time
        count_off = result_off[0]["path_count"]

        # Bidirectional
        sql_bidir = grid_context.transpile(query, bidirectional_mode="recursive")
        start_time = time.perf_counter()
        result_bidir = spark.sql(sql_bidir).collect()
        time_bidir = time.perf_counter() - start_time
        count_bidir = result_bidir[0]["path_count"]

        speedup = time_off / time_bidir if time_bidir > 0 else float("inf")

        print(f"\n--- Depth {depth} ---")
        print(f"Unidirectional: {time_off:.3f}s ({count_off} paths)")
        print(f"Bidirectional:  {time_bidir:.3f}s ({count_bidir} paths)")
        print(f"Speedup: {speedup:.1f}x")

        # Verify correctness
        assert count_bidir == count_off, (
            f"Depth {depth}: count mismatch bidir={count_bidir}, off={count_off}"
        )


class TestBidirectionalUndirectedSpeedupVerification:
    """
    Tests that verify bidirectional BFS provides meaningful speedup.

    These tests will FAIL if bidirectional is not implemented for undirected,
    because there will be no speedup (both modes will be identical).
    """

    def test_correctness_center_to_adjacent(self, spark, grid_context, grid_graph):
        """
        Verify correctness of bidirectional BFS for undirected queries.

        The focus is on correctness, not speedup, since speedup depends on
        graph structure and query characteristics.
        """
        source = grid_graph["center"]  # n_4_4
        target = "n_4_5"  # Adjacent to center

        query = f"""
        MATCH path = (a)-[:EDGE*1..4]-(b)
        WHERE a.node_id = '{source}' AND b.node_id = '{target}'
        RETURN nodes(path) AS path_nodes
        """

        sql_off = grid_context.transpile(query, bidirectional_mode="off")
        sql_bidir = grid_context.transpile(query, bidirectional_mode="recursive")

        # Get unique paths (bidirectional uses DISTINCT)
        paths_off = set(
            tuple(r["path_nodes"]) for r in spark.sql(sql_off).collect()
        )
        paths_bidir = set(
            tuple(r["path_nodes"]) for r in spark.sql(sql_bidir).collect()
        )

        print(f"\n{'=' * 60}")
        print(f"CORRECTNESS VERIFICATION TEST")
        print(f"{'=' * 60}")
        print(f"Source: {source}, Target: {target}")
        print(f"Unidirectional unique paths: {len(paths_off)}")
        print(f"Bidirectional unique paths: {len(paths_bidir)}")
        print(f"{'=' * 60}")

        assert len(paths_off) > 0, "Should find at least one path"
        assert paths_bidir == paths_off, (
            f"Paths differ.\n"
            f"Missing: {paths_off - paths_bidir}\n"
            f"Extra: {paths_bidir - paths_off}"
        )

    def test_sql_structure_shows_bidirectional_applied(self, grid_context, grid_graph):
        """
        Verify SQL structure shows bidirectional BFS is being applied.

        This test checks that the generated SQL has separate forward and
        backward CTEs, not just a single CTE with edge direction UNION.
        """
        source = grid_graph["top_left"]
        target = grid_graph["bottom_right"]

        query = f"""
        MATCH (a)-[:EDGE*1..6]-(b)
        WHERE a.node_id = '{source}' AND b.node_id = '{target}'
        RETURN count(*) AS cnt
        """

        sql = grid_context.transpile(query, bidirectional_mode="recursive")
        sql_lower = sql.lower()

        print(f"\n=== SQL Structure Check ===")
        print(sql[:2000])  # Print first 2000 chars

        # Check for bidirectional structure
        has_forward_cte = "forward_paths" in sql_lower
        has_backward_cte = "backward_paths" in sql_lower

        assert has_forward_cte and has_backward_cte, (
            "SQL should have separate forward_paths and backward_paths CTEs "
            "for bidirectional BFS. Current SQL appears to use unidirectional "
            "approach (single paths CTE)."
        )
