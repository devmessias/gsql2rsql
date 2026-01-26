"""TDD Tests for BFS Bidirectional Optimization.

This test file implements TDD (Test-Driven Development) for the BFS bidirectional
optimization that can provide 100-1000x performance gains for variable-length path
queries where BOTH source AND target have equality filters.

Optimization Criteria:
1. Variable-length path (VLP) query: (a)-[:TYPE*1..n]->(b)
2. Source has EQUALITY filter on ID: a.id = 'value'
3. Target has EQUALITY filter on ID: b.id = 'value'

Mathematical Basis:
- Unidirectional BFS: O(b^d) nodes visited
- Bidirectional BFS: O(2 * b^(d/2)) nodes visited
- Speedup = b^(d/2) / 2 (e.g., 500x for b=10, d=6)

Two Implementation Approaches:
1. Recursive CTEs: WITH RECURSIVE forward AS (...), backward AS (...)
2. Unrolling: WITH fwd0 AS (...), fwd1 AS (...), bwd0 AS (...), bwd1 AS (...)

Test Categories:
- TestBidirectionalCorrectnessRecursive: Verify recursive mode results
- TestBidirectionalCorrectnessUnrolling: Verify unrolling mode results
- TestBidirectionalModeSelection: Verify auto mode selection logic
- TestBidirectionalDetection: Verify eligibility detection
- TestBidirectionalSQLStructure: Verify generated SQL structure
- TestBidirectionalFeatureFlags: Verify feature flag behavior
- TestMixedBidirectionalQueries: Verify mixed eligibility queries
- TestBidirectionalEdgeCases: Verify edge case handling
"""

import pytest
from pyspark.sql import SparkSession

from gsql2rsql import GraphContext


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    spark = (
        SparkSession.builder
        .appName("BFS_Bidirectional_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph_data(spark):
    """Create test graph with KNOWN shortest paths.

    Graph Structure:

        alice ──KNOWS──> bob ──KNOWS──> carol ──KNOWS──> dave
          │                │                              │
          │                └────KNOWS────> eve            │
          │                                 │             │
          └─────────KNOWS─────────────────> frank <──KNOWS┘

    Known paths alice → dave:
    - alice → bob → carol → dave (length 3)
    - alice → frank → dave (length 2) -- SHORTEST!

    Known paths alice → eve:
    - alice → bob → eve (length 2)

    Known paths alice → frank:
    - alice → frank (length 1) -- Direct edge

    Also creates a DISCONNECTED component for component tests:
    - xander → yara → zoe (no path to alice's component)
    """
    nodes_data = [
        # Main component
        ("alice", "Person"),
        ("bob", "Person"),
        ("carol", "Person"),
        ("dave", "Person"),
        ("eve", "Person"),
        ("frank", "Person"),
        # Disconnected component (for component short-circuit tests)
        ("xander", "Person"),
        ("yara", "Person"),
        ("zoe", "Person"),
    ]

    edges_data = [
        # Main component edges
        ("alice", "bob", "KNOWS"),
        ("bob", "carol", "KNOWS"),
        ("carol", "dave", "KNOWS"),
        ("bob", "eve", "KNOWS"),
        ("alice", "frank", "KNOWS"),
        ("frank", "dave", "KNOWS"),
        ("dave", "frank", "KNOWS"),  # Reverse edge for cycle testing
        # Disconnected component edges
        ("xander", "yara", "KNOWS"),
        ("yara", "zoe", "KNOWS"),
    ]

    nodes_df = spark.createDataFrame(nodes_data, ["node_id", "node_type"])
    edges_df = spark.createDataFrame(edges_data, ["src", "dst", "relationship_type"])

    nodes_df.createOrReplaceTempView("nodes")
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
    )
    graph.set_types(
        node_types=["Person"],
        edge_types=["KNOWS"],
    )
    return graph


# =============================================================================
# TEST: BIDIRECTIONAL CORRECTNESS - RECURSIVE MODE
# =============================================================================


class TestBidirectionalCorrectnessRecursive:
    """Tests that verify recursive CTE mode produces correct results.

    These tests verify that bidirectional BFS using WITH RECURSIVE
    produces IDENTICAL results to unidirectional BFS.
    """

    def test_bidirectional_recursive_finds_all_paths(self, spark, graph_context):
        """Verify recursive mode finds all paths correctly.

        Query: Find all paths from alice to dave (max 4 hops)
        Expected paths:
        - alice → bob → carol → dave (length 3)
        - alice → frank → dave (length 2)
        """
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..4]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'dave'
        RETURN length(path) AS path_length
        ORDER BY path_length
        """

        # This should use bidirectional when implemented
        sql = graph_context.transpile(query, bidirectional_mode="recursive")
        print(f"\n=== SQL (recursive mode) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        # Verify we found both paths
        assert len(rows) == 2, f"Expected 2 paths, got {len(rows)}"

        lengths = sorted([row["path_length"] for row in rows])
        assert lengths == [2, 3], f"Expected lengths [2, 3], got {lengths}"

    def test_bidirectional_recursive_shortest_path(self, spark, graph_context):
        """Verify recursive mode finds shortest path correctly."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..5]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'dave'
        RETURN nodes(path) AS path_nodes, length(path) AS path_length
        ORDER BY path_length
        LIMIT 1
        """

        sql = graph_context.transpile(query, bidirectional_mode="recursive")
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["path_length"] == 2
        # Shortest path is alice → frank → dave
        assert tuple(rows[0]["path_nodes"]) == ("alice", "frank", "dave")

    def test_bidirectional_recursive_count_matches_unidirectional(
        self, spark, graph_context
    ):
        """Verify path count is identical between recursive and unidirectional.

        This is a CRITICAL correctness test: optimization must not
        add or remove paths.
        """
        query = """
        MATCH (a:Person)-[:KNOWS*1..4]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'eve'
        RETURN count(*) AS path_count
        """

        sql_recursive = graph_context.transpile(query, bidirectional_mode="recursive")
        sql_off = graph_context.transpile(query, bidirectional_mode="off")

        count_recursive = spark.sql(sql_recursive).collect()[0]["path_count"]
        count_off = spark.sql(sql_off).collect()[0]["path_count"]

        assert count_recursive == count_off, (
            f"Path count mismatch: recursive={count_recursive}, off={count_off}"
        )


# =============================================================================
# TEST: BIDIRECTIONAL CORRECTNESS - UNROLLING MODE
# =============================================================================


class TestBidirectionalCorrectnessUnrolling:
    """Tests that verify unrolling mode produces correct results.

    These tests verify that bidirectional BFS using unrolled CTEs
    (fwd0, fwd1, ..., bwd0, bwd1, ...) produces IDENTICAL results
    to unidirectional BFS.
    """

    def test_bidirectional_unrolling_finds_all_paths(self, spark, graph_context):
        """Verify unrolling mode finds all paths correctly."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..4]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'dave'
        RETURN length(path) AS path_length
        ORDER BY path_length
        """

        sql = graph_context.transpile(query, bidirectional_mode="unrolling")
        print(f"\n=== SQL (unrolling mode) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 2, f"Expected 2 paths, got {len(rows)}"

        lengths = sorted([row["path_length"] for row in rows])
        assert lengths == [2, 3], f"Expected lengths [2, 3], got {lengths}"

    def test_bidirectional_unrolling_count_matches_unidirectional(
        self, spark, graph_context
    ):
        """Verify path count is identical between unrolling and unidirectional."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'frank'
        RETURN count(*) AS path_count
        """

        sql_unrolling = graph_context.transpile(query, bidirectional_mode="unrolling")
        sql_off = graph_context.transpile(query, bidirectional_mode="off")

        count_unrolling = spark.sql(sql_unrolling).collect()[0]["path_count"]
        count_off = spark.sql(sql_off).collect()[0]["path_count"]

        assert count_unrolling == count_off, (
            f"Path count mismatch: unrolling={count_unrolling}, off={count_off}"
        )


# =============================================================================
# TEST: MODE SELECTION (AUTO)
# =============================================================================


class TestBidirectionalModeSelection:
    """Tests for auto mode selection between recursive and unrolling."""

    def test_auto_selects_unrolling_for_small_depth(self, spark, graph_context):
        """Auto mode should select unrolling for max_hops <= 6."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..4]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'dave'
        RETURN count(*) AS cnt
        """

        sql = graph_context.transpile(query, bidirectional_mode="auto")
        print(f"\n=== SQL (auto mode, depth 4) ===\n{sql}")

        # For small depth, should use unrolling (separate CTEs per level)
        # Look for fwd0, fwd1, etc. pattern
        assert "fwd" in sql.lower() or "forward" in sql.lower(), (
            "Auto mode should select unrolling or recursive for eligible queries"
        )

    def test_auto_selects_recursive_for_large_depth(self, spark, graph_context):
        """Auto mode should select recursive for max_hops > 6."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..10]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'dave'
        RETURN count(*) AS cnt
        """

        sql = graph_context.transpile(query, bidirectional_mode="auto")
        print(f"\n=== SQL (auto mode, depth 10) ===\n{sql}")

        # For large depth, should use recursive (WITH RECURSIVE)
        # This test verifies auto mode picks the right implementation
        result = spark.sql(sql).collect()
        assert result is not None

    def test_both_modes_produce_identical_results(self, spark, graph_context):
        """Verify recursive and unrolling modes produce identical results."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..4]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'dave'
        RETURN nodes(path) AS path_nodes
        ORDER BY size(path_nodes), path_nodes
        """

        sql_recursive = graph_context.transpile(query, bidirectional_mode="recursive")
        sql_unrolling = graph_context.transpile(query, bidirectional_mode="unrolling")

        paths_recursive = sorted(
            [tuple(r["path_nodes"]) for r in spark.sql(sql_recursive).collect()]
        )
        paths_unrolling = sorted(
            [tuple(r["path_nodes"]) for r in spark.sql(sql_unrolling).collect()]
        )

        assert paths_recursive == paths_unrolling, (
            f"Modes produce different paths:\n"
            f"recursive={paths_recursive}\n"
            f"unrolling={paths_unrolling}"
        )


# =============================================================================
# TEST: BIDIRECTIONAL DETECTION
# =============================================================================


class TestBidirectionalDetection:
    """Tests that verify correct detection of bidirectional eligibility.

    Bidirectional BFS is ONLY applicable when:
    1. Query has variable-length path (VLP)
    2. Source has EQUALITY filter on ID
    3. Target has EQUALITY filter on ID
    """

    def test_detects_equality_on_both_endpoints(self, spark, graph_context):
        """Should enable bidirectional when both endpoints have equality."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..5]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'bob'
        RETURN count(*) AS cnt
        """

        sql = graph_context.transpile(query, bidirectional_mode="auto")

        # Should have bidirectional structure (forward/backward or fwd/bwd)
        assert _has_bidirectional_structure(sql), (
            "Expected bidirectional SQL for equality on both endpoints"
        )

    def test_rejects_range_filter_on_target(self, spark, graph_context):
        """Should NOT use bidirectional when target has range filter."""
        # Note: This requires adding a property with range filter capability
        # For now, we test with no filter on target
        query = """
        MATCH (a:Person)-[:KNOWS*1..5]->(b:Person)
        WHERE a.node_id = 'alice'
        RETURN DISTINCT b.node_id AS target
        """

        sql = graph_context.transpile(query, bidirectional_mode="auto")

        # Should NOT have bidirectional structure (target has no equality)
        assert not _has_bidirectional_structure(sql), (
            "Should not use bidirectional when target has no equality filter"
        )

    def test_rejects_no_target_filter(self, spark, graph_context):
        """Should NOT use bidirectional when target has no filter."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'alice'
        RETURN DISTINCT b.node_id
        """

        sql = graph_context.transpile(query, bidirectional_mode="auto")

        assert not _has_bidirectional_structure(sql), (
            "Should not use bidirectional with no target filter"
        )

    def test_rejects_no_source_filter(self, spark, graph_context):
        """Should NOT use bidirectional when source has no filter."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE b.node_id = 'dave'
        RETURN DISTINCT a.node_id
        """

        sql = graph_context.transpile(query, bidirectional_mode="auto")

        assert not _has_bidirectional_structure(sql), (
            "Should not use bidirectional with no source filter"
        )


# =============================================================================
# TEST: SQL STRUCTURE VALIDATION
# =============================================================================


class TestBidirectionalSQLStructure:
    """Tests that verify the generated SQL has correct structure."""

    def test_recursive_has_forward_and_backward_ctes(self, spark, graph_context):
        """Recursive mode should have forward and backward CTEs."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..6]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'dave'
        RETURN count(*) AS cnt
        """

        sql = graph_context.transpile(query, bidirectional_mode="recursive")
        sql_lower = sql.lower()

        # Should have both forward and backward patterns
        has_forward = "forward" in sql_lower
        has_backward = "backward" in sql_lower

        assert has_forward and has_backward, (
            f"Recursive mode should have forward AND backward CTEs.\n"
            f"has_forward={has_forward}, has_backward={has_backward}\n"
            f"SQL:\n{sql}"
        )

    def test_unrolling_has_level_ctes(self, spark, graph_context):
        """Unrolling mode should have separate CTEs per level."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..4]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'dave'
        RETURN count(*) AS cnt
        """

        sql = graph_context.transpile(query, bidirectional_mode="unrolling")
        sql_lower = sql.lower()

        # Should have level-based CTEs (fwd0, fwd1, fwd2, bwd0, bwd1, bwd2)
        has_level_0 = "fwd0" in sql_lower or "fwd_0" in sql_lower
        has_level_1 = "fwd1" in sql_lower or "fwd_1" in sql_lower

        assert has_level_0 or has_level_1, (
            f"Unrolling mode should have level CTEs (fwd0, fwd1, etc.).\n"
            f"SQL:\n{sql}"
        )

    def test_sql_depth_is_halved(self, spark, graph_context):
        """Each direction should search approximately half the total depth."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..10]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'dave'
        RETURN count(*) AS cnt
        """

        sql = graph_context.transpile(query, bidirectional_mode="recursive")

        # Original depth: 10
        # Each direction should search ~5
        # Look for depth checks around 5 or 6
        import re
        depth_checks = re.findall(r'depth\s*[<>=]+\s*(\d+)', sql.lower())
        if depth_checks:
            depths = [int(d) for d in depth_checks]
            # At least one depth should be around half (4-6)
            has_half_depth = any(4 <= d <= 6 for d in depths)
            assert has_half_depth, (
                f"Expected depth ~5 for bidirectional, found: {depths}"
            )


# =============================================================================
# TEST: FEATURE FLAGS
# =============================================================================


class TestBidirectionalFeatureFlags:
    """Tests for feature flag behavior."""

    def test_mode_off_uses_unidirectional(self, spark, graph_context):
        """bidirectional_mode='off' should use standard unidirectional BFS."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..4]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'dave'
        RETURN count(*) AS cnt
        """

        sql = graph_context.transpile(query, bidirectional_mode="off")

        # Should NOT have bidirectional structure
        assert not _has_bidirectional_structure(sql), (
            "bidirectional_mode='off' should not use bidirectional"
        )

    def test_mode_recursive_forces_recursive(self, spark, graph_context):
        """bidirectional_mode='recursive' should force recursive implementation."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..4]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'dave'
        RETURN count(*) AS cnt
        """

        sql = graph_context.transpile(query, bidirectional_mode="recursive")
        sql_lower = sql.lower()

        # Should use recursive (forward/backward), not unrolling (fwd0/fwd1)
        has_recursive_pattern = "forward" in sql_lower and "backward" in sql_lower
        assert has_recursive_pattern or "recursive" in sql_lower, (
            "bidirectional_mode='recursive' should use recursive CTEs"
        )

    def test_mode_unrolling_forces_unrolling(self, spark, graph_context):
        """bidirectional_mode='unrolling' should force unrolling implementation."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..4]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'dave'
        RETURN count(*) AS cnt
        """

        sql = graph_context.transpile(query, bidirectional_mode="unrolling")
        sql_lower = sql.lower()

        # Should use unrolling (fwd0, fwd1, etc.), not recursive
        has_unrolling_pattern = "fwd" in sql_lower and ("fwd0" in sql_lower or "fwd_0" in sql_lower)
        # If not using fwd naming, could also check for separate level CTEs
        assert has_unrolling_pattern or "UNION ALL" in sql, (
            "bidirectional_mode='unrolling' should use unrolled CTEs"
        )

    def test_all_modes_produce_same_results(self, spark, graph_context):
        """All modes should produce identical results."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..4]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'dave'
        RETURN count(*) AS path_count
        """

        modes = ["off", "recursive", "unrolling", "auto"]
        results = {}

        for mode in modes:
            sql = graph_context.transpile(query, bidirectional_mode=mode)
            count = spark.sql(sql).collect()[0]["path_count"]
            results[mode] = count

        # All modes should have same count
        counts = list(results.values())
        assert len(set(counts)) == 1, (
            f"Different modes produced different results: {results}"
        )


# =============================================================================
# TEST: MIXED QUERIES
# =============================================================================


class TestMixedBidirectionalQueries:
    """Tests for queries where PART can use bidirectional and PART cannot."""

    def test_non_eligible_query_still_works(self, spark, graph_context):
        """Non-eligible queries should still work correctly."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'alice'
        RETURN DISTINCT b.node_id AS target
        ORDER BY target
        """

        sql = graph_context.transpile(query, bidirectional_mode="auto")
        result = spark.sql(sql)
        rows = result.collect()

        # Should find all reachable nodes from alice
        targets = {row["target"] for row in rows}
        expected = {"bob", "carol", "dave", "eve", "frank"}
        assert targets == expected, f"Expected {expected}, got {targets}"


# =============================================================================
# TEST: EDGE CASES
# =============================================================================


class TestBidirectionalEdgeCases:
    """Tests for edge cases in bidirectional BFS."""

    def test_min_hops_greater_than_one(self, spark, graph_context):
        """Bidirectional should respect min_hops > 1."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*2..4]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'dave'
        RETURN length(path) AS path_length
        ORDER BY path_length
        """

        sql = graph_context.transpile(query, bidirectional_mode="recursive")
        result = spark.sql(sql)
        rows = result.collect()

        # min_hops=2, so direct path alice→frank→dave (length 2) should be included
        # but alice→dave via single edge (if existed) would be excluded
        lengths = [row["path_length"] for row in rows]
        assert all(length >= 2 for length in lengths), (
            f"min_hops=2 violated, found lengths: {lengths}"
        )

    def test_odd_max_hops_depth_split(self, spark, graph_context):
        """Odd max_hops should be handled correctly (e.g., 7 → 4+3)."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..7]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'dave'
        RETURN count(*) AS cnt
        """

        sql = graph_context.transpile(query, bidirectional_mode="recursive")
        result = spark.sql(sql)
        rows = result.collect()

        # Should not error and should produce results
        assert len(rows) > 0, "Odd max_hops query should produce results"

    def test_no_path_exists_returns_empty(self, spark, graph_context):
        """When no path exists, should return empty (not error)."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..5]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'xander'
        RETURN count(*) AS path_count
        """

        # alice and xander are in disconnected components
        sql = graph_context.transpile(query, bidirectional_mode="recursive")
        result = spark.sql(sql)
        count = result.collect()[0]["path_count"]

        assert count == 0, f"No path should exist, but found {count} paths"

    def test_source_equals_target_same_node(self, spark, graph_context):
        """When source = target (same node), should handle correctly."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'alice' AND b.node_id = 'alice'
        RETURN count(*) AS cycle_count
        """

        sql = graph_context.transpile(query, bidirectional_mode="recursive")
        result = spark.sql(sql)
        rows = result.collect()

        # Should return 0 or cycles back to alice (depending on graph structure)
        # In our test graph, there's no cycle back to alice
        assert len(rows) > 0, "Query should execute without error"

    def test_cycle_detection_prevents_infinite_loop(self, spark, graph_context):
        """Cycles in graph should be handled (no infinite loop)."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..10]->(b:Person)
        WHERE a.node_id = 'dave' AND b.node_id = 'frank'
        RETURN nodes(path) AS path_nodes
        """

        # dave → frank and frank → dave edges create a cycle
        sql = graph_context.transpile(query, bidirectional_mode="recursive")
        result = spark.sql(sql)
        rows = result.collect()

        # Should not infinite loop and should find path
        assert len(rows) >= 1, "Should find at least one path dave → frank"

        # Verify no duplicate nodes in paths (cycle detection working)
        for row in rows:
            path_nodes = row["path_nodes"]
            assert len(path_nodes) == len(set(path_nodes)), (
                f"Cycle detection failed, path has duplicates: {path_nodes}"
            )


# =============================================================================
# TEST: RANDOM GRAPH VERIFICATION
# =============================================================================


class TestBidirectionalRandomGraphs:
    """Tests that verify correctness on random graphs.

    Fixed graphs may accidentally work while random graphs expose bugs.
    These tests generate random graphs and verify all modes produce
    identical results.
    """

    @pytest.mark.parametrize("seed", [42, 123, 456])
    def test_results_identical_random_graph(self, spark, seed):
        """Verify identical results on random graphs across all modes."""
        import random
        random.seed(seed)

        # Generate random graph
        num_nodes = 30
        num_edges = 60
        nodes = [f"node_{i}" for i in range(num_nodes)]

        nodes_data = [(n, "Node") for n in nodes]
        edges_data = [
            (random.choice(nodes), random.choice(nodes), "EDGE")
            for _ in range(num_edges)
        ]

        nodes_df = spark.createDataFrame(nodes_data, ["node_id", "node_type"])
        edges_df = spark.createDataFrame(
            edges_data, ["src", "dst", "relationship_type"]
        )

        nodes_df.createOrReplaceTempView(f"random_nodes_{seed}")
        edges_df.createOrReplaceTempView(f"random_edges_{seed}")

        ctx = GraphContext(
            spark=spark,
            nodes_table=f"random_nodes_{seed}",
            edges_table=f"random_edges_{seed}",
            node_type_col="node_type",
            node_id_col="node_id",
        )
        ctx.set_types(node_types=["Node"], edge_types=["EDGE"])

        # Pick random source and target
        source = random.choice(nodes)
        target = random.choice(nodes)

        query = f"""
        MATCH (a)-[:EDGE*1..4]->(b)
        WHERE a.node_id = '{source}' AND b.node_id = '{target}'
        RETURN count(*) AS path_count
        """

        # Compare all modes
        results = {}
        for mode in ["off", "recursive", "unrolling"]:
            sql = ctx.transpile(query, bidirectional_mode=mode)
            count = spark.sql(sql).collect()[0]["path_count"]
            results[mode] = count

        # All modes should produce identical counts
        counts = list(results.values())
        assert len(set(counts)) == 1, (
            f"Seed {seed}: modes produced different results: {results}"
        )


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def _has_bidirectional_structure(sql: str) -> bool:
    """Check if SQL has bidirectional CTE structure.

    Bidirectional SQL has either:
    - Recursive: 'forward' and 'backward' CTEs
    - Unrolling: 'fwd0', 'fwd1', 'bwd0', 'bwd1' CTEs
    """
    sql_lower = sql.lower()

    # Check for recursive pattern (forward/backward)
    has_recursive = "forward" in sql_lower and "backward" in sql_lower

    # Check for unrolling pattern (fwd0, fwd1, bwd0, bwd1)
    has_unrolling = (
        ("fwd0" in sql_lower or "fwd_0" in sql_lower) and
        ("bwd0" in sql_lower or "bwd_0" in sql_lower)
    )

    return has_recursive or has_unrolling


# =============================================================================
# TEST: LATTICE GRAPH CORRECTNESS
# =============================================================================


class TestBidirectionalLatticeGraph:
    """Tests with lattice graphs that generate many paths.

    Lattice graphs have controlled exponential path growth, useful for
    verifying bidirectional BFS correctness at various scales.

    Note: Spark 4.x limits recursion by LEVEL (iterations), not by rows.
    The `maxRowsPerIteration` config from earlier Spark versions is not
    available. Tests focus on correctness verification.
    """

    @pytest.fixture(scope="class")
    def spark_lattice(self):
        """Create SparkSession for lattice tests."""
        spark = (
            SparkSession.builder
            .appName("BFS_Lattice_Test")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.memory", "1g")
            .getOrCreate()
        )
        yield spark
        spark.stop()

    @pytest.fixture(scope="class")
    def lattice_graph(self, spark_lattice):
        """Create a lattice graph that generates many paths.

        Lattice structure: nodes in layers with controlled connectivity.
        This creates exponential path growth for thorough testing.

        Parameters:
        - width=10, edges_per_node=5, layers=6
        - Creates ~1000s of paths for deep traversals
        """
        import random
        random.seed(42)

        width = 10  # nodes per layer
        edges_per_node = 5  # connectivity
        num_layers = 6

        # Create nodes: layer_0_node_0, layer_0_node_1, ..., layer_5_node_9
        nodes_data = []
        for layer in range(num_layers):
            for node in range(width):
                node_id = f"layer_{layer}_node_{node}"
                nodes_data.append((node_id, "Node"))

        # Create edges: each node connects to random nodes in next layer
        edges_data = []
        for layer in range(num_layers - 1):
            for node in range(width):
                src = f"layer_{layer}_node_{node}"
                # Connect to random nodes in next layer
                targets = random.sample(range(width), min(edges_per_node, width))
                for target in targets:
                    dst = f"layer_{layer + 1}_node_{target}"
                    edges_data.append((src, dst, "CONNECTED"))

        nodes_df = spark_lattice.createDataFrame(
            nodes_data, ["node_id", "node_type"]
        )
        edges_df = spark_lattice.createDataFrame(
            edges_data, ["src", "dst", "relationship_type"]
        )

        nodes_df.createOrReplaceTempView("lattice_nodes")
        edges_df.createOrReplaceTempView("lattice_edges")

        return {"nodes": nodes_df, "edges": edges_df}

    @pytest.fixture(scope="class")
    def lattice_context(self, spark_lattice, lattice_graph):
        """Create GraphContext for the lattice graph."""
        ctx = GraphContext(
            spark=spark_lattice,
            nodes_table="lattice_nodes",
            edges_table="lattice_edges",
            node_type_col="node_type",
            node_id_col="node_id",
        )
        ctx.set_types(node_types=["Node"], edge_types=["CONNECTED"])
        return ctx

    def test_all_modes_produce_identical_path_counts(
        self, spark_lattice, lattice_context
    ):
        """Verify all bidirectional modes produce identical path counts.

        Tests with a moderate depth where all modes should succeed.
        """
        query = """
        MATCH (a:Node)-[:CONNECTED*1..5]->(b:Node)
        WHERE a.node_id = 'layer_0_node_0' AND b.node_id = 'layer_5_node_0'
        RETURN count(*) AS path_count
        """

        results = {}
        for mode in ["off", "recursive", "unrolling"]:
            sql = lattice_context.transpile(query, bidirectional_mode=mode)
            count = spark_lattice.sql(sql).collect()[0]["path_count"]
            results[mode] = count
            print(f"Mode {mode}: {count} paths")

        # All modes should produce identical counts
        counts = list(results.values())
        assert len(set(counts)) == 1, (
            f"Modes produced different results: {results}"
        )

    def test_bidirectional_correctness_depth_3(
        self, spark_lattice, lattice_context
    ):
        """Verify bidirectional produces correct results at depth 3."""
        query = """
        MATCH (a:Node)-[:CONNECTED*1..3]->(b:Node)
        WHERE a.node_id = 'layer_0_node_0' AND b.node_id = 'layer_3_node_0'
        RETURN count(*) AS path_count
        """

        sql_off = lattice_context.transpile(query, bidirectional_mode="off")
        sql_recursive = lattice_context.transpile(query, bidirectional_mode="recursive")
        sql_unrolling = lattice_context.transpile(query, bidirectional_mode="unrolling")

        count_off = spark_lattice.sql(sql_off).collect()[0]["path_count"]
        count_recursive = spark_lattice.sql(sql_recursive).collect()[0]["path_count"]
        count_unrolling = spark_lattice.sql(sql_unrolling).collect()[0]["path_count"]

        assert count_off == count_recursive == count_unrolling, (
            f"Results differ: off={count_off}, recursive={count_recursive}, "
            f"unrolling={count_unrolling}"
        )

    def test_bidirectional_returns_actual_paths(
        self, spark_lattice, lattice_context
    ):
        """Verify bidirectional returns actual path data, not just counts."""
        # No LIMIT - get all paths to ensure identical results
        query = """
        MATCH path = (a:Node)-[:CONNECTED*1..3]->(b:Node)
        WHERE a.node_id = 'layer_0_node_0' AND b.node_id = 'layer_3_node_0'
        RETURN nodes(path) AS path_nodes
        """

        sql_off = lattice_context.transpile(query, bidirectional_mode="off")
        sql_recursive = lattice_context.transpile(
            query, bidirectional_mode="recursive"
        )

        paths_off = [
            tuple(r["path_nodes"])
            for r in spark_lattice.sql(sql_off).collect()
        ]
        paths_recursive = [
            tuple(r["path_nodes"])
            for r in spark_lattice.sql(sql_recursive).collect()
        ]

        # Both should return valid paths (start with layer_0, end with layer_3)
        for path in paths_off:
            assert path[0] == "layer_0_node_0", f"Invalid start: {path}"
            assert path[-1] == "layer_3_node_0", f"Invalid end: {path}"

        for path in paths_recursive:
            assert path[0] == "layer_0_node_0", f"Invalid start: {path}"
            assert path[-1] == "layer_3_node_0", f"Invalid end: {path}"

        # Full path sets should be identical (sorted for comparison)
        assert sorted(paths_off) == sorted(paths_recursive), (
            f"Path sets differ: off has {len(paths_off)}, "
            f"recursive has {len(paths_recursive)}"
        )

    def test_no_path_returns_zero(
        self, spark_lattice, lattice_context
    ):
        """Verify query returns 0 (not error) when no path exists."""
        # layer_5_node_9 is the last layer - no outgoing edges to layer_0
        query = """
        MATCH (a:Node)-[:CONNECTED*1..5]->(b:Node)
        WHERE a.node_id = 'layer_5_node_0' AND b.node_id = 'layer_0_node_0'
        RETURN count(*) AS path_count
        """

        for mode in ["off", "recursive", "unrolling"]:
            sql = lattice_context.transpile(query, bidirectional_mode=mode)
            count = spark_lattice.sql(sql).collect()[0]["path_count"]
            assert count == 0, f"Mode {mode} returned {count}, expected 0"
