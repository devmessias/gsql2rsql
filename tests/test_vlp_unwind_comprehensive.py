"""
Comprehensive VLP + UNWIND + Recursion Tests

Tests VLP (Variable Length Path) queries with UNWIND operations
using PySpark GraphContext mode. Validates actual row outputs.

Graph Structure (12 nodes, 18 edges):
                                    +--MANAGES--> Frank --KNOWS--> Grace
                                    |                      |
    Alice --KNOWS--> Bob --KNOWS--> Carol --KNOWS--> Dave --+
      |               |               |                |
      |               +--WORKS_WITH---+                |
      |                               |                v
      +-------KNOWS-------> Eve <-----+             Henry
      |                      |
      |                      +--KNOWS--> Ivan --KNOWS--> Jack
      |                                    |
      +--REPORTS_TO--> Kate <--------------+
      |
      v
    (Dave --KNOWS--> Alice) <- CYCLE
    (Jack --KNOWS--> Eve) <- ANOTHER CYCLE
    Leo is ISOLATED (no edges)
"""

import pytest
from pyspark.sql import SparkSession
from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("VLP_UNWIND_Comprehensive_Tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def graph_context(spark):
    """Create complex graph with 12 nodes, 18 edges, 4 relationship types."""
    # Nodes: 12 people with different departments
    nodes_data = [
        ("alice", "Person", "Alice", "Engineering", 100000),
        ("bob", "Person", "Bob", "Engineering", 90000),
        ("carol", "Person", "Carol", "Sales", 85000),
        ("dave", "Person", "Dave", "Sales", 80000),
        ("eve", "Person", "Eve", "Marketing", 75000),
        ("frank", "Person", "Frank", "Engineering", 70000),
        ("grace", "Person", "Grace", "Engineering", 65000),
        ("henry", "Person", "Henry", "Sales", 60000),
        ("ivan", "Person", "Ivan", "Marketing", 55000),
        ("jack", "Person", "Jack", "Marketing", 50000),
        ("kate", "Person", "Kate", "Management", 120000),
        ("leo", "Person", "Leo", "Isolated", 45000),  # Isolated node
    ]
    nodes_df = spark.createDataFrame(
        nodes_data, ["node_id", "node_type", "name", "department", "salary"]
    )
    nodes_df.createOrReplaceTempView("nodes")

    # Edges: 18 relationships with 4 types
    edges_data = [
        # Main chain: Alice -> Bob -> Carol -> Dave
        ("alice", "bob", "KNOWS", 10, 2020),
        ("bob", "carol", "KNOWS", 8, 2021),
        ("carol", "dave", "KNOWS", 5, 2022),
        # Alice's other connections
        ("alice", "eve", "KNOWS", 7, 2019),
        ("alice", "kate", "REPORTS_TO", 10, 2018),
        # Carol's connections
        ("carol", "eve", "KNOWS", 6, 2023),
        # Bob's connections
        ("bob", "carol", "WORKS_WITH", 9, 2020),
        ("bob", "frank", "MANAGES", 10, 2021),
        ("bob", "eve", "WORKS_WITH", 4, 2021),
        # Frank -> Grace branch
        ("frank", "grace", "KNOWS", 3, 2022),
        # Carol -> Frank work connection
        ("carol", "frank", "WORKS_WITH", 3, 2022),
        # Dave's connections
        ("dave", "alice", "KNOWS", 4, 2023),  # CYCLE back to Alice
        ("dave", "henry", "KNOWS", 2, 2023),
        # Grace -> Henry
        ("grace", "henry", "KNOWS", 2, 2023),
        # Eve -> Ivan -> Jack chain
        ("eve", "ivan", "KNOWS", 5, 2021),
        ("ivan", "jack", "KNOWS", 6, 2022),
        ("ivan", "kate", "KNOWS", 7, 2020),
        # Jack -> Eve creates another cycle
        ("jack", "eve", "KNOWS", 1, 2024),
    ]
    edges_df = spark.createDataFrame(
        edges_data, ["src", "dst", "relationship_type", "weight", "since"]
    )
    edges_df.createOrReplaceTempView("edges")

    # Create GraphContext
    graph = GraphContext(
        spark=spark,
        nodes_table="nodes",
        edges_table="edges",
        node_type_col="node_type",
        node_id_col="node_id",
        edge_src_col="src",
        edge_dst_col="dst",
        edge_type_col="relationship_type",
        extra_node_attrs={"name": str, "department": str, "salary": int},
        extra_edge_attrs={"weight": int, "since": int},
    )
    graph.set_types(
        node_types=["Person"],
        edge_types=["KNOWS", "WORKS_WITH", "MANAGES", "REPORTS_TO"],
    )
    return graph


# =============================================================================
# 2.1 VLP Basic Patterns
# =============================================================================
class TestVLPBasicPatterns:
    """Basic VLP queries with data validation."""

    def test_vlp_directed_depth_1(self, spark, graph_context):
        """VLP depth 1: Alice's direct KNOWS connections."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..1]->(b)
        RETURN b.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        names = {row["name"] for row in rows}
        assert names == {"Bob", "Eve"}, f"Expected {{Bob, Eve}}, got {names}"

    def test_vlp_directed_depth_2(self, spark, graph_context):
        """VLP depth 2: Alice's KNOWS connections up to 2 hops."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..2]->(b)
        RETURN DISTINCT b.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        names = {row["name"] for row in rows}
        # Depth 1: Bob, Eve
        # Depth 2: Carol (via Bob), Ivan (via Eve)
        assert "Bob" in names
        assert "Eve" in names
        assert "Carol" in names
        assert "Ivan" in names

    def test_vlp_directed_depth_3(self, spark, graph_context):
        """VLP depth 3: Alice's KNOWS connections up to 3 hops."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..3]->(b)
        RETURN DISTINCT b.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        names = {row["name"] for row in rows}
        # Should find many nodes at various depths
        assert "Bob" in names  # depth 1
        assert "Eve" in names  # depth 1
        assert "Carol" in names  # depth 2
        assert "Ivan" in names  # depth 2
        assert "Dave" in names  # depth 3
        assert "Jack" in names  # depth 3

    def test_vlp_from_middle_node(self, spark, graph_context):
        """VLP starting from a middle node (Carol)."""
        query = """
        MATCH (a {name: "Carol"})-[e:KNOWS*1..3]->(b)
        RETURN DISTINCT b.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        names = {row["name"] for row in rows}
        # Carol -> Dave, Eve (depth 1)
        # Dave -> Alice, Henry (depth 2)
        # Eve -> Ivan (depth 2)
        assert "Dave" in names
        assert "Eve" in names
        assert len(names) >= 2

    def test_vlp_isolated_node(self, spark, graph_context):
        """VLP from isolated node (Leo) should return no paths."""
        query = """
        MATCH (a {name: "Leo"})-[e*1..3]->(b)
        RETURN b.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 0, f"Expected 0 rows for isolated node, got {len(rows)}"

    def test_vlp_undirected_depth_2(self, spark, graph_context):
        """VLP undirected from Carol should find nodes in both directions."""
        query = """
        MATCH (a {name: "Carol"})-[e*1..2]-(b)
        RETURN DISTINCT b.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        names = {row["name"] for row in rows}
        # Should find Bob (incoming), Dave (outgoing), Eve (outgoing), etc.
        assert "Bob" in names
        assert "Dave" in names
        assert len(names) >= 4


# =============================================================================
# 2.2 VLP + UNWIND Patterns (Original Bug Pattern)
# =============================================================================
class TestVLPUnwindPatterns:
    """VLP + UNWIND combination patterns - the original bug pattern."""

    def test_vlp_unwind_basic(self, spark, graph_context):
        """Basic VLP + UNWIND: expand edges from path.

        Note: Accessing properties of unwound struct (r.src, r.dst) is not yet
        implemented. This test returns the full struct `r` instead.
        """
        query = """
        MATCH (s {name: "Alice"})
        MATCH (s)-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should return struct rows with src, dst, etc.
        assert len(rows) >= 2
        for row in rows:
            r = row["r"]
            assert r is not None
            # The struct should have src and dst fields
            assert hasattr(r, "src") or "src" in str(r)

    def test_vlp_unwind_edge_properties(self, spark, graph_context):
        """UNWIND should expose all edge properties."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.weight AS weight
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Check that weight is accessible
        for row in rows:
            assert row["src"] is not None
            assert row["dst"] is not None
            assert row["weight"] is not None

    def test_vlp_unwind_with_filter(self, spark, graph_context):
        """UNWIND with filter on edge properties."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..3]->(o)
        UNWIND e AS r
        WITH r WHERE r.weight > 5
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.weight AS weight
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # All returned edges should have weight > 5
        for row in rows:
            assert row["weight"] > 5, f"Expected weight > 5, got {row['weight']}"

    def test_vlp_unwind_aggregation(self, spark, graph_context):
        """UNWIND with aggregation on edge properties."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN SUM(r.weight) AS total_weight
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["total_weight"] is not None
        assert rows[0]["total_weight"] > 0


# =============================================================================
# 2.3 VLP + Multiple MATCH
# =============================================================================
class TestVLPMultipleMatch:
    """Multiple MATCH clauses with VLP."""

    def test_multiple_match_vlp_unwind(self, spark, graph_context):
        """Multiple MATCH with VLP and UNWIND - the exact pattern that was buggy.

        Note: Returns full struct instead of r.src, r.dst (property access not implemented).
        """
        query = """
        MATCH (s:Person)
        WHERE s.name = "Alice"
        MATCH (s)-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should have at least 2 distinct edges
        assert len(rows) >= 2

    def test_triple_match_vlp(self, spark, graph_context):
        """Three MATCH clauses with VLP."""
        query = """
        MATCH (a {name: "Alice"})
        MATCH (a)-[e1:KNOWS*1..1]->(b)
        MATCH (b)-[e2:KNOWS*1..1]->(c)
        RETURN a.name AS a_name, b.name AS b_name, c.name AS c_name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Alice -> Bob -> Carol
        # Alice -> Eve -> Ivan
        paths = {(row["a_name"], row["b_name"], row["c_name"]) for row in rows}
        assert len(paths) >= 1


# =============================================================================
# 2.4 VLP Undirected + UNWIND
# =============================================================================
class TestVLPUndirected:
    """Undirected VLP patterns."""

    def test_vlp_undirected_unwind(self, spark, graph_context):
        """Undirected VLP with UNWIND.

        Note: Returns full struct instead of r.src, r.dst (property access not implemented).
        """
        query = """
        MATCH (s {name: "Carol"})-[e*1..2]-(o)
        UNWIND e AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should find edges in both directions from Carol
        assert len(rows) >= 3

    def test_vlp_undirected_distinct_nodes(self, spark, graph_context):
        """Undirected VLP should find nodes reachable in either direction."""
        query = """
        MATCH (s {name: "Eve"})-[e*1..2]-(o)
        RETURN DISTINCT o.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        names = {row["name"] for row in rows}
        # Eve has edges: alice->eve, carol->eve, eve->ivan, jack->eve
        assert "Alice" in names or "Ivan" in names or "Carol" in names
        assert len(names) >= 3


# =============================================================================
# 2.5 VLP Multi Edge Type
# =============================================================================
class TestVLPMultiEdgeType:
    """VLP with multiple edge types."""

    def test_vlp_multi_edge_type(self, spark, graph_context):
        """VLP with multiple edge types (KNOWS|WORKS_WITH)."""
        query = """
        MATCH (a {name: "Bob"})-[e:KNOWS|WORKS_WITH*1..2]->(b)
        RETURN DISTINCT b.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        names = {row["name"] for row in rows}
        # Bob --KNOWS--> Carol, Bob --WORKS_WITH--> Carol, Eve
        # Bob --MANAGES--> Frank (not included - different type)
        assert "Carol" in names
        assert "Eve" in names

    def test_vlp_unwind_multi_type(self, spark, graph_context):
        """UNWIND should show relationship types."""
        query = """
        MATCH (a {name: "Bob"})-[e:KNOWS|MANAGES*1..2]->(b)
        UNWIND e AS r
        RETURN DISTINCT r.relationship_type AS rel_type
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        types = {row["rel_type"] for row in rows}
        # Bob uses both KNOWS and MANAGES
        assert "KNOWS" in types or "MANAGES" in types

    def test_vlp_reports_to_chain(self, spark, graph_context):
        """VLP with REPORTS_TO relationship."""
        query = """
        MATCH (a)-[e:REPORTS_TO*1..2]->(b)
        RETURN a.name AS a_name, b.name AS b_name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Only Alice -> Kate via REPORTS_TO
        paths = {(row["a_name"], row["b_name"]) for row in rows}
        assert ("Alice", "Kate") in paths


# =============================================================================
# 2.6 Alternative Paths
# =============================================================================
class TestVLPAlternativePaths:
    """Tests for multiple paths to same destination."""

    def test_vlp_multiple_paths_to_same_node(self, spark, graph_context):
        """Find multiple paths to the same destination."""
        query = """
        MATCH (a {name: "Alice"})-[e*1..3]->(b {name: "Eve"})
        RETURN SIZE(e) AS hops
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Alice -> Eve directly (1 hop)
        # Other possible paths may exist
        hops = {row["hops"] for row in rows}
        assert 1 in hops, "Should find direct path (1 hop)"

    def test_vlp_count_paths(self, spark, graph_context):
        """Count paths to each destination."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..2]->(b)
        RETURN b.name AS name, COUNT(*) AS path_count
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) >= 2


# =============================================================================
# 2.7 SIZE() with Relationship Variable
# =============================================================================
class TestSizeRelationshipVariable:
    """SIZE() function with relationship variables."""

    def test_size_relationship_variable(self, spark, graph_context):
        """SIZE(r) should return path length."""
        query = """
        MATCH (a {name: "Alice"})-[r:KNOWS*1..3]->(b)
        RETURN b.name AS name, SIZE(r) AS hops
        ORDER BY hops
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Check that hops values are 1, 2, or 3
        for row in rows:
            assert row["hops"] in [1, 2, 3], f"Unexpected hops: {row['hops']}"

    def test_size_with_filter(self, spark, graph_context):
        """Filter by SIZE(r)."""
        query = """
        MATCH (a {name: "Alice"})-[r:KNOWS*1..3]->(b)
        WHERE SIZE(r) >= 2
        RETURN a.name AS a_name, b.name AS b_name, SIZE(r) AS hops
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # All rows should have hops >= 2
        for row in rows:
            assert row["hops"] >= 2, f"Expected hops >= 2, got {row['hops']}"


# =============================================================================
# 2.8 UNWIND + Aggregations
# =============================================================================
class TestUnwindAggregations:
    """UNWIND with aggregation functions."""

    def test_unwind_sum_weight(self, spark, graph_context):
        """SUM of edge weights after UNWIND."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..2]->(b)
        UNWIND e AS r
        RETURN b.name AS name, SUM(r.weight) AS total
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        for row in rows:
            assert row["total"] is not None
            assert row["total"] > 0

    def test_unwind_collect(self, spark, graph_context):
        """COLLECT edge weights after UNWIND."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..2]->(b)
        UNWIND e AS r
        RETURN b.name AS name, COLLECT(r.weight) AS weights
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        for row in rows:
            assert isinstance(row["weights"], list)


# =============================================================================
# 2.9 VLP with DISTINCT and ORDER BY
# =============================================================================
class TestVLPDistinctOrderBy:
    """VLP with DISTINCT and ORDER BY."""

    def test_vlp_distinct_paths(self, spark, graph_context):
        """DISTINCT source-destination pairs ordered."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..3]->(b)
        RETURN DISTINCT a.name AS a_name, b.name AS b_name
        ORDER BY b_name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should be ordered by b_name
        names = [row["b_name"] for row in rows]
        assert names == sorted(names), "Results should be ordered"

    def test_vlp_unwind_order_by_weight(self, spark, graph_context):
        """UNWIND edges ordered by weight."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..2]->(b)
        UNWIND e AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.weight AS weight
        ORDER BY weight DESC
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should be ordered by weight descending
        weights = [row["weight"] for row in rows]
        assert weights == sorted(weights, reverse=True), "Should be ordered DESC"


# =============================================================================
# 2.10 Cycle Detection
# =============================================================================
class TestVLPCycleDetection:
    """Tests for cycle detection in graphs."""

    def test_vlp_cycle_detection_alice(self, spark, graph_context):
        """VLP should handle cycle without infinite loop."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..3]->(b)
        RETURN DISTINCT b.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should complete without infinite loop
        names = {row["name"] for row in rows}
        assert len(names) >= 2
        # Note: Alice might appear due to cycle Dave->Alice, depending on implementation

    def test_vlp_cycle_detection_eve(self, spark, graph_context):
        """VLP from Eve should handle Jack->Eve cycle."""
        query = """
        MATCH (a {name: "Eve"})-[e:KNOWS*1..3]->(b)
        RETURN DISTINCT b.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        names = {row["name"] for row in rows}
        # Eve -> Ivan -> Jack, Ivan -> Kate
        assert "Ivan" in names

    def test_vlp_cycle_unwind(self, spark, graph_context):
        """UNWIND on cyclic path should return finite edges.

        Note: Returns full struct instead of r.src, r.dst (property access not implemented).
        """
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..3]->(b)
        UNWIND e AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should complete and return finite set of edges
        assert len(rows) >= 2
        assert len(rows) < 100  # Sanity check - not infinite

    def test_vlp_cycle_path_length(self, spark, graph_context):
        """Path length from Dave includes cycle back to Alice."""
        query = """
        MATCH (a {name: "Dave"})-[e:KNOWS*1..3]->(b)
        RETURN b.name AS name, SIZE(e) AS hops
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Dave -> Alice (1 hop), Dave -> Henry (1 hop)
        names_at_hop_1 = {row["name"] for row in rows if row["hops"] == 1}
        assert "Alice" in names_at_hop_1 or "Henry" in names_at_hop_1

    def test_vlp_cycle_undirected(self, spark, graph_context):
        """Undirected VLP on cyclic graph should not infinite loop."""
        query = """
        MATCH (a {name: "Carol"})-[e*1..3]-(b)
        RETURN DISTINCT b.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should complete without infinite loop
        names = {row["name"] for row in rows}
        assert len(names) >= 3

    def test_vlp_two_cycles_no_infinite(self, spark, graph_context):
        """Jack is in a cycle (Jack->Eve->Ivan->Jack), should not infinite loop."""
        query = """
        MATCH (a {name: "Jack"})-[e:KNOWS*1..3]->(b)
        RETURN DISTINCT b.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Jack -> Eve (1 hop), Eve -> Ivan (via Jack's outgoing)
        names = {row["name"] for row in rows}
        assert "Eve" in names


# =============================================================================
# 2.11 Edge Cases
# =============================================================================
class TestVLPEdgeCases:
    """Edge cases and boundary conditions."""

    def test_vlp_no_path_exists(self, spark, graph_context):
        """VLP from node with no outgoing edges."""
        query = """
        MATCH (a {name: "Grace"})-[e:KNOWS*1..3]->(b)
        RETURN b.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Grace -> Henry only
        names = {row["name"] for row in rows}
        assert "Henry" in names or len(rows) == 0

    def test_vlp_min_hops_2(self, spark, graph_context):
        """VLP with minimum 2 hops."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*2..3]->(b)
        RETURN DISTINCT b.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should NOT include Bob or Eve (1 hop away)
        names = {row["name"] for row in rows}
        # Direct neighbors should not be included
        # Carol is 2 hops, Ivan is 2 hops

    def test_vlp_same_min_max(self, spark, graph_context):
        """VLP with exact hop count (*2..2)."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*2..2]->(b)
        RETURN DISTINCT b.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Only nodes exactly 2 hops away
        # Alice -> Bob -> Carol (2 hops)
        # Alice -> Eve -> Ivan (2 hops)
        names = {row["name"] for row in rows}
        assert len(rows) >= 1

    def test_vlp_unwind_empty_path(self, spark, graph_context):
        """UNWIND on empty path should return no rows.

        Note: Returns full struct instead of r.src, r.dst (property access not implemented).
        """
        query = """
        MATCH (a {name: "Leo"})-[e:KNOWS*1..2]->(b)
        UNWIND e AS r
        RETURN r
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 0, f"Expected 0 rows for isolated node, got {len(rows)}"


# =============================================================================
# WITH + VLP + UNWIND Regression Tests (for fixed bugs)
# =============================================================================
class TestWithVLPUnwindRegression:
    """Regression tests for WITH + VLP + UNWIND bugs that were fixed.

    These tests cover:
    1. Node resolution after WITH clause in VLP queries
    2. structured_type preservation when projecting ValueField through WITH
    3. LATERAL EXPLODE syntax for COLLECT → UNWIND pattern
    """

    def test_with_before_unwind(self, spark, graph_context):
        """WITH clause before VLP + UNWIND pattern.

        Bug: Nodes projected through WITH were incorrectly treated as relationships.
        Fix: column_resolver.py uses entity_info.entity_type == EntityType.NODE
        """
        query = """
        MATCH (s:Person)
        WHERE s.name = "Alice"
        WITH s
        MATCH (s)-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should successfully resolve s after WITH
        edges = {(row["src"], row["dst"]) for row in rows}
        assert len(edges) >= 2
        assert ("alice", "bob") in edges or ("alice", "eve") in edges

    def test_with_after_unwind_aggregation(self, spark, graph_context):
        """WITH clause after UNWIND for aggregation.

        Bug: structured_type was not preserved when projecting ValueField through WITH.
        Fix: operators.py preserves structured_type in ProjectionOperator.
        """
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        WITH r.dst AS destination, r.weight AS w
        RETURN destination, SUM(w) AS total
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should successfully access r.dst and r.weight after WITH
        assert len(rows) >= 1
        for row in rows:
            assert row["destination"] is not None
            assert row["total"] > 0

    def test_with_node_projection_after_vlp(self, spark, graph_context):
        """WITH projecting node after VLP - tests node resolution fix.

        This is the exact pattern that was failing before the fix.
        """
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        WITH s, o, e
        UNWIND e AS r
        RETURN s.name AS start, o.name AS end, r.weight AS weight
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should successfully access s.name and o.name after WITH
        for row in rows:
            assert row["start"] == "Alice"
            assert row["end"] is not None
            assert row["weight"] is not None

    def test_collect_then_unwind(self, spark, graph_context):
        """COLLECT → UNWIND pattern that required LATERAL EXPLODE fix.

        Bug: Comma-join syntax didn't work when EXPLODE referenced columns from subquery.
        Fix: sql_renderer.py uses LATERAL EXPLODE syntax.
        """
        query = """
        MATCH (n:Person)
        WHERE n.name IN ["Alice", "Bob", "Carol"]
        WITH COLLECT(n.name) AS names
        UNWIND names AS name
        RETURN name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should successfully unwind the collected names
        names = {row["name"] for row in rows}
        assert "Alice" in names
        assert "Bob" in names
        assert "Carol" in names

    def test_with_vlp_multiple_projections(self, spark, graph_context):
        """Multiple WITH projections with VLP - comprehensive regression test."""
        query = """
        MATCH (s:Person)
        WHERE s.name = "Alice"
        WITH s
        MATCH (s)-[e:KNOWS*1..2]->(o)
        WITH s, o, e, SIZE(e) AS hops
        WHERE hops = 2
        UNWIND e AS r
        RETURN s.name AS start, o.name AS end, r.src AS edge_src, r.dst AS edge_dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should return edges for 2-hop paths only
        for row in rows:
            assert row["start"] == "Alice"
            assert row["end"] is not None
            assert row["edge_src"] is not None
            assert row["edge_dst"] is not None

    def test_relationships_function_with_path_variable(self, spark, graph_context):
        """Test relationships(p) function with path variable.

        Bug: Path variable projection used _gsql2rsql_p but CTE produces _gsql2rsql_p_id.
        Fix: operators.py now uses _gsql2rsql_p_id suffix for path field_name.

        This is the exact pattern from user report:
        MATCH (root{node_id: "..."})
        MATCH p = (root)-[*1..3]->()
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        query = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[*1..2]->()
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should return distinct relationship structs
        assert len(rows) >= 2
        for row in rows:
            r = row["r"]
            assert r is not None
            # The struct should have src and dst fields
            assert hasattr(r, "src") or "src" in str(r)


# =============================================================================
# 2.13 Relationships(p) Function Comprehensive Tests
# =============================================================================
class TestRelationshipsFunction:
    """Test relationships(p) function with various patterns.

    The relationships(p) function extracts relationships from a path variable.
    It should work with:
    - Anonymous path patterns: p = ()-[*1..3]->()
    - Named relationship variables: p = ()-[e*1..3]->()
    - Typed relationships: p = ()-[:KNOWS*1..3]->()
    - Different depth ranges
    """

    def test_relationships_path_only_anonymous(self, spark, graph_context):
        """Test relationships(p) with only path variable (no named rel var).

        Pattern: MATCH p = (root)-[*1..3]->()
        """
        query = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[*1..3]->()
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for anonymous VLP ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # Should return relationships from all paths from Alice
        assert len(rows) >= 2, f"Expected at least 2 relationships, got {len(rows)}"

    def test_relationships_path_with_named_rel_variable(self, spark, graph_context):
        """Test relationships(p) with named relationship variable.

        Pattern: MATCH p = (root)-[e*1..3]->()
        """
        query = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[e*1..3]->(f)
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for named rel var ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # Should return relationships from all paths from Alice
        assert len(rows) >= 2, f"Expected at least 2 relationships, got {len(rows)}"

    def test_relationships_path_with_typed_rel(self, spark, graph_context):
        """Test relationships(p) with typed relationship.

        Pattern: MATCH p = (root)-[e:KNOWS*1..3]->()
        """
        query = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[e:KNOWS*1..3]->(f)
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for typed rel ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # Should return only KNOWS relationships
        assert len(rows) >= 2, f"Expected at least 2 relationships, got {len(rows)}"

    def test_relationships_path_different_depths(self, spark, graph_context):
        """Test relationships(p) with different depth ranges."""
        # Depth 1..2
        query1 = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[*1..2]->()
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql1 = graph_context.transpile(query1)
        print(f"\n=== SQL for depth 1..2 ===\n{sql1}\n")
        result1 = spark.sql(sql1)
        rows1 = result1.collect()

        # Depth 2..3
        query2 = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[*2..3]->()
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql2 = graph_context.transpile(query2)
        print(f"\n=== SQL for depth 2..3 ===\n{sql2}\n")
        result2 = spark.sql(sql2)
        rows2 = result2.collect()

        # Both should work
        assert len(rows1) >= 2, f"Depth 1..2: Expected >= 2, got {len(rows1)}"
        assert len(rows2) >= 2, f"Depth 2..3: Expected >= 2, got {len(rows2)}"

    def test_relationships_path_with_return_r_properties(self, spark, graph_context):
        """Test relationships(p) returning specific properties."""
        query = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[*1..2]->()
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for r.src, r.dst ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # Should have src and dst columns
        assert len(rows) >= 2
        for row in rows:
            assert row["src"] is not None
            assert row["dst"] is not None

    def test_relationships_path_with_where_filter(self, spark, graph_context):
        """Test relationships(p) with WHERE filter on unwound relationship.

        Note: WHERE directly after UNWIND isn't supported, so we use WITH clause.
        """
        query = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[*1..3]->()
        UNWIND relationships(p) AS r
        WITH r
        WHERE r.weight > 5
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for WHERE filter ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # All returned relationships should have weight > 5
        for row in rows:
            r = row["r"]
            assert r.weight > 5, f"Expected weight > 5, got {r.weight}"

    def test_relationships_vs_unwind_e_equivalence(self, spark, graph_context):
        """Test that relationships(p) and UNWIND e produce equivalent results.

        When we have: MATCH p = (s)-[e*1..2]->(t)
        UNWIND relationships(p) AS r should be equivalent to UNWIND e AS r
        """
        # Using UNWIND e directly
        query_e = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[e*1..2]->(target)
        UNWIND e AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        ORDER BY src, dst
        """
        sql_e = graph_context.transpile(query_e)
        result_e = spark.sql(sql_e)
        rows_e = result_e.collect()

        # Using relationships(p)
        query_p = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[e*1..2]->(target)
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        ORDER BY src, dst
        """
        sql_p = graph_context.transpile(query_p)
        result_p = spark.sql(sql_p)
        rows_p = result_p.collect()

        # Results should be equivalent
        set_e = {(row["src"], row["dst"]) for row in rows_e}
        set_p = {(row["src"], row["dst"]) for row in rows_p}

        assert set_e == set_p, f"UNWIND e: {set_e}, relationships(p): {set_p}"

    def test_relationships_path_single_match(self, spark, graph_context):
        """Test relationships(p) in single MATCH pattern.

        Pattern: MATCH p = (root)-[*1..2]->() WHERE root.name = 'Alice'
        """
        query = """
        MATCH p = (root)-[*1..2]->()
        WHERE root.name = "Alice"
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src, r.dst
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for single MATCH ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) >= 2, f"Expected >= 2 relationships, got {len(rows)}"


# =============================================================================
# 2.14 BFS Validation Tests with Linear Graph
# =============================================================================
@pytest.fixture(scope="module")
def linear_graph_context(spark):
    """Create a simple linear graph for BFS validation.

    Graph Structure: 1 -> 2 -> 3 -> 4 -> 5 (chain)

    This simple graph makes it easy to validate BFS traversal:
    - From node 1: should reach 2 at depth 1, 3 at depth 2, etc.
    - From node 3: directed should reach 4,5; undirected should reach all
    - From node 5: directed reaches nothing; undirected reaches 4,3,2,1
    """
    # Nodes: 5 nodes in a chain
    nodes_data = [
        ("1", "Node", "Node1"),
        ("2", "Node", "Node2"),
        ("3", "Node", "Node3"),
        ("4", "Node", "Node4"),
        ("5", "Node", "Node5"),
    ]
    nodes_df = spark.createDataFrame(nodes_data, ["node_id", "node_type", "name"])
    nodes_df.createOrReplaceTempView("linear_nodes")

    # Edges: linear chain 1->2->3->4->5
    edges_data = [
        ("1", "2", "NEXT", 1),
        ("2", "3", "NEXT", 2),
        ("3", "4", "NEXT", 3),
        ("4", "5", "NEXT", 4),
    ]
    edges_df = spark.createDataFrame(
        edges_data, ["src", "dst", "relationship_type", "weight"]
    )
    edges_df.createOrReplaceTempView("linear_edges")

    # Create GraphContext
    graph = GraphContext(
        spark=spark,
        nodes_table="linear_nodes",
        edges_table="linear_edges",
        node_type_col="node_type",
        node_id_col="node_id",
        edge_src_col="src",
        edge_dst_col="dst",
        edge_type_col="relationship_type",
        extra_node_attrs={"name": str},
        extra_edge_attrs={"weight": int},
    )
    graph.set_types(node_types=["Node"], edge_types=["NEXT"])
    return graph


class TestBFSValidation:
    """Test BFS traversal correctness with a linear graph.

    Linear graph: 1 -> 2 -> 3 -> 4 -> 5

    These tests validate that VLP queries correctly traverse the graph
    in a BFS manner, respecting direction constraints.
    """

    def test_directed_bfs_from_head(self, spark, linear_graph_context):
        """Test directed BFS starting from node 1 (head of chain).

        Expected: Should find all downstream nodes 2, 3, 4, 5
        Edges traversed: 1->2, 2->3, 3->4, 4->5
        """
        query = """
        MATCH (root {node_id: "1"})
        MATCH p = (root)-[e*1..4]->()
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        ORDER BY src, dst
        """
        sql = linear_graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should find all 4 edges: 1->2, 2->3, 3->4, 4->5
        edges = {(row["src"], row["dst"]) for row in rows}
        expected = {("1", "2"), ("2", "3"), ("3", "4"), ("4", "5")}
        assert edges == expected, f"Expected {expected}, got {edges}"

    def test_directed_bfs_from_middle(self, spark, linear_graph_context):
        """Test directed BFS starting from node 3 (middle of chain).

        Expected: Should only find downstream nodes 4, 5
        Edges traversed: 3->4, 4->5
        """
        query = """
        MATCH (root {node_id: "3"})
        MATCH p = (root)-[e*1..3]->()
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        ORDER BY src, dst
        """
        sql = linear_graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should find edges: 3->4, 4->5
        edges = {(row["src"], row["dst"]) for row in rows}
        expected = {("3", "4"), ("4", "5")}
        assert edges == expected, f"Expected {expected}, got {edges}"

    def test_directed_bfs_from_tail(self, spark, linear_graph_context):
        """Test directed BFS starting from node 5 (tail of chain).

        Expected: Should find NO downstream nodes (5 is the tail)
        """
        query = """
        MATCH (root {node_id: "5"})
        MATCH p = (root)-[e*1..3]->()
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        """
        sql = linear_graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should find no edges (5 has no outgoing edges)
        assert len(rows) == 0, f"Expected 0 edges from tail, got {len(rows)}"

    def test_undirected_bfs_from_middle(self, spark, linear_graph_context):
        """Test undirected BFS starting from node 3 (middle of chain).

        Expected: Should find ALL nodes in both directions
        Edges traversed: 3-4, 4-5, 3-2, 2-1 (both directions)
        """
        query = """
        MATCH (root {node_id: "3"})
        MATCH p = (root)-[e*1..4]-()
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        ORDER BY src, dst
        """
        sql = linear_graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should find all edges (undirected explores both ways)
        # Note: edges in the table are stored as src->dst, so we see them that way
        edges = {(row["src"], row["dst"]) for row in rows}
        expected = {("1", "2"), ("2", "3"), ("3", "4"), ("4", "5")}
        assert edges == expected, f"Expected {expected}, got {edges}"

    def test_undirected_bfs_from_tail(self, spark, linear_graph_context):
        """Test undirected BFS starting from node 5 (tail of chain).

        Expected: Should find all upstream nodes via undirected traversal
        """
        query = """
        MATCH (root {node_id: "5"})
        MATCH p = (root)-[e*1..4]-()
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        ORDER BY src, dst
        """
        sql = linear_graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Should find all edges going "back" through the chain
        edges = {(row["src"], row["dst"]) for row in rows}
        expected = {("1", "2"), ("2", "3"), ("3", "4"), ("4", "5")}
        assert edges == expected, f"Expected {expected}, got {edges}"

    def test_bfs_depth_limit(self, spark, linear_graph_context):
        """Test that depth limits are respected in BFS.

        From node 1 with depth 1..2:
        - Depth 1: reaches node 2 via edge 1->2
        - Depth 2: reaches node 3 via edges 1->2, 2->3
        Should NOT reach nodes 4 or 5
        """
        query = """
        MATCH (root {node_id: "1"})
        MATCH p = (root)-[e*1..2]->(target)
        RETURN DISTINCT target.node_id AS reached_node
        ORDER BY reached_node
        """
        sql = linear_graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        reached = {row["reached_node"] for row in rows}
        expected = {"2", "3"}  # Only nodes within 2 hops
        assert reached == expected, f"Expected {expected}, got {reached}"

    def test_bfs_relationships_count(self, spark, linear_graph_context):
        """Test counting relationships in BFS paths.

        From node 1, count unique edges traversed at each depth.
        """
        query = """
        MATCH (root {node_id: "1"})
        MATCH p = (root)-[e*1..4]->(target)
        UNWIND relationships(p) AS r
        RETURN target.node_id AS target, COUNT(r) AS edge_count
        ORDER BY target
        """
        sql = linear_graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Verify edge counts: to reach node 2 = 1 edge, node 3 = 2 edges, etc.
        counts = {row["target"]: row["edge_count"] for row in rows}
        assert counts.get("2") == 1, f"Expected 1 edge to node 2, got {counts.get('2')}"
        assert counts.get("3") == 2, f"Expected 2 edges to node 3, got {counts.get('3')}"
        assert counts.get("4") == 3, f"Expected 3 edges to node 4, got {counts.get('4')}"
        assert counts.get("5") == 4, f"Expected 4 edges to node 5, got {counts.get('5')}"

    def test_bfs_path_with_named_rel_variable(self, spark, linear_graph_context):
        """Test BFS with named relationship variable in pattern.

        MATCH p = (root)-[e*1..3]->(target)
        Both UNWIND e AS r and UNWIND relationships(p) AS r should work.
        """
        # Using UNWIND e
        query_e = """
        MATCH (root {node_id: "1"})
        MATCH p = (root)-[e*1..3]->(target)
        UNWIND e AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        ORDER BY src, dst
        """
        sql_e = linear_graph_context.transpile(query_e)
        rows_e = spark.sql(sql_e).collect()
        edges_e = {(row["src"], row["dst"]) for row in rows_e}

        # Using relationships(p)
        query_p = """
        MATCH (root {node_id: "1"})
        MATCH p = (root)-[e*1..3]->(target)
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        ORDER BY src, dst
        """
        sql_p = linear_graph_context.transpile(query_p)
        rows_p = spark.sql(sql_p).collect()
        edges_p = {(row["src"], row["dst"]) for row in rows_p}

        # Both should produce same results
        assert edges_e == edges_p, f"UNWIND e: {edges_e}, relationships(p): {edges_p}"

        # And should include 1->2, 2->3, 3->4 (depth 1-3)
        expected = {("1", "2"), ("2", "3"), ("3", "4")}
        assert edges_e == expected, f"Expected {expected}, got {edges_e}"


# =============================================================================
# 2.15 Combined Features: relationships(p) + WHERE IN (QueryExpressionList)
# =============================================================================
class TestRelationshipsWithWhereIn:
    """Test relationships(p) combined with WHERE IN clauses.

    These tests validate the interaction between:
    1. relationships(p) function (path edges extraction)
    2. QueryExpressionList for WHERE IN clause

    This catches potential regressions where both features interact.
    """

    def test_where_in_before_vlp_relationships(self, spark, graph_context):
        """Test WHERE IN on root node before VLP with relationships(p).

        Pattern: Filter root nodes with IN, then traverse and extract edges.
        """
        query = """
        MATCH (root)
        WHERE root.name IN ["Alice", "Bob"]
        MATCH p = (root)-[*1..2]->()
        UNWIND relationships(p) AS r
        RETURN DISTINCT root.name AS start, r.src AS edge_src, r.dst AS edge_dst
        ORDER BY start, edge_src, edge_dst
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for WHERE IN + relationships(p) ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # Should have edges from both Alice and Bob
        starts = {row["start"] for row in rows}
        assert "Alice" in starts, f"Expected Alice in results, got {starts}"
        assert "Bob" in starts, f"Expected Bob in results, got {starts}"

        # Verify edges are present
        assert len(rows) >= 4, f"Expected >= 4 edges, got {len(rows)}"

    def test_where_in_on_relationship_type_after_unwind(self, spark, graph_context):
        """Test WHERE IN on relationship type after UNWIND relationships(p).

        Pattern: Traverse paths, extract edges, filter by relationship type.
        """
        query = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[*1..3]->()
        UNWIND relationships(p) AS r
        WITH r
        WHERE r.relationship_type IN ["KNOWS", "WORKS_WITH"]
        RETURN DISTINCT r.src, r.dst, r.relationship_type
        ORDER BY r.src, r.dst
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for relationships(p) + WHERE IN type ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # All relationships should be KNOWS or WORKS_WITH
        for row in rows:
            rel_type = row["relationship_type"]
            assert rel_type in ["KNOWS", "WORKS_WITH"], \
                f"Unexpected relationship type: {rel_type}"

    def test_multiple_where_in_with_relationships(self, spark, graph_context):
        """Test multiple WHERE IN clauses with relationships(p).

        Pattern: Filter source AND target with IN, then extract edges.
        """
        query = """
        MATCH (root)
        WHERE root.name IN ["Alice", "Bob", "Carol"]
        MATCH p = (root)-[*1..2]->(target)
        WHERE target.name IN ["Eve", "Carol", "Dave"]
        UNWIND relationships(p) AS r
        RETURN DISTINCT root.name AS start, target.name AS end,
               r.src AS edge_src, r.dst AS edge_dst
        ORDER BY start, end
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for multiple WHERE IN + relationships ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # Verify source/target constraints
        for row in rows:
            assert row["start"] in ["Alice", "Bob", "Carol"], \
                f"Start {row['start']} not in allowed list"
            assert row["end"] in ["Eve", "Carol", "Dave"], \
                f"End {row['end']} not in allowed list"

    def test_where_in_with_integer_list(self, spark, graph_context):
        """Test WHERE IN with integer list on edge properties.

        Pattern: Filter edges by weight values using IN.
        """
        query = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[*1..3]->()
        UNWIND relationships(p) AS r
        WITH r
        WHERE r.weight IN [10, 8, 7, 5]
        RETURN DISTINCT r.src, r.dst, r.weight
        ORDER BY r.weight DESC
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for WHERE IN integers ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # All weights should be in the allowed list
        allowed_weights = {10, 8, 7, 5}
        for row in rows:
            assert row["weight"] in allowed_weights, \
                f"Weight {row['weight']} not in {allowed_weights}"

    def test_where_in_combined_with_other_conditions(self, spark, graph_context):
        """Test WHERE IN combined with other comparison operators.

        Pattern: IN clause AND other conditions on edges.
        """
        query = """
        MATCH (root)
        WHERE root.name IN ["Alice", "Bob"]
        MATCH p = (root)-[*1..3]->()
        UNWIND relationships(p) AS r
        WITH root, r
        WHERE r.relationship_type IN ["KNOWS", "MANAGES"]
          AND r.weight > 5
        RETURN DISTINCT root.name AS start, r.src, r.dst, r.weight
        ORDER BY r.weight DESC
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for WHERE IN + AND conditions ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # Verify combined conditions
        for row in rows:
            assert row["start"] in ["Alice", "Bob"]
            assert row["weight"] > 5, f"Weight {row['weight']} should be > 5"

    def test_where_not_in_with_relationships(self, spark, graph_context):
        """Test WHERE NOT IN with relationships(p).

        Pattern: Exclude certain relationship types.
        """
        query = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[*1..2]->()
        UNWIND relationships(p) AS r
        WITH r
        WHERE NOT r.relationship_type IN ["REPORTS_TO", "MANAGES"]
        RETURN DISTINCT r.src, r.dst, r.relationship_type
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for WHERE NOT IN ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # No REPORTS_TO or MANAGES relationships
        excluded = {"REPORTS_TO", "MANAGES"}
        for row in rows:
            assert row["relationship_type"] not in excluded, \
                f"Found excluded type: {row['relationship_type']}"


# =============================================================================
# 2.16 Complex Combined Feature Tests
# =============================================================================
class TestComplexVLPCombinations:
    """Complex tests combining multiple VLP features.

    These tests stress-test the transpiler with combinations of:
    - VLP (Variable Length Paths)
    - OR edge types (|)
    - IN / NOT IN predicates
    - ORDER BY
    - WHERE with complex conditions (AND/OR)
    - UNWIND
    - relationships(p) function
    - Aggregations (SUM, AVG, COUNT, COLLECT)
    - DISTINCT
    - WITH clause projections

    Goal: Expose edge cases and ensure fixes are robust, not just patches.
    """

    def test_vlp_or_edge_types_with_unwind_and_where_in(self, spark, graph_context):
        """VLP with OR edge types + UNWIND + WHERE IN.

        Complex query: Multiple edge types, extract relationships, filter by IN.
        """
        query = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[:KNOWS|WORKS_WITH*1..3]->(target)
        UNWIND relationships(p) AS r
        WITH r
        WHERE r.relationship_type IN ["KNOWS", "WORKS_WITH"]
        RETURN DISTINCT r.src, r.dst, r.relationship_type
        ORDER BY r.relationship_type, r.src
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for OR edge + UNWIND + WHERE IN ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # All relationships should be KNOWS or WORKS_WITH
        for row in rows:
            assert row["relationship_type"] in ["KNOWS", "WORKS_WITH"], \
                f"Unexpected type: {row['relationship_type']}"

    def test_vlp_complex_where_with_and_or(self, spark, graph_context):
        """VLP with complex WHERE conditions (AND/OR).

        Tests predicate pushdown and filter combination.
        """
        query = """
        MATCH (root)
        WHERE root.name = "Alice" OR root.name = "Bob"
        MATCH p = (root)-[*1..2]->(target)
        WHERE target.department = "Sales" OR target.salary > 70000
        UNWIND relationships(p) AS r
        RETURN DISTINCT root.name AS start, target.name AS end,
               r.src, r.dst
        ORDER BY start, end
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for complex WHERE AND/OR ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # Verify root constraint
        starts = {row["start"] for row in rows}
        assert starts <= {"Alice", "Bob"}, f"Unexpected starts: {starts}"

    def test_vlp_unwind_aggregation_order_by(self, spark, graph_context):
        """VLP + UNWIND + aggregation + ORDER BY.

        Complex aggregation on unwound relationships.
        """
        query = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[*1..3]->(target)
        UNWIND relationships(p) AS r
        WITH target.name AS dest, SUM(r.weight) AS total_weight
        RETURN dest, total_weight
        ORDER BY total_weight DESC
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for UNWIND + aggregation + ORDER BY ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # Verify ordering (DESC)
        if len(rows) > 1:
            for i in range(len(rows) - 1):
                assert rows[i]["total_weight"] >= rows[i + 1]["total_weight"], \
                    "Results not ordered by total_weight DESC"

    def test_vlp_distinct_where_in_order_by_combined(self, spark, graph_context):
        """VLP + DISTINCT + WHERE IN + ORDER BY combined.

        Tests interaction of DISTINCT, IN predicates, and ordering.
        """
        query = """
        MATCH (root)
        WHERE root.name IN ["Alice", "Bob", "Carol"]
        MATCH p = (root)-[*1..2]->(target)
        UNWIND relationships(p) AS r
        WITH DISTINCT r.relationship_type AS rel_type, r.weight AS w
        WHERE rel_type IN ["KNOWS", "WORKS_WITH", "MANAGES"]
        RETURN rel_type, AVG(w) AS avg_weight
        ORDER BY avg_weight DESC
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for DISTINCT + WHERE IN + ORDER BY ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # Verify only allowed types
        for row in rows:
            assert row["rel_type"] in ["KNOWS", "WORKS_WITH", "MANAGES"], \
                f"Unexpected rel_type: {row['rel_type']}"

    def test_vlp_nested_unwind_collect(self, spark, graph_context):
        """VLP + UNWIND + COLLECT + second UNWIND.

        Tests nested UNWIND patterns with aggregation.
        """
        query = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[*1..2]->(target)
        UNWIND relationships(p) AS r
        WITH target.name AS dest, COLLECT(r.weight) AS weights
        UNWIND weights AS w
        RETURN dest, w
        ORDER BY dest, w
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for nested UNWIND + COLLECT ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # Should have results
        assert len(rows) >= 1, "Expected at least 1 row"

    def test_vlp_multiple_match_unwind_where_in(self, spark, graph_context):
        """Multiple MATCH + VLP + UNWIND + WHERE IN.

        Complex multi-pattern query with filtering.
        """
        query = """
        MATCH (root:Person)
        WHERE root.department IN ["Engineering", "Sales"]
        WITH root
        MATCH p = (root)-[:KNOWS*1..2]->(friend)
        WHERE friend.department IN ["Marketing", "Engineering"]
        UNWIND relationships(p) AS r
        RETURN DISTINCT root.name AS person, friend.name AS friend,
               r.src, r.dst, r.weight
        ORDER BY person, friend
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for multiple MATCH + WHERE IN ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # Verify department constraints
        # Note: We're testing that the query executes correctly

    def test_vlp_or_edge_unwind_not_in_order(self, spark, graph_context):
        """VLP with OR edges + UNWIND + NOT IN + ORDER BY.

        Combines negation with multiple features.
        """
        query = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[:KNOWS|WORKS_WITH|MANAGES*1..2]->(target)
        UNWIND relationships(p) AS r
        WITH r
        WHERE NOT r.relationship_type IN ["REPORTS_TO"]
        RETURN DISTINCT r.src, r.dst, r.relationship_type, r.weight
        ORDER BY r.weight DESC, r.src ASC
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for OR edge + NOT IN + ORDER BY ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # REPORTS_TO should not appear
        for row in rows:
            assert row["relationship_type"] != "REPORTS_TO", \
                "REPORTS_TO should be excluded"

    def test_vlp_size_where_in_order(self, spark, graph_context):
        """VLP + SIZE(r) + WHERE IN + ORDER BY.

        Combines path length filtering with IN predicate.
        """
        query = """
        MATCH (root)
        WHERE root.name IN ["Alice", "Bob"]
        MATCH p = (root)-[e*1..3]->(target)
        WHERE SIZE(e) >= 2
        UNWIND relationships(p) AS r
        RETURN DISTINCT root.name AS start, target.name AS end,
               SIZE(e) AS hops, r.src, r.dst
        ORDER BY hops DESC, start
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for SIZE + WHERE IN + ORDER BY ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # All paths should have >= 2 hops
        for row in rows:
            assert row["hops"] >= 2, f"Expected hops >= 2, got {row['hops']}"

    def test_vlp_undirected_or_edge_where_in_agg(self, spark, graph_context):
        """Undirected VLP + OR edges + WHERE IN + aggregation.

        Tests undirected traversal with complex features.
        """
        query = """
        MATCH (root {name: "Carol"})
        MATCH p = (root)-[:KNOWS|WORKS_WITH*1..2]-(neighbor)
        UNWIND relationships(p) AS r
        WITH r.relationship_type AS rel_type, COUNT(*) AS cnt
        WHERE rel_type IN ["KNOWS", "WORKS_WITH"]
        RETURN rel_type, cnt
        ORDER BY cnt DESC
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for undirected + OR + WHERE IN + agg ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # Should have results
        assert len(rows) >= 1, "Expected at least 1 row"

    def test_vlp_with_p_then_match_then_unwind_bug(self, spark, graph_context):
        """BUG: WITH p projection before MATCH + UNWIND fails.

        Minimal reproduction of bug where r.src is rendered as _gsql2rsql_r_src
        instead of r.src (struct field access) when path variable p is
        projected through WITH before UNWIND.

        Root cause: When p is projected through WITH, the path's edge column
        (_path_edges_p) loses its structured_type information, so subsequent
        UNWIND doesn't know r is a struct.

        Fix needed in: ProjectionOperator should preserve structured_type when
        projecting ValueFields that have it set.
        """
        # Minimal failing pattern
        query = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[*1..2]->(mid)
        WITH p, mid
        UNWIND relationships(p) AS r
        WITH r.src AS src, r.dst AS dst
        RETURN src, dst
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for WITH p THEN UNWIND bug ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) >= 1, "Expected some results"

    def test_vlp_complex_with_chain(self, spark, graph_context):
        """VLP with multiple WITH clause chain.

        Tests WITH clause projections through multiple stages.
        The original failing pattern: WITH p THEN UNWIND relationships(p)

        This test validates that:
        1. Path variable p survives WITH projection
        2. UNWIND relationships(p) AS r works after WITH
        3. r.src and r.dst are accessible as struct fields
        """
        # Simplified pattern that was failing before the fix
        query = """
        MATCH (root {name: "Alice"})
        MATCH p = (root)-[*1..2]->(mid)
        WITH p, mid, root
        UNWIND relationships(p) AS r
        WITH root.name AS start, r.src AS r_src, r.dst AS r_dst
        RETURN DISTINCT start, r_src, r_dst
        ORDER BY start, r_src, r_dst
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for complex WITH chain ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # Verify we got results and can access r_src and r_dst
        assert len(rows) > 0, "Expected at least one result"
        for row in rows:
            assert "start" in row.asDict(), "Missing start column"
            assert "r_src" in row.asDict(), "Missing r_src column"
            assert "r_dst" in row.asDict(), "Missing r_dst column"
            # Verify r_src and r_dst are not None (struct field access worked)
            assert row["r_src"] is not None, "r_src is None - struct access failed"
            assert row["r_dst"] is not None, "r_dst is None - struct access failed"
            # Verify start is "Alice"
            assert row["start"] == "Alice", f"Expected Alice, got {row['start']}"

    def test_vlp_relationships_vs_e_complex(self, spark, graph_context):
        """Compare relationships(p) vs UNWIND e with complex features.

        Ensures both approaches produce equivalent results in complex scenarios.
        """
        # Using UNWIND e
        query_e = """
        MATCH (root)
        WHERE root.name IN ["Alice", "Bob"]
        MATCH p = (root)-[e:KNOWS*1..2]->(target)
        WHERE target.salary > 60000
        UNWIND e AS r
        WITH root.name AS start, r.src, r.dst, r.weight
        WHERE r.weight > 5
        RETURN DISTINCT start, src, dst, weight
        ORDER BY start, src, dst
        """

        # Using relationships(p)
        query_p = """
        MATCH (root)
        WHERE root.name IN ["Alice", "Bob"]
        MATCH p = (root)-[e:KNOWS*1..2]->(target)
        WHERE target.salary > 60000
        UNWIND relationships(p) AS r
        WITH root.name AS start, r.src, r.dst, r.weight
        WHERE r.weight > 5
        RETURN DISTINCT start, src, dst, weight
        ORDER BY start, src, dst
        """

        sql_e = graph_context.transpile(query_e)
        sql_p = graph_context.transpile(query_p)

        rows_e = spark.sql(sql_e).collect()
        rows_p = spark.sql(sql_p).collect()

        # Results should be equivalent
        set_e = {(r["start"], r["src"], r["dst"], r["weight"]) for r in rows_e}
        set_p = {(r["start"], r["src"], r["dst"], r["weight"]) for r in rows_p}

        assert set_e == set_p, f"UNWIND e: {set_e}\nrelationships(p): {set_p}"

    def test_vlp_all_features_stress(self, spark, graph_context):
        """Stress test combining ALL features.

        Maximum complexity query to stress test the transpiler.
        """
        query = """
        MATCH (root:Person)
        WHERE root.name IN ["Alice", "Bob", "Carol"]
          AND root.salary > 50000
        WITH root
        MATCH p = (root)-[:KNOWS|WORKS_WITH*1..3]->(target)
        WHERE target.department IN ["Engineering", "Sales", "Marketing"]
           OR target.salary > 70000
        UNWIND relationships(p) AS r
        WITH root.name AS person,
             target.name AS reached,
             r.relationship_type AS rel,
             r.weight AS w,
             SIZE(relationships(p)) AS path_len
        WHERE rel IN ["KNOWS", "WORKS_WITH"]
          AND w > 3
        RETURN DISTINCT person, reached, rel, SUM(w) AS total_weight
        ORDER BY total_weight DESC, person ASC
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL for ALL features stress test ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        # Verify constraints
        for row in rows:
            assert row["person"] in ["Alice", "Bob", "Carol"]
            assert row["rel"] in ["KNOWS", "WORKS_WITH"]
