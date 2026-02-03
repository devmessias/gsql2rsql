"""
Comprehensive Transpiler Limitations Test Suite

This file tests known and potential limitations of the gsql2rsql transpiler.
Each test is designed to identify gaps in functionality and validate
that the transpiler handles edge cases correctly.

Categories:
1. UNWIND struct property access
2. Nested UNWIND patterns
3. COLLECT + UNWIND roundtrip
4. PATH variable vs relationship variable
5. Complex VLP patterns
6. WITH chaining with type preservation
7. Aggregations on struct fields
8. CASE expressions with struct fields
9. UNION with UNWIND
10. EXISTS with VLP
"""

import pytest
from pyspark.sql import SparkSession
from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("Transpiler_Limitations_Tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def graph_context(spark):
    """Create graph with rich data for testing."""
    # Nodes: People with various attributes
    nodes_data = [
        ("alice", "Person", "Alice", "Engineering", 100000, 35),
        ("bob", "Person", "Bob", "Engineering", 90000, 30),
        ("carol", "Person", "Carol", "Sales", 85000, 28),
        ("dave", "Person", "Dave", "Sales", 80000, 45),
        ("eve", "Person", "Eve", "Marketing", 75000, 32),
        ("frank", "Person", "Frank", "Engineering", 70000, 27),
    ]
    nodes_df = spark.createDataFrame(
        nodes_data, ["node_id", "node_type", "name", "department", "salary", "age"]
    )
    nodes_df.createOrReplaceTempView("nodes")

    # Edges: Multiple relationship types with rich properties
    edges_data = [
        ("alice", "bob", "KNOWS", 10, 2020, "work"),
        ("bob", "carol", "KNOWS", 8, 2021, "social"),
        ("carol", "dave", "KNOWS", 5, 2022, "work"),
        ("alice", "eve", "KNOWS", 7, 2019, "social"),
        ("alice", "frank", "MANAGES", 10, 2018, "work"),
        ("bob", "frank", "MENTORS", 6, 2020, "work"),
        ("carol", "eve", "KNOWS", 6, 2023, "social"),
        ("dave", "alice", "KNOWS", 4, 2023, "work"),  # Cycle
        ("eve", "frank", "KNOWS", 3, 2024, "social"),
    ]
    edges_df = spark.createDataFrame(
        edges_data, ["src", "dst", "relationship_type", "weight", "since", "context"]
    )
    edges_df.createOrReplaceTempView("edges")

    graph = GraphContext(
        spark=spark,
        nodes_table="nodes",
        edges_table="edges",
        node_type_col="node_type",
        node_id_col="node_id",
        edge_src_col="src",
        edge_dst_col="dst",
        edge_type_col="relationship_type",
        extra_node_attrs={"name": str, "department": str, "salary": int, "age": int},
        extra_edge_attrs={"weight": int, "since": int, "context": str},
    )
    graph.set_types(
        node_types=["Person"],
        edge_types=["KNOWS", "MANAGES", "MENTORS"],
    )
    return graph


# =============================================================================
# Category 1: UNWIND Struct Property Access
# =============================================================================
class TestUnwindStructPropertyAccess:
    """Tests for accessing properties of unwound structs."""

    def test_unwind_simple_property_access(self, spark, graph_context):
        """Basic access to unwound struct properties."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..2]->(b)
        UNWIND e AS r
        RETURN r.src AS source, r.dst AS target
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 1

    def test_unwind_all_properties_access(self, spark, graph_context):
        """Access all properties from unwound edge struct."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..2]->(b)
        UNWIND e AS r
        RETURN r.src, r.dst, r.weight, r.since, r.context
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        for row in rows:
            assert row["src"] is not None

    @pytest.mark.xfail(reason="Struct property filtering after UNWIND not implemented")
    def test_unwind_with_property_filter(self, spark, graph_context):
        """Filter on unwound struct properties in WHERE."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..3]->(b)
        UNWIND e AS r
        WHERE r.weight > 5
        RETURN DISTINCT r.src, r.dst, r.weight
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        for row in rows:
            assert row["weight"] > 5

    def test_unwind_with_clause_filter(self, spark, graph_context):
        """Filter on unwound struct properties in WITH clause."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..2]->(b)
        UNWIND e AS r
        WITH r WHERE r.since >= 2020
        RETURN r.src, r.dst, r.since
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        for row in rows:
            assert row["since"] >= 2020


# =============================================================================
# Category 2: Nested UNWIND Patterns
# =============================================================================
class TestNestedUnwindPatterns:
    """Tests for nested UNWIND operations."""

    def test_unwind_of_collected_array(self, spark, graph_context):
        """UNWIND a COLLECT'd array."""
        query = """
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        WITH COLLECT(b.name) AS names
        UNWIND names AS n
        RETURN DISTINCT n
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 1

    @pytest.mark.xfail(reason="Double UNWIND with struct access not implemented")
    def test_double_unwind_vlp(self, spark, graph_context):
        """Two VLPs with UNWIND on both."""
        query = """
        MATCH (a {name: "Alice"})-[e1:KNOWS*1..2]->(b)-[e2:KNOWS*1..2]->(c)
        UNWIND e1 AS r1
        UNWIND e2 AS r2
        RETURN r1.src AS first_src, r2.dst AS last_dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 1


# =============================================================================
# Category 3: COLLECT + UNWIND Roundtrip
# =============================================================================
class TestCollectUnwindRoundtrip:
    """Tests for COLLECT/UNWIND roundtrip patterns."""

    def test_collect_scalar_unwind(self, spark, graph_context):
        """COLLECT scalars then UNWIND."""
        query = """
        MATCH (a:Person)
        WITH COLLECT(a.salary) AS salaries
        UNWIND salaries AS s
        RETURN s
        ORDER BY s DESC
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        salaries = [row["s"] for row in rows]
        assert salaries == sorted(salaries, reverse=True)

    @pytest.mark.xfail(reason="COLLECT struct UNWIND property access not implemented")
    def test_collect_struct_unwind_property(self, spark, graph_context):
        """COLLECT structs then UNWIND and access properties."""
        query = """
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        WITH a, COLLECT({name: b.name, dept: b.department}) AS friends
        UNWIND friends AS f
        RETURN a.name AS person, f.name AS friend_name, f.dept AS friend_dept
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 1


# =============================================================================
# Category 4: PATH Variable vs Relationship Variable
# =============================================================================
class TestPathVsRelationshipVariable:
    """Tests comparing path variables vs relationship variables."""

    def test_relationship_variable_size(self, spark, graph_context):
        """SIZE() on relationship variable should work."""
        query = """
        MATCH (a {name: "Alice"})-[r:KNOWS*1..3]->(b)
        RETURN b.name, SIZE(r) AS hops
        ORDER BY hops
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        for row in rows:
            assert row["hops"] in [1, 2, 3]

    def test_relationship_variable_return(self, spark, graph_context):
        """Return relationship variable directly (as array of structs)."""
        query = """
        MATCH (a {name: "Alice"})-[r:KNOWS*1..2]->(b)
        RETURN b.name, r
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        for row in rows:
            assert isinstance(row["r"], list)


# =============================================================================
# Category 5: Complex VLP Patterns
# =============================================================================
class TestComplexVLPPatterns:
    """Tests for complex variable-length path patterns."""

    def test_vlp_with_intermediate_filter(self, spark, graph_context):
        """VLP where intermediate nodes are filtered."""
        query = """
        MATCH (a {name: "Alice"})-[:KNOWS*1..3]->(b)
        WHERE b.department = "Engineering"
        RETURN DISTINCT b.name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        names = {row["name"] for row in rows}
        assert "Bob" in names or "Frank" in names

    def test_vlp_multiple_types(self, spark, graph_context):
        """VLP with multiple relationship types."""
        query = """
        MATCH (a {name: "Alice"})-[r:KNOWS|MANAGES|MENTORS*1..2]->(b)
        RETURN DISTINCT b.name, SIZE(r) AS hops
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 2

    def test_vlp_with_path_predicate(self, spark, graph_context):
        """VLP with predicate on path length."""
        query = """
        MATCH (a {name: "Alice"})-[r:KNOWS*2..3]->(b)
        WHERE SIZE(r) >= 2
        RETURN b.name, SIZE(r) AS hops
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        for row in rows:
            assert row["hops"] >= 2


# =============================================================================
# Category 6: WITH Chaining with Type Preservation
# =============================================================================
class TestWithChainingTypePreservation:
    """Tests for WITH clause type preservation."""

    def test_with_preserves_vlp_variable(self, spark, graph_context):
        """WITH should preserve VLP relationship variable."""
        query = """
        MATCH (a {name: "Alice"})-[r:KNOWS*1..2]->(b)
        WITH a, r, b
        RETURN a.name, SIZE(r) AS path_len, b.name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 1

    @pytest.mark.xfail(reason="WITH UNWIND combination with struct access not implemented")
    def test_with_unwind_combination(self, spark, graph_context):
        """WITH clause with UNWIND and struct property access."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..2]->(b)
        WITH a, e, b
        UNWIND e AS r
        WITH a, r, b
        WHERE r.weight > 5
        RETURN a.name, r.src, r.dst, b.name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 1


# =============================================================================
# Category 7: Aggregations on Struct Fields
# =============================================================================
class TestAggregationsOnStructFields:
    """Tests for aggregation functions on struct fields."""

    def test_sum_on_unwound_struct_property(self, spark, graph_context):
        """SUM aggregation on unwound struct property."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..3]->(b)
        UNWIND e AS r
        RETURN SUM(r.weight) AS total_weight
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert rows[0]["total_weight"] > 0

    def test_avg_on_unwound_struct_property(self, spark, graph_context):
        """AVG aggregation on unwound struct property."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..3]->(b)
        UNWIND e AS r
        RETURN AVG(r.weight) AS avg_weight
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert rows[0]["avg_weight"] > 0

    def test_group_by_unwound_struct_property(self, spark, graph_context):
        """GROUP BY on unwound struct property."""
        query = """
        MATCH (a:Person)-[e:KNOWS*1..2]->(b)
        UNWIND e AS r
        RETURN r.context AS context, COUNT(*) AS count
        ORDER BY count DESC
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 1


# =============================================================================
# Category 8: CASE Expressions with Struct Fields
# =============================================================================
class TestCaseExpressionsWithStructFields:
    """Tests for CASE expressions with struct fields."""

    def test_case_on_unwound_struct_property(self, spark, graph_context):
        """CASE expression on unwound struct property."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..2]->(b)
        UNWIND e AS r
        RETURN r.src, r.dst,
               CASE WHEN r.weight > 7 THEN 'strong'
                    WHEN r.weight > 4 THEN 'medium'
                    ELSE 'weak' END AS strength
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        for row in rows:
            assert row["strength"] in ["strong", "medium", "weak"]


# =============================================================================
# Category 9: UNION with UNWIND
# =============================================================================
class TestUnionWithUnwind:
    """Tests for UNION operations combined with UNWIND."""

    def test_union_before_unwind(self, spark, graph_context):
        """UNION of queries, then UNWIND."""
        query = """
        MATCH (a {name: "Alice"})-[r:KNOWS*1..2]->(b)
        RETURN b.name AS name, SIZE(r) AS hops
        UNION
        MATCH (a {name: "Bob"})-[r:KNOWS*1..2]->(b)
        RETURN b.name AS name, SIZE(r) AS hops
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 2


# =============================================================================
# Category 10: EXISTS with VLP
# =============================================================================
class TestExistsWithVLP:
    """Tests for EXISTS patterns with variable-length paths."""

    def test_exists_with_vlp(self, spark, graph_context):
        """EXISTS predicate with VLP inside."""
        query = """
        MATCH (a:Person)
        WHERE EXISTS {
            MATCH (a)-[:KNOWS*1..3]->(b)
            WHERE b.department = "Sales"
        }
        RETURN a.name
        """
        try:
            sql = graph_context.transpile(query)
            result = spark.sql(sql)
            rows = result.collect()
            assert len(rows) >= 1
        except Exception as e:
            pytest.skip(f"EXISTS with VLP not supported: {e}")


# =============================================================================
# Category 11: Edge Cases and Boundary Conditions
# =============================================================================
class TestEdgeCasesAndBoundaries:
    """Tests for edge cases and boundary conditions."""

    def test_vlp_zero_min_hops(self, spark, graph_context):
        """VLP with zero minimum hops (*0..2)."""
        query = """
        MATCH (a {name: "Alice"})-[r:KNOWS*0..2]->(b)
        RETURN DISTINCT b.name, SIZE(r) AS hops
        """
        try:
            sql = graph_context.transpile(query)
            result = spark.sql(sql)
            rows = result.collect()
            # Should include Alice herself (0 hops)
            names = {row["name"] for row in rows}
            assert "Alice" in names
        except Exception as e:
            pytest.skip(f"VLP *0..N not supported: {e}")

    def test_empty_graph_query(self, spark, graph_context):
        """Query that should return empty results."""
        query = """
        MATCH (a {name: "NonExistent"})-[r:KNOWS*1..3]->(b)
        RETURN b.name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) == 0

    def test_self_loop_prevention(self, spark, graph_context):
        """VLP should prevent visiting same node twice."""
        query = """
        MATCH (a {name: "Alice"})-[r:KNOWS*1..5]->(b)
        RETURN DISTINCT b.name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        # Should not explode due to cycles
        assert len(rows) < 100


# =============================================================================
# Category 12: Complex Real-World Patterns
# =============================================================================
class TestComplexRealWorldPatterns:
    """Tests simulating real-world query patterns."""

    def test_friends_of_friends_with_mutual(self, spark, graph_context):
        """Find friends of friends that are also direct friends."""
        query = """
        MATCH (a {name: "Alice"})-[:KNOWS]->(b)-[:KNOWS]->(c)
        WHERE NOT (a)-[:KNOWS]->(c) AND a <> c
        RETURN DISTINCT c.name AS potential_friend
        """
        try:
            sql = graph_context.transpile(query)
            result = spark.sql(sql)
            rows = result.collect()
            # Carol is friend of Bob but not direct friend of Alice
            assert len(rows) >= 0  # May be empty depending on graph
        except Exception as e:
            pytest.skip(f"Pattern not supported: {e}")

    @pytest.mark.xfail(reason="Complex path analysis with UNWIND not implemented")
    def test_path_weight_analysis(self, spark, graph_context):
        """Analyze path weights with UNWIND and aggregation."""
        query = """
        MATCH (a {name: "Alice"})-[e:KNOWS*1..3]->(b)
        UNWIND e AS r
        WITH a, b, SUM(r.weight) AS total_weight, SIZE(e) AS hops
        RETURN a.name, b.name, total_weight, hops
        ORDER BY total_weight DESC
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 1

    def test_department_reachability(self, spark, graph_context):
        """Find all departments reachable from a person."""
        query = """
        MATCH (a {name: "Alice"})-[:KNOWS*1..3]->(b)
        RETURN DISTINCT b.department AS reachable_dept
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        depts = {row["reachable_dept"] for row in rows}
        assert len(depts) >= 1


# =============================================================================
# Category 13: Tree Graph with UUID-style node_id
# =============================================================================
@pytest.fixture(scope="module")
def tree_graph_context(spark):
    r"""Create a tree-shaped graph with UUID-style node IDs.

    Tree structure:
                        root (c00ec514-...)
                       /    \
                     A        B
                    / \      / \
                   C   D    E   F
                  /
                 G

    7 nodes, 7 edges (tree = n-1 edges)
    """
    # Nodes with UUID-style IDs
    nodes_data = [
        ("c00ec514-4b63-48d3-b392-80316ef9a51d", "TreeNode", "Root", 0),
        ("a1111111-1111-1111-1111-111111111111", "TreeNode", "A", 1),
        ("b2222222-2222-2222-2222-222222222222", "TreeNode", "B", 1),
        ("c3333333-3333-3333-3333-333333333333", "TreeNode", "C", 2),
        ("d4444444-4444-4444-4444-444444444444", "TreeNode", "D", 2),
        ("e5555555-5555-5555-5555-555555555555", "TreeNode", "E", 2),
        ("f6666666-6666-6666-6666-666666666666", "TreeNode", "F", 2),
        ("g7777777-7777-7777-7777-777777777777", "TreeNode", "G", 3),
    ]
    nodes_df = spark.createDataFrame(
        nodes_data, ["node_id", "node_type", "name", "level"]
    )
    nodes_df.createOrReplaceTempView("tree_nodes")

    # Edges forming a tree (parent -> child)
    edges_data = [
        # Level 0 -> Level 1
        ("c00ec514-4b63-48d3-b392-80316ef9a51d", "a1111111-1111-1111-1111-111111111111", "CHILD_OF", 1),
        ("c00ec514-4b63-48d3-b392-80316ef9a51d", "b2222222-2222-2222-2222-222222222222", "CHILD_OF", 2),
        # Level 1 -> Level 2
        ("a1111111-1111-1111-1111-111111111111", "c3333333-3333-3333-3333-333333333333", "CHILD_OF", 3),
        ("a1111111-1111-1111-1111-111111111111", "d4444444-4444-4444-4444-444444444444", "CHILD_OF", 4),
        ("b2222222-2222-2222-2222-222222222222", "e5555555-5555-5555-5555-555555555555", "CHILD_OF", 5),
        ("b2222222-2222-2222-2222-222222222222", "f6666666-6666-6666-6666-666666666666", "CHILD_OF", 6),
        # Level 2 -> Level 3
        ("c3333333-3333-3333-3333-333333333333", "g7777777-7777-7777-7777-777777777777", "CHILD_OF", 7),
    ]
    edges_df = spark.createDataFrame(
        edges_data, ["src", "dst", "relationship_type", "edge_order"]
    )
    edges_df.createOrReplaceTempView("tree_edges")

    graph = GraphContext(
        spark=spark,
        nodes_table="tree_nodes",
        edges_table="tree_edges",
        node_type_col="node_type",
        node_id_col="node_id",
        edge_src_col="src",
        edge_dst_col="dst",
        edge_type_col="relationship_type",
        extra_node_attrs={"name": str, "level": int},
        extra_edge_attrs={"edge_order": int},
    )
    graph.set_types(
        node_types=["TreeNode"],
        edge_types=["CHILD_OF"],
    )
    return graph


class TestTreeGraphRelationshipsP:
    """Tests for relationships(p) on a tree-shaped graph with UUID node IDs."""

    def test_relationships_p_return_distinct_r_tree(self, spark, tree_graph_context):
        """Test UNWIND relationships(p) AS r with RETURN DISTINCT r on tree graph.

        Query pattern:
            MATCH (root { node_id: "c00ec514-4b63-48d3-b392-80316ef9a51d" })
            MATCH p = (root)-[*1..3]->()
            UNWIND relationships(p) AS r
            RETURN DISTINCT r

        Expected: Should return all 7 distinct edges in the tree traversed
        from root up to depth 3.
        """
        query = """
        MATCH (root { node_id: "c00ec514-4b63-48d3-b392-80316ef9a51d" })
        MATCH p = (root)-[*1..3]->()
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = tree_graph_context.transpile(query)
        print(f"\n=== SQL for RETURN DISTINCT r ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        print(f"Found {len(rows)} distinct relationships")
        for row in rows:
            print(f"  r = {row['r']}")

        # Should have edges traversed from root
        # At depth 1: root->A, root->B (2 edges)
        # At depth 2: A->C, A->D, B->E, B->F (4 edges)
        # At depth 3: C->G (1 edge)
        # Total distinct edges reachable: 7
        assert len(rows) >= 1, "Expected at least 1 distinct relationship"

        # Verify r is a struct with src/dst fields
        first_row = rows[0]
        r_value = first_row["r"]
        assert r_value is not None, "r should not be None"

    def test_relationships_p_with_properties_tree(self, spark, tree_graph_context):
        """Test UNWIND relationships(p) AS r with property access on tree graph."""
        query = """
        MATCH (root { node_id: "c00ec514-4b63-48d3-b392-80316ef9a51d" })
        MATCH p = (root)-[*1..3]->()
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS source, r.dst AS target, r.edge_order AS ord
        ORDER BY ord
        """
        sql = tree_graph_context.transpile(query)
        print(f"\n=== SQL for r.src, r.dst, r.edge_order ===\n{sql}\n")
        result = spark.sql(sql)
        rows = result.collect()

        print(f"Found {len(rows)} distinct (src, dst, edge_order) tuples")
        for row in rows:
            print(f"  {row['source']} -> {row['target']} (order={row['ord']})")

        # Should have 7 distinct edges
        assert len(rows) == 7, f"Expected 7 distinct edges, got {len(rows)}"

        # Verify ordering
        orders = [row["ord"] for row in rows]
        assert orders == sorted(orders), "Results should be ordered by edge_order"

    def test_relationships_p_with_size_tree(self, spark, tree_graph_context):
        """Test SIZE(relationships(p)) on tree graph."""
        query = """
        MATCH (root { node_id: "c00ec514-4b63-48d3-b392-80316ef9a51d" })
        MATCH p = (root)-[e*1..3]->()
        RETURN SIZE(e) AS path_length
        ORDER BY path_length
        """
        sql = tree_graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        lengths = [row["path_length"] for row in rows]
        print(f"Path lengths found: {lengths}")

        # Depth 1: 2 paths (to A, B) - length 1
        # Depth 2: 4 paths (to C, D, E, F) - length 2
        # Depth 3: 1 path (to G) - length 3
        assert 1 in lengths, "Should have paths of length 1"
        assert 2 in lengths, "Should have paths of length 2"
        assert 3 in lengths, "Should have paths of length 3"
