"""
Tests for list predicates (none/all/any/single) with relationships(p) and property access.

Bug: When using functions like coalesce() on properties of the loop variable inside
list predicates, the renderer incorrectly resolves them as schema columns instead of
lambda variable field access.

Example failing query:
    MATCH p = (root)-[*1..2]-()
    WHERE none(rel IN relationships(p) WHERE coalesce(rel.weight, 0) > 5)
    RETURN root.name

Error: _gsql2rsql_rel_weight cannot be resolved.

Graph Structure (simple chain with properties):

    Alice --KNOWS(weight=10, status='verified')--> Bob
      |                                              |
      |                                              v
      +--KNOWS(weight=3, status='flagged')-----> Carol
                                                    |
                                                    v
                              KNOWS(weight=7, status='verified')
                                                    |
                                                    v
                                                  Dave --KNOWS(weight=1, status='flagged')--> Eve

    Also:
    Alice --WORKS_WITH(weight=5, status='pending')--> Frank

Edges summary:
    Alice->Bob:   KNOWS,      weight=10, status='verified'
    Alice->Carol: KNOWS,      weight=3,  status='flagged'
    Alice->Frank: WORKS_WITH, weight=5,  status='pending'
    Bob->Carol:   KNOWS,      weight=7,  status='verified'
    Carol->Dave:  KNOWS,      weight=2,  status='flagged'
    Dave->Eve:    KNOWS,      weight=1,  status='flagged'
"""

import pytest
from pyspark.sql import SparkSession
from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("ListPredicate_RelProperty_Tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def graph_context(spark):
    """Create test graph with edge properties for filtering."""
    nodes_data = [
        ("alice", "Person", "Alice"),
        ("bob", "Person", "Bob"),
        ("carol", "Person", "Carol"),
        ("dave", "Person", "Dave"),
        ("eve", "Person", "Eve"),
        ("frank", "Person", "Frank"),
    ]
    nodes_df = spark.createDataFrame(
        nodes_data, ["node_id", "node_type", "name"]
    )
    nodes_df.createOrReplaceTempView("nodes")

    edges_data = [
        ("alice", "bob", "KNOWS", 10, "verified"),
        ("alice", "carol", "KNOWS", 3, "flagged"),
        ("alice", "frank", "WORKS_WITH", 5, "pending"),
        ("bob", "carol", "KNOWS", 7, "verified"),
        ("carol", "dave", "KNOWS", 2, "flagged"),
        ("dave", "eve", "KNOWS", 1, "flagged"),
    ]
    edges_df = spark.createDataFrame(
        edges_data, ["src", "dst", "relationship_type", "weight", "status"]
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
        extra_node_attrs={"name": str},
        extra_edge_attrs={"weight": int, "status": str},
    )
    graph.set_types(
        node_types=["Person"],
        edge_types=["KNOWS", "WORKS_WITH"],
    )
    return graph


# =============================================================================
# 1. none() with relationships(p) - The original bug
# =============================================================================
class TestNonePredicateRelProperty:
    """Tests for none(rel IN relationships(p) WHERE <prop_access>)."""

    def test_none_with_simple_property_comparison(self, spark, graph_context):
        """none(rel IN relationships(p) WHERE rel.status = 'flagged')

        From Alice with depth 1..2:
        Paths from Alice:
          Alice->Bob (status=verified)            → none flagged? YES
          Alice->Carol (status=flagged)            → none flagged? NO
          Alice->Bob->Carol (verified, verified)   → none flagged? YES
          Alice->Carol->Dave (flagged, flagged)    → none flagged? NO

        Expected destinations with no flagged edges: {Bob, Carol(via Bob)}
        But Carol appears twice (once via flagged, once not), so DISTINCT matters.
        Using DISTINCT on destination names: {Bob, Carol}
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        RETURN DISTINCT dest.name AS name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none + simple property) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = {row["name"] for row in rows}
        # Only paths with NO flagged edges:
        # Alice->Bob (verified) ✓
        # Alice->Bob->Carol (verified, verified) ✓
        assert names == {"Bob", "Carol"}, f"Expected {{Bob, Carol}}, got {names}"

    def test_none_with_coalesce_property(self, spark, graph_context):
        """none(rel IN relationships(p) WHERE coalesce(rel.status, '') = 'flagged')

        This is the exact pattern from the user's bug report.
        coalesce wraps the property access - the renderer must handle this.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE coalesce(rel.status, '') = 'flagged')
        RETURN DISTINCT dest.name AS name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none + coalesce) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = {row["name"] for row in rows}
        assert names == {"Bob", "Carol"}, f"Expected {{Bob, Carol}}, got {names}"

    def test_none_with_numeric_property_comparison(self, spark, graph_context):
        """none(rel IN relationships(p) WHERE rel.weight > 5)

        From Alice with depth 1..2:
          Alice->Bob (w=10)                      → has >5? YES → none? NO
          Alice->Carol (w=3)                      → has >5? NO  → none? YES
          Alice->Bob->Carol (w=10, w=7)           → has >5? YES → none? NO
          Alice->Carol->Dave (w=3, w=2)           → has >5? NO  → none? YES

        Destinations with none(weight > 5): Carol (direct), Dave (via Carol)
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.weight > 5)
        RETURN DISTINCT dest.name AS name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none + numeric) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = {row["name"] for row in rows}
        assert names == {"Carol", "Dave"}, f"Expected {{Carol, Dave}}, got {names}"


# =============================================================================
# 2. all() with relationships(p)
# =============================================================================
class TestAllPredicateRelProperty:
    """Tests for all(rel IN relationships(p) WHERE <prop_access>)."""

    def test_all_with_simple_property(self, spark, graph_context):
        """all(rel IN relationships(p) WHERE rel.status = 'verified')

        From Alice depth 1..2:
          Alice->Bob (verified)                    → all verified? YES
          Alice->Carol (flagged)                   → all verified? NO
          Alice->Bob->Carol (verified, verified)   → all verified? YES
          Alice->Carol->Dave (flagged, flagged)    → all verified? NO

        Destinations where ALL edges are verified: {Bob, Carol(via Bob)}
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE all(rel IN relationships(p) WHERE rel.status = 'verified')
        RETURN DISTINCT dest.name AS name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (all + simple property) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = {row["name"] for row in rows}
        assert names == {"Bob", "Carol"}, f"Expected {{Bob, Carol}}, got {names}"

    def test_all_with_coalesce_property(self, spark, graph_context):
        """all(rel IN relationships(p) WHERE coalesce(rel.weight, 0) > 0)

        All edges have weight > 0 in our test data, so all paths pass.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE all(rel IN relationships(p) WHERE coalesce(rel.weight, 0) > 0)
        RETURN DISTINCT dest.name AS name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (all + coalesce) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = {row["name"] for row in rows}
        # All edges have weight > 0, so all paths pass
        # Depth 1: Bob, Carol; Depth 2: Carol(via Bob), Dave(via Carol)
        assert names == {"Bob", "Carol", "Dave"}, f"Expected {{Bob, Carol, Dave}}, got {names}"


# =============================================================================
# 3. any() with relationships(p)
# =============================================================================
class TestAnyPredicateRelProperty:
    """Tests for any(rel IN relationships(p) WHERE <prop_access>)."""

    def test_any_with_simple_property(self, spark, graph_context):
        """any(rel IN relationships(p) WHERE rel.status = 'flagged')

        From Alice depth 1..2:
          Alice->Bob (verified)                    → any flagged? NO
          Alice->Carol (flagged)                   → any flagged? YES
          Alice->Bob->Carol (verified, verified)   → any flagged? NO
          Alice->Carol->Dave (flagged, flagged)    → any flagged? YES

        Destinations where ANY edge is flagged: {Carol(direct), Dave(via Carol)}
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE any(rel IN relationships(p) WHERE rel.status = 'flagged')
        RETURN DISTINCT dest.name AS name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (any + simple property) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = {row["name"] for row in rows}
        assert names == {"Carol", "Dave"}, f"Expected {{Carol, Dave}}, got {names}"

    def test_any_with_function_wrapped_property(self, spark, graph_context):
        """any(rel IN relationships(p) WHERE coalesce(rel.weight, 0) >= 10)

        From Alice depth 1..2:
          Alice->Bob (w=10)                      → any >=10? YES
          Alice->Carol (w=3)                     → any >=10? NO
          Alice->Bob->Carol (w=10, w=7)          → any >=10? YES
          Alice->Carol->Dave (w=3, w=2)          → any >=10? NO
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE any(rel IN relationships(p) WHERE coalesce(rel.weight, 0) >= 10)
        RETURN DISTINCT dest.name AS name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (any + coalesce numeric) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = {row["name"] for row in rows}
        assert names == {"Bob", "Carol"}, f"Expected {{Bob, Carol}}, got {names}"


# =============================================================================
# 4. single() with relationships(p)
# =============================================================================
class TestSinglePredicateRelProperty:
    """Tests for single(rel IN relationships(p) WHERE <prop_access>)."""

    def test_single_with_property(self, spark, graph_context):
        """single(rel IN relationships(p) WHERE rel.status = 'verified')

        From Alice depth 1..2:
          Alice->Bob (verified)                    → exactly 1 verified? YES
          Alice->Carol (flagged)                   → exactly 1 verified? NO (0)
          Alice->Bob->Carol (verified, verified)   → exactly 1 verified? NO (2)
          Alice->Carol->Dave (flagged, flagged)    → exactly 1 verified? NO (0)
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE single(rel IN relationships(p) WHERE rel.status = 'verified')
        RETURN DISTINCT dest.name AS name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (single + property) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = {row["name"] for row in rows}
        # Only Alice->Bob has exactly 1 verified edge
        assert names == {"Bob"}, f"Expected {{Bob}}, got {names}"


# =============================================================================
# 5. Multiple property accesses in one predicate
# =============================================================================
class TestMultiplePropertyAccess:
    """Tests with multiple property accesses in a single predicate."""

    def test_none_with_and_condition(self, spark, graph_context):
        """none(rel IN relationships(p) WHERE rel.status = 'flagged' AND rel.weight < 5)

        From Alice depth 1..2:
          Alice->Bob (verified, w=10)              → flagged AND w<5? NO
          Alice->Carol (flagged, w=3)              → flagged AND w<5? YES → none? NO
          Alice->Bob->Carol (ver/w=10, ver/w=7)    → none match? → none? YES
          Alice->Carol->Dave (flag/w=3, flag/w=2)  → both match → none? NO
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged' AND rel.weight < 5)
        RETURN DISTINCT dest.name AS name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none + AND multiple props) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = {row["name"] for row in rows}
        # Alice->Bob: no edge is (flagged AND w<5) → passes
        # Alice->Bob->Carol: no edge is (flagged AND w<5) → passes
        assert names == {"Bob", "Carol"}, f"Expected {{Bob, Carol}}, got {names}"


# =============================================================================
# 6. Combined: list predicate + UNWIND in same query (exact user pattern)
# =============================================================================
class TestCombinedPredicateAndUnwind:
    """The exact pattern from the user's bug report: predicate + UNWIND."""

    def test_none_predicate_then_unwind(self, spark, graph_context):
        """Exact user pattern: filter paths with none(), then UNWIND relationships.

        MATCH (root {name: "Alice"})
        MATCH p = (root)-[*1..2]->()
        WHERE none(rel IN relationships(p) WHERE coalesce(rel.status, '') = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE coalesce(rel.status, '') = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none + UNWIND combined) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        edges = {(row["src"], row["dst"]) for row in rows}
        # Paths with no flagged edges (untyped [*1..2] matches ALL edge types):
        #   Alice->Bob (KNOWS, verified) ✓
        #   Alice->Frank (WORKS_WITH, pending) ✓
        #   Alice->Bob->Carol (KNOWS verified, KNOWS verified) ✓
        # Distinct edges from those paths:
        assert edges == {
            ("alice", "bob"),
            ("alice", "frank"),
            ("bob", "carol"),
        }, f"Expected alice->bob, alice->frank, bob->carol, got {edges}"

    def test_all_predicate_then_unwind_with_aggregation(self, spark, graph_context):
        """Filter paths where all edges are verified, then count distinct edges."""
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE all(rel IN relationships(p) WHERE rel.status = 'verified')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.weight AS weight
        ORDER BY weight DESC
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (all + UNWIND + ORDER BY) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        # Same paths as above: Alice->Bob, Alice->Bob->Carol
        # Edges: (alice,bob,10), (bob,carol,7)
        assert len(rows) == 2
        assert rows[0]["weight"] == 10  # alice->bob
        assert rows[1]["weight"] == 7  # bob->carol


# =============================================================================
# 7. List comprehension with relationships(p) property access (related pattern)
# =============================================================================
class TestListComprehensionRelProperty:
    """Tests for [rel IN relationships(p) WHERE rel.prop ... | rel.prop]."""

    def test_list_comprehension_filter_and_map(self, spark, graph_context):
        """[rel IN relationships(p) WHERE rel.weight > 5 | rel.weight]

        From Alice depth 1..2:
          Alice->Bob (w=10)                      → [10]
          Alice->Carol (w=3)                     → []
          Alice->Bob->Carol (w=10, w=7)          → [10, 7]
          Alice->Carol->Dave (w=3, w=2)          → []
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        RETURN dest.name AS name,
               [rel IN relationships(p) WHERE rel.weight > 5 | rel.weight] AS heavy_weights
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (list comprehension + property) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        results = {row["name"]: sorted(row["heavy_weights"]) for row in rows}
        # Bob (via Alice->Bob): [10]
        # Carol (via Alice->Carol): []
        # Carol (via Alice->Bob->Carol): [7, 10]  (two rows for Carol)
        # Dave (via Alice->Carol->Dave): []
        bob_weights = [r["heavy_weights"] for r in rows if r["name"] == "Bob"]
        assert any(sorted(w) == [10] for w in bob_weights), f"Bob should have [10], got {bob_weights}"


# =============================================================================
# 8. UNWIND of list comprehension with property filter (user's exact pattern)
#    UNWIND [r IN relationships(p) WHERE r.prop <> 'x'] AS r
#    RETURN DISTINCT r
# =============================================================================
class TestUnwindListComprehensionWithFilter:
    """Tests for UNWIND [r IN relationships(p) WHERE r.prop ...] AS r.

    This combines:
    - List comprehension with property access on loop variable
    - UNWIND of the filtered result
    - Access to the unwound struct (r.src, r.dst, etc.)

    This is the exact pattern from the user's query:
        UNWIND [r IN relationships(p) WHERE r.fraud_classification <> 'Não Fraude'] AS r
        RETURN DISTINCT r
    """

    def test_unwind_filtered_list_comprehension_return_struct_fields(self, spark, graph_context):
        """UNWIND [r IN relationships(p) WHERE r.status <> 'flagged'] AS r
        RETURN DISTINCT r.src, r.dst

        From Alice depth 1..2 (KNOWS only):
        Paths and their edges:
          Alice->Bob: edges=[(alice,bob,verified)]
            filtered (status<>'flagged'): [(alice,bob,verified)]
          Alice->Carol: edges=[(alice,carol,flagged)]
            filtered: [] (removed)
          Alice->Bob->Carol: edges=[(alice,bob,verified),(bob,carol,verified)]
            filtered: [(alice,bob,verified),(bob,carol,verified)]
          Alice->Carol->Dave: edges=[(alice,carol,flagged),(carol,dave,flagged)]
            filtered: [] (both removed)

        After UNWIND + DISTINCT: (alice,bob), (bob,carol)
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [r IN relationships(p) WHERE r.status <> 'flagged'] AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (UNWIND list comprehension + struct fields) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        edges = {(row["src"], row["dst"]) for row in rows}
        assert edges == {
            ("alice", "bob"),
            ("bob", "carol"),
        }, f"Expected (alice,bob),(bob,carol), got {edges}"

    def test_unwind_filtered_list_comprehension_with_coalesce(self, spark, graph_context):
        """UNWIND [r IN relationships(p) WHERE coalesce(r.status, '') <> 'flagged'] AS r

        Same as above but with coalesce wrapping the property - the original bug pattern.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [r IN relationships(p) WHERE coalesce(r.status, '') <> 'flagged'] AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (UNWIND list comprehension + coalesce) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        edges = {(row["src"], row["dst"]) for row in rows}
        assert edges == {
            ("alice", "bob"),
            ("bob", "carol"),
        }, f"Expected (alice,bob),(bob,carol), got {edges}"

    def test_unwind_filtered_list_comprehension_numeric_filter(self, spark, graph_context):
        """UNWIND [r IN relationships(p) WHERE r.weight > 5] AS r

        From Alice depth 1..2 (KNOWS):
          Alice->Bob: [(alice,bob,w=10)] → filtered: [(alice,bob,w=10)]
          Alice->Carol: [(alice,carol,w=3)] → filtered: []
          Alice->Bob->Carol: [(alice,bob,w=10),(bob,carol,w=7)] → both pass
          Alice->Carol->Dave: [(alice,carol,w=3),(carol,dave,w=2)] → both fail

        After UNWIND + DISTINCT: (alice,bob,10), (bob,carol,7)
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [r IN relationships(p) WHERE r.weight > 5] AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.weight AS weight
        ORDER BY weight DESC
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (UNWIND list comprehension + numeric) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        edges = {(row["src"], row["dst"], row["weight"]) for row in rows}
        assert edges == {
            ("alice", "bob", 10),
            ("bob", "carol", 7),
        }, f"Expected (alice,bob,10),(bob,carol,7), got {edges}"

    def test_unwind_filtered_then_all_struct_properties(self, spark, graph_context):
        """Access ALL struct properties after UNWIND filtered list."""
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [rel IN relationships(p) WHERE rel.status <> 'flagged'] AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.weight AS weight, r.status AS status
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        edges = {(row["src"], row["dst"]) for row in rows}
        assert edges == {("alice", "bob"), ("bob", "carol")}
        for row in rows:
            assert row["status"] != "flagged", f"Got flagged edge: {row}"
            assert row["weight"] > 0


# =============================================================================
# 9. Bug 1: list predicates with WITH clauses and complex downstream patterns
# =============================================================================
class TestListPredicateWithComplexDownstream:
    """Tests for none/all/any predicates combined with WITH, aggregation, ORDER BY.

    These test that the lambda property access fix works when the predicate
    appears after WITH clauses and is combined with other downstream operations.
    """

    def test_none_coalesce_with_intermediate_WITH(self, spark, graph_context):
        """none() with coalesce after a WITH clause that passes p through.

        WITH root, p, dest
        WHERE none(rel IN relationships(p) WHERE coalesce(rel.status, '') = 'flagged')
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WITH root, p, dest
        WHERE none(rel IN relationships(p) WHERE coalesce(rel.status, '') = 'flagged')
        RETURN DISTINCT dest.name AS name
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        # Paths with NO flagged edges: Alice->Bob (verified), Alice->Bob->Carol (ver,ver)
        assert names == ["Bob", "Carol"]

    def test_any_with_WITH_and_path_length(self, spark, graph_context):
        """any() after WITH, returning path metadata alongside filtered results.

        WITH root, p, dest
        WHERE any(rel IN relationships(p) WHERE rel.weight >= 10)
        RETURN dest.name, size(relationships(p)) AS path_len
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WITH root, p, dest
        WHERE any(rel IN relationships(p) WHERE rel.weight >= 10)
        RETURN dest.name AS name, size(relationships(p)) AS path_len
        ORDER BY path_len
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        results = [(row["name"], row["path_len"]) for row in rows]
        # Alice->Bob has weight=10 (>=10) → path_len=1
        # Alice->Bob->Carol has weight=10 on first hop (>=10) → path_len=2
        assert results == [("Bob", 1), ("Carol", 2)]

    def test_all_with_WITH_WHERE_and_length(self, spark, graph_context):
        """all() inside WITH ... WHERE combined with length() on path.

        WITH root, dest, p, length(p) AS hops
        WHERE all(rel IN relationships(p) WHERE rel.status = 'verified')
        RETURN dest.name, hops
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WITH root, dest, p, length(p) AS hops
        WHERE all(rel IN relationships(p) WHERE rel.status = 'verified')
        RETURN dest.name AS name, hops
        ORDER BY hops DESC
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        results = [(row["name"], row["hops"]) for row in rows]
        # All-verified paths: Alice->Bob (1 hop), Alice->Bob->Carol (2 hops)
        assert results == [("Carol", 2), ("Bob", 1)]


# =============================================================================
# 10. Bug 2: UNWIND list comprehension + WITH, aggregation, ORDER BY
# =============================================================================
class TestUnwindListComprehensionComplex:
    """Tests for UNWIND [r IN relationships(p) WHERE ...] AS r with complex
    downstream operations: WITH, SUM, COUNT, COLLECT, ORDER BY, WHERE on struct.
    """

    def test_unwind_listcomp_with_aggregation(self, spark, graph_context):
        """UNWIND filtered list + SUM/COUNT aggregation per destination.

        Paths from Alice (KNOWS, depth 1..2), filtering out flagged edges:
          Alice->Bob: edges after filter = [(a,b,w=10,verified)]
          Alice->Carol: edges after filter = [] (flagged removed, UNWIND produces 0 rows)
          Alice->Bob->Carol: edges after filter = [(a,b,w=10,ver),(b,c,w=7,ver)]
          Alice->Carol->Dave: edges after filter = [] (both flagged)

        Aggregation by dest_name:
          Bob:   from path Alice->Bob → 1 edge, total_weight=10
          Carol: from path Alice->Bob->Carol → 2 edges, total_weight=17
          (Carol via direct Alice->Carol produces 0 UNWIND rows → no contribution)
          (Dave produces 0 rows)
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [rel IN relationships(p) WHERE rel.status <> 'flagged'] AS r
        WITH dest.name AS dest_name, SUM(r.weight) AS total_weight, COUNT(r) AS edge_count
        RETURN dest_name, total_weight, edge_count
        ORDER BY total_weight DESC
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        results = [(row["dest_name"], row["total_weight"], row["edge_count"]) for row in rows]
        assert results == [
            ("Carol", 17, 2),  # Alice->Bob->Carol: sum(10+7)=17, count=2
            ("Bob", 10, 1),    # Alice->Bob: sum(10)=10, count=1
        ]

    def test_unwind_listcomp_with_where_on_struct_property(self, spark, graph_context):
        """UNWIND filtered list + WITH r, dest + WHERE on struct property.

        Filter verified edges, then further filter where weight >= 7.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [rel IN relationships(p) WHERE rel.status = 'verified'] AS r
        WITH r, dest
        WHERE r.weight >= 7
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.weight AS weight
        ORDER BY weight DESC
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        results = [(row["src"], row["dst"], row["weight"]) for row in rows]
        # Verified edges: (alice,bob,10), (bob,carol,7)
        # Both have weight >= 7
        assert results == [("alice", "bob", 10), ("bob", "carol", 7)]

    def test_unwind_listcomp_collect_weights(self, spark, graph_context):
        """UNWIND filtered list + COLLECT to aggregate edge weights per destination.

        COLLECT(r.weight) groups the unwound edge weights by destination.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [rel IN relationships(p) WHERE rel.weight > 5] AS r
        RETURN dest.name AS dest_name, COLLECT(r.weight) AS weights
        ORDER BY dest_name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dest_name"]: sorted(row["weights"]) for row in rows}
        # Alice->Bob: [10] (weight=10 > 5)
        # Alice->Bob->Carol: [10, 7] (both > 5)
        # Alice->Carol: [] (weight=3, filtered out → no UNWIND rows)
        # Alice->Carol->Dave: [] (weight=3,2 both filtered → no UNWIND rows)
        assert results == {
            "Bob": [10],
            "Carol": [7, 10],
        }

    def test_unwind_listcomp_coalesce_filter_orderby_struct(self, spark, graph_context):
        """UNWIND with coalesce in filter + ORDER BY on struct property.

        This is the exact user bug pattern: coalesce() in list comprehension filter
        + ORDER BY on unwound struct field.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [rel IN relationships(p) WHERE coalesce(rel.status, '') <> 'flagged'] AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.weight AS weight
        ORDER BY r.weight DESC
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        results = [(row["src"], row["dst"], row["weight"]) for row in rows]
        assert results == [("alice", "bob", 10), ("bob", "carol", 7)]

    def test_unwind_listcomp_with_multiple_with_clauses(self, spark, graph_context):
        """UNWIND list comp piped through multiple WITH clauses.

        WITH root, p, dest → UNWIND → WITH r, dest → WITH dest_name, weight → RETURN
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WITH root, p, dest
        UNWIND [rel IN relationships(p) WHERE rel.status = 'verified'] AS r
        WITH dest.name AS dest_name, r.weight AS weight
        RETURN dest_name, weight
        ORDER BY weight DESC
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        results = [(row["dest_name"], row["weight"]) for row in rows]
        # Verified edges from Alice (depth 1..2):
        #   Alice->Bob (w=10): dest=Bob
        #   Alice->Bob->Carol (w=10 + w=7): unwound → (Carol,10) and (Carol,7)
        # Note: Carol as 1-hop dest has flagged edge, so no UNWIND rows there
        assert ("Bob", 10) in results
        assert ("Carol", 10) in results
        assert ("Carol", 7) in results
        assert len(results) == 3


# =============================================================================
# 11. IN, OR, NOT operators inside list predicates and list comprehensions
# =============================================================================
class TestListPredicateOperatorVariety:
    """Tests for various Cypher operators inside list predicate/comprehension filters.

    Operators tested:
      - IN [...] (membership)
      - OR (logical disjunction)
      - NOT (negation)
      - AND + OR combined
      - IN + coalesce combined
    """

    def test_none_with_IN_operator(self, spark, graph_context):
        """none(rel IN relationships(p) WHERE rel.status IN ['flagged', 'pending'])

        From Alice (KNOWS, depth 1..2):
          Alice->Bob: status=verified, not in list → passes none
          Alice->Carol: status=flagged, in list → fails none
          Alice->Bob->Carol: both verified → passes none
          Alice->Carol->Dave: flagged, flagged → fails none

        Only paths where NO edge has status in ['flagged','pending']:
        Alice->Bob and Alice->Bob->Carol → dest = {Bob, Carol}
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status IN ['flagged', 'pending'])
        RETURN DISTINCT dest.name AS name
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Bob", "Carol"]

    def test_any_with_OR_operator(self, spark, graph_context):
        """any(rel IN ... WHERE rel.status = 'flagged' OR rel.weight > 8)

        From Alice (KNOWS, depth 1..2):
          Alice->Bob (verified, w=10): w>8 → YES → any=YES
          Alice->Carol (flagged, w=3): flagged → YES → any=YES
          Alice->Bob->Carol (ver/w=10, ver/w=7): w=10>8 → YES
          Alice->Carol->Dave (flag/w=3, flag/w=2): flagged → YES

        All paths have at least one matching edge → all dests: {Bob, Carol, Dave}
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE any(rel IN relationships(p) WHERE rel.status = 'flagged' OR rel.weight > 8)
        RETURN DISTINCT dest.name AS name
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Bob", "Carol", "Dave"]

    def test_none_with_NOT_operator(self, spark, graph_context):
        """none(rel IN relationships(p) WHERE NOT rel.status = 'verified')

        Equivalent to: all edges must be verified.
        Same as all(rel WHERE rel.status = 'verified').

        Alice->Bob (verified): NOT verified=False → none(False)=YES
        Alice->Carol (flagged): NOT verified=True → none=NO
        Alice->Bob->Carol (ver,ver): none(False,False)=YES
        Alice->Carol->Dave (flag,flag): none=NO

        Dests: {Bob, Carol}
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE NOT rel.status = 'verified')
        RETURN DISTINCT dest.name AS name
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Bob", "Carol"]

    def test_any_with_OR_and_coalesce_combined(self, spark, graph_context):
        """any(rel IN ... WHERE coalesce(rel.status, '') = 'flagged' OR rel.weight > 8)

        Combines coalesce (function) with OR (operator) inside any().
        Same results as test_any_with_OR_operator since data has no NULLs.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE any(rel IN relationships(p) WHERE coalesce(rel.status, '') = 'flagged' OR rel.weight > 8)
        RETURN DISTINCT dest.name AS name
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Bob", "Carol", "Dave"]

    def test_none_with_IN_and_coalesce_combined(self, spark, graph_context):
        """none(rel IN ... WHERE coalesce(rel.status, 'unknown') IN ['flagged', 'unknown'])

        Combines coalesce (function) with IN (operator) inside none().
        Same results as test_none_with_IN_operator.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE coalesce(rel.status, 'unknown') IN ['flagged', 'unknown'])
        RETURN DISTINCT dest.name AS name
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Bob", "Carol"]

    def test_all_with_AND_and_OR_combined(self, spark, graph_context):
        """all(rel IN ... WHERE (rel.status = 'verified' AND rel.weight > 0) OR rel.weight >= 10)

        From Alice (KNOWS, depth 1..2):
          Alice->Bob (ver, w=10): (ver AND w>0)=T OR w>=10=T → T
          Alice->Carol (flag, w=3): (flag AND w>0)=F OR w>=10=F → F
          Alice->Bob->Carol:
            (ver,w=10): T, (ver,w=7): (ver AND w>0)=T → all=YES
          Alice->Carol->Dave:
            (flag,w=3): F, (flag,w=2): F → all=NO

        Dests where all edges match: {Bob, Carol(via Bob)}
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE all(rel IN relationships(p) WHERE (rel.status = 'verified' AND rel.weight > 0) OR rel.weight >= 10)
        RETURN DISTINCT dest.name AS name
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Bob", "Carol"]


# =============================================================================
# 12. IN, OR, NOT inside UNWIND list comprehension filters
# =============================================================================
class TestUnwindListComprehensionOperatorVariety:
    """Tests for IN, OR, NOT operators inside UNWIND [r IN ... WHERE ...] AS r."""

    def test_unwind_listcomp_with_IN_operator(self, spark, graph_context):
        """UNWIND [rel IN relationships(p) WHERE rel.status IN ['verified']] AS r

        Only keeps edges with status='verified'.
        From Alice (KNOWS, depth 1..2):
          All verified edges: (alice,bob,10), (bob,carol,7)
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [rel IN relationships(p) WHERE rel.status IN ['verified']] AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        ORDER BY src, dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        edges = [(row["src"], row["dst"]) for row in rows]
        assert edges == [("alice", "bob"), ("bob", "carol")]

    def test_unwind_listcomp_with_OR_operator(self, spark, graph_context):
        """UNWIND [rel IN ... WHERE rel.status = 'flagged' OR rel.weight >= 10] AS r

        Keeps flagged edges and heavy edges.
        From Alice (KNOWS, depth 1..2):
          Alice->Bob: ver, w=10 → w>=10=YES
          Alice->Carol: flagged → YES
          Alice->Bob->Carol: (a,b,ver,w=10)→YES, (b,c,ver,w=7)→NO
          Alice->Carol->Dave: (a,c,flag)→YES, (c,d,flag)→YES

        Distinct edges: (alice,bob), (alice,carol), (carol,dave)
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [rel IN relationships(p) WHERE rel.status = 'flagged' OR rel.weight >= 10] AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.weight AS weight
        ORDER BY src, dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        edges = {(row["src"], row["dst"]) for row in rows}
        assert ("alice", "bob") in edges  # weight >= 10
        assert ("alice", "carol") in edges  # flagged
        assert ("carol", "dave") in edges  # flagged

    def test_unwind_listcomp_with_NOT_operator(self, spark, graph_context):
        """UNWIND [rel IN ... WHERE NOT rel.status = 'flagged'] AS r

        Same as status <> 'flagged'. Keep only verified edges.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [rel IN relationships(p) WHERE NOT rel.status = 'flagged'] AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        ORDER BY src, dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        edges = [(row["src"], row["dst"]) for row in rows]
        assert edges == [("alice", "bob"), ("bob", "carol")]

    def test_unwind_listcomp_IN_with_aggregation(self, spark, graph_context):
        """UNWIND filtered by IN + GROUP BY + SUM aggregation.

        Keep only verified edges, then sum weights per destination.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [rel IN relationships(p) WHERE rel.status IN ['verified']] AS r
        WITH dest.name AS dest_name, SUM(r.weight) AS total_weight
        RETURN dest_name, total_weight
        ORDER BY total_weight DESC
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        results = [(row["dest_name"], row["total_weight"]) for row in rows]
        # Bob: path Alice->Bob has 1 verified edge (w=10) → sum=10
        # Carol: path Alice->Bob->Carol has 2 verified edges (w=10+7) → sum=17
        assert results == [("Carol", 17), ("Bob", 10)]

    def test_unwind_listcomp_OR_with_WITH_WHERE(self, spark, graph_context):
        """UNWIND filtered by OR + WITH + WHERE on struct property.

        Keep flagged or heavy edges, then filter to only heavy ones via WITH WHERE.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [rel IN relationships(p) WHERE rel.status = 'flagged' OR rel.weight >= 7] AS r
        WITH r, dest
        WHERE r.weight >= 7
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.weight AS weight
        ORDER BY weight DESC
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        results = [(row["src"], row["dst"], row["weight"]) for row in rows]
        assert results == [("alice", "bob", 10), ("bob", "carol", 7)]


# =============================================================================
# 13. STARTS WITH / ENDS WITH / CONTAINS in regular WHERE
# =============================================================================
class TestStringPredicateOperators:
    """Tests for openCypher string predicate operators: STARTS WITH, ENDS WITH, CONTAINS.

    These are core openCypher operators defined in oC_StringPredicateExpression.
    They should work in regular WHERE clauses, inside list predicates, and
    inside list comprehensions.
    """

    def test_starts_with_basic(self, spark, graph_context):
        """WHERE n.name STARTS WITH 'Al' — basic string prefix match."""
        query = """
        MATCH (n:Person)
        WHERE n.name STARTS WITH 'Al'
        RETURN n.name AS name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (STARTS WITH basic) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Alice"]

    def test_ends_with_basic(self, spark, graph_context):
        """WHERE n.name ENDS WITH 'ob' — basic string suffix match."""
        query = """
        MATCH (n:Person)
        WHERE n.name ENDS WITH 'ob'
        RETURN n.name AS name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (ENDS WITH basic) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Bob"]

    def test_contains_basic(self, spark, graph_context):
        """WHERE n.name CONTAINS 'ar' — basic string containment."""
        query = """
        MATCH (n:Person)
        WHERE n.name CONTAINS 'ar'
        RETURN n.name AS name
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (CONTAINS basic) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Carol"]

    def test_starts_with_in_combined_where(self, spark, graph_context):
        """STARTS WITH combined with AND in WHERE clause."""
        query = """
        MATCH (s:Person)-[e:KNOWS]->(t:Person)
        WHERE s.name STARTS WITH 'Al' AND e.status = 'verified'
        RETURN t.name AS name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Bob"]


# =============================================================================
# 14. STARTS WITH / ENDS WITH / CONTAINS inside list predicates
# =============================================================================
class TestStringPredicateInListPredicates:
    """Tests for string operators inside none/all/any list predicates.

    These test that STARTS WITH / ENDS WITH / CONTAINS work correctly
    inside lambda bodies where property access must use struct field syntax.
    """

    def test_none_starts_with_in_list_predicate(self, spark, graph_context):
        """none(rel IN relationships(p) WHERE rel.status STARTS WITH 'flag')

        From Alice (KNOWS, depth 1..2):
          Alice->Bob (verified):       starts_with('flag')=F → passes none
          Alice->Carol (flagged):      starts_with('flag')=T → fails none
          Alice->Bob->Carol (ver,ver): starts_with=F,F → passes none
          Alice->Carol->Dave (flag,flag): fails none

        Dests with no flagged-prefix edges: {Bob, Carol(via Bob)}
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status STARTS WITH 'flag')
        RETURN DISTINCT dest.name AS name
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none + STARTS WITH) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Bob", "Carol"]

    def test_any_ends_with_in_list_predicate(self, spark, graph_context):
        """any(rel IN relationships(p) WHERE rel.status ENDS WITH 'ified')

        From Alice (KNOWS, depth 1..2):
          Alice->Bob (verified):    ends_with('ified')=T → any=T
          Alice->Carol (flagged):   ends_with('ified')=F → any=F
          Alice->Bob->Carol (ver,ver): any=T
          Alice->Carol->Dave (flag,flag): any=F

        Dests with at least one '-ified' edge: {Bob, Carol}
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE any(rel IN relationships(p) WHERE rel.status ENDS WITH 'ified')
        RETURN DISTINCT dest.name AS name
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (any + ENDS WITH) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Bob", "Carol"]

    def test_all_contains_in_list_predicate(self, spark, graph_context):
        """all(rel IN relationships(p) WHERE rel.status CONTAINS 'eri')

        'verified' contains 'eri' → True
        'flagged' contains 'eri' → False

        Same results as test_any_ends_with effectively:
        All-verified paths: Alice->Bob, Alice->Bob->Carol → {Bob, Carol}
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE all(rel IN relationships(p) WHERE rel.status CONTAINS 'eri')
        RETURN DISTINCT dest.name AS name
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (all + CONTAINS) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Bob", "Carol"]


# =============================================================================
# 15. STARTS WITH / ENDS WITH / CONTAINS inside UNWIND list comprehension
# =============================================================================
class TestStringPredicateInUnwindListComp:
    """Tests for string operators inside UNWIND [r IN ... WHERE ...] AS r."""

    def test_unwind_listcomp_starts_with(self, spark, graph_context):
        """UNWIND [rel IN relationships(p) WHERE rel.status STARTS WITH 'ver'] AS r

        Keep only edges whose status starts with 'ver' (i.e., 'verified').
        Distinct edges: (alice,bob,10), (bob,carol,7)
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [rel IN relationships(p) WHERE rel.status STARTS WITH 'ver'] AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.weight AS weight
        ORDER BY weight DESC
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (UNWIND listcomp STARTS WITH) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        results = [(row["src"], row["dst"], row["weight"]) for row in rows]
        assert results == [("alice", "bob", 10), ("bob", "carol", 7)]

    def test_unwind_listcomp_ends_with_aggregation(self, spark, graph_context):
        """UNWIND + ENDS WITH + SUM aggregation.

        Keep edges with status ending in 'ged' (i.e., 'flagged').
        Flagged edges from Alice KNOWS paths:
          (alice,carol,3), (carol,dave,2)
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [rel IN relationships(p) WHERE rel.status ENDS WITH 'ged'] AS r
        WITH dest.name AS dest_name, SUM(r.weight) AS total_flagged_weight
        RETURN dest_name, total_flagged_weight
        ORDER BY dest_name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (UNWIND listcomp ENDS WITH + aggregation) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        results = [(row["dest_name"], row["total_flagged_weight"]) for row in rows]
        # Carol (via Alice->Carol): flagged edges = [(alice,carol,3)] → sum=3
        # Dave (via Alice->Carol->Dave): flagged = [(alice,carol,3),(carol,dave,2)] → sum=5
        assert results == [("Carol", 3), ("Dave", 5)]

    def test_unwind_listcomp_contains_with_where(self, spark, graph_context):
        """UNWIND + CONTAINS + WITH WHERE on struct property.

        Keep edges with status containing 'eri' (verified), then filter weight >= 8.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [rel IN relationships(p) WHERE rel.status CONTAINS 'eri'] AS r
        WITH r, dest
        WHERE r.weight >= 8
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.weight AS weight
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (UNWIND listcomp CONTAINS + WITH WHERE) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        edges = {(row["src"], row["dst"], row["weight"]) for row in rows}
        assert edges == {("alice", "bob", 10)}


# =============================================================================
# 16. IS NOT NULL / IS NULL inside list predicates and list comprehensions
# =============================================================================
class TestNullPredicateInListContexts:
    """Tests for IS NOT NULL / IS NULL inside list predicates and comprehensions.

    IS NULL / IS NOT NULL are fully supported in the visitor and renderer.
    These tests verify they work correctly inside lambda contexts too.
    """

    def test_none_is_not_null_in_list_predicate(self, spark, graph_context):
        """none(rel IN relationships(p) WHERE rel.status IS NOT NULL)

        All edges in our test data have non-null status, so IS NOT NULL = True for all.
        This means: none where status IS NOT NULL → only paths where all edges have null status.
        Since all edges have status, no path matches → empty result.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status IS NOT NULL)
        RETURN DISTINCT dest.name AS name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none + IS NOT NULL) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        # All edges have non-null status → no path has none with IS NOT NULL true
        assert len(rows) == 0, f"Expected empty result, got {[r['name'] for r in rows]}"

    def test_all_is_not_null_in_list_predicate(self, spark, graph_context):
        """all(rel IN relationships(p) WHERE rel.status IS NOT NULL)

        All edges have non-null status → all paths should match.
        From Alice KNOWS depth 1..2: {Bob, Carol, Dave}
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE all(rel IN relationships(p) WHERE rel.status IS NOT NULL)
        RETURN DISTINCT dest.name AS name
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (all + IS NOT NULL) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Bob", "Carol", "Dave"]

    def test_unwind_listcomp_is_not_null(self, spark, graph_context):
        """UNWIND [rel IN relationships(p) WHERE rel.status IS NOT NULL] AS r

        All edges have status → keeps all edges.
        Same as plain UNWIND relationships(p) AS r for this data.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND [rel IN relationships(p) WHERE rel.status IS NOT NULL] AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        ORDER BY src, dst
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (UNWIND listcomp IS NOT NULL) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        edges = [(row["src"], row["dst"]) for row in rows]
        # All KNOWS edges from Alice at depth 1..2
        assert ("alice", "bob") in edges
        assert ("alice", "carol") in edges
        assert ("bob", "carol") in edges
        assert ("carol", "dave") in edges

    def test_none_is_null_in_list_predicate(self, spark, graph_context):
        """none(rel IN relationships(p) WHERE rel.status IS NULL)

        All edges have non-null status → IS NULL = False for all edges.
        none(all False) = True → all paths match.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status IS NULL)
        RETURN DISTINCT dest.name AS name
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none + IS NULL) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Bob", "Carol", "Dave"]


# =============================================================================
# 17. STARTS WITH / CONTAINS combined with coalesce inside list predicates
# =============================================================================
class TestStringPredicateWithCoalesce:
    """Tests combining STARTS WITH / CONTAINS with coalesce() in lambda bodies.

    This is the hardest case: function wrapping + string predicate operator
    inside a list predicate filter.
    """

    def test_none_coalesce_starts_with(self, spark, graph_context):
        """none(rel IN relationships(p) WHERE coalesce(rel.status, '') STARTS WITH 'flag')

        Same semantics as none(... WHERE rel.status STARTS WITH 'flag') since no NULLs.
        Dests: {Bob, Carol(via Bob)}
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE coalesce(rel.status, '') STARTS WITH 'flag')
        RETURN DISTINCT dest.name AS name
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none + coalesce + STARTS WITH) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Bob", "Carol"]

    def test_any_coalesce_contains(self, spark, graph_context):
        """any(rel IN relationships(p) WHERE coalesce(rel.status, '') CONTAINS 'lag')

        'flagged' contains 'lag' → True.
        Paths with at least one 'lag'-containing edge:
          Alice->Carol (flagged): YES
          Alice->Carol->Dave (flagged, flagged): YES
          Alice->Bob (verified): NO
          Alice->Bob->Carol (ver, ver): NO

        Dests: {Carol, Dave}
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE any(rel IN relationships(p) WHERE coalesce(rel.status, '') CONTAINS 'lag')
        RETURN DISTINCT dest.name AS name
        ORDER BY name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (any + coalesce + CONTAINS) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        names = [row["name"] for row in rows]
        assert names == ["Carol", "Dave"]


# =============================================================================
# 18. VLP + none/all + UNWIND relationships(p) + RETURN DISTINCT r (struct)
# =============================================================================
class TestVLPNoneUnwindReturnDistinctStruct:
    """Tests for the exact fraud-detection query pattern:

        MATCH (root { node_id: "xxx" })
        MATCH p = (root)-[*1..2]-()
        WHERE none(rel IN relationships(p)
              WHERE coalesce(rel.fraud_classification, '') = 'xxx')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r

    Key behaviors tested:
    - VLP path with list predicate filter (none/all)
    - UNWIND relationships(p) AS r to extract individual edges
    - RETURN DISTINCT r returns a STRUCT (named tuple) with all edge fields
    - Struct fields (src, dst, weight, status, relationship_type) are accessible

    Graph (KNOWS only, from Alice, depth 1..2):
        Alice->Bob:   weight=10, status='verified'
        Alice->Carol: weight=3,  status='flagged'
        Bob->Carol:   weight=7,  status='verified'
        Carol->Dave:  weight=2,  status='flagged'

    Paths from Alice:
        Alice->Bob          edges: [(alice,bob,10,verified)]
        Alice->Carol        edges: [(alice,carol,3,flagged)]
        Alice->Bob->Carol   edges: [(alice,bob,10,verified),(bob,carol,7,verified)]
        Alice->Carol->Dave  edges: [(alice,carol,3,flagged),(carol,dave,2,flagged)]
    """

    def test_none_coalesce_unwind_return_distinct_r(self, spark, graph_context):
        """Exact fraud pattern: none() + coalesce + UNWIND + RETURN DISTINCT r.

        WHERE none(rel IN ... WHERE coalesce(rel.status, '') = 'flagged')
        keeps paths with NO flagged edges:
          Alice->Bob: passes (verified)
          Alice->Bob->Carol: passes (both verified)

        UNWIND extracts: (alice,bob), (alice,bob), (bob,carol)
        DISTINCT r: {(alice,bob,...), (bob,carol,...)}

        RETURN DISTINCT r must return structs with accessible fields.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE coalesce(rel.status, '') = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none+coalesce+UNWIND+RETURN DISTINCT r) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()

        # Should return 2 distinct edges as structs
        assert len(rows) == 2, f"Expected 2 distinct edges, got {len(rows)}: {rows}"

        # Extract struct fields — r should be a named struct
        edges = set()
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)

            # Verify struct has all edge fields
            assert "src" in r_dict, f"Struct missing 'src': {r_dict}"
            assert "dst" in r_dict, f"Struct missing 'dst': {r_dict}"
            assert "weight" in r_dict, f"Struct missing 'weight': {r_dict}"
            assert "status" in r_dict, f"Struct missing 'status': {r_dict}"

            edges.add((r_dict["src"], r_dict["dst"]))

        assert edges == {("alice", "bob"), ("bob", "carol")}

    def test_none_simple_unwind_return_distinct_r(self, spark, graph_context):
        """Same as above but without coalesce — simpler predicate.

        none(rel IN ... WHERE rel.status = 'flagged')
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none+simple+UNWIND+RETURN DISTINCT r) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 2
        edges = set()
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            edges.add((r_dict["src"], r_dict["dst"], r_dict["weight"]))

        assert edges == {("alice", "bob", 10), ("bob", "carol", 7)}

    def test_all_verified_unwind_return_distinct_r(self, spark, graph_context):
        """all(rel IN ... WHERE rel.status = 'verified') + UNWIND + RETURN DISTINCT r.

        Semantically same as none(flagged) for this data.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE all(rel IN relationships(p) WHERE rel.status = 'verified')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (all+verified+UNWIND+RETURN DISTINCT r) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 2
        edges = set()
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            assert r_dict["status"] == "verified"
            edges.add((r_dict["src"], r_dict["dst"]))

        assert edges == {("alice", "bob"), ("bob", "carol")}

    def test_none_unwind_return_distinct_r_struct_values(self, spark, graph_context):
        """Verify ALL struct field values are correct, not just src/dst."""
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        edge_dicts = []
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            edge_dicts.append(r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct))

        # Sort by weight for deterministic comparison
        edge_dicts.sort(key=lambda d: d["weight"])

        # bob->carol (weight=7)
        assert edge_dicts[0]["src"] == "bob"
        assert edge_dicts[0]["dst"] == "carol"
        assert edge_dicts[0]["weight"] == 7
        assert edge_dicts[0]["status"] == "verified"

        # alice->bob (weight=10)
        assert edge_dicts[1]["src"] == "alice"
        assert edge_dicts[1]["dst"] == "bob"
        assert edge_dicts[1]["weight"] == 10
        assert edge_dicts[1]["status"] == "verified"


# =============================================================================
# 19. Variations: none/all + UNWIND + RETURN DISTINCT r + WITH / OR / aggregation
# =============================================================================
class TestVLPNoneUnwindReturnDistinctVariations:
    """Variations on the core VLP+none+UNWIND+RETURN DISTINCT r pattern.

    Tests combining with:
    - OR in the none() predicate
    - WITH clause before RETURN
    - WITH WHERE after UNWIND
    - Aggregation on struct fields after UNWIND
    - Multiple edge types
    - Undirected traversal
    """

    def test_none_with_OR_in_predicate(self, spark, graph_context):
        """none(rel IN ... WHERE rel.status = 'flagged' OR rel.weight < 5)
        + UNWIND + RETURN DISTINCT r.

        From Alice KNOWS depth 1..2:
          Alice->Bob (ver, w=10): flag=F, w<5=F → none matches → passes
          Alice->Carol (flag, w=3): flag=T → fails none
          Alice->Bob->Carol (ver/10, ver/7): all pass (no flag, w>=5) → passes
          Alice->Carol->Dave (flag/3, flag/2): fails

        Passing paths: Alice->Bob, Alice->Bob->Carol
        Distinct edges: (alice,bob), (bob,carol)
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged' OR rel.weight < 5)
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none+OR+UNWIND+RETURN DISTINCT r) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()

        edges = set()
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            edges.add((r_dict["src"], r_dict["dst"]))

        assert edges == {("alice", "bob"), ("bob", "carol")}

    def test_none_unwind_with_before_return(self, spark, graph_context):
        """none() + UNWIND + WITH r, dest + RETURN DISTINCT r.

        Piping through a WITH clause should preserve the struct.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        WITH r, dest
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none+UNWIND+WITH+RETURN DISTINCT r) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 2
        edges = set()
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            edges.add((r_dict["src"], r_dict["dst"]))

        assert edges == {("alice", "bob"), ("bob", "carol")}

    def test_none_unwind_with_where_filter(self, spark, graph_context):
        """none() + UNWIND + WITH r WHERE r.weight >= 8 + RETURN DISTINCT r.

        After UNWIND, filter to keep only heavy edges.
        From passing paths (no flagged):
          Edges: (alice,bob,10), (bob,carol,7)
          After WHERE r.weight >= 8: only (alice,bob,10)
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        WITH r
        WHERE r.weight >= 8
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none+UNWIND+WITH WHERE+RETURN DISTINCT r) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 1
        r_struct = rows[0].r if hasattr(rows[0], "r") else rows[0]["r"]
        r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
        assert r_dict["src"] == "alice"
        assert r_dict["dst"] == "bob"
        assert r_dict["weight"] == 10

    def test_none_unwind_aggregation_on_struct(self, spark, graph_context):
        """none() + UNWIND + aggregation on struct fields.

        SUM(r.weight), COUNT(r), COLLECT(r.dst) after UNWIND.
        From passing paths (no flagged): edges (alice,bob,10), (bob,carol,7)
        But since paths overlap, UNWIND produces duplicates before DISTINCT:
          Path Alice->Bob: [(alice,bob)]
          Path Alice->Bob->Carol: [(alice,bob), (bob,carol)]
        So UNWIND yields: (alice,bob), (alice,bob), (bob,carol) = 3 rows

        SUM(r.weight) = 10 + 10 + 7 = 27
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        RETURN SUM(r.weight) AS total_weight, COUNT(r.src) AS edge_count
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none+UNWIND+aggregation) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["total_weight"] == 27  # 10 + 10 + 7
        assert rows[0]["edge_count"] == 3  # 3 unwound edges (with duplicates)

    def test_none_unwind_group_by_dest(self, spark, graph_context):
        """none() + UNWIND + GROUP BY dest + COLLECT(r.weight).

        Group unwound edges by destination, collecting weights.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        RETURN dest.name AS dest_name, COLLECT(r.weight) AS weights
        ORDER BY dest_name
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none+UNWIND+GROUP BY dest) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dest_name"]: sorted(row["weights"]) for row in rows}

        # Bob (path Alice->Bob): edges=[alice->bob] → weights=[10]
        # Carol (path Alice->Bob->Carol): edges=[alice->bob, bob->carol] → weights=[10,7]
        assert results == {"Bob": [10], "Carol": [7, 10]}

    def test_none_unwind_multi_edge_types(self, spark, graph_context):
        """none() + UNWIND with KNOWS|WORKS_WITH edges + RETURN DISTINCT r.

        Using untyped edges [*1..1] (depth 1 only) from Alice:
          Alice->Bob (KNOWS, verified, w=10)
          Alice->Carol (KNOWS, flagged, w=3)
          Alice->Frank (WORKS_WITH, pending, w=5)

        none(... WHERE rel.status = 'flagged'):
          Alice->Bob: passes (verified)
          Alice->Carol: fails (flagged)
          Alice->Frank: passes (pending)

        RETURN DISTINCT r: {(alice,bob,...), (alice,frank,...)}
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[*1..1]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none+UNWIND multi-type+RETURN DISTINCT r) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()

        edges = set()
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            edges.add((r_dict["src"], r_dict["dst"]))

        assert edges == {("alice", "bob"), ("alice", "frank")}

    def test_any_unwind_return_distinct_r(self, spark, graph_context):
        """any(rel IN ... WHERE rel.weight > 8) + UNWIND + RETURN DISTINCT r.

        From Alice KNOWS depth 1..2:
          Alice->Bob (w=10): w>8=T → any=T
          Alice->Carol (w=3): w>8=F → any=F
          Alice->Bob->Carol (w=10,w=7): w=10>8 → any=T
          Alice->Carol->Dave (w=3,w=2): w>8=F → any=F

        Passing paths: Alice->Bob, Alice->Bob->Carol
        UNWIND: (alice,bob), (alice,bob), (bob,carol)
        DISTINCT: {(alice,bob), (bob,carol)}
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE any(rel IN relationships(p) WHERE rel.weight > 8)
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (any+UNWIND+RETURN DISTINCT r) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()

        edges = set()
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            edges.add((r_dict["src"], r_dict["dst"]))

        assert edges == {("alice", "bob"), ("bob", "carol")}

    def test_none_unwind_with_chain_return_distinct_r(self, spark, graph_context):
        """none() + UNWIND + WITH r, dest + WITH r WHERE ... + RETURN DISTINCT r.

        Multiple WITH hops preserving the struct.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        WITH r, dest
        WITH r
        WHERE r.weight > 5
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none+UNWIND+WITH chain+RETURN DISTINCT r) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()

        edges = set()
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            edges.add((r_dict["src"], r_dict["dst"], r_dict["weight"]))

        # Only edges with weight > 5 from non-flagged paths
        assert edges == {("alice", "bob", 10), ("bob", "carol", 7)}

    def test_none_coalesce_starts_with_unwind_return_distinct_r(self, spark, graph_context):
        """Combining coalesce + STARTS WITH in none() + UNWIND + RETURN DISTINCT r.

        none(rel IN ... WHERE coalesce(rel.status, '') STARTS WITH 'flag')
        Same semantics as status = 'flagged' for this data.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE coalesce(rel.status, '') STARTS WITH 'flag')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none+coalesce+STARTS WITH+UNWIND+RETURN DISTINCT r) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 2
        edges = set()
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            edges.add((r_dict["src"], r_dict["dst"]))

        assert edges == {("alice", "bob"), ("bob", "carol")}

    def test_none_is_not_null_unwind_return_distinct_r(self, spark, graph_context):
        """none(rel IN ... WHERE rel.status IS NOT NULL) + UNWIND + RETURN DISTINCT r.

        All edges have non-null status → IS NOT NULL = True for all.
        none(all True) = False → no paths pass → empty result.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status IS NOT NULL)
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none+IS NOT NULL+UNWIND+RETURN DISTINCT r) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()

        # All edges have status → none where IS NOT NULL = impossible
        assert len(rows) == 0

    def test_none_unwind_return_r_properties_mixed(self, spark, graph_context):
        """none() + UNWIND + RETURN r.src, r.dst, r.weight (explicit properties, not struct).

        Contrast with RETURN DISTINCT r: here we return individual fields.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.weight AS weight
        ORDER BY weight DESC
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (none+UNWIND+RETURN r.props) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        results = [(row["src"], row["dst"], row["weight"]) for row in rows]

        assert results == [("alice", "bob", 10), ("bob", "carol", 7)]


# =============================================================================
# 20. Exact user fraud query pattern: undirected + untyped + no-label root
# =============================================================================
class TestUndirectedUntypedVLPNoneUnwind:
    """Tests for the exact real-world fraud query pattern:

        MATCH (root { node_id: "xxx" })
        MATCH p = (root)-[*1..2]-()
        WHERE none(rel IN relationships(p)
              WHERE coalesce(rel.status, '') = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r

    Key differences from previous tests:
    - Root has NO label (just {node_id: ...})
    - VLP is UNDIRECTED (-[*1..2]-)
    - VLP is UNTYPED (no edge type label)
    - Destination is ANONYMOUS (unnamed `()`)

    This triggered a real bug where the lambda variable `rel` was rendered
    as `_gsql2rsql_rel_status` (column prefix) instead of `rel.status`
    (struct field access) inside the `none()` predicate.

    Graph (all edges, undirected traversal from Alice):
        Alice->Bob:   KNOWS,      weight=10, status='verified'
        Alice->Carol: KNOWS,      weight=3,  status='flagged'
        Alice->Frank: WORKS_WITH, weight=5,  status='pending'
        Bob->Carol:   KNOWS,      weight=7,  status='verified'
        Carol->Dave:  KNOWS,      weight=2,  status='flagged'
        Dave->Eve:    KNOWS,      weight=1,  status='flagged'

    Undirected depth 1 from Alice (all edge types):
        Alice->Bob (KNOWS, verified)
        Alice->Carol (KNOWS, flagged)
        Alice->Frank (WORKS_WITH, pending)

    Undirected depth 2 from Alice (all edge types):
        Alice->Bob->Carol (ver, ver)
        Alice->Carol->Bob (flag, ver)   [backward via Bob->Carol]
        Alice->Carol->Dave (flag, flag)
        Alice->Frank->... (if any)
    """

    def test_undirected_untyped_none_coalesce_unwind_return_distinct_r(
        self, spark, graph_context
    ):
        """Exact user pattern: undirected, untyped, no-label root + RETURN DISTINCT r.

        none(... WHERE coalesce(rel.status, '') = 'flagged')
        filters to paths with NO flagged edges.

        Undirected depth 1 from Alice:
          Alice->Bob: verified -> passes none
          Alice<-Bob: (same edge viewed backward) -> verified -> passes
          Alice->Carol: flagged -> fails
          Alice<-Carol: same edge -> flagged -> fails
          Alice->Frank: pending -> passes

        The distinct edges returned should NOT include any with status='flagged'.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[*1..2]-()
        WHERE none(rel IN relationships(p) WHERE coalesce(rel.status, '') = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (undirected+untyped+none+coalesce+RETURN DISTINCT r) ===\n{sql}")

        # Verify the lambda uses struct field access, NOT column prefix
        assert "_gsql2rsql_rel_" not in sql, (
            f"Lambda should use 'rel.status' not '_gsql2rsql_rel_status'. "
            f"Found column-prefix in SQL."
        )

        result = spark.sql(sql)
        rows = result.collect()

        # All returned edges must NOT be flagged
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            assert r_dict["status"] != "flagged", (
                f"Edge {r_dict['src']}->{r_dict['dst']} has status='flagged' "
                f"but should have been filtered by none()"
            )

        # Should have at least the verified/pending edges
        edges = set()
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            edges.add((r_dict["src"], r_dict["dst"]))

        assert ("alice", "bob") in edges, f"Missing alice->bob edge. Got: {edges}"

    def test_undirected_untyped_none_simple_unwind_return_distinct_r(
        self, spark, graph_context
    ):
        """Undirected + untyped + none(rel.status = 'flagged') + RETURN DISTINCT r."""
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[*1..2]-()
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (undirected+untyped+none+simple+RETURN DISTINCT r) ===\n{sql}")

        assert "_gsql2rsql_rel_" not in sql

        result = spark.sql(sql)
        rows = result.collect()

        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            assert r_dict["status"] != "flagged"

    def test_undirected_untyped_nolabel_root_by_node_id(
        self, spark, graph_context
    ):
        """Root matched by {node_id: ...} without label -- closest to real query.

        MATCH (root { node_id: "alice" })  -- no :Person label
        """
        query = """
        MATCH (root { node_id: "alice" })
        MATCH p = (root)-[*1..2]-()
        WHERE none(rel IN relationships(p) WHERE coalesce(rel.status, '') = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (no-label root+undirected+untyped+none) ===\n{sql}")

        assert "_gsql2rsql_rel_" not in sql

        result = spark.sql(sql)
        rows = result.collect()

        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            assert r_dict["status"] != "flagged"

        edges = set()
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            edges.add((r_dict["src"], r_dict["dst"]))

        assert ("alice", "bob") in edges

    def test_undirected_untyped_none_with_or_unwind_return_distinct_r(
        self, spark, graph_context
    ):
        """Undirected + untyped + none() with OR + UNWIND + RETURN DISTINCT r.

        none(rel IN ... WHERE rel.status = 'flagged' OR rel.weight < 3)
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[*1..2]-()
        WHERE none(rel IN relationships(p)
              WHERE rel.status = 'flagged' OR rel.weight < 3)
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (undirected+untyped+none+OR+RETURN DISTINCT r) ===\n{sql}")

        assert "_gsql2rsql_rel_" not in sql

        result = spark.sql(sql)
        rows = result.collect()

        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            assert r_dict["status"] != "flagged", f"Flagged edge found: {r_dict}"
            assert r_dict["weight"] >= 3, f"Low-weight edge found: {r_dict}"

    def test_undirected_untyped_none_unwind_with_where_return_distinct_r(
        self, spark, graph_context
    ):
        """Undirected + untyped + none() + UNWIND + WITH WHERE + RETURN DISTINCT r."""
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[*1..2]-()
        WHERE none(rel IN relationships(p) WHERE coalesce(rel.status, '') = 'flagged')
        UNWIND relationships(p) AS r
        WITH r
        WHERE r.weight >= 5
        RETURN DISTINCT r
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (undirected+untyped+none+UNWIND+WITH WHERE+RETURN DISTINCT r) ===\n{sql}")

        assert "_gsql2rsql_rel_" not in sql

        result = spark.sql(sql)
        rows = result.collect()

        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            assert r_dict["status"] != "flagged"
            assert r_dict["weight"] >= 5

    def test_undirected_untyped_none_unwind_aggregation(
        self, spark, graph_context
    ):
        """Undirected + untyped + none() + UNWIND + aggregation on struct fields."""
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[*1..2]-()
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        RETURN r.src AS src, r.dst AS dst, COUNT(*) AS cnt
        ORDER BY src, dst
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (undirected+untyped+none+UNWIND+aggregation) ===\n{sql}")

        assert "_gsql2rsql_rel_" not in sql

        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) > 0

    def test_undirected_untyped_all_is_not_null_unwind_return_distinct_r(
        self, spark, graph_context
    ):
        """Undirected + untyped + all(IS NOT NULL) + UNWIND + RETURN DISTINCT r."""
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[*1..2]-()
        WHERE all(rel IN relationships(p) WHERE rel.status IS NOT NULL)
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.status AS status
        ORDER BY src, dst
        """
        sql = graph_context.transpile(query)
        print(f"\n=== SQL (undirected+untyped+all+IS NOT NULL) ===\n{sql}")

        assert "_gsql2rsql_rel_" not in sql

        result = spark.sql(sql)
        rows = result.collect()
        # All edges in test data have non-null status, so all paths should match
        assert len(rows) > 0
        for row in rows:
            assert row["status"] is not None


# =============================================================================
# 21. DISTINCT on structs with MAP columns (Spark limitation workaround)
# =============================================================================
@pytest.fixture(scope="module")
def graph_context_with_map(spark):
    """Graph context with MAP-type edge property to test DISTINCT on MAP structs.

    This reproduces the real-world scenario where edges have a metadata column
    of type MAP<STRING, STRING>. Spark cannot do SELECT DISTINCT on structs
    containing MAP fields.
    """
    nodes_data = [
        ("alice", "Person", "Alice"),
        ("bob", "Person", "Bob"),
        ("carol", "Person", "Carol"),
        ("dave", "Person", "Dave"),
    ]
    nodes_df = spark.createDataFrame(
        nodes_data, ["node_id", "node_type", "name"]
    )
    nodes_df.createOrReplaceTempView("nodes_map")

    edges_data = [
        ("alice", "bob", "KNOWS", 10, "verified", {"risk": "low", "source": "manual"}),
        ("alice", "carol", "KNOWS", 3, "flagged", {"risk": "high", "source": "auto"}),
        ("bob", "carol", "KNOWS", 7, "verified", {"risk": "low", "source": "manual"}),
        ("carol", "dave", "KNOWS", 2, "flagged", {"risk": "high", "source": "auto"}),
    ]
    edges_df = spark.createDataFrame(
        edges_data,
        ["src", "dst", "relationship_type", "weight", "status", "metadata"],
    )
    edges_df.createOrReplaceTempView("edges_map")

    graph = GraphContext(
        spark=spark,
        nodes_table="nodes_map",
        edges_table="edges_map",
        node_type_col="node_type",
        node_id_col="node_id",
        edge_src_col="src",
        edge_dst_col="dst",
        edge_type_col="relationship_type",
        extra_node_attrs={"name": str},
        extra_edge_attrs={"weight": int, "status": str, "metadata": dict},
    )
    graph.set_types(
        node_types=["Person"],
        edge_types=["KNOWS"],
    )
    return graph


class TestDistinctOnStructWithMapType:
    """Tests for RETURN DISTINCT r when the edge struct contains MAP fields.

    Spark error without fix:
        [UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE]
        Cannot have MAP type columns in DataFrame which calls set operations
        (INTERSECT, EXCEPT, etc.), but the type of column `r` is
        "STRUCT<src: STRING, dst: STRING, metadata: MAP<STRING, STRING>, ...>"

    The fix: when DISTINCT is applied to a struct from UNWIND that may contain
    MAP fields, use GROUP BY TO_JSON(r) with FIRST(r) instead of SELECT DISTINCT.
    """

    def test_return_distinct_r_with_map_basic(
        self, spark, graph_context_with_map
    ):
        """RETURN DISTINCT r where r struct contains metadata: MAP<STRING,STRING>.

        This is the exact pattern that triggers the Spark MAP DISTINCT error.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context_with_map.transpile(query)
        print(f"\n=== SQL (DISTINCT r with MAP) ===\n{sql}")

        # Should NOT use SELECT DISTINCT directly (would fail with MAP)
        # Should use GROUP BY TO_JSON approach instead
        result = spark.sql(sql)
        rows = result.collect()

        assert len(rows) == 2
        edges = set()
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            edges.add((r_dict["src"], r_dict["dst"]))
            # MAP field should be preserved as dict
            assert isinstance(r_dict["metadata"], dict), (
                f"metadata should be dict/MAP, got {type(r_dict['metadata'])}"
            )

        assert edges == {("alice", "bob"), ("bob", "carol")}

    def test_return_distinct_r_with_map_undirected(
        self, spark, graph_context_with_map
    ):
        """Undirected + untyped VLP with MAP struct + RETURN DISTINCT r."""
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[*1..2]-()
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context_with_map.transpile(query)
        print(f"\n=== SQL (DISTINCT r undirected+MAP) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        # All returned edges must NOT be flagged
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            assert r_dict["status"] != "flagged"
            assert isinstance(r_dict["metadata"], dict)

    def test_return_distinct_r_with_map_metadata_values(
        self, spark, graph_context_with_map
    ):
        """Verify MAP field values are correct and accessible after DISTINCT."""
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context_with_map.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        edge_dicts = []
        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            edge_dicts.append(
                r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            )

        edge_dicts.sort(key=lambda d: d["weight"])

        # bob->carol (weight=7, metadata={risk:low, source:manual})
        assert edge_dicts[0]["src"] == "bob"
        assert edge_dicts[0]["dst"] == "carol"
        assert edge_dicts[0]["metadata"]["risk"] == "low"
        assert edge_dicts[0]["metadata"]["source"] == "manual"

        # alice->bob (weight=10, metadata={risk:low, source:manual})
        assert edge_dicts[1]["src"] == "alice"
        assert edge_dicts[1]["dst"] == "bob"
        assert edge_dicts[1]["metadata"]["risk"] == "low"

    def test_return_distinct_r_with_map_nolabel_root(
        self, spark, graph_context_with_map
    ):
        """No-label root + MAP struct + RETURN DISTINCT r — exact user pattern."""
        query = """
        MATCH (root { node_id: "alice" })
        MATCH p = (root)-[*1..2]-()
        WHERE none(rel IN relationships(p) WHERE coalesce(rel.status, '') = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r
        """
        sql = graph_context_with_map.transpile(query)
        print(f"\n=== SQL (no-label+MAP+DISTINCT r) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        for row in rows:
            r_struct = row.r if hasattr(row, "r") else row["r"]
            r_dict = r_struct.asDict() if hasattr(r_struct, "asDict") else dict(r_struct)
            assert r_dict["status"] != "flagged"
            assert isinstance(r_dict["metadata"], dict)

    def test_return_distinct_r_properties_still_works_with_map(
        self, spark, graph_context_with_map
    ):
        """RETURN DISTINCT r.src, r.dst — individual properties, not struct.

        This should work even without the fix because individual fields
        are simple types (not MAP).
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst, r.weight AS weight
        ORDER BY weight DESC
        """
        sql = graph_context_with_map.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        results = [(row["src"], row["dst"], row["weight"]) for row in rows]
        assert results == [("alice", "bob", 10), ("bob", "carol", 7)]


class TestDistinctMapPropertyAccess:
    """Tests for RETURN DISTINCT r.metadata where metadata is MAP type.

    Gap: The DISTINCT workaround only triggers for bare variables (r),
    not for property access (r.metadata). But r.metadata IS a MAP,
    so SELECT DISTINCT r.metadata also fails in Spark.
    """

    def test_return_distinct_map_property(
        self, spark, graph_context_with_map
    ):
        """RETURN DISTINCT r.metadata — direct MAP property access.

        This extracts the MAP field from the struct. Spark cannot do
        SELECT DISTINCT on MAP columns.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.metadata AS metadata
        """
        sql = graph_context_with_map.transpile(query)
        print(f"\n=== SQL ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        # Should have distinct metadata values
        assert len(rows) >= 1
        for row in rows:
            assert isinstance(row["metadata"], dict)

    def test_return_distinct_map_property_with_scalar(
        self, spark, graph_context_with_map
    ):
        """RETURN DISTINCT r.src, r.metadata — mixed scalar + MAP.

        Even one MAP column in a DISTINCT makes the whole query fail.
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.metadata AS metadata
        """
        sql = graph_context_with_map.transpile(query)
        print(f"\n=== SQL ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 1
        for row in rows:
            assert isinstance(row["metadata"], dict)
            assert isinstance(row["src"], str)

    def test_return_distinct_map_property_values_correct(
        self, spark, graph_context_with_map
    ):
        """Verify MAP values are preserved correctly after DISTINCT."""
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.metadata AS metadata
        ORDER BY src
        """
        sql = graph_context_with_map.transpile(query)
        print(f"\n=== SQL ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        results = [(row["src"], row["metadata"]) for row in rows]
        # Only verified edges: alice->bob (risk=low), bob->carol (risk=low)
        assert len(results) == 2
        for src, meta in results:
            assert meta["risk"] == "low"
            assert meta["source"] == "manual"


class TestWithDistinctAtIntermediateDepth:
    """Tests for WITH DISTINCT r at depth > 0 where r is a struct with MAP.

    Gap: The DISTINCT workaround has depth==0 constraint, so WITH DISTINCT
    at intermediate depths uses regular SELECT DISTINCT, which fails on MAP.
    """

    def test_with_distinct_unwind_struct_then_return_fields(
        self, spark, graph_context_with_map
    ):
        """WITH DISTINCT r RETURN r.src, r.dst — depth > 0 DISTINCT on struct.

        The WITH clause creates an intermediate projection (depth > 0).
        """
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        WITH DISTINCT r
        RETURN r.src AS src, r.dst AS dst
        ORDER BY src, dst
        """
        sql = graph_context_with_map.transpile(query)
        print(f"\n=== SQL ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        results = [(row["src"], row["dst"]) for row in rows]
        assert results == [("alice", "bob"), ("bob", "carol")]

    def test_with_distinct_struct_preserves_map_field(
        self, spark, graph_context_with_map
    ):
        """WITH DISTINCT r RETURN r.metadata — MAP field survives WITH."""
        query = """
        MATCH (root:Person {name: "Alice"})
        MATCH p = (root)-[:KNOWS*1..2]->(dest)
        WHERE none(rel IN relationships(p) WHERE rel.status = 'flagged')
        UNWIND relationships(p) AS r
        WITH DISTINCT r
        RETURN r.src AS src, r.metadata AS metadata
        ORDER BY src
        """
        sql = graph_context_with_map.transpile(query)
        print(f"\n=== SQL ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        results = [(row["src"], row["metadata"]) for row in rows]
        assert len(results) == 2
        for src, meta in results:
            assert meta["risk"] == "low"


# =============================================================================
# 24. DISTINCT on ARRAY and STRUCT edge properties
# =============================================================================
@pytest.fixture(scope="module")
def graph_context_with_complex_types(spark):
    """Graph context with ARRAY<STRING> and STRUCT edge properties.

    Tests DISTINCT behavior across complex Spark types:
    - ARRAY: Cannot use SELECT DISTINCT in Spark (like MAP)
    - STRUCT: Can use SELECT DISTINCT, but GROUP BY TO_JSON also works

    Edge data has duplicates so DISTINCT actually deduplicates.
    """
    from pyspark.sql.types import (
        StructType as SparkStructType,
        StructField,
        StringType,
        LongType,
        ArrayType as SparkArrayType,
    )

    nodes_data = [
        ("alice", "Person", "Alice"),
        ("bob", "Person", "Bob"),
        ("carol", "Person", "Carol"),
    ]
    nodes_df = spark.createDataFrame(
        nodes_data, ["node_id", "node_type", "name"]
    )
    nodes_df.createOrReplaceTempView("nodes_complex")

    edge_schema = SparkStructType([
        StructField("src", StringType()),
        StructField("dst", StringType()),
        StructField("relationship_type", StringType()),
        StructField("status", StringType()),
        StructField("tags", SparkArrayType(StringType())),
        StructField("profile", SparkStructType([
            StructField("category", StringType()),
            StructField("score", LongType()),
        ])),
    ])

    # Duplicate alice→bob and alice→carol to make DISTINCT meaningful
    edges_data = [
        ("alice", "bob", "KNOWS", "active", ["friend", "work"], ("professional", 10)),
        ("alice", "carol", "KNOWS", "active", ["friend", "work"], ("professional", 10)),
        ("bob", "carol", "KNOWS", "inactive", ["family"], ("personal", 5)),
    ]
    edges_df = spark.createDataFrame(edges_data, edge_schema)
    edges_df.createOrReplaceTempView("edges_complex")

    graph = GraphContext(
        spark=spark,
        nodes_table="nodes_complex",
        edges_table="edges_complex",
        node_type_col="node_type",
        node_id_col="node_id",
        edge_src_col="src",
        edge_dst_col="dst",
        edge_type_col="relationship_type",
        extra_node_attrs={"name": str},
        extra_edge_attrs={
            "status": str,
            "tags": list,      # ARRAY<STRING> in Spark
            "profile": dict,   # STRUCT in DataFrame, dict for schema compat
        },
    )
    graph.set_types(node_types=["Person"], edge_types=["KNOWS"])
    return graph


class TestDistinctOnComplexEdgeProperties:
    """Test DISTINCT with ARRAY and STRUCT edge properties.

    Covers the 6 patterns:
    - RETURN DISTINCT r.field_struct
    - RETURN DISTINCT r.field_array
    - RETURN r.field_array, r.scr_string  (no DISTINCT, control)
    - RETURN r.field_struct, r.scr_string  (no DISTINCT, control)
    - RETURN DISTINCT r.field_array, r.scr_string
    - RETURN DISTINCT r.field_struct, r.scr_string
    """

    def test_return_distinct_struct_property(
        self, spark, graph_context_with_complex_types
    ):
        """RETURN DISTINCT r.profile where profile is STRUCT<category, score>."""
        query = """
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        RETURN DISTINCT r.profile AS profile
        """
        sql = graph_context_with_complex_types.transpile(query)
        print(f"\n=== SQL (DISTINCT struct property) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) == 2
        profiles = {
            (r["profile"]["category"], r["profile"]["score"]) for r in rows
        }
        assert profiles == {("professional", 10), ("personal", 5)}

    def test_return_distinct_array_property(
        self, spark, graph_context_with_complex_types
    ):
        """RETURN DISTINCT r.tags where tags is ARRAY<STRING>."""
        query = """
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        RETURN DISTINCT r.tags AS tags
        """
        sql = graph_context_with_complex_types.transpile(query)
        print(f"\n=== SQL (DISTINCT array property) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) == 2
        tag_sets = {tuple(sorted(r["tags"])) for r in rows}
        assert tag_sets == {("friend", "work"), ("family",)}

    def test_return_array_and_scalar_no_distinct(
        self, spark, graph_context_with_complex_types
    ):
        """RETURN r.tags, r.status — no DISTINCT, control test."""
        query = """
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        RETURN r.tags AS tags, r.status AS status
        """
        sql = graph_context_with_complex_types.transpile(query)
        print(f"\n=== SQL (array + scalar, no DISTINCT) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) == 3

    def test_return_struct_and_scalar_no_distinct(
        self, spark, graph_context_with_complex_types
    ):
        """RETURN r.profile, r.status — no DISTINCT, control test."""
        query = """
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        RETURN r.profile AS profile, r.status AS status
        """
        sql = graph_context_with_complex_types.transpile(query)
        print(f"\n=== SQL (struct + scalar, no DISTINCT) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) == 3

    def test_return_distinct_array_and_scalar(
        self, spark, graph_context_with_complex_types
    ):
        """RETURN DISTINCT r.tags, r.status — mixed ARRAY + scalar."""
        query = """
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        RETURN DISTINCT r.tags AS tags, r.status AS status
        """
        sql = graph_context_with_complex_types.transpile(query)
        print(f"\n=== SQL (DISTINCT array + scalar) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) == 2
        results = {
            (tuple(sorted(r["tags"])), r["status"]) for r in rows
        }
        assert results == {
            (("friend", "work"), "active"),
            (("family",), "inactive"),
        }

    def test_return_distinct_struct_and_scalar(
        self, spark, graph_context_with_complex_types
    ):
        """RETURN DISTINCT r.profile, r.status — mixed STRUCT + scalar."""
        query = """
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        RETURN DISTINCT r.profile AS profile, r.status AS status
        """
        sql = graph_context_with_complex_types.transpile(query)
        print(f"\n=== SQL (DISTINCT struct + scalar) ===\n{sql}")
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) == 2
        results = {
            ((r["profile"]["category"], r["profile"]["score"]), r["status"])
            for r in rows
        }
        assert results == {
            (("professional", 10), "active"),
            (("personal", 5), "inactive"),
        }
