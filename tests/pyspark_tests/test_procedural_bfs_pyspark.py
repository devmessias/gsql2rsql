"""PySpark integration tests for procedural BFS rendering mode.

Requires PySpark 4.2+ with SQL scripting support.
Tests execute real queries on Spark and verify exact result values.

Test Graph (single-type: KNOWS only):
    Alice ---KNOWS---> Bob ---KNOWS---> Carol
      |                 ^
      +---KNOWS---> Dave ---KNOWS---> Eve ---KNOWS---> Bob
                                       (amount=75)

Edges:
    Alice -> Bob   (amount=100)
    Bob   -> Carol (amount=200)
    Alice -> Dave  (amount=150)
    Dave  -> Eve   (amount=50)
    Eve   -> Bob   (amount=75)

Multi-type Test Graph (KNOWS + WORKS_AT):
    Alice ---KNOWS---> Bob ---KNOWS---> Carol
      |                                  |
      +---WORKS_AT---> Acme <---WORKS_AT-+
      |
      +---KNOWS---> Dave ---WORKS_AT---> BigCo
"""

import pytest

try:
    from pyspark.sql import SparkSession

    _spark = (
        SparkSession.builder
        .master("local[1]")
        .config("spark.sql.scripting.enabled", "true")
        .getOrCreate()
    )
    _spark.sql("BEGIN DECLARE x INT DEFAULT 1; END")
    HAS_PYSPARK_SCRIPTING = True
    _spark.stop()
except Exception:
    HAS_PYSPARK_SCRIPTING = False

pytestmark = pytest.mark.skipif(
    not HAS_PYSPARK_SCRIPTING,
    reason="Requires PySpark 4.2+ with SQL scripting support",
)


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .appName("ProceduralBFS_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.scripting.enabled", "true")
        .getOrCreate()
    )
    # Create test graph
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW test_nodes AS
        SELECT * FROM VALUES
            ('Alice', 'Person', 25),
            ('Bob',   'Person', 30),
            ('Carol', 'Person', 35),
            ('Dave',  'Person', 28),
            ('Eve',   'Person', 22)
        AS t(node_id, node_type, age)
    """)
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW test_edges AS
        SELECT * FROM VALUES
            ('Alice', 'Bob',   'KNOWS', 100),
            ('Bob',   'Carol', 'KNOWS', 200),
            ('Alice', 'Dave',  'KNOWS', 150),
            ('Dave',  'Eve',   'KNOWS', 50),
            ('Eve',   'Bob',   'KNOWS', 75)
        AS t(src, dst, relationship_type, amount)
    """)
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph(spark):
    from gsql2rsql import GraphContext

    ctx = GraphContext(
        spark=spark,
        nodes_table="test_nodes",
        edges_table="test_edges",
        node_id_col="node_id",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={"age": int},
        extra_edge_attrs={"amount": int},
    )
    return ctx


class TestProceduralBFSPySpark:
    """End-to-end procedural BFS tests with PySpark execution."""

    def test_directed_bfs_basic(self, spark, graph):
        """Basic directed BFS from Alice should find reachable nodes."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(query, vlp_rendering_mode="procedural", materialization_strategy="numbered_views")
        print(f"\n=== SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}

        # Alice->Bob, Alice->Dave, Bob->Carol, Dave->Eve, Eve->Bob (already visited)
        # With global visited: Bob at depth 1, Dave at depth 1,
        #   Carol at depth 2, Eve at depth 2
        # Bob is NOT re-discovered at depth 3 (global visited)
        expected = {"Bob", "Carol", "Dave", "Eve"}
        assert results == expected, (
            f"Expected {expected}, got {results}"
        )

    def test_directed_with_sink_filter(self, spark, graph):
        """Sink filter: only Carol should be returned."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice' AND b.node_id = 'Carol'
        RETURN b.node_id AS dst
        """
        sql = graph.transpile(query, vlp_rendering_mode="procedural", materialization_strategy="numbered_views")
        print(f"\n=== SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}
        assert results == {"Carol"}

    def test_backward_bfs(self, spark, graph):
        """Backward BFS from Carol should find nodes that reach Carol."""
        query = """
        MATCH (a:Person)<-[:KNOWS*1..3]-(b:Person)
        WHERE a.node_id = 'Carol'
        RETURN DISTINCT b.node_id AS src
        """
        sql = graph.transpile(query, vlp_rendering_mode="procedural", materialization_strategy="numbered_views")
        print(f"\n=== SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["src"] for row in rows}

        # Carol<-Bob, Bob<-Alice, Bob<-Eve, Eve<-Dave
        expected = {"Bob", "Alice", "Eve", "Dave"}
        assert results == expected, (
            f"Expected {expected}, got {results}"
        )

    def test_min_hops_filtering(self, spark, graph):
        """*2..3 should skip depth-1 results."""
        query = """
        MATCH (a:Person)-[:KNOWS*2..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(query, vlp_rendering_mode="procedural", materialization_strategy="numbered_views")
        print(f"\n=== SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}

        # Depth-1: Bob, Dave (skipped)
        # Depth-2: Carol, Eve
        # Depth-3: (Bob already visited)
        expected = {"Carol", "Eve"}
        assert results == expected, (
            f"Expected {expected}, got {results}"
        )

    def test_global_visited_vs_cte_paths(self, spark, graph):
        """Procedural BFS should return fewer rows than CTE (unique nodes vs all paths)."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id AS dst
        """

        sql_proc = graph.transpile(
            query, vlp_rendering_mode="procedural", materialization_strategy="numbered_views",
        )
        sql_cte = graph.transpile(
            query, vlp_rendering_mode="cte",
        )
        print(f"\n=== Procedural SQL ===\n{sql_proc}")
        print(f"\n=== CTE SQL ===\n{sql_cte}")

        rows_proc = spark.sql(sql_proc).collect()
        rows_cte = spark.sql(sql_cte).collect()

        # CTE mode finds all paths (may include Bob at depth 1 AND depth 3)
        # Procedural mode with global visited finds each node once
        assert len(rows_proc) <= len(rows_cte), (
            f"Procedural ({len(rows_proc)} rows) should have "
            f"<= CTE ({len(rows_cte)} rows)"
        )

    def test_unwind_relationships_returns_edges(self, spark, graph):
        """UNWIND relationships(path) should return edge data."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        UNWIND relationships(path) AS r
        RETURN r.src AS edge_src, r.dst AS edge_dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        edges = {(row["edge_src"], row["edge_dst"]) for row in rows}

        # Alice->Bob, Alice->Dave (depth 1)
        # Bob->Carol, Dave->Eve (depth 2)
        # Eve->Bob already visited, not discovered (depth 3)
        expected = {
            ("Alice", "Bob"),
            ("Alice", "Dave"),
            ("Bob", "Carol"),
            ("Dave", "Eve"),
        }
        assert edges == expected, f"Expected {expected}, got {edges}"


class TestBidirectionalProceduralBFSPySpark:
    """Bidirectional procedural BFS tests with PySpark execution.

    When both source AND target have equality filters, the
    bidirectional optimizer activates automatically. These tests
    verify that the reachable-set pruning approach produces
    correct results.

    Test Graph:
        Alice ---KNOWS---> Bob ---KNOWS---> Carol
          |                 ^
          +---KNOWS---> Dave ---KNOWS---> Eve ---KNOWS---> Bob
    """

    def test_bidir_directed_alice_to_carol(self, spark, graph):
        """Bidirectional: Alice->Carol should find Carol at depth 2."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice' AND b.node_id = 'Carol'
        RETURN b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Bidir SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}

        # Alice->Bob->Carol: depth 2
        assert results == {"Carol"}, (
            f"Expected {{'Carol'}}, got {results}"
        )

    def test_bidir_directed_alice_to_eve(self, spark, graph):
        """Bidirectional: Alice->Eve via Alice->Dave->Eve."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice' AND b.node_id = 'Eve'
        RETURN b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Bidir SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}

        # Alice->Dave->Eve: depth 2
        assert results == {"Eve"}, (
            f"Expected {{'Eve'}}, got {results}"
        )

    def test_bidir_no_path(self, spark, graph):
        """Bidirectional: Carol->Alice has no directed path."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Carol' AND b.node_id = 'Alice'
        RETURN b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Bidir SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) == 0, (
            f"Expected empty result, got {rows}"
        )

    def test_bidir_same_result_as_unidirectional(
        self, spark, graph
    ):
        """Bidirectional should produce same reachable set."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice' AND b.node_id = 'Carol'
        RETURN DISTINCT b.node_id AS dst
        """
        # Bidirectional (auto mode — optimizer will enable it)
        sql_bidir = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        # Force unidirectional
        sql_unidir = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
            bidirectional_mode="off",
        )
        print(f"\n=== Bidir SQL ===\n{sql_bidir}")
        print(f"\n=== Unidir SQL ===\n{sql_unidir}")

        rows_bidir = spark.sql(sql_bidir).collect()
        rows_unidir = spark.sql(sql_unidir).collect()

        set_bidir = {row["dst"] for row in rows_bidir}
        set_unidir = {row["dst"] for row in rows_unidir}

        assert set_bidir == set_unidir, (
            f"Bidir {set_bidir} != Unidir {set_unidir}"
        )


# ======================================================================
# Multi-type edge fixtures
# ======================================================================


@pytest.fixture(scope="module")
def multi_spark():
    """Spark session for multi-type tests (separate to avoid view conflicts)."""
    spark = (
        SparkSession.builder
        .appName("ProceduralBFS_MultiType_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.scripting.enabled", "true")
        .getOrCreate()
    )
    # Create multi-type graph:
    #   Nodes: Alice(Person), Bob(Person), Carol(Person),
    #          Dave(Person), Acme(Company), BigCo(Company)
    #   Edges:
    #     Alice -KNOWS->   Bob   (amount=100)
    #     Bob   -KNOWS->   Carol (amount=200)
    #     Alice -WORKS_AT-> Acme (amount=50)
    #     Carol -WORKS_AT-> Acme (amount=60)
    #     Alice -KNOWS->   Dave  (amount=150)
    #     Dave  -WORKS_AT-> BigCo(amount=70)
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW mt_nodes AS
        SELECT * FROM VALUES
            ('Alice', 'Person',  25),
            ('Bob',   'Person',  30),
            ('Carol', 'Person',  35),
            ('Dave',  'Person',  28),
            ('Acme',  'Company', 0),
            ('BigCo', 'Company', 0)
        AS t(node_id, node_type, age)
    """)
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW mt_edges AS
        SELECT * FROM VALUES
            ('Alice', 'Bob',   'KNOWS',    100),
            ('Bob',   'Carol', 'KNOWS',    200),
            ('Alice', 'Acme',  'WORKS_AT', 50),
            ('Carol', 'Acme',  'WORKS_AT', 60),
            ('Alice', 'Dave',  'KNOWS',    150),
            ('Dave',  'BigCo', 'WORKS_AT', 70)
        AS t(src, dst, relationship_type, amount)
    """)
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def multi_graph(multi_spark):
    """GraphContext with Person + Company node types, KNOWS + WORKS_AT edges."""
    from gsql2rsql import GraphContext

    ctx = GraphContext(
        spark=multi_spark,
        nodes_table="mt_nodes",
        edges_table="mt_edges",
        node_id_col="node_id",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={"age": int},
        extra_edge_attrs={"amount": int},
    )
    return ctx


class TestMultiTypeProceduralBFSPySpark:
    """Procedural BFS tests with multi-type edges (KNOWS + WORKS_AT).

    These tests verify the operator-precedence fix for WHERE clauses
    with OR-joined type filters AND other conditions.

    Graph:
        Alice ---KNOWS---> Bob ---KNOWS---> Carol
          |                                  |
          +---WORKS_AT---> Acme <---WORKS_AT-+
          |
          +---KNOWS---> Dave ---WORKS_AT---> BigCo
    """

    def test_multi_type_directed_bfs(self, multi_spark, multi_graph):
        """BFS with KNOWS|WORKS_AT should find all reachable nodes."""
        query = """
        MATCH (a:Person)-[:KNOWS|WORKS_AT*1..3]->(b)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = multi_graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Multi-type SQL ===\n{sql}")

        result = multi_spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}

        # Depth 1: Bob (KNOWS), Acme (WORKS_AT), Dave (KNOWS)
        # Depth 2: Carol (KNOWS from Bob), BigCo (WORKS_AT from Dave)
        #          Acme (WORKS_AT from Carol) — already visited
        # Depth 3: Acme from Carol — already visited
        expected = {"Bob", "Acme", "Dave", "Carol", "BigCo"}
        assert results == expected, (
            f"Expected {expected}, got {results}"
        )

    def test_multi_type_single_type_filter(self, multi_spark, multi_graph):
        """BFS with only KNOWS should not follow WORKS_AT edges."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = multi_graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Single-type SQL ===\n{sql}")

        result = multi_spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}

        # Only KNOWS edges: Alice->Bob, Alice->Dave, Bob->Carol
        expected = {"Bob", "Dave", "Carol"}
        assert results == expected, (
            f"Expected {expected}, got {results}"
        )

    def test_multi_type_vs_cte(self, multi_spark, multi_graph):
        """Multi-type procedural BFS should match CTE results (modulo duplicates)."""
        query = """
        MATCH (a:Person)-[:KNOWS|WORKS_AT*1..3]->(b)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql_proc = multi_graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        sql_cte = multi_graph.transpile(
            query,
            vlp_rendering_mode="cte",
        )
        print(f"\n=== Procedural SQL ===\n{sql_proc}")
        print(f"\n=== CTE SQL ===\n{sql_cte}")

        rows_proc = multi_spark.sql(sql_proc).collect()
        rows_cte = multi_spark.sql(sql_cte).collect()

        set_proc = {row["dst"] for row in rows_proc}
        set_cte = {row["dst"] for row in rows_cte}

        # Both modes should find the same set of reachable nodes
        assert set_proc == set_cte, (
            f"Procedural {set_proc} != CTE {set_cte}"
        )

    def test_multi_type_cross_type_traversal(self, multi_spark, multi_graph):
        """Cross-type BFS: Person -WORKS_AT-> Company."""
        query = """
        MATCH (a:Person)-[:WORKS_AT*1..2]->(b:Company)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS company
        """
        sql = multi_graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Cross-type SQL ===\n{sql}")

        result = multi_spark.sql(sql)
        rows = result.collect()
        results = {row["company"] for row in rows}

        # Alice -WORKS_AT-> Acme (depth 1)
        # No WORKS_AT->WORKS_AT paths exist, so only depth 1
        expected = {"Acme"}
        assert results == expected, (
            f"Expected {expected}, got {results}"
        )
