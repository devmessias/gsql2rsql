"""PySpark integration tests for BFS edge filter pushdown as barrier.

Tests whether ALL(r IN relationships(path) WHERE ...) with edge-level
properties (like src_is_hub) correctly acts as a traversal barrier
DURING expansion, not as a post-filter.

Test Graph:
    N1 (hub=F) --> N2 (hub=F) --> N3 (hub=T) --> N4 (hub=F) --> N5 (hub=T)

    N6 (hub=F) --> N7 (hub=F) --> N8 (hub=F) --> N9 (hub=T)
                                    |                ^
                                    +---> N5 (hub=T) |

Edge table is ENRICHED with src_is_hub (denormalized from source node):
    src | dst | src_is_hub
    N1  | N2  | false
    N2  | N3  | false
    N3  | N4  | true     ← edge FROM hub, blocked by barrier
    N4  | N5  | false
    N6  | N7  | false
    N7  | N8  | false
    N8  | N9  | false
    N8  | N5  | false

Key test:
  ALL(r IN relationships(path) WHERE NOT r.src_is_hub)
  = "don't use edges starting from a hub"
  = barrier: reach hubs but don't expand FROM them

  From N1 without barrier: N2, N3, N4, N5 all reachable
  From N1 with barrier:    N2, N3 reachable (N3→N4 blocked, src_is_hub=true)
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
        .appName("BFS_EdgeBarrier_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.scripting.enabled", "true")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW hub_nodes AS
        SELECT * FROM VALUES
            ('N1', 'Station', false),
            ('N2', 'Station', false),
            ('N3', 'Station', true),
            ('N4', 'Station', false),
            ('N5', 'Station', true),
            ('N6', 'Station', false),
            ('N7', 'Station', false),
            ('N8', 'Station', false),
            ('N9', 'Station', true)
        AS t(node_id, node_type, is_hub)
    """)

    # Enriched edge table: src_is_hub is denormalized from source node
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW hub_edges AS
        SELECT * FROM VALUES
            ('N1', 'N2', 'LINK', false),
            ('N2', 'N3', 'LINK', false),
            ('N3', 'N4', 'LINK', true),
            ('N4', 'N5', 'LINK', false),
            ('N6', 'N7', 'LINK', false),
            ('N7', 'N8', 'LINK', false),
            ('N8', 'N9', 'LINK', false),
            ('N8', 'N5', 'LINK', false)
        AS t(src, dst, relationship_type, src_is_hub)
    """)
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph(spark):
    from gsql2rsql import GraphContext

    ctx = GraphContext(
        spark=spark,
        nodes_table="hub_nodes",
        edges_table="hub_edges",
        node_id_col="node_id",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={"is_hub": bool},
        extra_edge_attrs={"src_is_hub": bool},
    )
    return ctx


class TestEdgeBarrierProceduralBFS:
    """Test ALL(r IN relationships(path) WHERE NOT r.src_is_hub) as barrier.

    Uses procedural BFS mode (numbered_views).
    The edge filter should be applied DURING expansion, preventing
    the frontier from including nodes reached via edges from hubs.
    """

    def test_without_barrier_reaches_all(self, spark, graph):
        """Without barrier filter, BFS from N1 reaches all downstream nodes."""
        query = """
        MATCH (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== No barrier SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}
        print(f"No barrier results: {results}")

        # Without barrier: N1→N2→N3→N4→N5, all reachable
        assert results == {"N2", "N3", "N4", "N5"}, (
            f"Expected all downstream nodes, got {results}"
        )

    def test_barrier_stops_at_hub(self, spark, graph):
        """With barrier ALL(... WHERE NOT r.src_is_hub), BFS stops at hub N3.

        N3 is reachable (edge N2→N3 has src_is_hub=false).
        N4 is NOT reachable (edge N3→N4 has src_is_hub=true → blocked).
        N5 is NOT reachable (only reachable through N3→N4→N5).
        """
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND ALL(r IN relationships(path) WHERE NOT r.src_is_hub)
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Barrier SQL (procedural) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}
        print(f"Barrier results: {results}")

        # With barrier: N3→N4 blocked (src_is_hub=true), so only N2 and N3
        assert results == {"N2", "N3"}, (
            f"Expected {{'N2', 'N3'}} (barrier at N3), got {results}. "
            f"If N4/N5 present, barrier is not applied during expansion."
        )

    def test_barrier_plus_sink_filter(self, spark, graph):
        """Barrier + sink filter: only hub endpoints reachable without traversing a hub.

        Combines:
          - ALL(... WHERE NOT r.src_is_hub) → barrier: don't expand from hubs
          - b.is_hub = true → sink filter: only return hubs as endpoints
        """
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND b.is_hub = true
          AND ALL(r IN relationships(path) WHERE NOT r.src_is_hub)
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
            bidirectional_mode="off",
        )
        print(f"\n=== Barrier + sink filter SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}
        print(f"Barrier + sink results: {results}")

        # Only N3 is a hub reachable without going through another hub
        assert results == {"N3"}, (
            f"Expected {{'N3'}}, got {results}"
        )

    def test_barrier_from_n6_multiple_hubs(self, spark, graph):
        """From N6 with barrier: N9 and N5 are hubs reachable without traversing a hub.

        N6→N7→N8→N9 (all edges have src_is_hub=false)
        N6→N7→N8→N5 (all edges have src_is_hub=false)
        """
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N6'
          AND b.is_hub = true
          AND ALL(r IN relationships(path) WHERE NOT r.src_is_hub)
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Barrier from N6 SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}
        print(f"Barrier from N6 results: {results}")

        # Both N9 and N5 reachable from N6 without traversing any hub
        assert results == {"N5", "N9"}, (
            f"Expected {{'N5', 'N9'}}, got {results}"
        )


class TestEdgeBarrierCTE:
    """Same tests using CTE mode to verify barrier works in both renderers."""

    def test_cte_without_barrier(self, spark, graph):
        """CTE without barrier reaches all downstream nodes."""
        query = """
        MATCH (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(query, vlp_rendering_mode="cte")
        print(f"\n=== CTE no barrier ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}
        print(f"CTE no barrier: {results}")

        assert results == {"N2", "N3", "N4", "N5"}, (
            f"Expected all downstream, got {results}"
        )

    def test_cte_barrier_stops_at_hub(self, spark, graph):
        """CTE with barrier: same behavior as procedural."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND ALL(r IN relationships(path) WHERE NOT r.src_is_hub)
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(query, vlp_rendering_mode="cte")
        print(f"\n=== CTE barrier SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}
        print(f"CTE barrier results: {results}")

        assert results == {"N2", "N3"}, (
            f"Expected {{'N2', 'N3'}}, got {results}"
        )

    def test_cte_barrier_plus_sink(self, spark, graph):
        """CTE barrier + sink filter: only N3."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND b.is_hub = true
          AND ALL(r IN relationships(path) WHERE NOT r.src_is_hub)
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(query, vlp_rendering_mode="cte")
        print(f"\n=== CTE barrier + sink SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}
        print(f"CTE barrier + sink: {results}")

        assert results == {"N3"}, (
            f"Expected {{'N3'}}, got {results}"
        )


class TestEdgeBarrierConsistency:
    """Verify procedural and CTE produce identical results with barrier."""

    def test_barrier_procedural_equals_cte(self, spark, graph):
        """Both modes should return same results with barrier."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND ALL(r IN relationships(path) WHERE NOT r.src_is_hub)
        RETURN DISTINCT b.node_id AS dst
        """
        sql_proc = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        sql_cte = graph.transpile(query, vlp_rendering_mode="cte")

        set_proc = {row["dst"] for row in spark.sql(sql_proc).collect()}
        set_cte = {row["dst"] for row in spark.sql(sql_cte).collect()}

        assert set_proc == set_cte, (
            f"Procedural {set_proc} != CTE {set_cte}"
        )

    def test_barrier_plus_sink_procedural_equals_cte(self, spark, graph):
        """Both modes: barrier + sink filter."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND b.is_hub = true
          AND ALL(r IN relationships(path) WHERE NOT r.src_is_hub)
        RETURN DISTINCT b.node_id AS dst
        """
        sql_proc = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        sql_cte = graph.transpile(query, vlp_rendering_mode="cte")

        set_proc = {row["dst"] for row in spark.sql(sql_proc).collect()}
        set_cte = {row["dst"] for row in spark.sql(sql_cte).collect()}

        assert set_proc == set_cte, (
            f"Procedural {set_proc} != CTE {set_cte}"
        )


class TestIsTerminatorProceduralBFS:
    """Test is_terminator() directive as a traversal barrier.

    is_terminator(b.is_hub = true) means:
    - Nodes with is_hub=true ARE included in results (they're reachable)
    - BFS does NOT expand FROM these nodes (they're barriers)

    Graph:
        N1(F)->N2(F)->N3(T)->N4(F)->N5(T)
        N6(F)->N7(F)->N8(F)->N9(T)
                       N8(F)->N5(T)
    """

    def test_without_terminator_reaches_all(self, spark, graph):
        """Without is_terminator, BFS from N1 reaches all downstream."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== No terminator SQL ===\n{sql}")
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        assert results == {"N2", "N3", "N4", "N5"}

    def test_terminator_stops_at_hub(self, spark, graph):
        """is_terminator(b.is_hub = true): N3 reached but not expanded.

        N1->N2 (N2 not hub, expand)
        N2->N3 (N3 is hub, reach it but DON'T expand from it)
        So N4 and N5 are NOT reachable.
        """
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND is_terminator(b.is_hub = true)
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Terminator SQL (procedural) ===\n{sql}")
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        print(f"Terminator results: {results}")
        assert results == {"N2", "N3"}, (
            f"Expected {{'N2', 'N3'}}, got {results}"
        )

    def test_terminator_plus_sink_filter(self, spark, graph):
        """is_terminator + sink filter: only hub endpoints reachable."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND b.is_hub = true
          AND is_terminator(b.is_hub = true)
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
            bidirectional_mode="off",
        )
        print(f"\n=== Terminator + sink SQL ===\n{sql}")
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        assert results == {"N3"}, f"Expected {{'N3'}}, got {results}"

    def test_terminator_from_n6(self, spark, graph):
        """From N6: N5 and N9 are hubs reachable without crossing hub."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N6'
          AND b.is_hub = true
          AND is_terminator(b.is_hub = true)
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Terminator from N6 SQL ===\n{sql}")
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        assert results == {"N5", "N9"}, (
            f"Expected {{'N5', 'N9'}}, got {results}"
        )


class TestIsTerminatorCTE:
    """Same is_terminator tests using CTE mode."""

    def test_cte_terminator_stops_at_hub(self, spark, graph):
        """CTE with is_terminator: same as procedural."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND is_terminator(b.is_hub = true)
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(query, vlp_rendering_mode="cte")
        print(f"\n=== CTE terminator SQL ===\n{sql}")
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        assert results == {"N2", "N3"}, (
            f"Expected {{'N2', 'N3'}}, got {results}"
        )

    def test_cte_terminator_plus_sink(self, spark, graph):
        """CTE terminator + sink filter: only N3."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND b.is_hub = true
          AND is_terminator(b.is_hub = true)
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(query, vlp_rendering_mode="cte")
        print(f"\n=== CTE terminator + sink SQL ===\n{sql}")
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        assert results == {"N3"}, f"Expected {{'N3'}}, got {results}"


class TestIsTerminatorConsistency:
    """Verify procedural and CTE produce identical results."""

    def test_terminator_procedural_equals_cte(self, spark, graph):
        """Both modes should produce same results."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND is_terminator(b.is_hub = true)
        RETURN DISTINCT b.node_id AS dst
        """
        sql_proc = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        sql_cte = graph.transpile(query, vlp_rendering_mode="cte")

        set_proc = {row["dst"] for row in spark.sql(sql_proc).collect()}
        set_cte = {row["dst"] for row in spark.sql(sql_cte).collect()}

        assert set_proc == set_cte, (
            f"Procedural {set_proc} != CTE {set_cte}"
        )

    def test_terminator_edge_filter_combined(self, spark, graph):
        """Both ALL(r...) edge filter and is_terminator can coexist."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND is_terminator(b.is_hub = true)
          AND ALL(r IN relationships(path) WHERE NOT r.src_is_hub)
        RETURN DISTINCT b.node_id AS dst
        """
        sql_proc = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        sql_cte = graph.transpile(query, vlp_rendering_mode="cte")

        set_proc = {row["dst"] for row in spark.sql(sql_proc).collect()}
        set_cte = {row["dst"] for row in spark.sql(sql_cte).collect()}

        # Both barrier + edge filter: should give N2, N3
        assert set_proc == set_cte, (
            f"Procedural {set_proc} != CTE {set_cte}"
        )
        assert set_proc == {"N2", "N3"}, (
            f"Expected {{'N2', 'N3'}}, got {set_proc}"
        )


class TestIsTerminatorWithINBlacklist:
    """Test is_terminator() with IN list as a node blacklist.

    Uses is_terminator(b.node_id IN [...]) to define a set of
    barrier nodes by ID, simulating a blacklist pattern.

    Graph:
        N1(F)->N2(F)->N3(T)->N4(F)->N5(T)
        N6(F)->N7(F)->N8(F)->N9(T)
                       N8(F)->N5(T)
    """

    def test_blacklist_single_node_procedural(self, spark, graph):
        """Blacklist N3: BFS from N1 stops at N3."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND is_terminator(b.node_id IN ['N3'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        # N3 is reached but not expanded, so N4/N5 unreachable
        assert results == {"N2", "N3"}, (
            f"Expected {{'N2', 'N3'}}, got {results}"
        )

    def test_blacklist_multiple_nodes_procedural(self, spark, graph):
        """Blacklist N2 and N3: BFS from N1 stops at N2."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND is_terminator(b.node_id IN ['N2', 'N3'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        # N2 is reached (barrier) but not expanded -> N3/N4/N5 unreachable
        assert results == {"N2"}, (
            f"Expected {{'N2'}}, got {results}"
        )

    def test_blacklist_from_n6_procedural(self, spark, graph):
        """Blacklist N8: from N6, BFS stops at N8.

        N6->N7->N8(barrier), so N9 and N5 are NOT reachable.
        """
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N6'
          AND is_terminator(b.node_id IN ['N8'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        assert results == {"N7", "N8"}, (
            f"Expected {{'N7', 'N8'}}, got {results}"
        )

    def test_blacklist_cte_matches_procedural(self, spark, graph):
        """CTE and procedural produce same results with IN blacklist."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND is_terminator(b.node_id IN ['N3'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql_proc = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        sql_cte = graph.transpile(query, vlp_rendering_mode="cte")

        set_proc = {row["dst"] for row in spark.sql(sql_proc).collect()}
        set_cte = {row["dst"] for row in spark.sql(sql_cte).collect()}

        assert set_proc == set_cte == {"N2", "N3"}, (
            f"Proc={set_proc}, CTE={set_cte}"
        )


class TestIsTerminatorCompoundConditions:
    """Test is_terminator() with AND/OR compound predicates.

    Graph:
        N1(hub=F)->N2(hub=F)->N3(hub=T)->N4(hub=F)->N5(hub=T)
    """

    def test_and_condition_procedural(self, spark, graph):
        """is_terminator(b.is_hub = true AND b.node_id = 'N3').

        Only N3 matches (hub AND specific ID).
        N5 is hub but node_id != 'N3', so N5 is NOT a barrier.
        From N1: N2, N3 (barrier), N4, N5 all reachable.
        N3 doesn't block because AND narrows the barrier to just N3,
        but N3 IS a barrier so no expansion from N3.
        N4/N5 would be reachable only through N3, so they're blocked.
        """
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND is_terminator(b.is_hub = true AND b.node_id = 'N3')
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        # N3 is barrier (hub=T AND id='N3'), not expanded
        # N4/N5 only reachable via N3, so blocked
        assert results == {"N2", "N3"}, (
            f"Expected {{'N2', 'N3'}}, got {results}"
        )

    def test_or_condition_procedural(self, spark, graph):
        """is_terminator(b.is_hub = true OR b.node_id = 'N2').

        Barriers: N2 (id match), N3 (hub), N5 (hub), N9 (hub).
        From N1: N2 is first neighbor and is a barrier -> not expanded.
        """
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND is_terminator(b.is_hub = true OR b.node_id = 'N2')
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        # N2 is barrier (node_id='N2'), reached but not expanded
        assert results == {"N2"}, (
            f"Expected {{'N2'}}, got {results}"
        )

    def test_and_condition_cte(self, spark, graph):
        """CTE with AND compound: same result as procedural."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND is_terminator(b.is_hub = true AND b.node_id = 'N3')
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(query, vlp_rendering_mode="cte")
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        assert results == {"N2", "N3"}, (
            f"Expected {{'N2', 'N3'}}, got {results}"
        )

    def test_or_condition_cte(self, spark, graph):
        """CTE with OR compound: same result as procedural."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND is_terminator(b.is_hub = true OR b.node_id = 'N2')
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(query, vlp_rendering_mode="cte")
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        assert results == {"N2"}, (
            f"Expected {{'N2'}}, got {results}"
        )

    def test_hub_or_blacklist_procedural(self, spark, graph):
        """is_terminator(b.is_hub = true OR b.node_id IN ['N7']).

        Barriers: hubs (N3, N5, N9) + blacklisted (N7).
        From N6: N7 is first neighbor and blacklisted -> barrier.
        N8/N9/N5 unreachable.
        """
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N6'
          AND is_terminator(b.is_hub = true OR b.node_id IN ['N7'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        assert results == {"N7"}, (
            f"Expected {{'N7'}}, got {results}"
        )

    def test_hub_or_blacklist_from_n1_procedural(self, spark, graph):
        """is_terminator(b.is_hub = true OR b.node_id IN ['N2']).

        From N1: N2 is blacklisted -> barrier, not expanded.
        N3/N4/N5 unreachable.
        """
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND is_terminator(b.is_hub = true OR b.node_id IN ['N2'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        assert results == {"N2"}, (
            f"Expected {{'N2'}}, got {results}"
        )

    def test_hub_or_blacklist_from_n6_reaches_more(self, spark, graph):
        """is_terminator(b.is_hub = true OR b.node_id IN ['N8']).

        From N6: N7 ok (expand), N8 blacklisted (barrier).
        N9/N5 unreachable (only via N8).
        """
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N6'
          AND is_terminator(b.is_hub = true OR b.node_id IN ['N8'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        assert results == {"N7", "N8"}, (
            f"Expected {{'N7', 'N8'}}, got {results}"
        )

    def test_hub_or_blacklist_cte_matches(self, spark, graph):
        """CTE produces same result as procedural for hub+blacklist."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N6'
          AND is_terminator(b.is_hub = true OR b.node_id IN ['N7'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql_proc = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        sql_cte = graph.transpile(query, vlp_rendering_mode="cte")

        set_proc = {row["dst"] for row in spark.sql(sql_proc).collect()}
        set_cte = {row["dst"] for row in spark.sql(sql_cte).collect()}

        assert set_proc == set_cte == {"N7"}, (
            f"Proc={set_proc}, CTE={set_cte}"
        )


class TestIsTerminatorWithEdgeBlacklist:
    """Test is_terminator(b.is_hub) combined with edge-level dst blacklist.

    Two independent barriers:
      1. is_terminator(b.is_hub = true) — node barrier: hubs reached but not expanded
      2. ALL(r IN relationships(path) WHERE NOT r.dst_node_id IN [...]) — edge barrier:
         edges whose dst is blacklisted are never traversed

    Requires ``dst`` exposed as an extra edge attribute so ``r.dst`` resolves.

    Graph:
        N1(hub=F)->N2(hub=F)->N3(hub=T)->N4(hub=F)->N5(hub=T)
        N6(hub=F)->N7(hub=F)->N8(hub=F)->N9(hub=T)
                               N8(hub=F)->N5(hub=T)
    """

    def test_from_n6_blacklist_n9_procedural(self, spark, graph):
        """is_terminator blocks hubs, edge blacklist blocks N9.

        From N6 without filters: {N7, N8, N9, N5}
        is_terminator alone:     {N7, N8, N9, N5} (hubs reached, not expanded)
        Edge blacklist [N9]:     N8->N9 blocked, N8->N5 allowed
        Combined:                {N7, N8, N5} — N9 never reached, N5 reached (hub barrier)
        """
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N6'
          AND is_terminator(b.is_hub = true)
          AND ALL(r IN relationships(path) WHERE NOT r.dst IN ['N9'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        assert results == {"N7", "N8", "N5"}, (
            f"Expected {{'N7', 'N8', 'N5'}}, got {results}"
        )

    def test_from_n1_blacklist_n3_procedural(self, spark, graph):
        """Edge blacklist [N3] is MORE restrictive than is_terminator.

        is_terminator alone: {N2, N3} (N3 hub, reached but not expanded)
        Edge blacklist [N3]: N2->N3 blocked, N3 never reached
        Combined:            {N2} — edge blacklist prevents reaching N3 at all
        """
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND is_terminator(b.is_hub = true)
          AND ALL(r IN relationships(path) WHERE NOT r.dst IN ['N3'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        assert results == {"N2"}, (
            f"Expected {{'N2'}}, got {results}"
        )

    def test_from_n6_blacklist_n5_procedural(self, spark, graph):
        """Edge blacklist [N5] blocks N8->N5, is_terminator blocks N9 expansion.

        From N6: N7 ok, N8 ok, N8->N9 (hub barrier), N8->N5 (edge blocked)
        Combined: {N7, N8, N9}
        """
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N6'
          AND is_terminator(b.is_hub = true)
          AND ALL(r IN relationships(path) WHERE NOT r.dst IN ['N5'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        assert results == {"N7", "N8", "N9"}, (
            f"Expected {{'N7', 'N8', 'N9'}}, got {results}"
        )

    def test_from_n6_blacklist_n9_cte(self, spark, graph):
        """CTE: same as procedural for hub + edge blacklist."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N6'
          AND is_terminator(b.is_hub = true)
          AND ALL(r IN relationships(path) WHERE NOT r.dst IN ['N9'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(query, vlp_rendering_mode="cte")
        result = spark.sql(sql)
        results = {row["dst"] for row in result.collect()}
        assert results == {"N7", "N8", "N5"}, (
            f"Expected {{'N7', 'N8', 'N5'}}, got {results}"
        )

    def test_consistency_procedural_vs_cte(self, spark, graph):
        """Both renderers agree on hub + edge blacklist."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N6'
          AND is_terminator(b.is_hub = true)
          AND ALL(r IN relationships(path) WHERE NOT r.dst IN ['N9'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql_proc = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        sql_cte = graph.transpile(query, vlp_rendering_mode="cte")

        set_proc = {row["dst"] for row in spark.sql(sql_proc).collect()}
        set_cte = {row["dst"] for row in spark.sql(sql_cte).collect()}

        assert set_proc == set_cte == {"N7", "N8", "N5"}, (
            f"Proc={set_proc}, CTE={set_cte}"
        )


class TestCustomEdgeColumnNames:
    """Test VLP with non-standard edge src/dst column names.

    Uses 'origin' and 'destination' instead of 'src' and 'dst' to verify
    that r.origin / r.destination are accessible in
    ALL(r IN relationships(path) WHERE ...) and that NAMED_STRUCT
    deduplication works correctly with arbitrary column names.

    Same graph topology as the rest of the file:
        N1->N2->N3(hub)->N4->N5(hub)
        N6->N7->N8->N9(hub), N8->N5(hub)
    """

    @pytest.fixture(scope="class")
    def custom_spark(self, spark):
        """Create tables with 'origin'/'destination' column names."""
        spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW custom_edges AS
            SELECT * FROM VALUES
                ('N1', 'N2', 'LINK', false),
                ('N2', 'N3', 'LINK', false),
                ('N3', 'N4', 'LINK', true),
                ('N4', 'N5', 'LINK', false),
                ('N6', 'N7', 'LINK', false),
                ('N7', 'N8', 'LINK', false),
                ('N8', 'N9', 'LINK', false),
                ('N8', 'N5', 'LINK', false)
            AS t(origin, destination, relationship_type, src_is_hub)
        """)
        return spark

    @pytest.fixture(scope="class")
    def custom_graph(self, custom_spark):
        from gsql2rsql import GraphContext

        return GraphContext(
            spark=custom_spark,
            nodes_table="hub_nodes",
            edges_table="custom_edges",
            node_id_col="node_id",
            node_type_col="node_type",
            edge_type_col="relationship_type",
            edge_src_col="origin",
            edge_dst_col="destination",
            extra_node_attrs={"is_hub": bool},
            extra_edge_attrs={"src_is_hub": bool},
        )

    def test_basic_vlp_custom_cols_procedural(self, custom_spark, custom_graph):
        """Basic VLP should work with custom edge column names."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = custom_graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        results = {row["dst"] for row in custom_spark.sql(sql).collect()}
        assert results == {"N2", "N3", "N4", "N5"}

    def test_basic_vlp_custom_cols_cte(self, custom_spark, custom_graph):
        """Basic VLP should work with custom edge column names (CTE mode)."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = custom_graph.transpile(query, vlp_rendering_mode="cte")
        results = {row["dst"] for row in custom_spark.sql(sql).collect()}
        assert results == {"N2", "N3", "N4", "N5"}

    def test_edge_barrier_custom_cols_procedural(self, custom_spark, custom_graph):
        """Edge barrier with r.src_is_hub works with custom src/dst names."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND ALL(r IN relationships(path) WHERE NOT r.src_is_hub)
        RETURN DISTINCT b.node_id AS dst
        """
        sql = custom_graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        results = {row["dst"] for row in custom_spark.sql(sql).collect()}
        assert results == {"N2", "N3"}, f"Expected N2,N3 got {results}"

    def test_edge_barrier_custom_cols_cte(self, custom_spark, custom_graph):
        """Edge barrier with r.src_is_hub works with custom src/dst names (CTE)."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND ALL(r IN relationships(path) WHERE NOT r.src_is_hub)
        RETURN DISTINCT b.node_id AS dst
        """
        sql = custom_graph.transpile(query, vlp_rendering_mode="cte")
        results = {row["dst"] for row in custom_spark.sql(sql).collect()}
        assert results == {"N2", "N3"}, f"Expected N2,N3 got {results}"

    def test_r_destination_blacklist_procedural(self, custom_spark, custom_graph):
        """r.destination (custom dst col) accessible in edge filter."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N6'
          AND ALL(r IN relationships(path) WHERE NOT r.destination IN ['N9'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql = custom_graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        results = {row["dst"] for row in custom_spark.sql(sql).collect()}
        # N8->N9 blocked, but N8->N5 still reachable
        assert results == {"N7", "N8", "N5"}, f"Expected N7,N8,N5 got {results}"

    def test_r_destination_blacklist_cte(self, custom_spark, custom_graph):
        """r.destination (custom dst col) accessible in edge filter (CTE)."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N6'
          AND ALL(r IN relationships(path) WHERE NOT r.destination IN ['N9'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql = custom_graph.transpile(query, vlp_rendering_mode="cte")
        results = {row["dst"] for row in custom_spark.sql(sql).collect()}
        assert results == {"N7", "N8", "N5"}, f"Expected N7,N8,N5 got {results}"

    def test_r_origin_blacklist_procedural(self, custom_spark, custom_graph):
        """r.origin (custom src col) accessible in edge filter."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1'
          AND ALL(r IN relationships(path) WHERE NOT r.origin IN ['N3'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql = custom_graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        results = {row["dst"] for row in custom_spark.sql(sql).collect()}
        # N3->N4 blocked (origin=N3), so only N2, N3 reachable
        assert results == {"N2", "N3"}, f"Expected N2,N3 got {results}"

    def test_terminator_plus_r_destination_procedural(self, custom_spark, custom_graph):
        """is_terminator + r.destination blacklist with custom col names."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N6'
          AND is_terminator(b.is_hub = true)
          AND ALL(r IN relationships(path) WHERE NOT r.destination IN ['N9'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql = custom_graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        results = {row["dst"] for row in custom_spark.sql(sql).collect()}
        assert results == {"N7", "N8", "N5"}, f"Expected N7,N8,N5 got {results}"

    def test_terminator_plus_r_destination_cte(self, custom_spark, custom_graph):
        """is_terminator + r.destination blacklist with custom col names (CTE)."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N6'
          AND is_terminator(b.is_hub = true)
          AND ALL(r IN relationships(path) WHERE NOT r.destination IN ['N9'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql = custom_graph.transpile(query, vlp_rendering_mode="cte")
        results = {row["dst"] for row in custom_spark.sql(sql).collect()}
        assert results == {"N7", "N8", "N5"}, f"Expected N7,N8,N5 got {results}"

    def test_consistency_procedural_vs_cte(self, custom_spark, custom_graph):
        """Both renderers agree with custom column names."""
        query = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N6'
          AND is_terminator(b.is_hub = true)
          AND ALL(r IN relationships(path) WHERE NOT r.destination IN ['N9'])
        RETURN DISTINCT b.node_id AS dst
        """
        sql_proc = custom_graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        sql_cte = custom_graph.transpile(query, vlp_rendering_mode="cte")

        set_proc = {row["dst"] for row in custom_spark.sql(sql_proc).collect()}
        set_cte = {row["dst"] for row in custom_spark.sql(sql_cte).collect()}

        assert set_proc == set_cte == {"N7", "N8", "N5"}, (
            f"Proc={set_proc}, CTE={set_cte}"
        )
