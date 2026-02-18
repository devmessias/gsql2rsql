"""PySpark tests for P0: VLP node type filters on source/sink.

Verifies that variable-length path queries with typed nodes apply
node_type filters to source and sink JOINs.

Test Graph (multi-type, designed to expose the bug):
    Nodes:
        Alice (Person), Bob (Person), Carol (Person),
        Acme (Company), BigCo (Company)

    Edges (all KNOWS):
        Alice -> Bob     (Person -> Person)
        Bob   -> Acme    (Person -> Company)  <- non-Person intermediate
        Acme  -> Carol   (Company -> Person)
        Alice -> BigCo   (Person -> Company)

Without type filters on sink JOIN, queries like:
    MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
    WHERE a.node_id = 'Alice'
would return Acme and BigCo (Company nodes) alongside Person nodes.
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
        .appName("VLP_TypeFilter_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.scripting.enabled", "true")
        .getOrCreate()
    )
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW type_nodes AS
        SELECT * FROM VALUES
            ('Alice', 'Person'),
            ('Bob',   'Person'),
            ('Carol', 'Person'),
            ('Acme',  'Company'),
            ('BigCo', 'Company')
        AS t(node_id, node_type)
    """)
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW type_edges AS
        SELECT * FROM VALUES
            ('Alice', 'Bob',   'KNOWS'),
            ('Bob',   'Acme',  'KNOWS'),
            ('Acme',  'Carol', 'KNOWS'),
            ('Alice', 'BigCo', 'KNOWS')
        AS t(src, dst, relationship_type)
    """)
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph(spark):
    from gsql2rsql import GraphContext

    ctx = GraphContext(
        spark=spark,
        nodes_table="type_nodes",
        edges_table="type_edges",
        node_id_col="node_id",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        edge_src_col="src",
        edge_dst_col="dst",
    )
    ctx.set_types(
        node_types=["Person", "Company"],
        edge_types=["KNOWS"],
    )
    return ctx


class TestVLPTypeFiltersCTE:
    """VLP type filter tests using WITH RECURSIVE CTE renderer."""

    def test_vlp_sink_any_type(self, spark, graph):
        """Source=Person, sink=any -> should return all reachable nodes."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL (sink=any) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}
        expected = {"Bob", "Acme", "Carol", "BigCo"}
        assert results == expected, f"Expected {expected}, got {results}"

    def test_vlp_sink_person_filter(self, spark, graph):
        """Source=Person, sink=Person -> Acme/BigCo must be filtered out."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL (sink=Person) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}
        expected = {"Bob", "Carol"}
        assert results == expected, (
            f"Expected {expected} (Person only), got {results}. "
            "Company nodes should be filtered out by sink type filter."
        )

    def test_vlp_sink_company_filter(self, spark, graph):
        """Source=Person, sink=Company -> only Company nodes returned."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Company)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL (sink=Company) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}
        expected = {"Acme", "BigCo"}
        assert results == expected, (
            f"Expected {expected} (Company only), got {results}. "
            "Person nodes should be filtered out by sink type filter."
        )


class TestVLPTypeFiltersProcedural:
    """VLP type filter tests using procedural BFS renderer."""

    def test_vlp_sink_person_filter_procedural(self, spark, graph):
        """Same as CTE test but with procedural BFS renderer."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== SQL (procedural, sink=Person) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}
        expected = {"Bob", "Carol"}
        assert results == expected, (
            f"Expected {expected} (Person only), got {results}. "
            "Company nodes should be filtered out by sink type filter."
        )

    def test_vlp_sink_company_filter_procedural(self, spark, graph):
        """Procedural BFS: sink=Company -> only Company nodes."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Company)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== SQL (procedural, sink=Company) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}
        expected = {"Acme", "BigCo"}
        assert results == expected, (
            f"Expected {expected} (Company only), got {results}. "
            "Person nodes should be filtered out by sink type filter."
        )

    def test_vlp_cross_renderer_consistency(self, spark, graph):
        """CTE and procedural BFS should produce identical results."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql_cte = graph.transpile(query)
        sql_proc = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )

        rows_cte = {r["dst"] for r in spark.sql(sql_cte).collect()}
        rows_proc = {r["dst"] for r in spark.sql(sql_proc).collect()}
        assert rows_cte == rows_proc, (
            f"CTE results {rows_cte} != procedural results {rows_proc}"
        )
