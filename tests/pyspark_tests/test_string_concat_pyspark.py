"""PySpark tests for P1b: String + translated to CONCAT().

Cypher uses + for both numeric addition and string concatenation.
Spark SQL requires CONCAT() for strings. The expression renderer must
detect string operands and emit CONCAT() instead of +.
"""

import pytest
from pyspark.sql import SparkSession

from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .appName("StringConcat_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW concat_nodes AS
        SELECT * FROM VALUES
            ('Alice', 'Person', 'Alice'),
            ('Bob',   'Person', 'Bob')
        AS t(node_id, node_type, name)
    """)
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW concat_edges AS
        SELECT * FROM VALUES
            ('Alice', 'Bob', 'KNOWS')
        AS t(src, dst, relationship_type)
    """)
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph(spark):
    ctx = GraphContext(
        spark=spark,
        nodes_table="concat_nodes",
        edges_table="concat_edges",
        node_id_col="node_id",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={"name": str},
    )
    ctx.set_types(
        node_types=["Person"],
        edge_types=["KNOWS"],
    )
    return ctx


class TestStringConcat:
    """Tests for Cypher + string concatenation → CONCAT()."""

    def test_string_literal_concat(self, spark, graph):
        """String literal concatenation within a MATCH context."""
        query = """
        MATCH (a:Person)
        WHERE a.node_id = 'Alice'
        RETURN 'hello' + ' ' + 'world' AS s
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL (literal concat) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        assert rows[0]["s"] == "hello world"

    def test_property_plus_literal(self, spark, graph):
        """Property + string literal concatenation."""
        query = """
        MATCH (a:Person)
        WHERE a.node_id = 'Alice'
        RETURN a.name + '!' AS greeting
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL (property + literal) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        assert rows[0]["greeting"] == "Alice!"

    def test_numeric_add_unchanged(self, spark, graph):
        """Numeric + should remain as addition, not CONCAT."""
        query = """
        MATCH (a:Person)
        WHERE a.node_id = 'Alice'
        RETURN 1 + 2 AS n
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL (numeric add) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        assert rows[0]["n"] == 3

    def test_chained_property_concat(self, spark, graph):
        """Property + literal + property chained concatenation."""
        query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        RETURN a.name + ' -> ' + b.name AS label
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL (chained concat) ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        assert rows[0]["label"] == "Alice -> Bob"
