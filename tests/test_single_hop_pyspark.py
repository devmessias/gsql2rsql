"""Test: Single-hop traversal patterns with real data verification.

Tests for:
1. Directed single-hop: (a)-[:REL]->(b)
2. Undirected single-hop: (a)-[:REL]-(b)
3. Untyped directed: (a)-[]->(b)
4. Untyped undirected: (a)-[]-(b)

Each test verifies actual returned values, not just query execution.

Test Graph:
    Alice ---KNOWS---> Bob ---KNOWS---> Carol
      |                 ^
      v                 |
    Dave ---KNOWS---> Eve ---DEFRAUDED---> Frank
                        |
                        v
                      Grace (DEFRAUDED)

Edges (directed):
  Alice -> Bob (KNOWS)
  Alice -> Dave (KNOWS)
  Bob -> Carol (KNOWS)
  Dave -> Eve (KNOWS)
  Eve -> Bob (KNOWS)
  Eve -> Frank (DEFRAUDED)
  Eve -> Grace (DEFRAUDED)
"""

import pytest
from pyspark.sql import SparkSession
from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    spark = (
        SparkSession.builder
        .appName("SingleHop_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph_data(spark):
    """Create test graph data with multiple edge types."""
    nodes_data = [
        ("Alice", "Person", 25),
        ("Bob", "Person", 30),
        ("Carol", "Person", 28),
        ("Dave", "Person", 35),
        ("Eve", "Person", 27),
        ("Frank", "Person", 40),
        ("Grace", "Person", 22),
    ]
    nodes_df = spark.createDataFrame(
        nodes_data, ["node_id", "node_type", "age"]
    )
    nodes_df.createOrReplaceTempView("nodes")

    edges_data = [
        ("Alice", "Bob", "KNOWS"),
        ("Alice", "Dave", "KNOWS"),
        ("Bob", "Carol", "KNOWS"),
        ("Dave", "Eve", "KNOWS"),
        ("Eve", "Bob", "KNOWS"),
        ("Eve", "Frank", "DEFRAUDED"),
        ("Eve", "Grace", "DEFRAUDED"),
    ]
    edges_df = spark.createDataFrame(
        edges_data, ["src", "dst", "relationship_type"]
    )
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
        extra_node_attrs={"age": int},
    )
    graph.set_types(
        node_types=["Person"],
        edge_types=["KNOWS", "DEFRAUDED"],
    )
    return graph


class TestDirectedSingleHop:
    """Tests for directed single-hop: (a)-[:REL]->(b)."""

    def test_directed_knows_from_alice(self, spark, graph_context):
        """Alice -[:KNOWS]-> should return Bob, Dave."""
        query = """
        MATCH (origem:Person)-[:KNOWS]->(destino:Person)
        WHERE origem.node_id = 'Alice'
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        # Verify count
        assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"

        # Verify exact values
        results = [(row["src"], row["dst"]) for row in rows]
        expected = [("Alice", "Bob"), ("Alice", "Dave")]
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ Directed KNOWS from Alice: {results}")

    def test_directed_defrauded_from_eve(self, spark, graph_context):
        """Eve -[:DEFRAUDED]-> should return Frank, Grace."""
        query = """
        MATCH (origem:Person)-[:DEFRAUDED]->(destino:Person)
        WHERE origem.node_id = 'Eve'
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = [(row["src"], row["dst"]) for row in rows]
        expected = [("Eve", "Frank"), ("Eve", "Grace")]
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ Directed DEFRAUDED from Eve: {results}")

    def test_directed_knows_all_edges(self, spark, graph_context):
        """All KNOWS edges should match exactly."""
        query = """
        MATCH (origem:Person)-[:KNOWS]->(destino:Person)
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY src, dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = {(row["src"], row["dst"]) for row in rows}
        expected = {
            ("Alice", "Bob"),
            ("Alice", "Dave"),
            ("Bob", "Carol"),
            ("Dave", "Eve"),
            ("Eve", "Bob"),
        }
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ All directed KNOWS edges: {results}")


class TestUndirectedSingleHop:
    """Tests for undirected single-hop: (a)-[:REL]-(b)."""

    def test_undirected_knows_from_bob(self, spark, graph_context):
        """Bob -[:KNOWS]- should return Alice, Carol, Eve (both directions).

        Edges involving Bob:
          Alice -> Bob (incoming)
          Bob -> Carol (outgoing)
          Eve -> Bob (incoming)
        """
        query = """
        MATCH (origem:Person)-[:KNOWS]-(destino:Person)
        WHERE origem.node_id = 'Bob'
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        print(f"\n=== SQL for undirected from Bob ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        expected = {"Alice", "Carol", "Eve"}
        assert results == expected, f"Expected {expected}, got {results}"

        # All rows should have src = Bob
        sources = {row["src"] for row in rows}
        assert sources == {"Bob"}, f"All sources should be Bob, got {sources}"

        print(f"✅ Undirected KNOWS from Bob: {results}")

    def test_undirected_knows_from_carol(self, spark, graph_context):
        """Carol -[:KNOWS]- should return only Bob (via Bob -> Carol backward).

        Carol has only one edge: Bob -> Carol
        Undirected should find: Bob
        """
        query = """
        MATCH (origem:Person)-[:KNOWS]-(destino:Person)
        WHERE origem.node_id = 'Carol'
        RETURN origem.node_id AS src, destino.node_id AS dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        expected = {"Bob"}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ Undirected KNOWS from Carol: {results}")

    def test_undirected_defrauded_from_frank(self, spark, graph_context):
        """Frank -[:DEFRAUDED]- should return Eve (via Eve -> Frank backward).

        Frank has only one edge: Eve -> Frank
        Undirected should find: Eve
        """
        query = """
        MATCH (origem:Person)-[:DEFRAUDED]-(destino:Person)
        WHERE origem.node_id = 'Frank'
        RETURN origem.node_id AS src, destino.node_id AS dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        expected = {"Eve"}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ Undirected DEFRAUDED from Frank: {results}")

    def test_undirected_defrauded_from_eve(self, spark, graph_context):
        """Eve -[:DEFRAUDED]- should return Frank, Grace (outgoing edges)."""
        query = """
        MATCH (origem:Person)-[:DEFRAUDED]-(destino:Person)
        WHERE origem.node_id = 'Eve'
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        expected = {"Frank", "Grace"}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ Undirected DEFRAUDED from Eve: {results}")


class TestUntypedDirected:
    """Tests for untyped directed: (a)-[]->(b)."""

    def test_untyped_directed_from_eve(self, spark, graph_context):
        """Eve -[]-> should return Bob (KNOWS), Frank, Grace (DEFRAUDED)."""
        query = """
        MATCH (origem:Person)-[]->(destino:Person)
        WHERE origem.node_id = 'Eve'
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        print(f"\n=== SQL for untyped directed from Eve ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        expected = {"Bob", "Frank", "Grace"}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ Untyped directed from Eve: {results}")

    def test_untyped_directed_from_alice(self, spark, graph_context):
        """Alice -[]-> should return Bob, Dave (only KNOWS edges)."""
        query = """
        MATCH (origem:Person)-[]->(destino:Person)
        WHERE origem.node_id = 'Alice'
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        expected = {"Bob", "Dave"}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ Untyped directed from Alice: {results}")


class TestUntypedUndirected:
    """Tests for untyped undirected: (a)-[]-(b)."""

    def test_untyped_undirected_from_bob(self, spark, graph_context):
        """Bob -[]- should return Alice, Carol, Eve (all directions, all types).

        KNOWS edges involving Bob:
          Alice -> Bob (incoming)
          Bob -> Carol (outgoing)
          Eve -> Bob (incoming)
        """
        query = """
        MATCH (origem:Person)-[]-(destino:Person)
        WHERE origem.node_id = 'Bob'
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        print(f"\n=== SQL for untyped undirected from Bob ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        expected = {"Alice", "Carol", "Eve"}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ Untyped undirected from Bob: {results}")

    def test_untyped_undirected_from_eve(self, spark, graph_context):
        """Eve -[]- should return Dave, Bob, Frank, Grace (all types, both directions).

        Edges involving Eve:
          Dave -> Eve (KNOWS, incoming)
          Eve -> Bob (KNOWS, outgoing)
          Eve -> Frank (DEFRAUDED, outgoing)
          Eve -> Grace (DEFRAUDED, outgoing)
        """
        query = """
        MATCH (origem:Person)-[]-(destino:Person)
        WHERE origem.node_id = 'Eve'
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        expected = {"Dave", "Bob", "Frank", "Grace"}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ Untyped undirected from Eve: {results}")

    def test_untyped_undirected_from_grace(self, spark, graph_context):
        """Grace -[]- should return only Eve (via Eve -> Grace backward).

        Grace has only one edge: Eve -> Grace (DEFRAUDED)
        """
        query = """
        MATCH (origem:Person)-[]-(destino:Person)
        WHERE origem.node_id = 'Grace'
        RETURN origem.node_id AS src, destino.node_id AS dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        expected = {"Eve"}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ Untyped undirected from Grace: {results}")


class TestCompareDirectedVsUndirected:
    """Compare directed vs undirected results to verify correctness."""

    def test_directed_subset_of_undirected(self, spark, graph_context):
        """Directed results should always be a subset of undirected results."""
        # Test for each starting node
        for start_node in ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace"]:
            directed_query = f"""
            MATCH (a:Person)-[:KNOWS]->(b:Person)
            WHERE a.node_id = '{start_node}'
            RETURN b.node_id AS dst
            """

            undirected_query = f"""
            MATCH (a:Person)-[:KNOWS]-(b:Person)
            WHERE a.node_id = '{start_node}'
            RETURN b.node_id AS dst
            """

            directed_sql = graph_context.transpile(directed_query)
            undirected_sql = graph_context.transpile(undirected_query)

            directed_result = {row["dst"] for row in spark.sql(directed_sql).collect()}
            undirected_result = {row["dst"] for row in spark.sql(undirected_sql).collect()}

            assert directed_result.issubset(undirected_result), (
                f"From {start_node}: directed {directed_result} "
                f"should be subset of undirected {undirected_result}"
            )

            extra = undirected_result - directed_result
            print(f"✅ {start_node}: directed={directed_result}, undirected adds={extra}")

    def test_edge_count_undirected_vs_directed(self, spark, graph_context):
        """Undirected edge count should equal 2x directed (each edge seen twice)."""
        directed_query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        RETURN a.node_id AS src, b.node_id AS dst
        """

        undirected_query = """
        MATCH (a:Person)-[:KNOWS]-(b:Person)
        RETURN a.node_id AS src, b.node_id AS dst
        """

        directed_sql = graph_context.transpile(directed_query)
        undirected_sql = graph_context.transpile(undirected_query)

        directed_count = spark.sql(directed_sql).count()
        undirected_count = spark.sql(undirected_sql).count()

        # Each directed edge appears twice in undirected (once from each endpoint)
        assert undirected_count == directed_count * 2, (
            f"Undirected count ({undirected_count}) should be "
            f"2x directed count ({directed_count})"
        )

        print(f"✅ Directed edges: {directed_count}, Undirected: {undirected_count}")


class TestEdgeProperties:
    """Test that edge traversal works with node properties."""

    def test_filter_by_destination_property(self, spark, graph_context):
        """Filter by destination node property."""
        query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE a.node_id = 'Alice' AND b.age > 28
        RETURN a.node_id AS src, b.node_id AS dst, b.age AS dst_age
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        # Bob has age 30, Dave has age 35 - both > 28
        results = {(row["src"], row["dst"], row["dst_age"]) for row in rows}
        expected = {("Alice", "Bob", 30), ("Alice", "Dave", 35)}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ Filter by age > 28: {results}")

    def test_return_both_node_properties(self, spark, graph_context):
        """Return properties from both source and destination nodes."""
        query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN a.node_id AS src, a.age AS src_age, b.node_id AS dst, b.age AS dst_age
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = [(row["src"], row["src_age"], row["dst"], row["dst_age"]) for row in rows]
        expected = [("Alice", 25, "Bob", 30), ("Alice", 25, "Dave", 35)]
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ Properties from both nodes: {results}")


class TestOrRelationshipType:
    """Tests for OR syntax in relationship types: [:KNOWS|DEFRAUDED]."""

    def test_or_directed_from_eve(self, spark, graph_context):
        """Eve -[:KNOWS|DEFRAUDED]-> should return Bob (KNOWS) + Frank, Grace (DEFRAUDED)."""
        query = """
        MATCH (origem:Person)-[:KNOWS|DEFRAUDED]->(destino:Person)
        WHERE origem.node_id = 'Eve'
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL for OR directed from Eve ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        expected = {"Bob", "Frank", "Grace"}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ OR directed from Eve: {results}")

    def test_or_undirected_from_bob(self, spark, graph_context):
        """Bob -[:KNOWS|DEFRAUDED]- should return all neighbors in both directions."""
        query = """
        MATCH (origem:Person)-[:KNOWS|DEFRAUDED]-(destino:Person)
        WHERE origem.node_id = 'Bob'
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL for OR undirected from Bob ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        # Bob's neighbors:
        # KNOWS: Alice->Bob, Bob->Carol, Eve->Bob (so Alice, Carol, Eve)
        # Note: No DEFRAUDED edges touch Bob in our test data
        expected = {"Alice", "Carol", "Eve"}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ OR undirected from Bob: {results}")

    def test_or_count_matches_sum_of_types(self, spark, graph_context):
        """OR count should equal sum of individual type counts (minus duplicates)."""
        # Count KNOWS directed edges
        knows_query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        RETURN a.node_id AS src, b.node_id AS dst
        """
        knows_sql = graph_context.transpile(knows_query)
        knows_count = spark.sql(knows_sql).count()

        # Count DEFRAUDED directed edges
        defrauded_query = """
        MATCH (a:Person)-[:DEFRAUDED]->(b:Person)
        RETURN a.node_id AS src, b.node_id AS dst
        """
        defrauded_sql = graph_context.transpile(defrauded_query)
        defrauded_count = spark.sql(defrauded_sql).count()

        # Count OR edges
        or_query = """
        MATCH (a:Person)-[:KNOWS|DEFRAUDED]->(b:Person)
        RETURN a.node_id AS src, b.node_id AS dst
        """
        or_sql = graph_context.transpile(or_query)
        or_count = spark.sql(or_sql).count()

        # OR count should be sum (no overlap in our test data)
        expected = knows_count + defrauded_count
        assert or_count == expected, (
            f"OR count ({or_count}) should equal KNOWS ({knows_count}) + "
            f"DEFRAUDED ({defrauded_count}) = {expected}"
        )

        print(f"✅ OR count: {or_count} = KNOWS({knows_count}) + DEFRAUDED({defrauded_count})")


class TestNoLabelNodes:
    """Tests for queries without node labels."""

    def test_no_label_source_directed(self, spark, graph_context):
        """(a)-[:KNOWS]->(b:Person) - no label on source."""
        query = """
        MATCH (origem)-[:KNOWS]->(destino:Person)
        WHERE origem.node_id = 'Alice'
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL for no-label source ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        expected = {"Bob", "Dave"}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ No-label source directed: {results}")

    def test_no_label_target_directed(self, spark, graph_context):
        """(a:Person)-[:KNOWS]->(b) - no label on target."""
        query = """
        MATCH (origem:Person)-[:KNOWS]->(destino)
        WHERE origem.node_id = 'Alice'
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        expected = {"Bob", "Dave"}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ No-label target directed: {results}")

    def test_no_label_both_directed(self, spark, graph_context):
        """(a)-[:KNOWS]->(b) - no labels on either node."""
        query = """
        MATCH (origem)-[:KNOWS]->(destino)
        WHERE origem.node_id = 'Bob'
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        expected = {"Carol"}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ No-label both directed: {results}")

    def test_no_label_undirected(self, spark, graph_context):
        """(a)-[:KNOWS]-(b) - undirected without labels."""
        query = """
        MATCH (origem)-[:KNOWS]-(destino)
        WHERE origem.node_id = 'Carol'
        RETURN origem.node_id AS src, destino.node_id AS dst
        ORDER BY dst
        """

        sql = graph_context.transpile(query)
        print("\n=== SQL for no-label undirected ===")
        print(sql)

        result = spark.sql(sql)
        rows = result.collect()

        results = {row["dst"] for row in rows}
        # Carol only has Bob->Carol edge, so undirected gives {Bob}
        expected = {"Bob"}
        assert results == expected, f"Expected {expected}, got {results}"

        print(f"✅ No-label undirected: {results}")


class TestExactCounts:
    """Tests that verify exact edge counts."""

    def test_total_knows_edges(self, spark, graph_context, graph_data):
        """Verify total KNOWS edges matches test data."""
        query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        RETURN a.node_id AS src, b.node_id AS dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)

        # Count from transpiled query
        transpiled_count = result.count()

        # Count from raw edges DataFrame
        raw_count = graph_data["edges"].filter("relationship_type = 'KNOWS'").count()

        assert transpiled_count == raw_count, (
            f"Transpiled count ({transpiled_count}) != raw count ({raw_count})"
        )
        assert transpiled_count == 5, f"Expected 5 KNOWS edges, got {transpiled_count}"

        print(f"✅ Total KNOWS edges: {transpiled_count}")

    def test_total_defrauded_edges(self, spark, graph_context, graph_data):
        """Verify total DEFRAUDED edges matches test data."""
        query = """
        MATCH (a:Person)-[:DEFRAUDED]->(b:Person)
        RETURN a.node_id AS src, b.node_id AS dst
        """

        sql = graph_context.transpile(query)
        result = spark.sql(sql)

        transpiled_count = result.count()
        raw_count = graph_data["edges"].filter("relationship_type = 'DEFRAUDED'").count()

        assert transpiled_count == raw_count, (
            f"Transpiled count ({transpiled_count}) != raw count ({raw_count})"
        )
        assert transpiled_count == 2, f"Expected 2 DEFRAUDED edges, got {transpiled_count}"

        print(f"✅ Total DEFRAUDED edges: {transpiled_count}")

    def test_undirected_doubles_count(self, spark, graph_context):
        """Undirected traversal should return 2x the directed count."""
        # Directed KNOWS
        directed_query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        RETURN a.node_id AS src, b.node_id AS dst
        """
        directed_sql = graph_context.transpile(directed_query)
        directed_count = spark.sql(directed_sql).count()

        # Undirected KNOWS
        undirected_query = """
        MATCH (a:Person)-[:KNOWS]-(b:Person)
        RETURN a.node_id AS src, b.node_id AS dst
        """
        undirected_sql = graph_context.transpile(undirected_query)
        undirected_count = spark.sql(undirected_sql).count()

        assert undirected_count == directed_count * 2, (
            f"Undirected ({undirected_count}) should be 2x directed ({directed_count})"
        )

        print(f"✅ Undirected 2x check: {undirected_count} = 2 * {directed_count}")

    def test_untyped_equals_sum_of_types(self, spark, graph_context):
        """Untyped edges should equal sum of all typed edges."""
        # Count each type
        knows_query = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.node_id, b.node_id"
        knows_count = spark.sql(graph_context.transpile(knows_query)).count()

        defrauded_query = "MATCH (a:Person)-[:DEFRAUDED]->(b:Person) RETURN a.node_id, b.node_id"
        defrauded_count = spark.sql(graph_context.transpile(defrauded_query)).count()

        # Untyped
        untyped_query = "MATCH (a:Person)-[]->(b:Person) RETURN a.node_id, b.node_id"
        untyped_count = spark.sql(graph_context.transpile(untyped_query)).count()

        expected = knows_count + defrauded_count
        assert untyped_count == expected, (
            f"Untyped ({untyped_count}) should equal "
            f"KNOWS ({knows_count}) + DEFRAUDED ({defrauded_count}) = {expected}"
        )

        print(f"✅ Untyped count: {untyped_count} = {knows_count} + {defrauded_count}")
