"""
Test: UNWIND Struct Property Access Variations

This file tests all variations of accessing properties from unwound VLP
relationship variables. Each test uses different variable names, aliases,
and property combinations to ensure the fix is not hardcoded.

Pattern under test:
    MATCH (s)-[e*1..N]->(o)
    UNWIND e AS r
    RETURN r.property AS alias
"""

import pytest
from pyspark.sql import SparkSession
from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("UNWIND_Struct_Property_Tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def graph_context(spark):
    """Create graph with known data."""
    nodes_data = [
        ("alice", "Person", "Alice", "Engineering", 100000),
        ("bob", "Person", "Bob", "Engineering", 90000),
        ("carol", "Person", "Carol", "Sales", 85000),
        ("dave", "Person", "Dave", "Sales", 80000),
        ("eve", "Person", "Eve", "Marketing", 75000),
    ]
    nodes_df = spark.createDataFrame(
        nodes_data, ["node_id", "node_type", "name", "department", "salary"]
    )
    nodes_df.createOrReplaceTempView("nodes")

    # Edges with all properties we want to test
    edges_data = [
        ("alice", "bob", "KNOWS", 10, 2020, "work"),
        ("bob", "carol", "KNOWS", 8, 2021, "social"),
        ("carol", "dave", "KNOWS", 5, 2022, "work"),
        ("alice", "eve", "KNOWS", 7, 2019, "social"),
        ("dave", "alice", "KNOWS", 4, 2023, "work"),  # Cycle
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
        extra_node_attrs={"name": str, "department": str, "salary": int},
        extra_edge_attrs={"weight": int, "since": int, "context": str},
    )
    graph.set_types(
        node_types=["Person"],
        edge_types=["KNOWS"],
    )
    return graph


# =============================================================================
# Category 1: Variable Name Variations
# =============================================================================
class TestVariableNameVariations:
    """Test different variable names for VLP and UNWIND."""

    def test_standard_names_e_r(self, spark, graph_context):
        """Standard pattern: e for VLP, r for unwound."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN r.src AS src, r.dst AS dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 2
        assert all(row["src"] is not None for row in rows)

    def test_descriptive_names_edges_edge(self, spark, graph_context):
        """Descriptive names: edges for VLP, edge for unwound."""
        query = """
        MATCH (s {name: "Alice"})-[edges:KNOWS*1..2]->(o)
        UNWIND edges AS edge
        RETURN edge.src AS source, edge.dst AS target
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 2
        assert all(row["source"] is not None for row in rows)

    def test_short_names_x_y(self, spark, graph_context):
        """Short names: x for VLP, y for unwound."""
        query = """
        MATCH (a {name: "Alice"})-[x:KNOWS*1..2]->(b)
        UNWIND x AS y
        RETURN y.src AS s, y.dst AS d
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 2

    def test_long_names_relationship_rel(self, spark, graph_context):
        """Long names: relationships for VLP, rel for unwound.

        Note: 'end' is a reserved word in Cypher (used in CASE...END),
        so we use 'target' instead.
        """
        query = """
        MATCH (start {name: "Alice"})-[relationships:KNOWS*1..2]->(target)
        UNWIND relationships AS rel
        RETURN rel.src AS from_node, rel.dst AS to_node, rel.weight AS strength
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 2

    def test_underscore_names(self, spark, graph_context):
        """Names with underscores."""
        query = """
        MATCH (src_node {name: "Alice"})-[path_edges:KNOWS*1..2]->(dst_node)
        UNWIND path_edges AS single_edge
        RETURN single_edge.src AS source_id, single_edge.dst AS target_id
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 2

    def test_camelcase_names(self, spark, graph_context):
        """CamelCase names."""
        query = """
        MATCH (startNode {name: "Alice"})-[pathEdges:KNOWS*1..2]->(endNode)
        UNWIND pathEdges AS singleEdge
        RETURN singleEdge.src AS sourceId, singleEdge.dst AS targetId
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 2


# =============================================================================
# Category 2: Property Access Variations
# =============================================================================
class TestPropertyAccessVariations:
    """Test accessing different properties from unwound struct."""

    def test_access_src_only(self, spark, graph_context):
        """Access only src property."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN DISTINCT r.src AS source
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        sources = {row["source"] for row in rows}
        assert "alice" in sources

    def test_access_dst_only(self, spark, graph_context):
        """Access only dst property."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN DISTINCT r.dst AS target
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        targets = {row["target"] for row in rows}
        assert "bob" in targets

    def test_access_weight_only(self, spark, graph_context):
        """Access only weight property."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN DISTINCT r.weight AS w
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        weights = {row["w"] for row in rows}
        assert 10 in weights  # alice->bob has weight 10

    def test_access_since_only(self, spark, graph_context):
        """Access only since property."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN DISTINCT r.since AS year
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        years = {row["year"] for row in rows}
        assert 2020 in years

    def test_access_context_only(self, spark, graph_context):
        """Access only context property (string)."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN DISTINCT r.context AS ctx
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        contexts = {row["ctx"] for row in rows}
        assert "work" in contexts or "social" in contexts

    def test_access_relationship_type(self, spark, graph_context):
        """Access relationship_type property."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN DISTINCT r.relationship_type AS type
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        types = {row["type"] for row in rows}
        assert "KNOWS" in types

    def test_access_all_properties(self, spark, graph_context):
        """Access all properties at once."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN r.src, r.dst, r.relationship_type, r.weight, r.since, r.context
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 2
        for row in rows:
            assert row["src"] is not None
            assert row["dst"] is not None
            assert row["relationship_type"] is not None
            assert row["weight"] is not None
            assert row["since"] is not None
            assert row["context"] is not None


# =============================================================================
# Category 3: Different VLP Depths
# =============================================================================
class TestVLPDepthVariations:
    """Test UNWIND with different VLP depths."""

    def test_depth_1_only(self, spark, graph_context):
        """VLP with depth 1 only (*1..1)."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..1]->(o)
        UNWIND e AS r
        RETURN r.src AS src, r.dst AS dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        # Alice has 2 direct KNOWS edges: to Bob and Eve
        assert len(rows) == 2

    def test_depth_2_only(self, spark, graph_context):
        """VLP with depth 2..2 only."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*2..2]->(o)
        UNWIND e AS r
        RETURN r.src AS src, r.dst AS dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        # Each 2-hop path has 2 edges, so UNWIND produces 2 rows per path
        assert len(rows) >= 2

    def test_depth_1_to_3(self, spark, graph_context):
        """VLP with depth 1..3."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..3]->(o)
        UNWIND e AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        # Should find multiple unique edges
        assert len(rows) >= 3


# =============================================================================
# Category 4: Directed vs Undirected
# =============================================================================
class TestDirectionVariations:
    """Test UNWIND with directed and undirected VLP."""

    def test_directed_forward(self, spark, graph_context):
        """Directed VLP forward (->)."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN r.src AS src, r.dst AS dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 2

    def test_directed_backward(self, spark, graph_context):
        """Directed VLP backward (<-)."""
        query = """
        MATCH (s {name: "Carol"})<-[e:KNOWS*1..2]-(o)
        UNWIND e AS r
        RETURN r.src AS src, r.dst AS dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 1

    def test_undirected(self, spark, graph_context):
        """Undirected VLP (-)."""
        query = """
        MATCH (s {name: "Bob"})-[e:KNOWS*1..2]-(o)
        UNWIND e AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        # Should find edges in both directions
        assert len(rows) >= 3


# =============================================================================
# Category 5: WITH WHERE Clause (Correct Cypher syntax)
# =============================================================================
class TestWithWhereClause:
    """Test UNWIND with WHERE filtering on struct properties.

    Note: In standard Cypher, WHERE cannot follow UNWIND directly.
    Use WITH r WHERE ... instead.
    """

    def test_where_on_weight_gt(self, spark, graph_context):
        """WITH r WHERE r.weight > value."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..3]->(o)
        UNWIND e AS r
        WITH r WHERE r.weight > 5
        RETURN r.src AS src, r.dst AS dst, r.weight AS w
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        for row in rows:
            assert row["w"] > 5

    def test_where_on_weight_eq(self, spark, graph_context):
        """WITH r WHERE r.weight = value."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..3]->(o)
        UNWIND e AS r
        WITH r WHERE r.weight = 10
        RETURN r.src AS src, r.dst AS dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        # alice->bob has weight 10
        assert any(row["src"] == "alice" and row["dst"] == "bob" for row in rows)

    def test_where_on_since(self, spark, graph_context):
        """WITH r WHERE r.since >= year."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..3]->(o)
        UNWIND e AS r
        WITH r WHERE r.since >= 2021
        RETURN r.src, r.dst, r.since
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        for row in rows:
            assert row["since"] >= 2021

    def test_where_on_context(self, spark, graph_context):
        """WITH r WHERE r.context = 'work'."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..3]->(o)
        UNWIND e AS r
        WITH r WHERE r.context = "work"
        RETURN DISTINCT r.src, r.dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 1

    def test_where_combined_conditions(self, spark, graph_context):
        """WITH r WHERE with multiple conditions on struct properties."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..3]->(o)
        UNWIND e AS r
        WITH r WHERE r.weight > 5 AND r.since >= 2020
        RETURN r.src, r.dst, r.weight, r.since
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        for row in rows:
            assert row["weight"] > 5
            assert row["since"] >= 2020


# =============================================================================
# Category 6: With Aggregations
# =============================================================================
class TestWithAggregations:
    """Test UNWIND with aggregation functions on struct properties."""

    def test_sum_weight(self, spark, graph_context):
        """SUM(r.weight)."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN SUM(r.weight) AS total
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert rows[0]["total"] > 0

    def test_avg_weight(self, spark, graph_context):
        """AVG(r.weight)."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN AVG(r.weight) AS average
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert rows[0]["average"] > 0

    def test_min_max_weight(self, spark, graph_context):
        """MIN and MAX on r.weight."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN MIN(r.weight) AS min_w, MAX(r.weight) AS max_w
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert rows[0]["min_w"] <= rows[0]["max_w"]

    def test_count_distinct_src(self, spark, graph_context):
        """COUNT(DISTINCT r.src)."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..3]->(o)
        UNWIND e AS r
        RETURN COUNT(DISTINCT r.src) AS unique_sources
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert rows[0]["unique_sources"] >= 1

    def test_collect_weights(self, spark, graph_context):
        """COLLECT(r.weight)."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN o.name AS dest, COLLECT(r.weight) AS weights
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        for row in rows:
            assert isinstance(row["weights"], list)

    def test_group_by_context(self, spark, graph_context):
        """GROUP BY r.context."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..3]->(o)
        UNWIND e AS r
        RETURN r.context AS ctx, COUNT(*) AS count, AVG(r.weight) AS avg_weight
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        contexts = {row["ctx"] for row in rows}
        assert "work" in contexts or "social" in contexts


# =============================================================================
# Category 7: With ORDER BY
# =============================================================================
class TestWithOrderBy:
    """Test UNWIND with ORDER BY on struct properties."""

    def test_order_by_weight_asc(self, spark, graph_context):
        """ORDER BY r.weight ASC."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN r.src, r.dst, r.weight
        ORDER BY r.weight ASC
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        weights = [row["weight"] for row in rows]
        assert weights == sorted(weights)

    def test_order_by_weight_desc(self, spark, graph_context):
        """ORDER BY r.weight DESC."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN r.src, r.dst, r.weight
        ORDER BY r.weight DESC
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        weights = [row["weight"] for row in rows]
        assert weights == sorted(weights, reverse=True)

    def test_order_by_since(self, spark, graph_context):
        """ORDER BY r.since."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN r.src, r.dst, r.since
        ORDER BY r.since DESC
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        years = [row["since"] for row in rows]
        assert years == sorted(years, reverse=True)


# =============================================================================
# Category 8: With DISTINCT
# =============================================================================
class TestWithDistinct:
    """Test UNWIND with DISTINCT on struct properties."""

    def test_distinct_src_dst(self, spark, graph_context):
        """DISTINCT r.src, r.dst."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..3]->(o)
        UNWIND e AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        # Check no duplicates
        pairs = [(row["src"], row["dst"]) for row in rows]
        assert len(pairs) == len(set(pairs))

    def test_distinct_weight(self, spark, graph_context):
        """DISTINCT r.weight."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..3]->(o)
        UNWIND e AS r
        RETURN DISTINCT r.weight AS w
        ORDER BY w
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        weights = [row["w"] for row in rows]
        assert len(weights) == len(set(weights))


# =============================================================================
# Category 9: Multiple MATCH with UNWIND
# =============================================================================
class TestMultipleMatchWithUnwind:
    """Test multiple MATCH clauses combined with UNWIND."""

    def test_two_match_then_unwind(self, spark, graph_context):
        """Two MATCH clauses, then UNWIND."""
        query = """
        MATCH (s:Person)
        WHERE s.name = "Alice"
        MATCH (s)-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN s.name AS start, r.src, r.dst, o.name AS end
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 2
        assert all(row["start"] == "Alice" for row in rows)

    def test_match_with_property_filter_then_unwind(self, spark, graph_context):
        """MATCH with inline property filter, then UNWIND."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o {department: "Sales"})
        UNWIND e AS r
        RETURN r.src, r.dst, r.weight
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        # Path to Carol (Sales) or Dave (Sales)
        assert len(rows) >= 1


# =============================================================================
# Category 10: WITH Clause Combinations
# =============================================================================
class TestWithClauseCombinations:
    """Test UNWIND with WITH clause combinations."""

    def test_with_before_unwind(self, spark, graph_context):
        """WITH clause before UNWIND."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        WITH s, e, o
        UNWIND e AS r
        RETURN s.name, r.src, r.dst, o.name
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 2

    def test_with_after_unwind_filter(self, spark, graph_context):
        """WITH clause after UNWIND with filter."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        WITH r WHERE r.weight > 5
        RETURN r.src, r.dst, r.weight
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        for row in rows:
            assert row["weight"] > 5

    def test_with_after_unwind_aggregation(self, spark, graph_context):
        """WITH clause after UNWIND with aggregation."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        WITH o, SUM(r.weight) AS total_weight
        RETURN o.name, total_weight
        ORDER BY total_weight DESC
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 1


# =============================================================================
# Category 11: Edge Cases
# =============================================================================
class TestEdgeCases:
    """Test edge cases for UNWIND struct property access."""

    def test_same_property_multiple_times(self, spark, graph_context):
        """Access same property multiple times."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN r.weight AS w1, r.weight AS w2, r.weight + 1 AS w_plus_1
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        for row in rows:
            assert row["w1"] == row["w2"]
            assert row["w_plus_1"] == row["w1"] + 1

    def test_property_in_expression(self, spark, graph_context):
        """Use property in expression."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN r.src, r.dst, r.weight * 2 AS double_weight
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) >= 2

    def test_property_in_case_expression(self, spark, graph_context):
        """Use property in CASE expression."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
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

    @pytest.mark.xfail(
        reason="Cypher uses + for string concatenation, Spark SQL requires CONCAT()",
        strict=True,
    )
    def test_property_concatenation(self, spark, graph_context):
        """Concatenate properties."""
        query = """
        MATCH (s {name: "Alice"})-[e:KNOWS*1..2]->(o)
        UNWIND e AS r
        RETURN r.src + " -> " + r.dst AS edge_str
        """
        sql = graph_context.transpile(query)
        result = spark.sql(sql)
        rows = result.collect()
        for row in rows:
            assert " -> " in row["edge_str"]
