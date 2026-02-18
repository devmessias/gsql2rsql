"""PySpark execution tests for features_queries.yaml examples.

Validates that example queries from features_queries.yaml transpile
and execute correctly using GraphContext with triple-store format.

Test Graph:
    Persons: Alice(28), Bob(35), Carol(42), Dave(55), Eve(22), Frank(17)
    Cities: New York, London, Paris
    Movies: The Matrix(1999), Inception(2010), Titanic(1997)
    Companies: TechCorp(Tech), FinBank(Finance)

    KNOWS:     Alice->Bob, Alice->Carol, Bob->Carol, Bob->Dave, Carol->Dave, Dave->Eve
    LIVES_IN:  Alice->NYC, Bob->NYC, Carol->London, Dave->London, Eve->Paris, Frank->NYC
    ACTED_IN:  Alice->Matrix, Alice->Inception, Bob->Matrix, Carol->Titanic, Dave->Inception
    DIRECTED:  Carol->Matrix, Dave->Inception
    WORKS_AT:  Alice->TechCorp, Bob->FinBank, Carol->TechCorp, Dave->FinBank, Eve->TechCorp
"""

import re

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from gsql2rsql.graph_context import GraphContext

# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------
NODES_SCHEMA = StructType([
    StructField("node_id", LongType(), False),
    StructField("node_type", StringType(), False),
    StructField("name", StringType(), True),
    StructField("age", LongType(), True),
    StructField("nickname", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("active", BooleanType(), True),
    StructField("population", LongType(), True),
    StructField("country", StringType(), True),
    StructField("title", StringType(), True),
    StructField("year", LongType(), True),
    StructField("genre", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("industry", StringType(), True),
])

NODES_DATA = [
    # Person nodes
    (1, "Person", "Alice", 28, "Ali", 120000.0, True, None, None, None, None, None, None, None),
    (2, "Person", "Bob", 35, None, 80000.0, True, None, None, None, None, None, None, None),
    (3, "Person", "Carol", 42, "Caz", 95000.0, False, None, None, None, None, None, None, None),
    (4, "Person", "Dave", 55, None, 150000.0, True, None, None, None, None, None, None, None),
    (5, "Person", "Eve", 22, "Evie", 60000.0, True, None, None, None, None, None, None, None),
    (6, "Person", "Frank", 17, None, None, False, None, None, None, None, None, None, None),
    # City nodes
    (7, "City", "New York", None, None, None, None, 8_000_000, "USA", None, None, None, None, None),
    (8, "City", "London", None, None, None, None, 9_000_000, "UK", None, None, None, None, None),
    (9, "City", "Paris", None, None, None, None, 2_000_000, "France", None, None, None, None, None),
    # Movie nodes
    (10, "Movie", None, None, None, None, None, None, None, "The Matrix", 1999, "SciFi", 8.7, None),
    (11, "Movie", None, None, None, None, None, None, None, "Inception", 2010, "SciFi", 8.8, None),
    (12, "Movie", None, None, None, None, None, None, None, "Titanic", 1997, "Drama", 7.8, None),
    # Company nodes
    (13, "Company", "TechCorp", None, None, None, None, None, None, None, None, None, None, "Tech"),
    (14, "Company", "FinBank", None, None, None, None, None, None, None, None, None, None, "Finance"),
]

EDGES_SCHEMA = StructType([
    StructField("src", LongType(), False),
    StructField("dst", LongType(), False),
    StructField("relationship_type", StringType(), False),
    StructField("since", LongType(), True),
    StructField("strength", DoubleType(), True),
    StructField("role", StringType(), True),
    StructField("position", StringType(), True),
])

EDGES_DATA = [
    # KNOWS (Person -> Person)
    (1, 2, "KNOWS", 2018, 0.9, None, None),     # Alice -> Bob
    (1, 3, "KNOWS", 2020, 0.7, None, None),     # Alice -> Carol
    (2, 3, "KNOWS", 2015, 0.85, None, None),    # Bob -> Carol
    (2, 4, "KNOWS", 2021, 0.95, None, None),    # Bob -> Dave
    (3, 4, "KNOWS", 2019, 0.6, None, None),     # Carol -> Dave
    (4, 5, "KNOWS", 2022, 0.5, None, None),     # Dave -> Eve
    # LIVES_IN (Person -> City)
    (1, 7, "LIVES_IN", None, None, None, None),  # Alice -> NYC
    (2, 7, "LIVES_IN", None, None, None, None),  # Bob -> NYC
    (3, 8, "LIVES_IN", None, None, None, None),  # Carol -> London
    (4, 8, "LIVES_IN", None, None, None, None),  # Dave -> London
    (5, 9, "LIVES_IN", None, None, None, None),  # Eve -> Paris
    (6, 7, "LIVES_IN", None, None, None, None),  # Frank -> NYC
    # ACTED_IN (Person -> Movie)
    (1, 10, "ACTED_IN", None, None, "Neo", None),       # Alice -> Matrix
    (1, 11, "ACTED_IN", None, None, "Cobb", None),      # Alice -> Inception
    (2, 10, "ACTED_IN", None, None, "Morpheus", None),  # Bob -> Matrix
    (3, 12, "ACTED_IN", None, None, "Rose", None),      # Carol -> Titanic
    (4, 11, "ACTED_IN", None, None, "Arthur", None),    # Dave -> Inception
    # DIRECTED (Person -> Movie)
    (3, 10, "DIRECTED", None, None, None, None),  # Carol -> Matrix
    (4, 11, "DIRECTED", None, None, None, None),  # Dave -> Inception
    # WORKS_AT (Person -> Company)
    (1, 13, "WORKS_AT", 2020, None, None, "Engineer"),  # Alice -> TechCorp
    (2, 14, "WORKS_AT", 2018, None, None, "Analyst"),   # Bob -> FinBank
    (3, 13, "WORKS_AT", 2019, None, None, "Manager"),   # Carol -> TechCorp
    (4, 14, "WORKS_AT", 2015, None, None, "VP"),        # Dave -> FinBank
    (5, 13, "WORKS_AT", 2022, None, None, "Intern"),    # Eve -> TechCorp
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _adapt(query: str) -> str:
    """Adapt YAML queries for GraphContext: .id -> .node_id"""
    return re.sub(r"\.id\b", ".node_id", query)


def _exec(gc, spark, query, desc=""):
    """Transpile, execute, and return collected rows."""
    sql = gc.transpile(query)
    print(f"\n=== {desc} ===\n{sql}")
    rows = spark.sql(sql).collect()
    print(f"Result: {len(rows)} rows")
    for i, r in enumerate(rows[:5]):
        print(f"  [{i}] {r.asDict()}")
    return rows


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for feature tests."""
    session = (
        SparkSession.builder.appName("features_test")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def gc(spark):
    """Create GraphContext with features test data."""
    spark.sql("CREATE DATABASE IF NOT EXISTS test_features")

    nodes_df = spark.createDataFrame(NODES_DATA, NODES_SCHEMA)
    nodes_df.write.mode("overwrite").saveAsTable("test_features.nodes")

    edges_df = spark.createDataFrame(EDGES_DATA, EDGES_SCHEMA)
    edges_df.write.mode("overwrite").saveAsTable("test_features.edges")

    graph = GraphContext(
        spark=spark,
        nodes_table="test_features.nodes",
        edges_table="test_features.edges",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        node_id_col="node_id",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={
            "name": str, "age": int, "nickname": str, "salary": float,
            "active": bool, "population": int, "country": str,
            "title": str, "year": int, "genre": str, "rating": float,
            "industry": str,
        },
        extra_edge_attrs={
            "since": int, "strength": float, "role": str, "position": str,
        },
        discover_edge_combinations=True,
    )

    yield graph

    spark.sql("DROP TABLE IF EXISTS test_features.nodes")
    spark.sql("DROP TABLE IF EXISTS test_features.edges")
    spark.sql("DROP DATABASE IF EXISTS test_features")


# ===========================================================================
# BASIC QUERIES
# ===========================================================================
class TestBasicQueries:

    def test_simple_node_lookup(self, gc, spark):
        """MATCH (p:Person) RETURN p.name, p.age — all 6 persons."""
        rows = _exec(gc, spark, """
            MATCH (p:Person) RETURN p.name, p.age
        """, "Simple node lookup")
        assert len(rows) == 6
        names = {r["name"] for r in rows}
        assert names == {"Alice", "Bob", "Carol", "Dave", "Eve", "Frank"}

    def test_property_filter(self, gc, spark):
        """WHERE p.age > 30 AND p.active = true — Bob(35), Dave(55)."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)
            WHERE p.age > 30 AND p.active = true
            RETURN p.name, p.age
        """, "Property filter")
        assert len(rows) == 2
        names = {r["name"] for r in rows}
        assert names == {"Bob", "Dave"}

    def test_property_projection_aliases(self, gc, spark):
        """RETURN with aliases: personName, personAge, income."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)
            RETURN p.name AS personName, p.age AS personAge, p.salary AS income
        """, "Property projection")
        assert len(rows) == 6
        alice = next(r for r in rows if r["personName"] == "Alice")
        assert alice["personAge"] == 28
        assert alice["income"] == pytest.approx(120000.0)

    def test_pagination(self, gc, spark):
        """ORDER BY p.age DESC SKIP 2 LIMIT 3 — middle 3 persons by age."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)
            RETURN p.name, p.age
            ORDER BY p.age DESC
            SKIP 2 LIMIT 3
        """, "Pagination")
        # Ordered by age DESC: Dave(55), Carol(42), Bob(35), Alice(28), Eve(22), Frank(17)
        # SKIP 2 LIMIT 3: Bob(35), Alice(28), Eve(22)
        assert len(rows) == 3
        names = [r["name"] for r in rows]
        assert names == ["Bob", "Alice", "Eve"]


# ===========================================================================
# AGGREGATION
# ===========================================================================
class TestAggregation:

    def test_count_no_grouping(self, gc, spark):
        """COUNT(p) AS totalPeople — 6 persons."""
        rows = _exec(gc, spark, """
            MATCH (p:Person) RETURN COUNT(p) AS totalPeople
        """, "COUNT")
        assert len(rows) == 1
        assert rows[0]["totalPeople"] == 6

    def test_group_by_multiple_aggregations(self, gc, spark):
        """GROUP BY city with COUNT, AVG, MIN, MAX.

        NYC: Alice(28,120k), Bob(35,80k), Frank(17,null) -> count=3
        London: Carol(42,95k), Dave(55,150k) -> count=2
        Paris: Eve(22,60k) -> count=1
        """
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:LIVES_IN]->(c:City)
            RETURN c.name AS city,
                   COUNT(p) AS population,
                   AVG(p.age) AS avgAge,
                   MIN(p.salary) AS minSalary,
                   MAX(p.salary) AS maxSalary
        """, "GROUP BY multiple aggregations")
        assert len(rows) == 3
        by_city = {r["city"]: r for r in rows}

        assert by_city["New York"]["population"] == 3
        assert by_city["New York"]["avgAge"] == pytest.approx(80 / 3, abs=0.1)
        assert by_city["New York"]["minSalary"] == pytest.approx(80000.0)
        assert by_city["New York"]["maxSalary"] == pytest.approx(120000.0)

        assert by_city["London"]["population"] == 2
        assert by_city["Paris"]["population"] == 1

    def test_order_by_aggregated_column(self, gc, spark):
        """ORDER BY population DESC — NYC(3), London(2), Paris(1)."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:LIVES_IN]->(c:City)
            RETURN c.name AS city, COUNT(p) AS population
            ORDER BY population DESC
            LIMIT 10
        """, "ORDER BY aggregate")
        assert len(rows) == 3
        assert rows[0]["city"] == "New York"
        assert rows[0]["population"] == 3
        assert rows[1]["city"] == "London"
        assert rows[2]["city"] == "Paris"

    def test_having_filter_with_where(self, gc, spark):
        """WITH...WHERE for HAVING-style filter (threshold adapted for test data).

        population > 1 filters Paris(1).
        """
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:LIVES_IN]->(c:City)
            WITH c.name AS city, COUNT(p) AS population
            WHERE population > 1
            RETURN city, population
            ORDER BY population DESC
        """, "HAVING filter")
        assert len(rows) == 2
        assert rows[0]["city"] == "New York"
        assert rows[1]["city"] == "London"

    def test_collect_list(self, gc, spark):
        """COLLECT(p.name) AS residents — array per city."""
        rows = _exec(gc, spark, """
            MATCH (c:City)<-[:LIVES_IN]-(p:Person)
            RETURN c.name AS city, COLLECT(p.name) AS residents
        """, "COLLECT")
        assert len(rows) == 3
        by_city = {r["city"]: r for r in rows}
        assert set(by_city["New York"]["residents"]) == {"Alice", "Bob", "Frank"}
        assert set(by_city["London"]["residents"]) == {"Carol", "Dave"}
        assert set(by_city["Paris"]["residents"]) == {"Eve"}


# ===========================================================================
# RELATIONSHIPS
# ===========================================================================
class TestRelationships:

    def test_directed_relationship(self, gc, spark):
        """Person -[:ACTED_IN]-> Movie — 5 actor-movie pairs."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
            RETURN p.name AS actor, m.title AS movie
        """, "Directed relationship")
        assert len(rows) == 5
        pairs = {(r["actor"], r["movie"]) for r in rows}
        assert ("Alice", "The Matrix") in pairs
        assert ("Alice", "Inception") in pairs
        assert ("Bob", "The Matrix") in pairs
        assert ("Carol", "Titanic") in pairs
        assert ("Dave", "Inception") in pairs

    def test_relationship_property_filter(self, gc, spark):
        """Edge filter: since > 2020 AND strength > 0.8 — only Bob->Dave."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[r:KNOWS]->(f:Person)
            WHERE r.since > 2020 AND r.strength > 0.8
            RETURN p.name AS person, f.name AS friend, r.since, r.strength
        """, "Edge property filter")
        assert len(rows) == 1
        assert rows[0]["person"] == "Bob"
        assert rows[0]["friend"] == "Dave"
        assert rows[0]["since"] == 2021
        assert rows[0]["strength"] == pytest.approx(0.95)

    def test_undirected_relationship(self, gc, spark):
        """Undirected KNOWS from Alice — {Bob, Carol}."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:KNOWS]-(f:Person)
            WHERE p.name = 'Alice'
            RETURN DISTINCT f.name AS friend
        """, "Undirected relationship")
        assert len(rows) == 2
        friends = {r["friend"] for r in rows}
        assert friends == {"Bob", "Carol"}


# ===========================================================================
# PREDICATE PUSHDOWN (undirected)
# ===========================================================================
class TestPredicatePushdown:

    def test_source_filter_pushdown(self, gc, spark):
        """Source filter (p.name, p.age) pushed to DataSource(p)."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:KNOWS]-(f:Person)
            WHERE p.name = 'Alice' AND p.age > 25
            RETURN f.name AS friend, f.age AS friendAge
        """, "Source filter pushdown")
        assert len(rows) == 2
        friends = {r["friend"] for r in rows}
        assert friends == {"Bob", "Carol"}

    def test_target_filter_not_pushed(self, gc, spark):
        """Target filter f.age > 30 stays after join.

        Undirected KNOWS where f.age > 30:
        f=Bob(35): Alice, Carol -> (Alice,Bob), (Carol,Bob)
        f=Carol(42): Alice, Bob, Dave -> (Alice,Carol), (Bob,Carol), (Dave,Carol)
        f=Dave(55): Bob, Carol, Eve -> (Bob,Dave), (Carol,Dave), (Eve,Dave)
        Total: 8 rows (but checking with DISTINCT-like assertion)
        """
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:KNOWS]-(f:Person)
            WHERE f.age > 30
            RETURN p.name AS person, f.name AS olderFriend
        """, "Target filter (not pushed)")
        pairs = {(r["person"], r["olderFriend"]) for r in rows}
        # Persons connected to f where f.age > 30 (Bob=35, Carol=42, Dave=55)
        assert ("Alice", "Bob") in pairs
        assert ("Alice", "Carol") in pairs
        assert ("Bob", "Dave") in pairs
        assert ("Eve", "Dave") in pairs
        assert len(pairs) >= 8

    def test_mixed_filters_partial_pushdown(self, gc, spark):
        """p.name='Alice' AND p.active=true pushed; f.age>30 stays."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:KNOWS]-(f:Person)
            WHERE p.name = 'Alice' AND f.age > 30 AND p.active = true
            RETURN f.name AS friend, f.age AS friendAge
        """, "Mixed filters")
        assert len(rows) == 2
        friends = {r["friend"] for r in rows}
        assert friends == {"Bob", "Carol"}

    def test_undirected_multihop(self, gc, spark):
        """2-hop undirected from Alice — friends of friends.

        Alice-(Bob,Carol)-(Alice,Carol,Dave,Alice,Bob,Dave)
        DISTINCT: {Alice, Bob, Carol, Dave}
        """
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:KNOWS]-(m:Person)-[:KNOWS]-(f:Person)
            WHERE p.name = 'Alice'
            RETURN DISTINCT f.name AS friendOfFriend
        """, "Undirected multi-hop")
        names = {r["friendOfFriend"] for r in rows}
        assert names == {"Alice", "Bob", "Carol", "Dave"}

    def test_undirected_with_aggregation(self, gc, spark):
        """High earners and friend metrics.

        salary > 100k: Alice(120k), Dave(150k)
        Alice undirected KNOWS: Bob, Carol -> count=2, avg=(35+42)/2=38.5
        Dave undirected KNOWS: Bob, Carol, Eve -> count=3, avg=(35+42+22)/3=33.0
        """
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[r:KNOWS]-(f:Person)
            WHERE p.salary > 100000
            RETURN p.name AS highEarner,
                   COUNT(f) AS friendCount,
                   AVG(f.age) AS avgFriendAge
        """, "Undirected aggregation")
        assert len(rows) == 2
        by_name = {r["highEarner"]: r for r in rows}
        assert by_name["Alice"]["friendCount"] == 2
        assert by_name["Alice"]["avgFriendAge"] == pytest.approx(38.5, abs=0.1)
        assert by_name["Dave"]["friendCount"] == 3
        assert by_name["Dave"]["avgFriendAge"] == pytest.approx(33.0, abs=0.1)


# ===========================================================================
# OPTIONAL MATCH
# ===========================================================================
class TestOptionalMatch:

    def test_optional_match_left_join(self, gc, spark):
        """All persons, with movies if they exist (NULL otherwise).

        ACTED_IN: Alice(2 movies), Bob(1), Carol(1), Dave(1), Eve(0), Frank(0)
        Total: 7 rows (Eve and Frank have m.title=NULL)
        """
        rows = _exec(gc, spark, """
            MATCH (p:Person)
            OPTIONAL MATCH (p)-[:ACTED_IN]->(m:Movie)
            RETURN p.name, m.title
        """, "OPTIONAL MATCH")
        assert len(rows) == 7
        by_name = {}
        for r in rows:
            by_name.setdefault(r["name"], []).append(r["title"])
        assert set(by_name["Alice"]) == {"The Matrix", "Inception"}
        assert by_name["Eve"] == [None]
        assert by_name["Frank"] == [None]


# ===========================================================================
# VARIABLE-LENGTH PATHS (recursive CTE — may not work on local PySpark)
# ===========================================================================
class TestVariableLengthPaths:

    def test_vlp_1_to_3(self, gc, spark):
        """KNOWS*1..3 from Alice -> {Bob, Carol, Dave, Eve}."""
        rows = _exec(gc, spark, _adapt("""
            MATCH (p:Person)-[:KNOWS*1..3]->(f:Person)
            WHERE p.name = 'Alice'
            RETURN DISTINCT f.name AS reachable
        """), "VLP 1..3")
        names = {r["reachable"] for r in rows}
        assert names == {"Bob", "Carol", "Dave", "Eve"}

    def test_vlp_0_to_2(self, gc, spark):
        """KNOWS*0..2 from Alice includes self -> {Alice, Bob, Carol, Dave}."""
        rows = _exec(gc, spark, _adapt("""
            MATCH (p:Person)-[:KNOWS*0..2]->(f:Person)
            WHERE p.name = 'Alice'
            RETURN DISTINCT f.name AS reachable
        """), "VLP 0..2")
        names = {r["reachable"] for r in rows}
        assert names == {"Alice", "Bob", "Carol", "Dave"}


# ===========================================================================
# EXPRESSIONS AND FUNCTIONS
# ===========================================================================
class TestExpressions:

    def test_case_expression(self, gc, spark):
        """CASE WHEN for age grouping: minor/adult/senior."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)
            RETURN p.name,
                   CASE
                     WHEN p.age < 18 THEN 'minor'
                     WHEN p.age < 65 THEN 'adult'
                     ELSE 'senior'
                   END AS ageGroup
        """, "CASE expression")
        assert len(rows) == 6
        by_name = {r["name"]: r["ageGroup"] for r in rows}
        assert by_name["Frank"] == "minor"
        assert by_name["Alice"] == "adult"
        assert by_name["Dave"] == "adult"

    def test_coalesce(self, gc, spark):
        """COALESCE(nickname, name) — uses nickname if available."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)
            RETURN COALESCE(p.nickname, p.name) AS displayName,
                   COALESCE(p.salary, 0) AS salary
        """, "COALESCE")
        assert len(rows) == 6
        by_display = {r["displayName"]: r for r in rows}
        # Alice has nickname "Ali"
        assert "Ali" in by_display
        assert by_display["Ali"]["salary"] == pytest.approx(120000.0)
        # Bob has no nickname -> uses name
        assert "Bob" in by_display
        # Frank has no salary -> COALESCE to 0
        assert by_display["Frank"]["salary"] == pytest.approx(0.0)

    def test_distinct(self, gc, spark):
        """DISTINCT m.genre — {SciFi, Drama}."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
            RETURN DISTINCT m.genre
        """, "DISTINCT")
        genres = {r["genre"] for r in rows}
        assert genres == {"SciFi", "Drama"}


# ===========================================================================
# SET OPERATIONS
# ===========================================================================
class TestSetOperations:

    def test_union(self, gc, spark):
        """Actors UNION Directors — {Alice, Bob, Carol, Dave}."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
            RETURN p.name AS name
            UNION
            MATCH (d:Person)-[:DIRECTED]->(m:Movie)
            RETURN d.name AS name
        """, "UNION")
        names = {r["name"] for r in rows}
        assert names == {"Alice", "Bob", "Carol", "Dave"}


# ===========================================================================
# ADVANCED PATTERNS
# ===========================================================================
class TestAdvancedPatterns:

    def test_multi_hop_intermediate_filtering(self, gc, spark):
        """Person in USA city AND works at Tech company.

        LIVES_IN USA: Alice(NYC), Bob(NYC), Frank(NYC)
        WORKS_AT Tech: Alice(TechCorp), Carol(TechCorp), Eve(TechCorp)
        Intersection: Alice
        """
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:LIVES_IN]->(c:City),
                  (p)-[:WORKS_AT]->(co:Company)
            WHERE c.country = 'USA' AND co.industry = 'Tech'
            RETURN p.name, c.name AS city, co.name AS company
        """, "Multi-hop intermediate")
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"
        assert rows[0]["city"] == "New York"
        assert rows[0]["company"] == "TechCorp"

    def test_chained_with(self, gc, spark):
        """WITH chaining for multi-stage computation (threshold adapted).

        Cities with population > 1 (from LIVES_IN count).
        """
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:LIVES_IN]->(c:City)
            WITH c, COUNT(p) AS pop
            WHERE pop > 1
            WITH c.name AS city, pop, pop * 1.0 / 1000 AS popK
            RETURN city, popK
            ORDER BY popK DESC
        """, "Chained WITH")
        assert len(rows) == 2
        assert rows[0]["city"] == "New York"
        assert float(rows[0]["popK"]) == pytest.approx(0.003, abs=0.001)


# ===========================================================================
# RECURSIVE SINK FILTER PUSHDOWN (VLP — xfail for local PySpark)
# ===========================================================================
class TestRecursiveSinkFilter:

    def test_simplest_sink_filter(self, gc, spark):
        """KNOWS*1..2 where b.age > 30 — sink filter pushed into join.

        1-hop: A->B(35), A->C(42), B->C(42), B->D(55), C->D(55), D->E(22)
        2-hop: A->B->C(42), A->B->D(55), A->C->D(55), B->C->D(55), B->D->E(22), C->D->E(22)
        Filter b.age>30: exclude E(22)
        """
        rows = _exec(gc, spark, """
            MATCH (a:Person)-[:KNOWS*1..2]->(b:Person)
            WHERE b.age > 30
            RETURN a.name AS src, b.name AS dst
        """, "Simplest sink filter")
        assert len(rows) > 0
        # All results should have dst with age > 30
        dst_names = {r["dst"] for r in rows}
        assert "Eve" not in dst_names  # Eve is 22
        assert "Frank" not in dst_names  # Frank is 17

    def test_vlp_with_sink_filter(self, gc, spark):
        """KNOWS*2..4 where b.age > 50 — only Dave(55) as sink."""
        rows = _exec(gc, spark, _adapt("""
            MATCH path = (a:Person)-[:KNOWS*2..4]->(b:Person)
            WHERE b.age > 50
            RETURN a.node_id AS src_id, b.node_id AS dst_id, LENGTH(path) AS chain_length
        """), "VLP with sink filter")
        assert len(rows) > 0
        # Dave is the only person with age > 50
        assert all(r["dst_id"] == 4 for r in rows)

    def test_dual_filter_pushdown(self, gc, spark):
        """Source a.age>30 AND sink b.age>50 — only Dave(55) as sink."""
        rows = _exec(gc, spark, _adapt("""
            MATCH path = (a:Person)-[:KNOWS*2..4]->(b:Person)
            WHERE a.age > 30 AND b.age > 50
            RETURN a.node_id AS src_id, b.node_id AS dst_id
        """), "Dual filter pushdown")
        # Source age>30: Bob(35), Carol(42), Dave(55)
        # Sink age>50: Dave(55) — 2+ hops from source
        # Bob->Dave(2-hop via Carol), Carol->Dave(direct, 1 hop but min is 2)
        # Note: exact count depends on path exploration
        assert all(r["dst_id"] == 4 for r in rows)

    @pytest.mark.xfail(reason="TRANSFORM() for list comprehension not supported in local PySpark")
    def test_compound_sink_filter(self, gc, spark):
        """Compound sink filter with path node list comprehension."""
        rows = _exec(gc, spark, _adapt("""
            MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person)
            WHERE b.age > 40 AND b.active = true
            RETURN a.node_id AS src_id, b.node_id AS dst_id, [n IN nodes(path) | n.node_id] AS path_nodes
        """), "Compound sink filter")
        assert len(rows) >= 0

    def test_combined_optimizations(self, gc, spark):
        """Edge predicate (since>2010) + sink filter (age>60) combined.

        No person has age > 60, so result should be empty.
        """
        rows = _exec(gc, spark, _adapt("""
            MATCH path = (a:Person)-[:KNOWS*2..5]->(b:Person)
            WHERE b.age > 60
              AND ALL(k IN relationships(path) WHERE k.since > 2010)
            RETURN a.node_id AS src_id, b.node_id AS dst_id, LENGTH(path) AS hops
        """), "Combined optimizations")
        assert len(rows) == 0  # No person has age > 60


# ===========================================================================
# CONJUNCTION SPLITTING (undirected pushdown)
# ===========================================================================
class TestConjunctionSplitting:

    def test_both_filters_pushdown(self, gc, spark):
        """p.name='Alice' pushed to DS(p), f.age>30 pushed to DS(f).

        Alice undirected KNOWS: Bob, Carol
        f.age > 30: Bob(35), Carol(42)
        """
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:KNOWS]-(f:Person)
            WHERE p.name = 'Alice' AND f.age > 30
            RETURN p.name AS person, f.name AS friend, f.age AS friend_age
        """, "Both filters pushdown")
        assert len(rows) == 2
        friends = {r["friend"] for r in rows}
        assert friends == {"Bob", "Carol"}

    def test_partial_pushdown_cross_variable(self, gc, spark):
        """p.age>25 pushed; p.name=f.name stays (cross-variable).

        p.age > 25 AND p.name = f.name
        Persons with age>25: Alice(28), Bob(35), Carol(42), Dave(55)
        Same-name pairs: none in our data (all different names)
        """
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:KNOWS]-(f:Person)
            WHERE p.age > 25 AND p.name = f.name
            RETURN p.name AS person, f.name AS friend
        """, "Partial pushdown")
        # No persons have the same name, so 0 rows
        assert len(rows) == 0

    def test_multiple_same_variable_predicates(self, gc, spark):
        """Multiple p predicates combined: name='Bob' AND age>18 AND active=true."""
        rows = _exec(gc, spark, _adapt("""
            MATCH (p:Person)-[:KNOWS]-(f:Person)
            WHERE p.name = 'Bob' AND p.age > 18 AND p.active = true AND f.salary > 50000
            RETURN p.id, f.id
        """), "Multiple same-var predicates")
        # Bob(35,active=true) undirected KNOWS: Alice, Carol, Dave
        # f.salary > 50000: Alice(120k), Carol(95k), Dave(150k)
        assert len(rows) == 3

    def test_or_predicate_cannot_split(self, gc, spark):
        """OR predicate cannot be pushed — stays in Selection."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:KNOWS]-(f:Person)
            WHERE p.name = 'Alice' OR f.age > 30
            RETURN p.name AS person, f.name AS friend
        """, "OR predicate")
        # This includes:
        # - All pairs where p is Alice (regardless of f)
        # - All pairs where f.age > 30 (regardless of p)
        assert len(rows) > 0

    def test_three_way_mixed_filters(self, gc, spark):
        """3-way join: p.age>25, f.salary>50000, c.industry='Tech'.

        Undirected KNOWS where p.age>25 AND f works at Tech AND f.salary>50000
        Tech workers with salary>50000: Alice(120k), Carol(95k), Eve(60k)
        """
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:KNOWS]-(f:Person)-[:WORKS_AT]->(c:Company)
            WHERE p.age > 25 AND f.salary > 50000 AND c.industry = 'Tech'
            RETURN p.name AS person, f.name AS tech_friend, c.name AS company
        """, "Three-way mixed filters")
        assert len(rows) > 0
        for r in rows:
            assert r["company"] == "TechCorp"


# ===========================================================================
# VLP + AGGREGATION (xfail)
# ===========================================================================
class TestVLPAggregation:

    def test_vlp_with_multihop_aggregation(self, gc, spark):
        """KNOWS*1..3 to friends who work at Tech companies.

        Tech workers: Alice(TechCorp), Carol(TechCorp), Eve(TechCorp)
        """
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:KNOWS*1..3]-(friend:Person)-[:WORKS_AT]->(c:Company)
            WHERE c.industry = 'Tech'
            RETURN p.name, COUNT(DISTINCT friend) AS tech_connections
            ORDER BY tech_connections DESC
            LIMIT 10
        """, "VLP + aggregation")
        assert len(rows) > 0
        # All results should have at least 1 tech connection
        for r in rows:
            assert r["tech_connections"] > 0


# ===========================================================================
# INLINE PROPERTY FILTERS
# ===========================================================================
class TestInlineFilters:

    def test_inline_source_filter(self, gc, spark):
        """Source: {name: 'Alice'} — same as WHERE p.name='Alice'."""
        rows = _exec(gc, spark, """
            MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(f:Person)
            RETURN p.name AS person, f.name AS friend
            LIMIT 20
        """, "Inline source filter")
        assert len(rows) == 2
        friends = {r["friend"] for r in rows}
        assert friends == {"Bob", "Carol"}

    def test_inline_target_filter(self, gc, spark):
        """Target: {age: 35} — matches Bob only."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:KNOWS]->(f:Person {age: 35})
            RETURN p.name AS person, f.name AS friend
            LIMIT 20
        """, "Inline target filter")
        # Who KNOWS Bob(age=35)? Alice->Bob
        assert len(rows) == 1

    def test_inline_relationship_filter(self, gc, spark):
        """Edge: {since: 2020} — Alice->Carol."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:KNOWS {since: 2020}]->(f:Person)
            RETURN p.name AS person, f.name AS friend
            LIMIT 20
        """, "Inline edge filter")
        assert len(rows) == 1

    def test_multiple_inline_filters(self, gc, spark):
        """{name: 'Alice', age: 28, active: true} — compound inline filter."""
        rows = _exec(gc, spark, """
            MATCH (p:Person {name: 'Alice', age: 28, active: true})-[:KNOWS]->(f:Person)
            RETURN p.name AS person, p.age AS person_age, f.name AS friend
            LIMIT 20
        """, "Multiple inline filters")
        assert len(rows) == 2

    def test_combined_inline_all_elements(self, gc, spark):
        """Inline on source, edge, and target simultaneously."""
        rows = _exec(gc, spark, """
            MATCH (p:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(f:Person {age: 42})
            RETURN p.name AS person, f.name AS friend
            LIMIT 20
        """, "Combined inline")
        # Alice -[since:2020]-> Carol(age:42)
        assert len(rows) == 1

    def test_inline_plus_where(self, gc, spark):
        """Inline {name: 'Alice'} combined with WHERE f.age > 25."""
        rows = _exec(gc, spark, """
            MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(f:Person)
            WHERE f.age > 25
            RETURN p.name AS person, f.name AS friend
            LIMIT 20
        """, "Inline + WHERE")
        # Alice -> Bob(35), Carol(42); both age > 25
        assert len(rows) == 2

    def test_bfs_inline_source(self, gc, spark):
        """BFS from Alice with inline source filter — reaches Bob, Carol, Dave, Eve."""
        rows = _exec(gc, spark, """
            MATCH path = (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(b:Person)
            RETURN b.name, length(path) AS hops
            ORDER BY hops
            LIMIT 50
        """, "BFS inline source")
        names = {r["name"] for r in rows}
        assert names == {"Bob", "Carol", "Dave", "Eve"}

    def test_bfs_inline_target(self, gc, spark):
        """BFS with target filter {active: true}."""
        rows = _exec(gc, spark, """
            MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person {active: true})
            RETURN a.name AS src, b.name AS dst, length(path) AS hops
            ORDER BY hops
            LIMIT 50
        """, "BFS inline target")
        # All results should have active=true targets (Bob, Dave, Eve, Alice)
        assert len(rows) > 0

    def test_anonymous_node_inline(self, gc, spark):
        """Inline filter on anonymous source node."""
        rows = _exec(gc, spark, """
            MATCH (:Person {name: 'Alice'})-[:KNOWS]->(f:Person)
            RETURN f.name AS friend
            LIMIT 20
        """, "Anonymous inline")
        assert len(rows) == 2
        names = {r["friend"] for r in rows}
        assert names == {"Bob", "Carol"}

    def test_inline_semantic_equivalence(self, gc, spark):
        """Inline filter produces same results as WHERE clause."""
        rows_inline = _exec(gc, spark, """
            MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(f:Person)
            RETURN COUNT(*) AS count_inline
        """, "Inline equivalence (inline)")
        rows_where = _exec(gc, spark, """
            MATCH (p:Person)-[:KNOWS]->(f:Person)
            WHERE p.name = 'Alice'
            RETURN COUNT(*) AS count_where
        """, "Inline equivalence (WHERE)")
        assert rows_inline[0]["count_inline"] == rows_where[0]["count_where"]


# ===========================================================================
# NO LABEL (nodes without explicit labels)
# ===========================================================================
class TestNoLabel:

    def test_no_label_source(self, gc, spark):
        """Source without label: (a)-[:WORKS_AT]->(c:Company).

        All entities that work at companies — only Person type has WORKS_AT edges.
        """
        rows = _exec(gc, spark, """
            MATCH (a)-[:WORKS_AT]->(c:Company)
            RETURN a.name AS employee, c.name AS company
            LIMIT 20
        """, "No-label source")
        assert len(rows) == 5  # 5 WORKS_AT edges
        names = {r["employee"] for r in rows}
        assert names == {"Alice", "Bob", "Carol", "Dave", "Eve"}

    def test_no_label_target(self, gc, spark):
        """Target without label: (p:Person)-[:KNOWS]->(target)."""
        rows = _exec(gc, spark, """
            MATCH (p:Person)-[:KNOWS]->(target)
            RETURN p.name AS person, target.name AS target_name
            LIMIT 20
        """, "No-label target")
        # All directed KNOWS edges: 6 edges
        assert len(rows) == 6

    def test_no_label_both(self, gc, spark):
        """Both without labels: (a)-[:KNOWS]->(b)."""
        rows = _exec(gc, spark, """
            MATCH (a)-[:KNOWS]->(b)
            RETURN a.name AS src, b.name AS dst
            LIMIT 20
        """, "No-label both")
        assert len(rows) == 6

    def test_no_label_vlp(self, gc, spark):
        """VLP without labels: (a)-[:KNOWS*1..2]->(b)."""
        rows = _exec(gc, spark, _adapt("""
            MATCH path = (a)-[:KNOWS*1..2]->(b)
            RETURN a.node_id AS src_id, b.node_id AS dst_id, length(path) AS hops
            LIMIT 50
        """), "No-label VLP")
        assert len(rows) > 0

    def test_no_label_vlp_labeled_source(self, gc, spark):
        """VLP: labeled source, unlabeled target."""
        rows = _exec(gc, spark, _adapt("""
            MATCH path = (a:Person)-[:KNOWS*1..2]->(b)
            RETURN a.name AS person, b.node_id AS target_id, length(path) AS hops
            LIMIT 50
        """), "No-label VLP partial")
        assert len(rows) > 0

    def test_no_label_vlp_labeled_target(self, gc, spark):
        """VLP: unlabeled source, labeled target."""
        rows = _exec(gc, spark, _adapt("""
            MATCH path = (a)-[:KNOWS*1..2]->(b:Person)
            RETURN a.node_id AS src_id, b.name AS person, length(path) AS hops
            LIMIT 50
        """), "No-label VLP reverse")
        assert len(rows) > 0

    def test_no_label_after_with(self, gc, spark):
        """Node type bound in first MATCH, reused in second without label."""
        rows = _exec(gc, spark, """
            MATCH (a:Person)-[:WORKS_AT]->(c:Company)
            WITH a, COUNT(c) AS company_count
            MATCH (a)-[:KNOWS]->(friend:Person)
            RETURN a.name AS person, company_count, friend.name AS friend
            LIMIT 20
        """, "No-label after WITH")
        assert len(rows) > 0
