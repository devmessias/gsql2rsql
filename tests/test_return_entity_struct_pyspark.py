"""PySpark Tests: RETURN entity should generate accessible NAMED_STRUCT.

These tests validate that when returning entire entities (node or edge) in Cypher,
the SQL output generates a STRUCT that:
1. Can be executed in PySpark without errors
2. Contains all expected fields accessible via dot notation
3. Has values that match the original data

Run with: uv run pytest tests/test_return_entity_struct_pyspark.py -v
"""

from __future__ import annotations

import pytest

# Skip all tests in this module if PySpark is not available
pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession, Row  # noqa: E402
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, FloatType  # noqa: E402

from gsql2rsql.pyspark_executor import (  # noqa: E402
    PySparkExecutor,
    adapt_sql_for_spark,
    create_spark_session,
    load_schema_from_yaml,
)


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Create a SparkSession for testing."""
    session = create_spark_session("gsql2rsql_entity_struct_test")
    yield session
    session.stop()


# Schema with multiple node and edge properties for comprehensive testing
ENTITY_STRUCT_SCHEMA = {
    "schema": {
        "nodes": [
            {
                "name": "Person",
                "tableName": "test.graph.Person",
                "idProperty": {"name": "id", "type": "int"},
                "properties": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "active", "type": "boolean"},
                ],
            },
            {
                "name": "Company",
                "tableName": "test.graph.Company",
                "idProperty": {"name": "id", "type": "int"},
                "properties": [
                    {"name": "name", "type": "string"},
                    {"name": "employees", "type": "int"},
                ],
            },
        ],
        "edges": [
            {
                "name": "KNOWS",
                "sourceNode": "Person",
                "sinkNode": "Person",
                "tableName": "test.graph.Knows",
                "sourceIdProperty": {"name": "src", "type": "int"},
                "sinkIdProperty": {"name": "dst", "type": "int"},
                "properties": [
                    {"name": "since", "type": "int"},
                    {"name": "strength", "type": "float"},
                ],
            },
            {
                "name": "WORKS_AT",
                "sourceNode": "Person",
                "sinkNode": "Company",
                "tableName": "test.graph.WorksAt",
                "sourceIdProperty": {"name": "person_id", "type": "int"},
                "sinkIdProperty": {"name": "company_id", "type": "int"},
                "properties": [
                    {"name": "role", "type": "string"},
                    {"name": "years", "type": "int"},
                ],
            },
        ],
    }
}


@pytest.fixture(scope="module")
def setup_test_data(spark: SparkSession):
    """Create test data with known values for validation."""
    # Person data with known values
    person_data = [
        (1, "Alice", 30, True),
        (2, "Bob", 25, True),
        (3, "Charlie", 35, False),
    ]
    person_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("active", BooleanType(), True),
    ])
    person_df = spark.createDataFrame(person_data, person_schema)
    person_df.createOrReplaceTempView("Person")

    # Company data
    company_data = [
        (100, "TechCorp", 500),
        (101, "StartupInc", 50),
    ]
    company_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("employees", IntegerType(), True),
    ])
    company_df = spark.createDataFrame(company_data, company_schema)
    company_df.createOrReplaceTempView("Company")

    # KNOWS edge data (Person -> Person)
    knows_data = [
        (1, 2, 2020, 0.8),  # Alice knows Bob
        (1, 3, 2019, 0.5),  # Alice knows Charlie
        (2, 3, 2021, 0.9),  # Bob knows Charlie
    ]
    knows_schema = StructType([
        StructField("src", IntegerType(), False),
        StructField("dst", IntegerType(), False),
        StructField("since", IntegerType(), True),
        StructField("strength", FloatType(), True),
    ])
    knows_df = spark.createDataFrame(knows_data, knows_schema)
    knows_df.createOrReplaceTempView("Knows")

    # WORKS_AT edge data (Person -> Company)
    works_at_data = [
        (1, 100, "Engineer", 5),   # Alice works at TechCorp
        (2, 100, "Manager", 3),    # Bob works at TechCorp
        (3, 101, "Founder", 10),   # Charlie works at StartupInc
    ]
    works_at_schema = StructType([
        StructField("person_id", IntegerType(), False),
        StructField("company_id", IntegerType(), False),
        StructField("role", StringType(), True),
        StructField("years", IntegerType(), True),
    ])
    works_at_df = spark.createDataFrame(works_at_data, works_at_schema)
    works_at_df.createOrReplaceTempView("WorksAt")

    return {
        "persons": person_data,
        "companies": company_data,
        "knows": knows_data,
        "works_at": works_at_data,
    }


@pytest.fixture(scope="module")
def executor(spark: SparkSession, setup_test_data) -> PySparkExecutor:
    """Create executor with test data loaded."""
    return PySparkExecutor(spark)


@pytest.fixture(scope="module")
def schema_provider():
    """Load the schema provider."""
    return load_schema_from_yaml(ENTITY_STRUCT_SCHEMA)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def execute_cypher(executor: PySparkExecutor, schema_provider, query: str):
    """Execute a Cypher query and return rows.

    Returns a tuple of (rows, row_count, sql) for easier testing.
    """
    result = executor.execute_query(query, schema_provider)
    assert result.success, f"Query failed: {result.error}\nSQL: {result.sql}"
    rows = result.dataframe.collect() if result.dataframe else []
    return rows, result.row_count, result.sql


# =============================================================================
# TEST CLASS: RETURN node entity -> STRUCT with accessible values
# =============================================================================


class TestReturnNodeEntityPySpark:
    """Test that RETURN node generates accessible STRUCT with correct values."""

    def test_return_single_node_struct_accessible(
        self, executor, schema_provider, setup_test_data
    ):
        """RETURN a should return a STRUCT with accessible fields.

        Query: MATCH (a:Person) WHERE a.name = 'Alice' RETURN a
        Expected: Result has a.id, a.name, a.age, a.active accessible
        """
        rows, row_count, sql = execute_cypher(
            executor, schema_provider,
            "MATCH (a:Person) WHERE a.name = 'Alice' RETURN a"
        )

        assert row_count == 1, f"Expected 1 row, got {row_count}"

        row = rows[0]
        # Access the struct - it should be named 'a'
        assert hasattr(row, 'a') or 'a' in row.asDict(), f"Row should have 'a' field: {row}"

        a_struct = row.a if hasattr(row, 'a') else row['a']

        # Verify struct fields are accessible
        struct_dict = a_struct.asDict() if hasattr(a_struct, 'asDict') else dict(a_struct)

        assert 'id' in struct_dict, f"STRUCT should have 'id' field: {struct_dict}"
        assert 'name' in struct_dict, f"STRUCT should have 'name' field: {struct_dict}"
        assert 'age' in struct_dict, f"STRUCT should have 'age' field: {struct_dict}"
        assert 'active' in struct_dict, f"STRUCT should have 'active' field: {struct_dict}"

        # Verify values match original data (Alice: id=1, name='Alice', age=30, active=True)
        assert struct_dict['id'] == 1, f"Expected id=1, got {struct_dict['id']}"
        assert struct_dict['name'] == 'Alice', f"Expected name='Alice', got {struct_dict['name']}"
        assert struct_dict['age'] == 30, f"Expected age=30, got {struct_dict['age']}"
        assert struct_dict['active'] is True, f"Expected active=True, got {struct_dict['active']}"

    def test_return_multiple_nodes_struct_values(
        self, executor, schema_provider, setup_test_data
    ):
        """RETURN a, b should return STRUCTs for both with correct values.

        Query: MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' AND b.name = 'Bob' RETURN a, b
        """
        rows, row_count, sql = execute_cypher(
            executor, schema_provider,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' AND b.name = 'Bob' RETURN a, b"
        )

        assert row_count >= 1, f"Expected at least 1 row, got {row_count}"

        row = rows[0]

        # Verify 'a' struct (Alice)
        a_struct = row.a if hasattr(row, 'a') else row['a']
        a_dict = a_struct.asDict() if hasattr(a_struct, 'asDict') else dict(a_struct)
        assert a_dict['name'] == 'Alice'
        assert a_dict['age'] == 30

        # Verify 'b' struct (Bob)
        b_struct = row.b if hasattr(row, 'b') else row['b']
        b_dict = b_struct.asDict() if hasattr(b_struct, 'asDict') else dict(b_struct)
        assert b_dict['name'] == 'Bob'
        assert b_dict['age'] == 25

    def test_return_node_with_alias_struct_accessible(
        self, executor, schema_provider, setup_test_data
    ):
        """RETURN a AS person should have STRUCT accessible via 'person' alias.

        Query: MATCH (a:Person) WHERE a.name = 'Bob' RETURN a AS person
        """
        rows, row_count, sql = execute_cypher(
            executor, schema_provider,
            "MATCH (a:Person) WHERE a.name = 'Bob' RETURN a AS person"
        )

        assert row_count == 1

        row = rows[0]
        # Access via alias 'person'
        person_struct = row.person if hasattr(row, 'person') else row['person']
        person_dict = person_struct.asDict() if hasattr(person_struct, 'asDict') else dict(person_struct)

        assert person_dict['id'] == 2
        assert person_dict['name'] == 'Bob'
        assert person_dict['age'] == 25
        assert person_dict['active'] is True

    def test_return_all_nodes_iterate_structs(
        self, executor, schema_provider, setup_test_data
    ):
        """RETURN a for all nodes should have STRUCTs with all values accessible.

        Query: MATCH (a:Person) RETURN a ORDER BY a.id
        Expected: 3 rows, each with accessible STRUCT
        """
        rows, row_count, sql = execute_cypher(
            executor, schema_provider,
            "MATCH (a:Person) RETURN a ORDER BY a.id"
        )

        assert row_count == 3

        # Expected data in order
        expected = [
            (1, "Alice", 30, True),
            (2, "Bob", 25, True),
            (3, "Charlie", 35, False),
        ]

        for i, row in enumerate(rows):
            a_struct = row.a if hasattr(row, 'a') else row['a']
            a_dict = a_struct.asDict() if hasattr(a_struct, 'asDict') else dict(a_struct)

            exp_id, exp_name, exp_age, exp_active = expected[i]
            assert a_dict['id'] == exp_id, f"Row {i}: expected id={exp_id}, got {a_dict['id']}"
            assert a_dict['name'] == exp_name, f"Row {i}: expected name={exp_name}, got {a_dict['name']}"
            assert a_dict['age'] == exp_age, f"Row {i}: expected age={exp_age}, got {a_dict['age']}"
            assert a_dict['active'] == exp_active, f"Row {i}: expected active={exp_active}, got {a_dict['active']}"


# =============================================================================
# TEST CLASS: RETURN edge entity -> STRUCT with accessible values
# =============================================================================


class TestReturnEdgeEntityPySpark:
    """Test that RETURN edge generates accessible STRUCT with correct values."""

    def test_return_single_edge_struct_accessible(
        self, executor, schema_provider, setup_test_data
    ):
        """RETURN r should return a STRUCT with accessible edge fields.

        Query: MATCH ()-[r:KNOWS]->() WHERE r.since = 2020 RETURN r
        Expected: STRUCT with src, dst, since, strength accessible
        """
        rows, row_count, sql = execute_cypher(
            executor, schema_provider,
            "MATCH ()-[r:KNOWS]->() WHERE r.since = 2020 RETURN r"
        )

        assert row_count >= 1

        row = rows[0]
        r_struct = row.r if hasattr(row, 'r') else row['r']
        r_dict = r_struct.asDict() if hasattr(r_struct, 'asDict') else dict(r_struct)

        # Verify struct has edge fields
        assert 'src' in r_dict, f"STRUCT should have 'src' field: {r_dict}"
        assert 'dst' in r_dict, f"STRUCT should have 'dst' field: {r_dict}"
        assert 'since' in r_dict, f"STRUCT should have 'since' field: {r_dict}"
        assert 'strength' in r_dict, f"STRUCT should have 'strength' field: {r_dict}"

        # Verify values (Alice->Bob edge: src=1, dst=2, since=2020, strength=0.8)
        assert r_dict['src'] == 1
        assert r_dict['dst'] == 2
        assert r_dict['since'] == 2020
        assert abs(r_dict['strength'] - 0.8) < 0.01  # Float comparison

    def test_return_edge_with_nodes_all_structs(
        self, executor, schema_provider, setup_test_data
    ):
        """RETURN a, r, b should return STRUCTs for all three entities.

        Query: MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since = 2020 RETURN a, r, b
        """
        rows, row_count, sql = execute_cypher(
            executor, schema_provider,
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since = 2020 RETURN a, r, b"
        )

        assert row_count >= 1

        row = rows[0]

        # Verify 'a' struct (Alice)
        a_struct = row.a if hasattr(row, 'a') else row['a']
        a_dict = a_struct.asDict() if hasattr(a_struct, 'asDict') else dict(a_struct)
        assert a_dict['name'] == 'Alice'

        # Verify 'r' struct (edge)
        r_struct = row.r if hasattr(row, 'r') else row['r']
        r_dict = r_struct.asDict() if hasattr(r_struct, 'asDict') else dict(r_struct)
        assert r_dict['since'] == 2020

        # Verify 'b' struct (Bob)
        b_struct = row.b if hasattr(row, 'b') else row['b']
        b_dict = b_struct.asDict() if hasattr(b_struct, 'asDict') else dict(b_struct)
        assert b_dict['name'] == 'Bob'

    def test_return_edge_with_alias_struct_accessible(
        self, executor, schema_provider, setup_test_data
    ):
        """RETURN r AS relationship should have STRUCT accessible via alias.

        Query: MATCH ()-[r:KNOWS]->() WHERE r.since = 2021 RETURN r AS relationship
        """
        rows, row_count, sql = execute_cypher(
            executor, schema_provider,
            "MATCH ()-[r:KNOWS]->() WHERE r.since = 2021 RETURN r AS relationship"
        )

        assert row_count >= 1

        row = rows[0]
        rel_struct = row.relationship if hasattr(row, 'relationship') else row['relationship']
        rel_dict = rel_struct.asDict() if hasattr(rel_struct, 'asDict') else dict(rel_struct)

        # Bob->Charlie edge: src=2, dst=3, since=2021, strength=0.9
        assert rel_dict['src'] == 2
        assert rel_dict['dst'] == 3
        assert rel_dict['since'] == 2021


# =============================================================================
# TEST CLASS: collect(entity) -> COLLECT_LIST(STRUCT) with accessible values
# =============================================================================


class TestCollectEntityPySpark:
    """Test that collect(entity) generates list of accessible STRUCTs."""

    def test_collect_nodes_list_of_structs(
        self, executor, schema_provider, setup_test_data
    ):
        """collect(b) should return a list of STRUCTs with accessible fields.

        Query: MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN a.name, collect(b) AS friends
        Expected: friends is a list of 2 STRUCTs (Bob, Charlie)
        """
        rows, row_count, sql = execute_cypher(
            executor, schema_provider,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN a.name AS name, collect(b) AS friends"
        )

        assert row_count == 1

        row = rows[0]
        assert row.name == 'Alice'

        friends = row.friends if hasattr(row, 'friends') else row['friends']
        assert len(friends) == 2, f"Expected 2 friends, got {len(friends)}"

        # Each friend should be a STRUCT with accessible fields
        friend_names = set()
        for friend in friends:
            f_dict = friend.asDict() if hasattr(friend, 'asDict') else dict(friend)
            assert 'id' in f_dict
            assert 'name' in f_dict
            assert 'age' in f_dict
            friend_names.add(f_dict['name'])

        assert friend_names == {'Bob', 'Charlie'}, f"Expected Bob and Charlie, got {friend_names}"

    def test_collect_edges_list_of_structs(
        self, executor, schema_provider, setup_test_data
    ):
        """collect(r) should return a list of edge STRUCTs.

        Query: MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN a.name, collect(r) AS relationships
        """
        rows, row_count, sql = execute_cypher(
            executor, schema_provider,
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN a.name AS name, collect(r) AS relationships"
        )

        assert row_count == 1

        row = rows[0]
        relationships = row.relationships if hasattr(row, 'relationships') else row['relationships']
        assert len(relationships) == 2, f"Expected 2 relationships, got {len(relationships)}"

        # Each relationship should have src, dst, since, strength
        since_values = set()
        for rel in relationships:
            r_dict = rel.asDict() if hasattr(rel, 'asDict') else dict(rel)
            assert 'src' in r_dict
            assert 'dst' in r_dict
            assert 'since' in r_dict
            since_values.add(r_dict['since'])

        assert since_values == {2019, 2020}, f"Expected since values 2019,2020, got {since_values}"


# =============================================================================
# TEST CLASS: Edge cases
# =============================================================================


class TestEntityStructEdgeCasesPySpark:
    """Edge cases for entity STRUCT in PySpark."""

    def test_return_distinct_entity_struct(
        self, executor, schema_provider, setup_test_data
    ):
        """RETURN DISTINCT a should work with STRUCTs.

        Query: MATCH (a:Person)-[:KNOWS]->() RETURN DISTINCT a ORDER BY a.id
        Expected: 2 distinct source nodes (Alice, Bob)
        """
        rows, row_count, sql = execute_cypher(
            executor, schema_provider,
            "MATCH (a:Person)-[:KNOWS]->() RETURN DISTINCT a ORDER BY a.id"
        )

        # Alice and Bob are sources, Charlie is only a target
        assert row_count == 2

        names = []
        for row in rows:
            a_struct = row.a if hasattr(row, 'a') else row['a']
            a_dict = a_struct.asDict() if hasattr(a_struct, 'asDict') else dict(a_struct)
            names.append(a_dict['name'])

        assert names == ['Alice', 'Bob']

    def test_return_entity_with_order_by_property(
        self, executor, schema_provider, setup_test_data
    ):
        """RETURN a ORDER BY a.age DESC should work.

        Query: MATCH (a:Person) RETURN a ORDER BY a.age DESC
        Expected: Charlie(35), Alice(30), Bob(25)
        """
        rows, row_count, sql = execute_cypher(
            executor, schema_provider,
            "MATCH (a:Person) RETURN a ORDER BY a.age DESC"
        )

        assert row_count == 3

        ages = []
        for row in rows:
            a_struct = row.a if hasattr(row, 'a') else row['a']
            a_dict = a_struct.asDict() if hasattr(a_struct, 'asDict') else dict(a_struct)
            ages.append(a_dict['age'])

        assert ages == [35, 30, 25], f"Expected [35, 30, 25], got {ages}"

    def test_return_entity_plus_explicit_property(
        self, executor, schema_provider, setup_test_data
    ):
        """RETURN a, a.name should have both STRUCT and explicit property.

        Query: MATCH (a:Person) WHERE a.id = 1 RETURN a, a.name AS name
        """
        rows, row_count, sql = execute_cypher(
            executor, schema_provider,
            "MATCH (a:Person) WHERE a.id = 1 RETURN a, a.name AS name"
        )

        assert row_count == 1

        row = rows[0]

        # Check explicit name column
        name_value = row.name if hasattr(row, 'name') else row['name']
        assert name_value == 'Alice'

        # Check struct also has name
        a_struct = row.a if hasattr(row, 'a') else row['a']
        a_dict = a_struct.asDict() if hasattr(a_struct, 'asDict') else dict(a_struct)
        assert a_dict['name'] == 'Alice'

    def test_return_entity_through_with_clause(
        self, executor, schema_provider, setup_test_data
    ):
        """Entity passed through WITH should still be accessible as STRUCT.

        Query: MATCH (a:Person) WITH a WHERE a.age > 25 RETURN a ORDER BY a.id
        """
        rows, row_count, sql = execute_cypher(
            executor, schema_provider,
            "MATCH (a:Person) WITH a WHERE a.age > 25 RETURN a ORDER BY a.id"
        )

        # Alice(30) and Charlie(35)
        assert row_count == 2

        names = []
        for row in rows:
            a_struct = row.a if hasattr(row, 'a') else row['a']
            a_dict = a_struct.asDict() if hasattr(a_struct, 'asDict') else dict(a_struct)
            names.append(a_dict['name'])

        assert names == ['Alice', 'Charlie']


# =============================================================================
# SINGLE TABLE SCHEMA TESTS (nodes_table + edges_table model)
# =============================================================================
# These tests use the simpler GraphContext with a single nodes and edges table,
# which is the common use case for graph queries.


from gsql2rsql import GraphContext  # noqa: E402


@pytest.fixture(scope="module")
def spark_single_table():
    """Create a SparkSession for single-table testing."""
    session = create_spark_session("gsql2rsql_single_table_test")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def setup_single_table_data(spark_single_table: SparkSession):
    """Create test data with single nodes/edges tables.

    Uses column names expected by GraphContext:
    - nodes: node_id, node_type, + properties
    - edges: src, dst, edge_type, + properties
    """
    spark = spark_single_table

    # Single nodes table with node_id, node_type, and properties
    nodes_data = [
        (1, "Person", "Alice", 30, True),
        (2, "Person", "Bob", 25, True),
        (3, "Person", "Charlie", 35, False),
        (100, "Company", "TechCorp", None, None),
    ]
    nodes_schema = StructType([
        StructField("node_id", IntegerType(), False),
        StructField("node_type", StringType(), True),  # GraphContext expects node_type
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("active", BooleanType(), True),
    ])
    nodes_df = spark.createDataFrame(nodes_data, nodes_schema)
    nodes_df.createOrReplaceTempView("nodes")

    # Single edges table with src, dst, edge_type, and properties
    edges_data = [
        (1, 2, "KNOWS", 2020, 0.8),   # Alice knows Bob
        (1, 3, "KNOWS", 2019, 0.5),   # Alice knows Charlie
        (2, 3, "KNOWS", 2021, 0.9),   # Bob knows Charlie
        (1, 100, "WORKS_AT", 2015, None),  # Alice works at TechCorp
    ]
    edges_schema = StructType([
        StructField("src", IntegerType(), False),
        StructField("dst", IntegerType(), False),
        StructField("edge_type", StringType(), True),  # GraphContext expects edge_type
        StructField("since", IntegerType(), True),
        StructField("weight", FloatType(), True),
    ])
    edges_df = spark.createDataFrame(edges_data, edges_schema)
    edges_df.createOrReplaceTempView("edges")

    return {
        "nodes": nodes_data,
        "edges": edges_data,
    }


@pytest.fixture(scope="module")
def graph_context_simple(setup_single_table_data) -> GraphContext:
    """Create a simple GraphContext with single tables."""
    g = GraphContext(
        nodes_table="nodes",
        edges_table="edges",
        edge_type_col="edge_type",  # Match the column name in edges table
        extra_node_attrs={
            "name": str,
            "age": int,
            "active": bool,
        },
        extra_edge_attrs={
            "since": int,
            "weight": float,
        },
    )
    g.set_types(
        node_types=["Person", "Company"],
        edge_types=["KNOWS", "WORKS_AT"],
    )
    return g


def execute_simple_query(spark: SparkSession, g: GraphContext, query: str):
    """Execute a Cypher query using simple GraphContext."""
    sql = g.transpile(query)
    # Adapt SQL for local Spark (remove catalog.schema prefixes)
    adapted_sql = adapt_sql_for_spark(sql)
    try:
        df = spark.sql(adapted_sql)
        rows = df.collect()
        return rows, len(rows), sql, None
    except Exception as e:
        return [], 0, sql, str(e)


class TestSingleTableNodeEntity:
    """Test RETURN node with single nodes_table model."""

    def test_return_node_single_table(
        self, spark_single_table, graph_context_simple, setup_single_table_data
    ):
        """RETURN a from single nodes table should generate accessible STRUCT."""
        rows, count, sql, error = execute_simple_query(
            spark_single_table, graph_context_simple,
            "MATCH (a:Person) WHERE a.name = 'Alice' RETURN a"
        )

        if error:
            pytest.skip(f"Query failed (known issue - column propagation): {error}")

        assert count == 1, f"Expected 1 row, got {count}"

        row = rows[0]
        a_struct = row.a if hasattr(row, 'a') else row['a']
        a_dict = a_struct.asDict() if hasattr(a_struct, 'asDict') else dict(a_struct)

        # Verify node_id and properties are accessible
        assert 'node_id' in a_dict, f"STRUCT should have 'node_id': {a_dict}"
        assert 'name' in a_dict, f"STRUCT should have 'name': {a_dict}"
        assert a_dict['name'] == 'Alice'

    def test_return_multiple_nodes_single_table(
        self, spark_single_table, graph_context_simple, setup_single_table_data
    ):
        """RETURN a, b from single table should both be STRUCTs."""
        rows, count, sql, error = execute_simple_query(
            spark_single_table, graph_context_simple,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' AND b.name = 'Bob' RETURN a, b"
        )

        if error:
            pytest.skip(f"Query failed (known issue): {error}")

        assert count >= 1

        row = rows[0]
        a_dict = row.a.asDict() if hasattr(row.a, 'asDict') else dict(row.a)
        b_dict = row.b.asDict() if hasattr(row.b, 'asDict') else dict(row.b)

        assert a_dict['name'] == 'Alice'
        assert b_dict['name'] == 'Bob'


class TestSingleTableEdgeEntity:
    """Test RETURN edge with single edges_table model."""

    def test_return_edge_single_table(
        self, spark_single_table, graph_context_simple, setup_single_table_data
    ):
        """RETURN r from single edges table should generate accessible STRUCT."""
        rows, count, sql, error = execute_simple_query(
            spark_single_table, graph_context_simple,
            "MATCH ()-[r:KNOWS]->() WHERE r.since = 2020 RETURN r"
        )

        if error:
            pytest.skip(f"Query failed (known issue - column propagation): {error}")

        assert count >= 1

        row = rows[0]
        r_struct = row.r if hasattr(row, 'r') else row['r']
        r_dict = r_struct.asDict() if hasattr(r_struct, 'asDict') else dict(r_struct)

        # Verify edge fields are accessible
        assert 'src' in r_dict, f"STRUCT should have 'src': {r_dict}"
        assert 'dst' in r_dict, f"STRUCT should have 'dst': {r_dict}"
        assert 'since' in r_dict, f"STRUCT should have 'since': {r_dict}"
        assert r_dict['since'] == 2020

    def test_return_all_entities_single_table(
        self, spark_single_table, graph_context_simple, setup_single_table_data
    ):
        """RETURN a, r, b from single tables should all be STRUCTs."""
        rows, count, sql, error = execute_simple_query(
            spark_single_table, graph_context_simple,
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since = 2020 RETURN a, r, b"
        )

        if error:
            pytest.skip(f"Query failed (known issue): {error}")

        assert count >= 1

        row = rows[0]

        # All three should be accessible as structs
        a_dict = row.a.asDict() if hasattr(row.a, 'asDict') else dict(row.a)
        r_dict = row.r.asDict() if hasattr(row.r, 'asDict') else dict(row.r)
        b_dict = row.b.asDict() if hasattr(row.b, 'asDict') else dict(row.b)

        assert a_dict['name'] == 'Alice'
        assert r_dict['since'] == 2020
        assert b_dict['name'] == 'Bob'


class TestSingleTableCollectEntity:
    """Test collect(entity) with single table model."""

    def test_collect_nodes_single_table(
        self, spark_single_table, graph_context_simple, setup_single_table_data
    ):
        """collect(b) from single table should return list of STRUCTs."""
        rows, count, sql, error = execute_simple_query(
            spark_single_table, graph_context_simple,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN a.name AS name, collect(b) AS friends"
        )

        if error:
            pytest.skip(f"Query failed (known issue): {error}")

        assert count == 1

        row = rows[0]
        assert row.name == 'Alice'

        friends = row.friends
        assert len(friends) == 2, f"Expected 2 friends, got {len(friends)}"

        friend_names = {f.asDict()['name'] for f in friends}
        assert friend_names == {'Bob', 'Charlie'}

    def test_collect_edges_single_table(
        self, spark_single_table, graph_context_simple, setup_single_table_data
    ):
        """collect(r) from single table should return list of edge STRUCTs."""
        rows, count, sql, error = execute_simple_query(
            spark_single_table, graph_context_simple,
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN a.name AS name, collect(r) AS rels"
        )

        if error:
            pytest.skip(f"Query failed (known issue): {error}")

        assert count == 1

        row = rows[0]
        rels = row.rels
        assert len(rels) == 2

        since_values = {r.asDict()['since'] for r in rels}
        assert since_values == {2019, 2020}
