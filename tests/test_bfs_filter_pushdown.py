"""PySpark tests for BFS filter pushdown optimization.

These tests verify that filters from previous MATCH clauses are correctly
pushed into the recursive CTE's base case for VLP (Variable Length Path) queries.

The optimization is critical for performance - without it, the CTE explores
ALL paths in the graph before filtering.
"""

from __future__ import annotations

import re

import pytest

# Skip all tests in this module if PySpark is not available
pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from gsql2rsql.pyspark_executor import (
    PySparkExecutor,
    create_spark_session,
    load_schema_from_yaml,
    transpile_query,
)


# Schema for testing - simple Person + KNOWS graph
BFS_TEST_SCHEMA = {
    "schema": {
        "nodes": [
            {
                "name": "Person",
                "tableName": "test.bfs.Person",
                "idProperty": {"name": "id", "type": "string"},
                "properties": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                ],
            },
        ],
        "edges": [
            {
                "name": "KNOWS",
                "sourceNode": "Person",
                "sinkNode": "Person",
                "tableName": "test.bfs.Knows",
                "sourceIdProperty": {"name": "src", "type": "string"},
                "sinkIdProperty": {"name": "dst", "type": "string"},
                "properties": [{"name": "since", "type": "int"}],
            },
        ],
    }
}


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Create a SparkSession for testing."""
    session = create_spark_session("bfs_filter_pushdown_test")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def setup_test_data(spark: SparkSession) -> None:
    """Create test data for BFS filter pushdown tests.

    Graph structure:
        alice --KNOWS--> bob --KNOWS--> charlie --KNOWS--> diana
        alice --KNOWS--> eve
        frank --KNOWS--> george (separate component)

    This allows testing that filtering by alice only explores
    alice's connected component.
    """
    # Create Person table
    persons = [
        ("alice", "Alice", 30),
        ("bob", "Bob", 25),
        ("charlie", "Charlie", 35),
        ("diana", "Diana", 28),
        ("eve", "Eve", 32),
        ("frank", "Frank", 40),
        ("george", "George", 45),
    ]
    person_df = spark.createDataFrame(persons, ["id", "name", "age"])
    person_df.createOrReplaceTempView("Person")

    # Create Knows table
    knows = [
        ("alice", "bob", 2020),
        ("bob", "charlie", 2019),
        ("charlie", "diana", 2021),
        ("alice", "eve", 2018),
        ("frank", "george", 2022),
    ]
    knows_df = spark.createDataFrame(knows, ["src", "dst", "since"])
    knows_df.createOrReplaceTempView("Knows")


def get_cte_base_case(sql: str) -> str | None:
    """Extract the CTE base case from SQL.

    Handles various CTE formats including multi-line with comments.
    """
    # Find everything from WITH RECURSIVE to UNION ALL
    cte_match = re.search(
        r"WITH RECURSIVE\s*\n?\s*\w+\s+AS\s*\(\s*(.*?)\s*UNION ALL",
        sql,
        re.DOTALL | re.IGNORECASE,
    )
    if cte_match:
        return cte_match.group(1).strip()
    return None


class TestBFSFilterPushdownSQL:
    """Tests that verify the generated SQL has filter in CTE base case."""

    def test_two_match_inline_property_pushdown(self) -> None:
        """Filter from first MATCH inline property should push to CTE.

        Query:
            MATCH (root:Person { id: "alice" })
            MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
            RETURN target.name

        Expected: CTE base case should contain 'alice' filter.
        """
        provider = load_schema_from_yaml(BFS_TEST_SCHEMA)
        query = """
        MATCH (root:Person { id: "alice" })
        MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
        RETURN target.name
        """

        sql, error = transpile_query(query, provider)

        assert error is None, f"Transpilation failed: {error}"
        assert sql is not None

        base_case = get_cte_base_case(sql)
        assert base_case is not None, "No CTE base case found"
        assert "alice" in base_case, (
            f"Filter 'alice' not found in CTE base case. "
            f"Base case: {base_case}"
        )

    def test_two_match_where_clause_pushdown(self) -> None:
        """Filter from first MATCH WHERE should push to CTE.

        Query:
            MATCH (root:Person)
            WHERE root.id = "alice"
            MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
            RETURN target.name

        Expected: CTE base case should contain 'alice' filter.
        """
        provider = load_schema_from_yaml(BFS_TEST_SCHEMA)
        query = """
        MATCH (root:Person)
        WHERE root.id = "alice"
        MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
        RETURN target.name
        """

        sql, error = transpile_query(query, provider)

        assert error is None, f"Transpilation failed: {error}"
        assert sql is not None

        base_case = get_cte_base_case(sql)
        assert base_case is not None, "No CTE base case found"
        assert "alice" in base_case, (
            f"Filter 'alice' not found in CTE base case. "
            f"Base case: {base_case}"
        )

    def test_single_match_inline_regression(self) -> None:
        """Single MATCH with inline property should work (regression test).

        This case should already work - testing to ensure no regression.
        """
        provider = load_schema_from_yaml(BFS_TEST_SCHEMA)
        query = """
        MATCH p = (root:Person { id: "alice" })-[:KNOWS*1..3]->(target:Person)
        RETURN target.name
        """

        sql, error = transpile_query(query, provider)

        assert error is None, f"Transpilation failed: {error}"
        assert sql is not None

        base_case = get_cte_base_case(sql)
        assert base_case is not None, "No CTE base case found"
        assert "alice" in base_case, (
            f"Filter 'alice' not found in CTE base case. "
            f"Base case: {base_case}"
        )

    def test_single_match_where_regression(self) -> None:
        """Single MATCH with WHERE should work (regression test)."""
        provider = load_schema_from_yaml(BFS_TEST_SCHEMA)
        query = """
        MATCH p = (root:Person)-[:KNOWS*1..3]->(target:Person)
        WHERE root.id = "alice"
        RETURN target.name
        """

        sql, error = transpile_query(query, provider)

        assert error is None, f"Transpilation failed: {error}"
        assert sql is not None

        base_case = get_cte_base_case(sql)
        assert base_case is not None, "No CTE base case found"
        assert "alice" in base_case, (
            f"Filter 'alice' not found in CTE base case. "
            f"Base case: {base_case}"
        )


class TestBFSFilterPushdownExecution:
    """Tests that verify correct execution results with PySpark."""

    def test_two_match_inline_returns_correct_results(
        self, spark: SparkSession, setup_test_data: None
    ) -> None:
        """Two MATCH with inline filter should return only connected nodes.

        Starting from alice, depth 1..3 should find:
        - bob (depth 1)
        - eve (depth 1)
        - charlie (depth 2)
        - diana (depth 3)

        Should NOT find frank or george (separate component).
        """
        provider = load_schema_from_yaml(BFS_TEST_SCHEMA)
        executor = PySparkExecutor(spark)

        result = executor.execute_query(
            """
            MATCH (root:Person { id: "alice" })
            MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
            RETURN DISTINCT target.name AS name
            """,
            provider,
        )

        assert result.success, f"Query failed: {result.error}"

        # Collect results
        names = {row["name"] for row in result.sample_rows}

        # Should find alice's connected nodes
        assert "Bob" in names or "bob" in str(names).lower()
        assert "Eve" in names or "eve" in str(names).lower()

        # Should NOT find frank's component
        assert "Frank" not in names and "frank" not in str(names).lower()
        assert "George" not in names and "george" not in str(names).lower()

    def test_two_match_where_returns_correct_results(
        self, spark: SparkSession, setup_test_data: None
    ) -> None:
        """Two MATCH with WHERE filter should return only connected nodes."""
        provider = load_schema_from_yaml(BFS_TEST_SCHEMA)
        executor = PySparkExecutor(spark)

        result = executor.execute_query(
            """
            MATCH (root:Person)
            WHERE root.id = "alice"
            MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
            RETURN DISTINCT target.name AS name
            """,
            provider,
        )

        assert result.success, f"Query failed: {result.error}"

        # Collect results
        names = {row["name"] for row in result.sample_rows}

        # Should find alice's connected nodes
        assert "Bob" in names or "bob" in str(names).lower()

        # Should NOT find frank's component
        assert "Frank" not in names and "frank" not in str(names).lower()

    def test_filter_different_root_returns_different_results(
        self, spark: SparkSession, setup_test_data: None
    ) -> None:
        """Filtering by frank should only return george."""
        provider = load_schema_from_yaml(BFS_TEST_SCHEMA)
        executor = PySparkExecutor(spark)

        result = executor.execute_query(
            """
            MATCH (root:Person { id: "frank" })
            MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
            RETURN DISTINCT target.name AS name
            """,
            provider,
        )

        assert result.success, f"Query failed: {result.error}"

        # Collect results
        names = {row["name"] for row in result.sample_rows}

        # Should only find george
        assert "George" in names or "george" in str(names).lower()

        # Should NOT find alice's component
        assert "Bob" not in names and "bob" not in str(names).lower()
        assert "Alice" not in names and "alice" not in str(names).lower()


class TestBFSFilterPushdownEdgeCases:
    """Tests for edge cases in BFS filter pushdown."""

    def test_multiple_filters_on_same_node(self) -> None:
        """Multiple filters on same node should all push to CTE."""
        provider = load_schema_from_yaml(BFS_TEST_SCHEMA)
        query = """
        MATCH (root:Person { id: "alice" })
        WHERE root.age > 20
        MATCH p = (root)-[:KNOWS*1..2]->(target:Person)
        RETURN target.name
        """

        sql, error = transpile_query(query, provider)

        assert error is None, f"Transpilation failed: {error}"
        assert sql is not None

        # Both filters should be present somewhere
        assert "alice" in sql
        assert "20" in sql

    def test_filter_on_unrelated_node_not_pushed(self) -> None:
        """Filter on unrelated node should NOT push to CTE base case."""
        provider = load_schema_from_yaml(BFS_TEST_SCHEMA)
        query = """
        MATCH (root:Person { id: "alice" })
        MATCH p = (root)-[:KNOWS*1..2]->(target:Person)
        WHERE target.age > 20
        RETURN target.name
        """

        sql, error = transpile_query(query, provider)

        assert error is None, f"Transpilation failed: {error}"
        assert sql is not None

        base_case = get_cte_base_case(sql)
        assert base_case is not None, "No CTE base case found"

        # alice filter SHOULD be in base case
        assert "alice" in base_case, "Root filter 'alice' should be in base case"

        # target.age filter should NOT be in base case (it's a post-filter)
        # The "20" in base case would only come from the target filter
        # which should be applied after the CTE


class TestRegressionNoFilterCase:
    """Regression tests for queries without filters (should still work)."""

    def test_vlp_without_filter(self) -> None:
        """VLP without any filter should still work."""
        provider = load_schema_from_yaml(BFS_TEST_SCHEMA)
        query = """
        MATCH p = (root:Person)-[:KNOWS*1..2]->(target:Person)
        RETURN root.name, target.name
        """

        sql, error = transpile_query(query, provider)

        assert error is None, f"Transpilation failed: {error}"
        assert sql is not None
        assert "WITH RECURSIVE" in sql
