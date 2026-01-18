"""Basic PySpark infrastructure tests.

These tests validate that the PySpark testing infrastructure works correctly
before running the full example suite.
"""

from __future__ import annotations

import pytest

# Skip all tests in this module if PySpark is not available
pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402, I001

from gsql2rsql.pyspark_executor import (  # noqa: E402, I001
    PySparkExecutor,
    adapt_sql_for_spark,
    create_spark_session,
    load_schema_from_yaml,
    transpile_query,
)
from tests.utils.sample_data_generator import (  # noqa: E402, I001
    SchemaDataGenerator,
    generate_sample_data_for_yaml,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Create a SparkSession for testing."""
    session = create_spark_session("gsql2rsql_basic_test")
    yield session
    session.stop()


# Simple test schema for basic validation
SIMPLE_SCHEMA = {
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
                "name": "City",
                "tableName": "test.graph.City",
                "idProperty": {"name": "id", "type": "int"},
                "properties": [
                    {"name": "name", "type": "string"},
                    {"name": "population", "type": "int"},
                ],
            },
        ],
        "edges": [
            {
                "name": "KNOWS",
                "sourceNode": "Person",
                "sinkNode": "Person",
                "tableName": "test.graph.Knows",
                "sourceIdProperty": {"name": "person_id", "type": "int"},
                "sinkIdProperty": {"name": "friend_id", "type": "int"},
                "properties": [{"name": "since", "type": "int"}],
            },
            {
                "name": "LIVES_IN",
                "sourceNode": "Person",
                "sinkNode": "City",
                "tableName": "test.graph.LivesIn",
                "sourceIdProperty": {"name": "person_id", "type": "int"},
                "sinkIdProperty": {"name": "city_id", "type": "int"},
            },
        ],
    }
}


class TestSQLAdaptation:
    """Tests for SQL adaptation from Databricks to local Spark."""

    def test_adapt_simple_table(self) -> None:
        sql = "SELECT * FROM catalog.schema.Person"
        adapted = adapt_sql_for_spark(sql)
        assert adapted == "SELECT * FROM Person"

    def test_adapt_backtick_table(self) -> None:
        sql = "SELECT * FROM `catalog`.`schema`.`Person`"
        adapted = adapt_sql_for_spark(sql)
        assert adapted == "SELECT * FROM Person"

    def test_adapt_multiple_tables(self) -> None:
        sql = """
        SELECT p.name, c.name
        FROM catalog.demo.Person p
        JOIN catalog.demo.City c ON p.city_id = c.id
        """
        adapted = adapt_sql_for_spark(sql)
        assert "Person p" in adapted
        assert "City c" in adapted
        assert "catalog.demo" not in adapted

    def test_preserve_string_literals(self) -> None:
        sql = "SELECT * FROM catalog.schema.Person WHERE name = 'catalog.schema.test'"
        adapted = adapt_sql_for_spark(sql)
        assert "catalog.schema.test" in adapted  # String preserved
        assert adapted.count("Person") == 1  # Table adapted


class TestSchemaLoading:
    """Tests for schema loading from YAML."""

    def test_load_simple_schema(self) -> None:
        provider = load_schema_from_yaml(SIMPLE_SCHEMA)

        # Check nodes are loaded
        assert provider.get_node_definition("Person") is not None
        assert provider.get_node_definition("City") is not None

        # Check edges are loaded
        assert provider.get_edge_definition("KNOWS", "Person", "Person") is not None
        assert provider.get_edge_definition("LIVES_IN", "Person", "City") is not None

    def test_schema_properties(self) -> None:
        provider = load_schema_from_yaml(SIMPLE_SCHEMA)
        person = provider.get_node_definition("Person")
        assert person is not None
        # Check id property
        assert person.node_id_property.property_name == "id"


class TestDataGeneration:
    """Tests for sample data generation."""

    def test_generate_node_data(self, spark: SparkSession) -> None:
        generator = SchemaDataGenerator(spark, SIMPLE_SCHEMA, seed=42)
        df = generator.generate_node_data("Person", count=5)

        assert df.count() == 5
        assert "id" in df.columns
        assert "name" in df.columns
        assert "age" in df.columns
        assert "active" in df.columns

    def test_generate_edge_data(self, spark: SparkSession) -> None:
        generator = SchemaDataGenerator(spark, SIMPLE_SCHEMA, seed=42)

        # Generate nodes first
        generator.generate_node_data("Person", count=5)

        # Then generate edges
        df = generator.generate_edge_data("KNOWS", density=0.5)

        assert df.count() >= 1
        assert "person_id" in df.columns
        assert "friend_id" in df.columns
        assert "since" in df.columns

    def test_generate_all_data(self, spark: SparkSession) -> None:
        generator = generate_sample_data_for_yaml(
            spark,
            SIMPLE_SCHEMA,
            default_node_count=5,
            edge_density=0.3,
            seed=42,
        )

        # Check all tables were created
        assert "Person" in generator.generated_dfs
        assert "City" in generator.generated_dfs
        assert "Knows" in generator.generated_dfs
        assert "LivesIn" in generator.generated_dfs

    def test_registered_views(self, spark: SparkSession) -> None:
        _ = generate_sample_data_for_yaml(
            spark,
            SIMPLE_SCHEMA,
            default_node_count=5,
            seed=42,
        )

        # Views should be registered
        result = spark.sql("SELECT COUNT(*) FROM Person").collect()
        assert result[0][0] == 5


class TestTranspilation:
    """Tests for query transpilation."""

    def test_transpile_simple_match(self) -> None:
        provider = load_schema_from_yaml(SIMPLE_SCHEMA)
        query = "MATCH (p:Person) RETURN p.name, p.age"

        sql, error = transpile_query(query, provider)

        assert error is None
        assert sql is not None
        assert "SELECT" in sql.upper()

    def test_transpile_with_where(self) -> None:
        provider = load_schema_from_yaml(SIMPLE_SCHEMA)
        query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name"

        sql, error = transpile_query(query, provider)

        assert error is None
        assert sql is not None
        assert "WHERE" in sql.upper() or "HAVING" in sql.upper() or ">" in sql

    def test_transpile_invalid_query(self) -> None:
        provider = load_schema_from_yaml(SIMPLE_SCHEMA)
        query = "INVALID CYPHER QUERY"

        sql, error = transpile_query(query, provider)

        assert sql is None
        assert error is not None


class TestQueryExecution:
    """Tests for end-to-end query execution."""

    def test_execute_simple_query(self, spark: SparkSession) -> None:
        # Setup data
        _ = generate_sample_data_for_yaml(
            spark,
            SIMPLE_SCHEMA,
            default_node_count=10,
            seed=42,
        )

        # Create executor
        executor = PySparkExecutor(spark)
        provider = load_schema_from_yaml(SIMPLE_SCHEMA)

        # Execute query
        result = executor.execute_query(
            "MATCH (p:Person) RETURN p.name, p.age",
            provider,
        )

        assert result.success, f"Query failed: {result.error}"
        assert result.row_count == 10
        assert "name" in result.columns or "p.name" in str(result.columns).lower()

    def test_execute_filtered_query(self, spark: SparkSession) -> None:
        # Setup data with predictable values
        _ = generate_sample_data_for_yaml(
            spark,
            SIMPLE_SCHEMA,
            default_node_count=20,
            seed=42,
        )

        executor = PySparkExecutor(spark)
        provider = load_schema_from_yaml(SIMPLE_SCHEMA)

        # Query with filter
        result = executor.execute_query(
            "MATCH (p:Person) WHERE p.age > 50 RETURN p.name, p.age",
            provider,
        )

        assert result.success, f"Query failed: {result.error}"
        # Should have some results (depends on random data)
        assert result.row_count is not None

    def test_execute_join_query(self, spark: SparkSession) -> None:
        _ = generate_sample_data_for_yaml(
            spark,
            SIMPLE_SCHEMA,
            default_node_count=10,
            edge_density=0.5,
            seed=42,
        )

        executor = PySparkExecutor(spark)
        provider = load_schema_from_yaml(SIMPLE_SCHEMA)

        # Query with relationship
        result = executor.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name",
            provider,
        )

        assert result.success, f"Query failed: {result.error}"

    def test_error_capture(self, spark: SparkSession) -> None:
        executor = PySparkExecutor(spark)
        provider = load_schema_from_yaml(SIMPLE_SCHEMA)

        # Query with syntax error
        result = executor.execute_query(
            "THIS IS NOT VALID CYPHER",
            provider,
        )

        assert not result.success
        assert result.error is not None
        assert result.error_stage == "transpile"


class TestResultDetails:
    """Tests for result details and sample collection."""

    def test_sample_rows_collected(self, spark: SparkSession) -> None:
        _ = generate_sample_data_for_yaml(
            spark,
            SIMPLE_SCHEMA,
            default_node_count=20,
            seed=42,
        )

        executor = PySparkExecutor(spark)
        provider = load_schema_from_yaml(SIMPLE_SCHEMA)

        result = executor.execute_query(
            "MATCH (p:Person) RETURN p.id, p.name, p.age",
            provider,
            collect_sample=5,
        )

        assert result.success
        assert len(result.sample_rows) <= 5
        # Check sample row structure
        if result.sample_rows:
            row = result.sample_rows[0]
            assert isinstance(row, dict)

    def test_columns_captured(self, spark: SparkSession) -> None:
        _ = generate_sample_data_for_yaml(
            spark,
            SIMPLE_SCHEMA,
            default_node_count=5,
            seed=42,
        )

        executor = PySparkExecutor(spark)
        provider = load_schema_from_yaml(SIMPLE_SCHEMA)

        result = executor.execute_query(
            "MATCH (p:Person) RETURN p.name AS person_name, p.age AS person_age",
            provider,
        )

        assert result.success
        assert len(result.columns) == 2
