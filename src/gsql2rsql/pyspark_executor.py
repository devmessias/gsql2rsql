"""PySpark executor for testing transpiled SQL queries.

This module provides utilities to execute transpiled Databricks SQL queries
using PySpark, enabling validation of the transpiler output against real
SQL execution.
"""

from __future__ import annotations

import contextlib
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from gsql2rsql import LogicalPlan, OpenCypherParser, SQLRenderer
from gsql2rsql.common.schema import EdgeSchema, EntityProperty, NodeSchema
from gsql2rsql.planner.subquery_optimizer import optimize_plan
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass
class PySparkExecutionResult:
    """Result of executing a transpiled query on PySpark."""

    success: bool
    query: str
    sql: str | None = None
    dataframe: DataFrame | None = None
    row_count: int | None = None
    columns: list[str] = field(default_factory=list)
    error: str | None = None
    error_stage: str | None = None  # "transpile", "parse_sql", "execute"
    sample_rows: list[dict[str, Any]] = field(default_factory=list)


def get_python_type(type_str: str) -> type:
    """Convert YAML/schema type string to Python type."""
    type_map = {
        "int": int,
        "integer": int,
        "long": int,
        "float": float,
        "double": float,
        "string": str,
        "str": str,
        "bool": bool,
        "boolean": bool,
    }
    return type_map.get(type_str.lower(), str)


def get_spark_type(type_str: str) -> str:
    """Convert YAML/schema type string to Spark SQL type."""
    type_map = {
        "int": "INT",
        "integer": "INT",
        "long": "BIGINT",
        "float": "DOUBLE",
        "double": "DOUBLE",
        "string": "STRING",
        "str": "STRING",
        "bool": "BOOLEAN",
        "boolean": "BOOLEAN",
    }
    return type_map.get(type_str.lower(), "STRING")


def load_schema_from_yaml(yaml_data: dict[str, Any]) -> SimpleSQLSchemaProvider:
    """Load schema from YAML data into a SimpleSQLSchemaProvider.

    Args:
        yaml_data: YAML data with 'schema' key containing nodes and edges.

    Returns:
        Configured SimpleSQLSchemaProvider.
    """
    provider = SimpleSQLSchemaProvider()
    schema = yaml_data.get("schema", {})

    # Load nodes
    for node in schema.get("nodes", []):
        id_prop = node.get("idProperty", {})
        props = [
            EntityProperty(
                property_name=p["name"],
                data_type=get_python_type(p["type"]),
            )
            for p in node.get("properties", [])
        ]

        node_schema = NodeSchema(
            name=node["name"],
            properties=props,
            node_id_property=EntityProperty(
                property_name=id_prop.get("name", "id"),
                data_type=get_python_type(id_prop.get("type", "int")),
            ),
        )
        provider.add_node(node_schema, SQLTableDescriptor(table_or_view_name=node["tableName"]))

    # Load edges
    for edge in schema.get("edges", []):
        source_id = edge.get("sourceIdProperty", {})
        sink_id = edge.get("sinkIdProperty", {})
        props = [
            EntityProperty(
                property_name=p["name"],
                data_type=get_python_type(p["type"]),
            )
            for p in edge.get("properties", [])
        ]

        edge_schema = EdgeSchema(
            name=edge["name"],
            source_node_id=edge["sourceNode"],
            sink_node_id=edge["sinkNode"],
            source_id_property=EntityProperty(
                property_name=source_id.get("name", "source_id"),
                data_type=get_python_type(source_id.get("type", "int")),
            ),
            sink_id_property=EntityProperty(
                property_name=sink_id.get("name", "sink_id"),
                data_type=get_python_type(sink_id.get("type", "int")),
            ),
            properties=props,
        )
        provider.add_edge(edge_schema, SQLTableDescriptor(table_or_view_name=edge["tableName"]))

    # Enable no-label support if configured
    no_label_config = schema.get("noLabelSupport")
    if no_label_config and no_label_config.get("enabled", False):
        table_name = no_label_config.get("tableName", "")
        node_id_columns = no_label_config.get("nodeIdColumns", ["id"])
        if table_name:
            # Collect properties from all nodes for wildcard
            all_properties: list[EntityProperty] = []
            seen_props: set[str] = set()
            for node_schema in provider.get_all_node_schemas():
                for prop in node_schema.properties:
                    if prop.property_name not in seen_props:
                        all_properties.append(prop)
                        seen_props.add(prop.property_name)
            provider.enable_no_label_support(
                table_name=table_name,
                node_id_columns=node_id_columns,
                properties=all_properties,
            )

    return provider


def adapt_sql_for_spark(sql: str) -> str:
    """Adapt Databricks SQL to work with local PySpark.

    This handles differences between Databricks SQL and PySpark SQL:
    - Converts catalog.schema.table to just table (for temp views)
    - Handles other Databricks-specific syntax

    Args:
        sql: Databricks SQL query.

    Returns:
        Adapted SQL for local PySpark execution.
    """
    # Replace catalog.schema.table patterns with just the table name
    # Pattern: catalog.schema.TableName -> TableName
    # But be careful not to replace inside strings

    # Split by strings to avoid replacing inside them
    parts = re.split(r"('(?:[^'\\]|\\.)*')", sql)
    result_parts = []

    for i, part in enumerate(parts):
        if i % 2 == 1:  # Inside a string
            result_parts.append(part)
        else:
            # Replace fully qualified table names with just the table name
            # Match patterns like: catalog.schema.TableName or `catalog`.`schema`.`TableName`
            part = re.sub(
                r"`?[\w]+`?\.`?[\w]+`?\.`?([\w]+)`?",
                r"\1",
                part,
            )
            result_parts.append(part)

    return "".join(result_parts)


def transpile_query(
    query: str, schema_provider: SimpleSQLSchemaProvider
) -> tuple[str | None, str | None]:
    """Transpile an OpenCypher query to SQL.

    Args:
        query: OpenCypher query string.
        schema_provider: Schema provider for the query.

    Returns:
        Tuple of (sql, error). If successful, error is None. If failed, sql is None.
    """
    try:
        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, schema_provider)
        optimize_plan(plan)
        plan.resolve(query)  # Resolve column references before rendering
        renderer = SQLRenderer(schema_provider)
        sql = renderer.render_plan(plan)
        return sql, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


class PySparkExecutor:
    """Executor for running transpiled SQL queries on PySpark."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the executor.

        Args:
            spark: Active SparkSession.
        """
        self.spark = spark
        self._registered_views: set[str] = set()

    def register_dataframe(self, name: str, df: DataFrame) -> None:
        """Register a DataFrame as a temporary view.

        Args:
            name: View name to register.
            df: DataFrame to register.
        """
        df.createOrReplaceTempView(name)
        self._registered_views.add(name)

    def unregister_all(self) -> None:
        """Unregister all registered views."""
        for view_name in self._registered_views:
            with contextlib.suppress(Exception):
                self.spark.catalog.dropTempView(view_name)
        self._registered_views.clear()

    def execute_sql(self, sql: str) -> tuple[DataFrame | None, str | None]:
        """Execute SQL and return the result DataFrame.

        Args:
            sql: SQL query to execute.

        Returns:
            Tuple of (dataframe, error). If successful, error is None.
        """
        try:
            df = self.spark.sql(sql)
            return df, None
        except Exception as e:
            return None, f"{type(e).__name__}: {e}"

    def execute_query(
        self,
        cypher_query: str,
        schema_provider: SimpleSQLSchemaProvider,
        *,
        collect_sample: int = 10,
    ) -> PySparkExecutionResult:
        """Execute a full OpenCypher query through transpilation and PySpark.

        Args:
            cypher_query: OpenCypher query string.
            schema_provider: Schema provider for the query.
            collect_sample: Number of sample rows to include in result.

        Returns:
            PySparkExecutionResult with execution details.
        """
        result = PySparkExecutionResult(success=False, query=cypher_query)

        # Step 1: Transpile
        sql, error = transpile_query(cypher_query, schema_provider)
        if error:
            result.error = error
            result.error_stage = "transpile"
            return result

        assert sql is not None
        result.sql = sql

        # Step 2: Adapt SQL for Spark
        try:
            adapted_sql = adapt_sql_for_spark(sql)
        except Exception as e:
            result.error = f"SQL adaptation error: {e}"
            result.error_stage = "parse_sql"
            return result

        # Step 3: Execute
        df, error = self.execute_sql(adapted_sql)
        if error:
            result.error = error
            result.error_stage = "execute"
            return result

        assert df is not None
        result.dataframe = df
        result.columns = df.columns
        result.success = True

        # Collect sample rows and count
        try:
            result.row_count = df.count()
            if collect_sample > 0:
                sample_rows = df.limit(collect_sample).collect()
                result.sample_rows = [row.asDict() for row in sample_rows]
        except Exception as e:
            # If collection fails, still consider the query successful
            # since it parsed and executed
            result.error = f"Sample collection warning: {e}"

        return result


def create_spark_session(app_name: str = "gsql2rsql_test") -> SparkSession:
    """Create a local SparkSession for testing.

    Args:
        app_name: Application name for the SparkSession.

    Returns:
        Configured SparkSession.
    """
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")  # Disable UI for tests
        .getOrCreate()
    )
