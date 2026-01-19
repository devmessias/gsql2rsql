"""PySpark execution tests for curated query examples.

This module tests all curated examples from the YAML files by:
1. Loading the schema from each YAML file
2. Generating sample data for the schema (cached per YAML file)
3. Transpiling each OpenCypher query to SQL
4. Executing the SQL on PySpark
5. Capturing and reporting any errors

This helps identify transpiler implementation bugs by running actual SQL.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import yaml

# Skip all tests in this module if PySpark is not available
pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402, I001

from gsql2rsql.pyspark_executor import (  # noqa: E402, I001
    PySparkExecutionResult,
    PySparkExecutor,
    create_spark_session,
    load_schema_from_yaml,
)
from tests.utils.sample_data_generator import (  # noqa: E402, I001
    generate_sample_data_for_yaml,
)

if TYPE_CHECKING:
    from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider


# Cache for generated data and providers per YAML file
# This avoids regenerating data for each test that uses the same YAML file
_yaml_data_cache: dict[str, tuple[dict[str, Any], "SimpleSQLSchemaProvider"]] = {}

# Directory containing example YAML files
EXAMPLES_DIR = Path(__file__).parent.parent / "examples"
OUTPUT_DIR = Path(__file__).parent / "output" / "pyspark"


@dataclass
class ExampleTestResult:
    """Result of testing a single example."""

    yaml_file: str
    example_index: int
    description: str
    application: str
    query: str
    transpile_success: bool
    transpiled_sql: str | None
    execution_success: bool
    error: str | None
    error_stage: str | None
    row_count: int | None
    columns: list[str] = field(default_factory=list)


def sanitize_filename(name: str, max_length: int = 60) -> str:
    """Convert description to a valid filename."""
    name = re.sub(r"[^\w\s-]", "", name.lower())
    name = re.sub(r"[\s]+", "_", name)
    return name[:max_length]


def load_yaml_examples(yaml_path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Load schema and examples from a YAML file.

    Args:
        yaml_path: Path to the YAML file.

    Returns:
        Tuple of (full yaml data, list of examples).
    """
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    return data, data.get("examples", [])


def get_all_yaml_files() -> list[Path]:
    """Get all example YAML files."""
    return sorted(EXAMPLES_DIR.glob("*_queries.yaml"))


def collect_all_examples() -> list[tuple[str, int, str, str]]:
    """Collect all examples for parametrization.

    Returns:
        List of tuples: (yaml_filename, example_index, description, application)
    """
    examples = []
    for yaml_path in get_all_yaml_files():
        _, yaml_examples = load_yaml_examples(yaml_path)
        for idx, example in enumerate(yaml_examples):
            examples.append(
                (
                    yaml_path.name,
                    idx,
                    example.get("description", f"example_{idx}"),
                    example.get("application", ""),
                )
            )
    return examples


# Collect examples for test parametrization
ALL_EXAMPLES = collect_all_examples()


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Create a SparkSession for testing."""
    session = create_spark_session("gsql2rsql_examples_test")
    yield session
    # Clear cache on session end
    _yaml_data_cache.clear()
    session.stop()


@pytest.fixture(scope="module")
def output_dir() -> Path:
    """Ensure output directory exists."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR


def get_or_create_yaml_data(
    spark: SparkSession, yaml_file: str
) -> tuple[dict[str, Any], "SimpleSQLSchemaProvider"]:
    """Get or create cached data for a YAML file.

    This function caches the generated data and schema provider for each YAML file,
    avoiding expensive data regeneration for each test that uses the same file.

    Args:
        spark: SparkSession for creating DataFrames.
        yaml_file: Name of the YAML file.

    Returns:
        Tuple of (yaml_data, schema_provider).
    """
    if yaml_file not in _yaml_data_cache:
        yaml_path = EXAMPLES_DIR / yaml_file
        yaml_data, _ = load_yaml_examples(yaml_path)

        # Generate sample data (registers views as side effect)
        _ = generate_sample_data_for_yaml(
            spark,
            yaml_data,
            default_node_count=5,  # Reduced for faster tests
            edge_density=0.2,  # Reduced density
            seed=42,  # Use fixed seed for reproducibility
        )

        # Create schema provider
        provider = load_schema_from_yaml(yaml_data)

        _yaml_data_cache[yaml_file] = (yaml_data, provider)

    return _yaml_data_cache[yaml_file]


class TestExamplesSetup:
    """Tests to verify examples are loaded correctly."""

    def test_yaml_files_exist(self) -> None:
        yaml_files = get_all_yaml_files()
        assert len(yaml_files) > 0, "No YAML example files found"

    def test_examples_collected(self) -> None:
        assert len(ALL_EXAMPLES) > 0, "No examples collected"
        # Print summary
        yaml_counts: dict[str, int] = {}
        for yaml_file, _, _, _ in ALL_EXAMPLES:
            yaml_counts[yaml_file] = yaml_counts.get(yaml_file, 0) + 1
        print(f"\nCollected {len(ALL_EXAMPLES)} examples from {len(yaml_counts)} files:")
        for yaml_file, count in yaml_counts.items():
            print(f"  {yaml_file}: {count} examples")


class TestExampleExecution:
    """Test each curated example by executing it on PySpark."""

    @pytest.mark.parametrize(
        "yaml_file,example_idx,description,application",
        ALL_EXAMPLES,
        ids=[f"{e[0]}:{e[1]:02d}-{sanitize_filename(e[2])[:30]}" for e in ALL_EXAMPLES],
    )
    @pytest.mark.timeout(60)  # 60 seconds timeout per test
    def test_example(
        self,
        spark: SparkSession,
        output_dir: Path,
        yaml_file: str,
        example_idx: int,
        description: str,
        application: str,
    ) -> None:
        """Test a single example by transpiling and executing it.

        This test:
        1. Gets or creates cached schema and data for the YAML file
        2. Transpiles the query
        3. Executes the transpiled SQL
        4. Reports success/failure with details
        """
        yaml_path = EXAMPLES_DIR / yaml_file
        _, examples = load_yaml_examples(yaml_path)

        example = examples[example_idx]
        query = example.get("query", "").strip()

        # Track result
        result = ExampleTestResult(
            yaml_file=yaml_file,
            example_index=example_idx,
            description=description,
            application=application,
            query=query,
            transpile_success=False,
            transpiled_sql=None,
            execution_success=False,
            error=None,
            error_stage=None,
            row_count=None,
        )

        # Get or create cached data and provider (avoids regenerating for each test)
        _, provider = get_or_create_yaml_data(spark, yaml_file)

        # Create executor
        executor = PySparkExecutor(spark)

        # Execute query
        exec_result = executor.execute_query(query, provider)

        # Update result
        result.transpile_success = exec_result.sql is not None
        result.transpiled_sql = exec_result.sql
        result.execution_success = exec_result.success
        result.error = exec_result.error
        result.error_stage = exec_result.error_stage
        result.row_count = exec_result.row_count
        result.columns = exec_result.columns

        # Save result to file
        self._save_result(output_dir, yaml_file, example_idx, description, result, exec_result)

        # Assert success
        if not result.transpile_success:
            pytest.fail(
                f"Transpilation failed for '{description}'\nQuery:\n{query}\nError: {result.error}"
            )

        if not result.execution_success:
            pytest.fail(
                f"Execution failed for '{description}'\n"
                f"Query:\n{query}\n"
                f"SQL:\n{result.transpiled_sql}\n"
                f"Error ({result.error_stage}): {result.error}"
            )

    def _save_result(
        self,
        output_dir: Path,
        yaml_file: str,
        example_idx: int,
        description: str,
        result: ExampleTestResult,
        _exec_result: PySparkExecutionResult,  # noqa: ARG002
    ) -> None:
        """Save test result to output directory."""
        yaml_name = yaml_file.replace("_queries.yaml", "")
        example_dir = output_dir / yaml_name / f"{example_idx:02d}_{sanitize_filename(description)}"
        example_dir.mkdir(parents=True, exist_ok=True)

        # Save query
        (example_dir / "query.cypher").write_text(result.query)

        # Save transpiled SQL
        if result.transpiled_sql:
            (example_dir / "transpiled.sql").write_text(result.transpiled_sql)

        # Save result summary
        summary = {
            "yaml_file": result.yaml_file,
            "example_index": result.example_index,
            "description": result.description,
            "application": result.application,
            "transpile_success": result.transpile_success,
            "execution_success": result.execution_success,
            "error": result.error,
            "error_stage": result.error_stage,
            "row_count": result.row_count,
            "columns": result.columns,
        }
        (example_dir / "result.json").write_text(json.dumps(summary, indent=2))

        # Save error if any
        if result.error:
            (example_dir / "error.txt").write_text(f"Stage: {result.error_stage}\n\n{result.error}")


class TestExamplesSummary:
    """Generate summary report after all examples are tested."""

    def test_generate_summary(
        self, output_dir: Path, spark: SparkSession  # noqa: ARG002
    ) -> None:
        """Generate a summary of all test results.

        This test runs after the parametrized tests and collects all results.
        It's useful for getting an overview of transpiler health.
        """
        results: dict[str, dict[str, Any]] = {}

        # Collect all result files
        for yaml_file in get_all_yaml_files():
            yaml_name = yaml_file.name.replace("_queries.yaml", "")
            yaml_dir = output_dir / yaml_name

            if not yaml_dir.exists():
                continue

            yaml_results = {
                "total": 0,
                "transpile_success": 0,
                "execution_success": 0,
                "failures": [],
            }

            for example_dir in sorted(yaml_dir.iterdir()):
                if not example_dir.is_dir():
                    continue

                result_file = example_dir / "result.json"
                if not result_file.exists():
                    continue

                yaml_results["total"] += 1
                result = json.loads(result_file.read_text())

                if result.get("transpile_success"):
                    yaml_results["transpile_success"] += 1

                if result.get("execution_success"):
                    yaml_results["execution_success"] += 1
                else:
                    yaml_results["failures"].append(
                        {
                            "description": result.get("description"),
                            "error_stage": result.get("error_stage"),
                            "error": result.get("error", "")[:200],  # Truncate
                        }
                    )

            results[yaml_name] = yaml_results

        # Generate summary
        summary = {
            "yaml_files": results,
            "totals": {
                "total_examples": sum(r["total"] for r in results.values()),
                "transpile_success": sum(r["transpile_success"] for r in results.values()),
                "execution_success": sum(r["execution_success"] for r in results.values()),
            },
        }

        # Save summary
        summary_path = output_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2))

        # Print summary
        print("\n" + "=" * 80)
        print("PYSPARK EXECUTION SUMMARY")
        print("=" * 80)

        totals = summary["totals"]
        print(f"\nTotal examples: {totals['total_examples']}")
        print(f"Transpile success: {totals['transpile_success']}")
        print(f"Execution success: {totals['execution_success']}")

        failed_count = totals["total_examples"] - totals["execution_success"]
        if failed_count > 0:
            print(f"\nFailed: {failed_count}")
            for yaml_name, yaml_results in results.items():
                if yaml_results["failures"]:
                    print(f"\n  {yaml_name}:")
                    for failure in yaml_results["failures"][:5]:  # Show first 5
                        print(f"    - {failure['description']}")
                        print(f"      Stage: {failure['error_stage']}")

        print(f"\nDetailed results saved to: {output_dir}")


# Additional test for specific error patterns
class TestKnownErrorPatterns:
    """Test for known error patterns to help debug transpiler issues."""

    @pytest.fixture(autouse=True)
    def setup_data(self, spark: SparkSession) -> None:
        """Setup test data once for all tests in this class."""
        # Use features schema for testing (uses cache)
        yaml_file = "features_queries.yaml"
        yaml_path = EXAMPLES_DIR / yaml_file
        if yaml_path.exists():
            _, self.provider = get_or_create_yaml_data(spark, yaml_file)
            self.executor = PySparkExecutor(spark)
        else:
            self.executor = None
            self.provider = None

    def test_simple_match_pattern(self, spark: SparkSession) -> None:  # noqa: ARG002
        """Test basic MATCH pattern works."""
        if self.executor is None:
            pytest.skip("features_queries.yaml not found")

        result = self.executor.execute_query(
            "MATCH (p:Person) RETURN p.name, p.id",
            self.provider,
        )
        assert result.success, f"Simple MATCH failed: {result.error}"

    def test_relationship_pattern(self, spark: SparkSession) -> None:  # noqa: ARG002
        """Test relationship MATCH pattern works."""
        if self.executor is None:
            pytest.skip("features_queries.yaml not found")

        result = self.executor.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name",
            self.provider,
        )
        assert result.success, f"Relationship MATCH failed: {result.error}"

    def test_where_clause(self, spark: SparkSession) -> None:  # noqa: ARG002
        """Test WHERE clause works."""
        if self.executor is None:
            pytest.skip("features_queries.yaml not found")

        result = self.executor.execute_query(
            "MATCH (p:Person) WHERE p.id > 2 RETURN p.name",
            self.provider,
        )
        assert result.success, f"WHERE clause failed: {result.error}"

    def test_aggregation(self, spark: SparkSession) -> None:  # noqa: ARG002
        """Test aggregation functions work."""
        if self.executor is None:
            pytest.skip("features_queries.yaml not found")

        result = self.executor.execute_query(
            "MATCH (p:Person) RETURN COUNT(p) AS total",
            self.provider,
        )
        assert result.success, f"Aggregation failed: {result.error}"

    def test_order_by(self, spark: SparkSession) -> None:  # noqa: ARG002
        """Test ORDER BY works."""
        if self.executor is None:
            pytest.skip("features_queries.yaml not found")

        result = self.executor.execute_query(
            "MATCH (p:Person) RETURN p.name, p.id ORDER BY p.id DESC",
            self.provider,
        )
        assert result.success, f"ORDER BY failed: {result.error}"

    def test_limit(self, spark: SparkSession) -> None:  # noqa: ARG002
        """Test LIMIT works."""
        if self.executor is None:
            pytest.skip("features_queries.yaml not found")

        result = self.executor.execute_query(
            "MATCH (p:Person) RETURN p.name LIMIT 5",
            self.provider,
        )
        assert result.success, f"LIMIT failed: {result.error}"
        assert result.row_count is not None and result.row_count <= 5


def run_quick_validation(spark: SparkSession) -> dict[str, Any]:
    """Run a quick validation of all examples without pytest.

    This function can be called directly for debugging or quick checks.

    Returns:
        Summary dictionary with results.
    """
    results = {"success": 0, "transpile_fail": 0, "execute_fail": 0, "errors": []}

    for yaml_path in get_all_yaml_files():
        _, examples = load_yaml_examples(yaml_path)

        # Get or create cached data (generates once per file)
        _, provider = get_or_create_yaml_data(spark, yaml_path.name)

        executor = PySparkExecutor(spark)

        for idx, example in enumerate(examples):
            query = example.get("query", "").strip()
            desc = example.get("description", f"example_{idx}")

            result = executor.execute_query(query, provider)

            if result.success:
                results["success"] += 1
            elif result.error_stage == "transpile":
                results["transpile_fail"] += 1
                results["errors"].append(
                    {
                        "file": yaml_path.name,
                        "index": idx,
                        "description": desc,
                        "stage": "transpile",
                        "error": result.error[:200] if result.error else None,
                    }
                )
            else:
                results["execute_fail"] += 1
                results["errors"].append(
                    {
                        "file": yaml_path.name,
                        "index": idx,
                        "description": desc,
                        "stage": result.error_stage,
                        "error": result.error[:200] if result.error else None,
                    }
                )

    return results


if __name__ == "__main__":
    # Allow running directly for quick debugging
    print("Running quick validation...")
    spark = create_spark_session("quick_validation")
    try:
        results = run_quick_validation(spark)
        print("\nResults:")
        print(f"  Success: {results['success']}")
        print(f"  Transpile failures: {results['transpile_fail']}")
        print(f"  Execution failures: {results['execute_fail']}")

        if results["errors"]:
            print("\nFirst 10 errors:")
            for error in results["errors"][:10]:
                print(f"  - [{error['file']}:{error['index']}] {error['description']}")
                print(f"    Stage: {error['stage']}")
                print(f"    Error: {error['error']}")
    finally:
        spark.stop()
