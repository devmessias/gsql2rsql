"""SQL test utilities for golden file comparison and normalization."""

from __future__ import annotations

import difflib
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import SimpleGraphSchemaProvider
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider


# Base paths for test artifacts
TESTS_DIR = Path(__file__).parent.parent
OUTPUT_DIR = TESTS_DIR / "output"
EXPECTED_DIR = OUTPUT_DIR / "expected"
ACTUAL_DIR = OUTPUT_DIR / "actual"
DIFF_DIR = OUTPUT_DIR / "diff"


def ensure_output_dirs() -> None:
    """Ensure output directories exist."""
    EXPECTED_DIR.mkdir(parents=True, exist_ok=True)
    ACTUAL_DIR.mkdir(parents=True, exist_ok=True)
    DIFF_DIR.mkdir(parents=True, exist_ok=True)


def normalize_sql(sql: str) -> str:
    """
    Normalize SQL for comparison.

    This applies consistent formatting to make comparison more reliable:
    - Strips leading/trailing whitespace
    - Normalizes whitespace (multiple spaces -> single space)
    - Removes backticks for identifier comparison
    - Lowercases SQL keywords
    - Normalizes line endings
    - Removes empty lines
    - Trims trailing whitespace on each line

    Args:
        sql: Raw SQL string to normalize

    Returns:
        Normalized SQL string
    """
    if not sql:
        return ""

    # Normalize line endings
    sql = sql.replace("\r\n", "\n").replace("\r", "\n")

    # Strip overall whitespace
    sql = sql.strip()

    # Remove backticks for comparison (Databricks quoting)
    sql = sql.replace("`", "")

    # Normalize multiple whitespace to single space (preserving newlines)
    lines = sql.split("\n")
    normalized_lines = []
    for line in lines:
        # Collapse multiple spaces to single space
        line = re.sub(r"  +", " ", line)
        # Trim trailing whitespace
        line = line.rstrip()
        # Keep non-empty lines
        if line:
            normalized_lines.append(line)

    sql = "\n".join(normalized_lines)

    # Lowercase common SQL keywords for consistent comparison
    keywords = [
        "SELECT",
        "FROM",
        "WHERE",
        "AND",
        "OR",
        "NOT",
        "IN",
        "IS",
        "NULL",
        "AS",
        "ON",
        "JOIN",
        "INNER",
        "LEFT",
        "RIGHT",
        "OUTER",
        "CROSS",
        "ORDER",
        "BY",
        "ASC",
        "DESC",
        "LIMIT",
        "OFFSET",
        "GROUP",
        "HAVING",
        "UNION",
        "ALL",
        "DISTINCT",
        "WITH",
        "RECURSIVE",
        "TRUE",
        "FALSE",
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "ARRAY",
        "ARRAY_AGG",
        "ARRAY_CONTAINS",
        "ARRAY_APPEND",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "COALESCE",
        "EXISTS",
        "ANY",
        "SOME",
    ]

    for kw in keywords:
        # Case-insensitive replacement, but only whole words
        pattern = re.compile(r"\b" + kw + r"\b", re.IGNORECASE)
        sql = pattern.sub(kw.lower(), sql)

    return sql


def write_golden_if_missing(path: str | Path, content: str) -> bool:
    """
    Write golden file if it doesn't exist.

    Args:
        path: Path to golden file
        content: SQL content to write

    Returns:
        True if file was created, False if it already existed
    """
    path = Path(path)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(normalize_sql(content))
        return True
    return False


def load_expected_sql(test_id: str, test_name: str) -> str | None:
    """
    Load expected SQL from golden file.

    Args:
        test_id: Test ID (e.g., "01")
        test_name: Test name (e.g., "simple_node_lookup")

    Returns:
        Expected SQL content, or None if file doesn't exist
    """
    path = EXPECTED_DIR / f"{test_id}_{test_name}.sql"
    if path.exists():
        return path.read_text()
    return None


def assert_sql_equal(
    expected_sql: str,
    actual_sql: str,
    test_id: str,
    test_name: str,
    *,
    write_artifacts: bool = True,
) -> None:
    """
    Assert that two SQL strings are semantically equal after normalization.

    On failure, writes actual output and diff to test artifacts directory.

    Args:
        expected_sql: Expected SQL string
        actual_sql: Actual SQL string from transpiler
        test_id: Test ID for file naming (e.g., "01")
        test_name: Test name for file naming (e.g., "simple_node_lookup")
        write_artifacts: Whether to write diff artifacts on failure

    Raises:
        AssertionError: If SQL strings don't match after normalization
    """
    ensure_output_dirs()

    exp_norm = normalize_sql(expected_sql)
    act_norm = normalize_sql(actual_sql)

    if exp_norm == act_norm:
        return  # Success

    # Write artifacts on failure
    if write_artifacts:
        base_name = f"{test_id}_{test_name}"
        expected_path = EXPECTED_DIR / f"{base_name}.sql"
        actual_path = ACTUAL_DIR / f"{base_name}.sql"
        diff_path = DIFF_DIR / f"{base_name}.diff"

        # Write actual output
        actual_path.write_text(act_norm)

        # Write expected (in case it was modified)
        expected_path.write_text(exp_norm)

        # Generate unified diff
        diff_lines = list(
            difflib.unified_diff(
                exp_norm.splitlines(keepends=True),
                act_norm.splitlines(keepends=True),
                fromfile="expected",
                tofile="actual",
                lineterm="",
            )
        )
        diff_content = "".join(diff_lines)
        diff_path.write_text(diff_content)

        raise AssertionError(
            f"SQL mismatch for {base_name}.\n"
            f"Expected: {expected_path}\n"
            f"Actual:   {actual_path}\n"
            f"Diff:     {diff_path}\n\n"
            f"--- Diff Preview ---\n{diff_content[:2000]}"
        )

    raise AssertionError(
        f"SQL mismatch.\n"
        f"Expected:\n{exp_norm}\n\n"
        f"Actual:\n{act_norm}"
    )


@dataclass
class TranspilerTestCase:
    """
    A structured test case for transpiler validation.

    This class encapsulates all the data needed for a single transpiler test,
    including schema setup, query, and expected output.
    """

    test_id: str
    test_name: str
    cypher_query: str
    graph_schema: SimpleGraphSchemaProvider
    sql_schema: SimpleSQLSchemaProvider
    expected_sql: str | None = None
    description: str = ""
    tags: list[str] = field(default_factory=list)

    def transpile(self) -> str:
        """
        Run the transpiler and return the generated SQL.

        Returns:
            Generated Databricks SQL string
        """
        parser = OpenCypherParser()
        ast = parser.parse(self.cypher_query)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        # Resolve before rendering (renderer now requires resolution)
        plan.resolve(original_query=self.cypher_query)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def get_golden_path(self) -> Path:
        """Get path to expected golden file."""
        return EXPECTED_DIR / f"{self.test_id}_{self.test_name}.sql"

    def get_actual_path(self) -> Path:
        """Get path to actual output file."""
        return ACTUAL_DIR / f"{self.test_id}_{self.test_name}.sql"

    def get_diff_path(self) -> Path:
        """Get path to diff file."""
        return DIFF_DIR / f"{self.test_id}_{self.test_name}.diff"

    def load_expected_sql(self) -> str | None:
        """Load expected SQL from golden file."""
        path = self.get_golden_path()
        if path.exists():
            return path.read_text()
        return self.expected_sql

    def save_expected_sql(self, sql: str) -> None:
        """Save expected SQL to golden file."""
        ensure_output_dirs()
        self.get_golden_path().write_text(normalize_sql(sql))

    def assert_transpiles_correctly(self) -> str:
        """
        Run transpiler and assert output matches expected.

        Returns:
            The generated SQL (for further assertions)

        Raises:
            AssertionError: If SQL doesn't match expected
        """
        actual_sql = self.transpile()
        expected = self.load_expected_sql()

        if expected is None:
            # No golden file - save actual as new expected and warn
            self.save_expected_sql(actual_sql)
            raise AssertionError(
                f"No expected SQL found for {self.test_id}_{self.test_name}. "
                f"Created golden file at {self.get_golden_path()}. "
                f"Review and re-run the test."
            )

        assert_sql_equal(
            expected,
            actual_sql,
            self.test_id,
            self.test_name,
        )
        return actual_sql


def generate_test_artifacts(
    test_id: str,
    test_name: str,
    cypher: str,
    graph_schema: SimpleGraphSchemaProvider,
    sql_schema: SimpleSQLSchemaProvider,
) -> dict[str, Any]:
    """
    Generate test artifacts for a query (useful for Makefile targets).

    Args:
        test_id: Test ID
        test_name: Test name
        cypher: OpenCypher query
        graph_schema: Graph schema
        sql_schema: SQL schema

    Returns:
        Dict with cypher, actual_sql, expected_sql, paths, and match status
    """
    ensure_output_dirs()

    test_case = TranspilerTestCase(
        test_id=test_id,
        test_name=test_name,
        cypher_query=cypher,
        graph_schema=graph_schema,
        sql_schema=sql_schema,
    )

    actual_sql = test_case.transpile()
    expected_sql = test_case.load_expected_sql()

    # Write actual
    test_case.get_actual_path().write_text(normalize_sql(actual_sql))

    result = {
        "test_id": test_id,
        "test_name": test_name,
        "cypher": cypher,
        "actual_sql": actual_sql,
        "actual_sql_normalized": normalize_sql(actual_sql),
        "expected_sql": expected_sql,
        "expected_sql_normalized": (
            normalize_sql(expected_sql) if expected_sql else None
        ),
        "actual_path": str(test_case.get_actual_path()),
        "expected_path": str(test_case.get_golden_path()),
        "diff_path": str(test_case.get_diff_path()),
        "match": False,
    }

    if expected_sql:
        exp_norm = normalize_sql(expected_sql)
        act_norm = normalize_sql(actual_sql)
        result["match"] = exp_norm == act_norm

        if not result["match"]:
            # Generate diff
            diff_lines = list(
                difflib.unified_diff(
                    exp_norm.splitlines(keepends=True),
                    act_norm.splitlines(keepends=True),
                    fromfile="expected",
                    tofile="actual",
                )
            )
            diff_content = "".join(diff_lines)
            test_case.get_diff_path().write_text(diff_content)
            result["diff"] = diff_content

    return result
