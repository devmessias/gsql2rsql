"""Tests for the CLI module."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import pytest
from click.testing import CliRunner

from gsql2rsql.cli import main


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def sample_schema() -> dict[str, Any]:
    """Create a sample graph schema for testing BFS."""
    return {
        "nodes": [
            {
                "name": "Person",
                "tableName": "graph.Person",
                "idProperty": {"name": "id", "type": "int"},
                "properties": [
                    {"name": "name", "type": "string"},
                ],
            },
        ],
        "edges": [
            {
                "name": "KNOWS",
                "sourceNode": "Person",
                "sinkNode": "Person",
                "tableName": "graph.Knows",
                "sourceIdProperty": {"name": "source_id", "type": "int"},
                "sinkIdProperty": {"name": "target_id", "type": "int"},
                "properties": [],
            },
        ],
    }


@pytest.fixture
def schema_file(sample_schema: dict[str, Any]) -> Path:
    """Create a temporary schema file."""
    with NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as f:
        json.dump(sample_schema, f)
        return Path(f.name)


class TestTranspileCommand:
    """Tests for the transpile command."""

    def test_transpile_simple_match(
        self,
        cli_runner: CliRunner,
        schema_file: Path,
    ) -> None:
        """Test transpiling a simple MATCH query."""
        query = "MATCH (p:Person) RETURN p.name"

        result = cli_runner.invoke(
            main,
            ["transpile", "--schema", str(schema_file)],
            input=query,
        )

        assert result.exit_code == 0
        assert "SELECT" in result.output
        assert "Person" in result.output

    def test_transpile_with_relationship(
        self,
        cli_runner: CliRunner,
        schema_file: Path,
    ) -> None:
        """Test transpiling a query with relationships."""
        query = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"

        result = cli_runner.invoke(
            main,
            ["transpile", "--schema", str(schema_file)],
            input=query,
        )

        assert result.exit_code == 0
        assert "SELECT" in result.output
        assert "JOIN" in result.output or "Person" in result.output

    def test_transpile_empty_query_fails(
        self,
        cli_runner: CliRunner,
        schema_file: Path,
    ) -> None:
        """Test that an empty query fails gracefully."""
        result = cli_runner.invoke(
            main,
            ["transpile", "--schema", str(schema_file)],
            input="",
        )

        assert result.exit_code != 0
        assert "Empty query" in result.output

    def test_transpile_string_literal_databricks_format(
        self,
        cli_runner: CliRunner,
        schema_file: Path,
    ) -> None:
        """Test that backticks are used for identifiers (Databricks format)."""
        query = "MATCH (p:Person) RETURN p.name"

        result = cli_runner.invoke(
            main,
            ["transpile", "--schema", str(schema_file)],
            input=query,
        )

        assert result.exit_code == 0
        # Databricks uses backticks for identifiers
        assert "`" in result.output
        # T-SQL uses brackets, we should NOT have them
        assert "[" not in result.output

    def test_transpile_boolean_databricks_format(
        self,
        cli_runner: CliRunner,
    ) -> None:
        """Test that backticks are used, not T-SQL brackets."""
        # Create schema with boolean property
        schema = {
            "nodes": [
                {
                    "name": "Item",
                    "tableName": "catalog.Items",
                    "idProperty": {"name": "id", "type": "int"},
                    "properties": [
                        {"name": "active", "type": "bool"},
                    ],
                },
            ],
            "edges": [],
        }

        with NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(schema, f)
            schema_path = Path(f.name)

        query = "MATCH (i:Item) RETURN i"

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["transpile", "--schema", str(schema_path)],
            input=query,
        )

        assert result.exit_code == 0
        # Databricks uses backticks
        assert "`catalog.Items`" in result.output


class TestParseCommand:
    """Tests for the parse command."""

    def test_parse_simple_query(self, cli_runner: CliRunner) -> None:
        """Test parsing a simple query outputs AST."""
        query = "MATCH (n) RETURN n"

        result = cli_runner.invoke(
            main,
            ["parse"],
            input=query,
        )

        assert result.exit_code == 0
        assert "SingleQuery" in result.output or "MATCH" in result.output

    def test_parse_empty_query_fails(self, cli_runner: CliRunner) -> None:
        """Test that parsing an empty query fails."""
        result = cli_runner.invoke(
            main,
            ["parse"],
            input="",
        )

        assert result.exit_code != 0
        assert "Empty query" in result.output


class TestInitSchemaCommand:
    """Tests for the init-schema command."""

    def test_init_schema_outputs_template(
        self, cli_runner: CliRunner
    ) -> None:
        """Test that init-schema outputs a valid JSON template."""
        result = cli_runner.invoke(main, ["init-schema"])

        assert result.exit_code == 0

        # Should be valid JSON
        schema = json.loads(result.output)

        # Should have nodes and edges
        assert "nodes" in schema
        assert "edges" in schema

        # Should use Databricks format (no dbo prefix)
        for node in schema["nodes"]:
            assert "dbo." not in node.get("tableName", "")


class TestVersionCommand:
    """Tests for version output."""

    def test_version_option(self, cli_runner: CliRunner) -> None:
        """Test --version flag."""
        result = cli_runner.invoke(main, ["--version"])

        assert result.exit_code == 0
        assert "gsql2rsql" in result.output


class TestDatabricksSQLOutput:
    """Tests specific to Databricks SQL output format."""

    def test_no_tsql_brackets(
        self,
        cli_runner: CliRunner,
        schema_file: Path,
    ) -> None:
        """Test that output uses backticks, not T-SQL brackets."""
        query = "MATCH (p:Person) RETURN p.name"

        result = cli_runner.invoke(
            main,
            ["transpile", "--schema", str(schema_file)],
            input=query,
        )

        assert result.exit_code == 0
        # Databricks uses backticks, not [brackets]
        assert "[" not in result.output or result.output.count("[") == 0

    def test_limit_syntax(
        self,
        cli_runner: CliRunner,
        schema_file: Path,
    ) -> None:
        """Test that LIMIT uses Databricks syntax."""
        query = "MATCH (p:Person) RETURN p.name LIMIT 10"

        result = cli_runner.invoke(
            main,
            ["transpile", "--schema", str(schema_file)],
            input=query,
        )

        assert result.exit_code == 0
        # Databricks uses LIMIT, not TOP
        assert "LIMIT" in result.output
        assert "TOP" not in result.output
