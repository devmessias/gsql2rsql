"""Command-line interface for the openCypher transpiler."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click
import yaml
from rich.console import Console

from gsql2rsql import __version__

# Rich console for beautiful output
console = Console()


@click.group()
@click.version_option(version=__version__, prog_name="gsql2rsql")
def main() -> None:
    """openCypher Transpiler - Convert openCypher queries to SQL."""
    pass


@main.command()
@click.option(
    "--input", "-i",
    "input_file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Input file containing the openCypher query. If not provided, reads from stdin.",
)
@click.option(
    "--output", "-o",
    "output_file",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Output file for the transpiled SQL. If not provided, writes to stdout.",
)
@click.option(
    "--schema", "-s",
    "schema_file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="JSON file containing the graph schema definition.",
)
@click.option(
    "--pretty/--no-pretty",
    default=True,
    help="Pretty-print the output SQL.",
)
@click.option(
    "--optimize/--no-optimize",
    default=True,
    help="Enable subquery flattening optimization (default: enabled).",
)
@click.option(
    "--resolve/--no-resolve",
    default=True,
    help="Enable column resolution validation (default: enabled). "
         "Validates all column references before rendering SQL.",
)
@click.option(
    "--explain-scopes",
    is_flag=True,
    default=False,
    help="Output scope information for each operator in the logical plan. "
         "Useful for debugging schema propagation issues.",
)
def transpile(
    input_file: Path | None,
    output_file: Path | None,
    schema_file: Path,
    pretty: bool,  # noqa: ARG001
    optimize: bool,
    resolve: bool,
    explain_scopes: bool,
) -> None:
    """Transpile an openCypher query to Databricks SQL."""
    # Read the query
    if input_file:  # noqa: SIM108
        query = input_file.read_text(encoding="utf-8")
    else:
        query = sys.stdin.read()

    if not query.strip():
        click.echo("Error: Empty query", err=True)
        sys.exit(1)

    # Load the schema
    try:
        schema_data = json.loads(schema_file.read_text(encoding="utf-8"))
        graph_def = _load_schema(schema_data)
    except json.JSONDecodeError as e:
        click.echo(f"Error parsing schema file: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error loading schema: {e}", err=True)
        sys.exit(1)

    # Transpile
    try:
        from gsql2rsql import LogicalPlan, OpenCypherParser, SQLRenderer
        from gsql2rsql.planner.subquery_optimizer import SubqueryFlatteningOptimizer
        from gsql2rsql.common.exceptions import ColumnResolutionError

        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, graph_def)

        # Apply subquery flattening optimization
        optimizer = SubqueryFlatteningOptimizer(enabled=optimize)
        optimizer.optimize(plan)

        # Perform column resolution (validates all references)
        if resolve:
            try:
                plan.resolve(original_query=query)
            except ColumnResolutionError as e:
                # Print the full context error message
                click.echo(str(e), err=True)
                sys.exit(1)

        # Output scope information if requested
        if explain_scopes:
            click.echo("=" * 60, err=True)
            click.echo("SCOPE INFORMATION (--explain-scopes)", err=True)
            click.echo("=" * 60, err=True)
            for op in plan.all_operators():
                click.echo(op.dump_scope(), err=True)
                click.echo("-" * 40, err=True)
            click.echo("=" * 60, err=True)
            click.echo("", err=True)

        renderer = SQLRenderer(graph_def)
        sql = renderer.render_plan(plan)
    except ColumnResolutionError as e:
        # Handle resolution errors with full context
        click.echo(str(e), err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error transpiling query: {e}", err=True)
        sys.exit(1)

    # Output
    if output_file:
        output_file.write_text(sql, encoding="utf-8")
        click.echo(f"SQL written to {output_file}")
    else:
        click.echo(sql)


@main.command()
@click.option(
    "--input", "-i",
    "input_file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Input file containing the openCypher query. If not provided, reads from stdin.",
)
@click.option(
    "--output", "-o",
    "output_file",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Output file for the AST. If not provided, writes to stdout.",
)
def parse(
    input_file: Path | None,
    output_file: Path | None,
) -> None:
    """Parse an openCypher query and output its AST."""
    # Read the query
    if input_file:  # noqa: SIM108
        query = input_file.read_text(encoding="utf-8")
    else:
        query = sys.stdin.read()

    if not query.strip():
        click.echo("Error: Empty query", err=True)
        sys.exit(1)

    # Parse
    try:
        from gsql2rsql import OpenCypherParser

        parser = OpenCypherParser()
        ast = parser.parse(query)
        result = ast.dump_tree()
    except Exception as e:
        click.echo(f"Error parsing query: {e}", err=True)
        sys.exit(1)

    # Output
    if output_file:
        output_file.write_text(result, encoding="utf-8")
        click.echo(f"AST written to {output_file}")
    else:
        click.echo(result)


@main.command()
@click.option(
    "--output", "-o",
    "output_file",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Output file for the schema template. If not provided, writes to stdout.",
)
def init_schema(output_file: Path | None) -> None:
    """Generate a template schema file."""
    template = {
        "nodes": [
            {
                "name": "Person",
                "tableName": "catalog.schema.Person",
                "idProperty": {"name": "id", "type": "int"},
                "properties": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                ],
            },
            {
                "name": "Movie",
                "tableName": "catalog.schema.Movie",
                "idProperty": {"name": "id", "type": "int"},
                "properties": [
                    {"name": "title", "type": "string"},
                    {"name": "released", "type": "int"},
                ],
            },
        ],
        "edges": [
            {
                "name": "ACTED_IN",
                "sourceNode": "Person",
                "sinkNode": "Movie",
                "tableName": "catalog.schema.ActedIn",
                "sourceIdProperty": {"name": "person_id", "type": "int"},
                "sinkIdProperty": {"name": "movie_id", "type": "int"},
                "properties": [
                    {"name": "role", "type": "string"},
                ],
            },
        ],
    }

    result = json.dumps(template, indent=2)

    if output_file:
        output_file.write_text(result, encoding="utf-8")
        click.echo(f"Schema template written to {output_file}")
    else:
        click.echo(result)


def _load_schema(schema_data: dict) -> Any:
    """Load a graph schema from JSON data."""
    from gsql2rsql.common.schema import EdgeSchema, EntityProperty, NodeSchema
    from gsql2rsql.renderer.schema_provider import (
        SimpleSQLSchemaProvider,
        SQLTableDescriptor,
    )

    provider = SimpleSQLSchemaProvider()

    type_mapping = {
        "int": int,
        "integer": int,
        "long": int,
        "float": float,
        "double": float,
        "string": str,
        "bool": bool,
        "boolean": bool,
    }

    # Load nodes
    for node_data in schema_data.get("nodes", []):
        properties = [
            EntityProperty(
                property_name=prop["name"],
                data_type=type_mapping.get(prop.get("type", "string"), str),
            )
            for prop in node_data.get("properties", [])
        ]

        id_prop_data = node_data.get("idProperty", {"name": "id", "type": "int"})
        id_property = EntityProperty(
            property_name=id_prop_data["name"],
            data_type=type_mapping.get(id_prop_data.get("type", "int"), int),
        )

        node_schema = NodeSchema(
            name=node_data["name"],
            properties=properties,
            node_id_property=id_property,
        )

        table_name = node_data.get("tableName", node_data["name"])
        table_desc = SQLTableDescriptor(table_or_view_name=table_name)

        provider.add_node(node_schema, table_desc)

    # Load edges
    for edge_data in schema_data.get("edges", []):
        properties = [
            EntityProperty(
                property_name=prop["name"],
                data_type=type_mapping.get(prop.get("type", "string"), str),
            )
            for prop in edge_data.get("properties", [])
        ]

        src_prop_data = edge_data.get("sourceIdProperty", {"name": "source_id", "type": "int"})
        sink_prop_data = edge_data.get("sinkIdProperty", {"name": "sink_id", "type": "int"})

        source_id_property = EntityProperty(
            property_name=src_prop_data["name"],
            data_type=type_mapping.get(src_prop_data.get("type", "int"), int),
        )
        sink_id_property = EntityProperty(
            property_name=sink_prop_data["name"],
            data_type=type_mapping.get(sink_prop_data.get("type", "int"), int),
        )

        edge_schema = EdgeSchema(
            name=edge_data["name"],
            properties=properties,
            source_node_id=edge_data["sourceNode"],
            sink_node_id=edge_data["sinkNode"],
            source_id_property=source_id_property,
            sink_id_property=sink_id_property,
        )

        table_name = edge_data.get("tableName", edge_data["name"])
        table_filter = edge_data.get("filter")  # Optional WHERE clause filter
        table_desc = SQLTableDescriptor(
            table_or_view_name=table_name,
            filter=table_filter,
        )

        provider.add_edge(edge_schema, table_desc)

    return provider


def _load_examples():
    """Load example queries and schemas from YAML files."""
    examples: dict[str, list[dict[str, Any]]] = {}
    schemas: dict[str, Any] = {}
    examples_dir = Path(__file__).parent.parent.parent / "examples"

    # Load fraud queries
    fraud_file = examples_dir / "fraud_queries.yaml"
    if fraud_file.exists():
        try:
            with fraud_file.open(encoding="utf-8") as f:
                data = yaml.safe_load(f)
                examples["fraud"] = data.get("examples", [])
                if "schema" in data:
                    schemas["fraud"] = data["schema"]
        except Exception:
            pass

    # Load credit queries
    credit_file = examples_dir / "credit_queries.yaml"
    if credit_file.exists():
        try:
            with credit_file.open(encoding="utf-8") as f:
                data = yaml.safe_load(f)
                examples["credit"] = data.get("examples", [])
                if "schema" in data:
                    schemas["credit"] = data["schema"]
        except Exception:
            pass

    # Load features queries (library feature showcase)
    features_file = examples_dir / "features_queries.yaml"
    if features_file.exists():
        try:
            with features_file.open(encoding="utf-8") as f:
                data = yaml.safe_load(f)
                examples["features"] = data.get("examples", [])
                if "schema" in data:
                    schemas["features"] = data["schema"]
        except Exception:
            pass

    return examples, schemas


def _load_schema_from_yaml(schema_data: dict) -> Any:
    """Load a graph schema from YAML schema data."""
    from gsql2rsql.common.schema import EdgeSchema, EntityProperty, NodeSchema
    from gsql2rsql.renderer.schema_provider import (
        SimpleSQLSchemaProvider,
        SQLTableDescriptor,
    )

    provider = SimpleSQLSchemaProvider()

    type_mapping = {
        "int": int,
        "integer": int,
        "long": int,
        "float": float,
        "double": float,
        "string": str,
        "bool": bool,
        "boolean": bool,
    }

    # Load nodes
    for node_data in schema_data.get("nodes", []):
        properties = [
            EntityProperty(
                property_name=prop["name"],
                data_type=type_mapping.get(prop.get("type", "string"), str),
            )
            for prop in node_data.get("properties", [])
        ]

        id_prop_data = node_data.get("idProperty", {"name": "id", "type": "int"})
        id_property = EntityProperty(
            property_name=id_prop_data["name"],
            data_type=type_mapping.get(id_prop_data.get("type", "int"), int),
        )

        node_schema = NodeSchema(
            name=node_data["name"],
            properties=properties,
            node_id_property=id_property,
        )

        table_name = node_data.get("tableName", node_data["name"])
        table_desc = SQLTableDescriptor(table_or_view_name=table_name)

        provider.add_node(node_schema, table_desc)

    # Load edges
    for edge_data in schema_data.get("edges", []):
        properties = [
            EntityProperty(
                property_name=prop["name"],
                data_type=type_mapping.get(prop.get("type", "string"), str),
            )
            for prop in edge_data.get("properties", [])
        ]

        src_prop_data = edge_data.get(
            "sourceIdProperty", {"name": "source_id", "type": "int"}
        )
        sink_prop_data = edge_data.get(
            "sinkIdProperty", {"name": "sink_id", "type": "int"}
        )

        source_id_property = EntityProperty(
            property_name=src_prop_data["name"],
            data_type=type_mapping.get(src_prop_data.get("type", "int"), int),
        )
        sink_id_property = EntityProperty(
            property_name=sink_prop_data["name"],
            data_type=type_mapping.get(sink_prop_data.get("type", "int"), int),
        )

        edge_schema = EdgeSchema(
            name=edge_data["name"],
            properties=properties,
            source_node_id=edge_data["sourceNode"],
            sink_node_id=edge_data["sinkNode"],
            source_id_property=source_id_property,
            sink_id_property=sink_id_property,
        )

        table_name = edge_data.get("tableName", edge_data["name"])
        table_filter = edge_data.get("filter")
        table_desc = SQLTableDescriptor(
            table_or_view_name=table_name,
            filter=table_filter,
        )

        provider.add_edge(edge_schema, table_desc)

    return provider


def _transpile_query(
    query: str,
    graph_def: Any,
    optimize: bool = True,
    resolve: bool = True,
) -> dict[str, Any]:
    """Transpile a query and return detailed results.

    Args:
        query: The openCypher query to transpile.
        graph_def: The graph schema definition.
        optimize: Whether to apply subquery flattening optimization.
        resolve: Whether to perform column resolution validation.

    Returns:
        A dictionary with transpilation results including success status,
        AST, logical plan, SQL output, and any error messages.
    """
    result: dict[str, Any] = {
        "success": False,
        "parse_success": False,
        "ast": None,
        "logical_plan": None,
        "sql": None,
        "parse_error": None,
        "transpile_error": None,
        "resolution_error": None,
    }

    if not query.strip():
        result["parse_error"] = "Empty query"
        return result

    try:
        from gsql2rsql import LogicalPlan, OpenCypherParser, SQLRenderer
        from gsql2rsql.planner.subquery_optimizer import SubqueryFlatteningOptimizer
        from gsql2rsql.common.exceptions import ColumnResolutionError

        parser = OpenCypherParser()
        ast = parser.parse(query)
        result["parse_success"] = True
        result["ast"] = ast.dump_tree()

        if graph_def:
            try:
                plan = LogicalPlan.process_query_tree(ast, graph_def)

                # Capture logical plan using dump_graph()
                result["logical_plan"] = plan.dump_graph()

                # Apply subquery flattening optimization
                optimizer = SubqueryFlatteningOptimizer(enabled=optimize)
                optimizer.optimize(plan)

                # Perform column resolution (validates all references)
                if resolve:
                    try:
                        plan.resolve(original_query=query)
                    except ColumnResolutionError as e:
                        result["resolution_error"] = str(e)
                        return result

                renderer = SQLRenderer(graph_def)
                sql = renderer.render_plan(plan)
                result["sql"] = sql
                result["success"] = True
            except ColumnResolutionError as e:
                result["resolution_error"] = str(e)
            except Exception as e:
                result["transpile_error"] = str(e)
        else:
            result["transpile_error"] = "No schema loaded"

    except Exception as e:
        result["parse_error"] = str(e)

    return result


@main.command()
@click.option(
    "--schema",
    "-s",
    "schema_file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="JSON file containing the graph schema definition (optional for TUI mode).",
)
def tui(schema_file: Path | None) -> None:
    """Launch interactive TUI mode for exploring and testing openCypher queries."""
    _run_tui(schema_file)


def _get_config_path() -> Path:
    """Get the config file path."""
    config_dir = Path.home() / ".config" / "gsql2rsql"
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir / "config.json"


def _load_config() -> dict[str, Any]:
    """Load config from file."""
    config_path = _get_config_path()
    if config_path.exists():
        try:
            return json.loads(config_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_config(config: dict[str, Any]) -> None:
    """Save config to file."""
    config_path = _get_config_path()
    config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")


def _run_tui(schema_file: Path | None) -> None:
    """Run the interactive Text User Interface using Textual."""
    import os

    from rich.syntax import Syntax
    from rich.text import Text
    from textual import work
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.containers import Center, Grid, Horizontal, Vertical, VerticalScroll
    from textual.screen import ModalScreen
    from textual.widgets import (
        Button,
        DataTable,
        DirectoryTree,
        Footer,
        Header,
        Input,
        Label,
        ListItem,
        ListView,
        ProgressBar,
        Select,
        Static,
        TextArea,
    )

    # Load schema if provided
    graph_def = None
    schema_status = "⚠️  No schema - AST only"
    if schema_file:
        try:
            schema_data = json.loads(schema_file.read_text(encoding="utf-8"))
            graph_def = _load_schema(schema_data)
            schema_status = f"✓ Schema: {schema_file.name}"
        except Exception as e:
            schema_status = f"✗ Error: {e}"

    # Load examples and schemas from YAML
    examples_raw, yaml_schemas = _load_examples()
    examples_data: list[dict[str, Any]] = []
    idx = 1
    for category, category_examples in examples_raw.items():
        for example in category_examples:
            examples_data.append(
                {
                    "idx": idx,
                    "category": category,
                    "description": example.get("description", "N/A"),
                    "application": example.get("application", "N/A"),
                    "query": example.get("query", ""),
                    "notes": example.get("notes", ""),
                }
            )
            idx += 1

    # Build schema providers from YAML schemas
    import contextlib

    category_schemas: dict[str, Any] = {}
    for cat, schema_data in yaml_schemas.items():
        with contextlib.suppress(Exception):
            category_schemas[cat] = _load_schema_from_yaml(schema_data)

    # Load saved config for last used schema
    saved_config = _load_config()
    last_schema_name = saved_config.get("last_schema", None)
    last_schema_path = saved_config.get("last_schema_path", None)

    # Try to load last used schema if no CLI schema provided
    if not graph_def and last_schema_name:
        if last_schema_name in category_schemas:
            graph_def = category_schemas[last_schema_name]
            schema_status = f"✓ Schema: {last_schema_name.title()} (remembered)"
        elif last_schema_name == "custom" and last_schema_path:
            try:
                custom_path = Path(last_schema_path)
                if custom_path.exists():
                    schema_data = json.loads(custom_path.read_text(encoding="utf-8"))
                    graph_def = _load_schema(schema_data)
                    schema_status = f"✓ Schema: {custom_path.name} (remembered)"
            except Exception:
                pass

    # =========================================================================
    # MODAL SCREENS
    # =========================================================================

    class SchemaSelectorModal(ModalScreen[str | None]):
        """Modal for selecting a schema."""

        BINDINGS = [
            Binding("escape", "cancel", "Cancel"),
        ]

        CSS = """
        SchemaSelectorModal {
            align: center middle;
        }

        #schema-modal-container {
            width: 60;
            height: auto;
            max-height: 80%;
            border: thick $primary;
            background: $surface;
            padding: 1 2;
        }

        #schema-modal-title {
            text-align: center;
            text-style: bold;
            margin-bottom: 1;
        }

        #schema-options {
            height: auto;
            max-height: 20;
        }

        .schema-btn {
            width: 100%;
            margin: 0 0 1 0;
        }

        #schema-current {
            text-align: center;
            color: $text-muted;
            margin-top: 1;
        }
        """

        def __init__(self, current_schema: str, available_schemas: list[str]) -> None:
            super().__init__()
            self.current_schema = current_schema
            self.available_schemas = available_schemas

        def compose(self) -> ComposeResult:
            with Vertical(id="schema-modal-container"):
                yield Static("🔧 Select Schema", id="schema-modal-title")
                with Vertical(id="schema-options"):
                    yield Button(
                        "🔍 Fraud Detection Schema",
                        id="schema-fraud",
                        classes="schema-btn",
                        variant="error" if self.current_schema == "fraud" else "default",
                    )
                    yield Button(
                        "💳 Credit Analysis Schema",
                        id="schema-credit",
                        classes="schema-btn",
                        variant="success" if self.current_schema == "credit" else "default",
                    )
                    yield Button(
                        "⚡ Features Demo Schema",
                        id="schema-features",
                        classes="schema-btn",
                        variant="primary" if self.current_schema == "features" else "default",
                    )
                    yield Button(
                        "📂 Load from JSON file...",
                        id="schema-file",
                        classes="schema-btn",
                    )
                    yield Button(
                        "🔨 Create New Schema (Wizard)...",
                        id="schema-wizard",
                        classes="schema-btn",
                    )
                    yield Button(
                        "⚠️  No Schema (AST only)",
                        id="schema-none",
                        classes="schema-btn",
                        variant="warning" if self.current_schema == "none" else "default",
                    )
                yield Static(
                    f"Current: {self.current_schema or 'None'}",
                    id="schema-current",
                )

        def on_button_pressed(self, event: Button.Pressed) -> None:
            button_id = event.button.id
            if button_id == "schema-fraud":
                self.dismiss("fraud")
            elif button_id == "schema-credit":
                self.dismiss("credit")
            elif button_id == "schema-features":
                self.dismiss("features")
            elif button_id == "schema-file":
                self.dismiss("__file__")
            elif button_id == "schema-wizard":
                self.dismiss("__wizard__")
            elif button_id == "schema-none":
                self.dismiss("none")

        def action_cancel(self) -> None:
            self.dismiss(None)

    class FileBrowserModal(ModalScreen[Path | None]):
        """Modal for browsing and selecting a JSON file."""

        BINDINGS = [
            Binding("escape", "cancel", "Cancel"),
        ]

        CSS = """
        FileBrowserModal {
            align: center middle;
        }

        #file-modal-container {
            width: 80;
            height: 40;
            border: thick $primary;
            background: $surface;
            padding: 1 2;
        }

        #file-modal-title {
            text-align: center;
            text-style: bold;
            margin-bottom: 1;
        }

        #file-tree {
            height: 1fr;
            border: solid $secondary;
        }

        #file-path-input {
            margin-top: 1;
        }

        #file-buttons {
            height: auto;
            margin-top: 1;
            align: center middle;
        }

        #file-buttons Button {
            margin: 0 1;
        }
        """

        def __init__(self, start_path: Path | None = None) -> None:
            super().__init__()
            self.start_path = start_path or Path.home()
            self.selected_path: Path | None = None

        def compose(self) -> ComposeResult:
            with Vertical(id="file-modal-container"):
                yield Static("📂 Select Schema JSON File", id="file-modal-title")
                yield DirectoryTree(str(self.start_path), id="file-tree")
                yield Input(
                    placeholder="Or type path directly...",
                    id="file-path-input",
                )
                with Horizontal(id="file-buttons"):
                    yield Button("Cancel", id="btn-cancel", variant="default")
                    yield Button("Select", id="btn-select", variant="primary")

        def on_directory_tree_file_selected(
            self, event: DirectoryTree.FileSelected
        ) -> None:
            path = Path(event.path)
            if path.suffix.lower() in (".json", ".yaml", ".yml"):
                self.selected_path = path
                self.query_one("#file-path-input", Input).value = str(path)

        def on_button_pressed(self, event: Button.Pressed) -> None:
            if event.button.id == "btn-cancel":
                self.dismiss(None)
            elif event.button.id == "btn-select":
                # Check input field first
                input_path = self.query_one("#file-path-input", Input).value.strip()
                if input_path:
                    path = Path(input_path)
                    if path.exists() and path.is_file():
                        self.dismiss(path)
                    else:
                        self.notify("File not found", severity="error")
                elif self.selected_path:
                    self.dismiss(self.selected_path)
                else:
                    self.notify("No file selected", severity="warning")

        def action_cancel(self) -> None:
            self.dismiss(None)

    class SchemaWizardModal(ModalScreen[dict | None]):
        """Modal wizard for creating a new schema step by step."""

        BINDINGS = [
            Binding("escape", "cancel", "Cancel"),
        ]

        CSS = """
        SchemaWizardModal {
            align: center middle;
        }

        #wizard-container {
            width: 90;
            height: 45;
            border: thick $primary;
            background: $surface;
            padding: 1 2;
        }

        #wizard-title {
            text-align: center;
            text-style: bold;
            margin-bottom: 1;
        }

        #wizard-step {
            text-align: center;
            color: $text-muted;
            margin-bottom: 1;
        }

        #wizard-content {
            height: 1fr;
            border: solid $secondary;
            padding: 1;
        }

        .wizard-form-row {
            height: auto;
            margin-bottom: 1;
        }

        .wizard-label {
            width: 20;
        }

        .wizard-input {
            width: 1fr;
        }

        #wizard-buttons {
            height: auto;
            margin-top: 1;
            align: center middle;
        }

        #wizard-buttons Button {
            margin: 0 1;
        }

        .hidden {
            display: none;
        }

        #step1-content, #step2-content, #step3-content, #step4-content {
            height: auto;
        }
        """

        def __init__(self) -> None:
            super().__init__()
            self.step = 1
            self.schema: dict[str, Any] = {"nodes": [], "edges": []}

        def compose(self) -> ComposeResult:
            with Vertical(id="wizard-container"):
                yield Static("Schema Creation Wizard", id="wizard-title")
                yield Static("Step 1 of 4", id="wizard-step")
                with VerticalScroll(id="wizard-content"):
                    # Step 1: Introduction
                    with Vertical(id="step1-content"):
                        yield Static(
                            "[bold cyan]Welcome![/bold cyan]\n\n"
                            "Create a graph schema for transpilation.\n\n"
                            "[bold]Schema defines:[/bold]\n"
                            "- Nodes: Entities (Person, Movie)\n"
                            "- Edges: Relationships (KNOWS, ACTED_IN)\n\n"
                            "Press Next to start."
                        )

                    # Step 2: Add Nodes
                    with Vertical(id="step2-content", classes="hidden"):
                        yield Static(
                            "[bold green]Define Nodes[/bold green]",
                            id="step2-title"
                        )
                        yield Static("Nodes added: 0", id="nodes-count")
                        yield Static("[bold]Add Node:[/bold]")
                        yield Input(placeholder="Label (e.g. Person)", id="nl")
                        yield Input(placeholder="Table (e.g. cat.sch.Person)", id="nt")
                        yield Input(placeholder="ID property (e.g. id)", id="nid")
                        yield Input(placeholder="Props: name:str,age:int", id="np")
                        yield Button("Add Node", id="btn-add-node")

                    # Step 3: Add Edges
                    with Vertical(id="step3-content", classes="hidden"):
                        yield Static(
                            "[bold blue]Define Edges[/bold blue]",
                            id="step3-title"
                        )
                        yield Static("Edges added: 0", id="edges-count")
                        yield Static("[bold]Add Edge:[/bold]")
                        yield Input(placeholder="Type (e.g. KNOWS)", id="et")
                        yield Input(placeholder="Source node label", id="es")
                        yield Input(placeholder="Target node label", id="ed")
                        yield Input(placeholder="Table name", id="etbl")
                        yield Input(placeholder="Source FK (e.g. person_id)", id="esfk")
                        yield Input(placeholder="Target FK (e.g. friend_id)", id="etfk")
                        yield Button("Add Edge", id="btn-add-edge")

                    # Step 4: Save
                    with Vertical(id="step4-content", classes="hidden"):
                        yield Static(
                            "[bold yellow]Save Schema[/bold yellow]",
                            id="step4-title"
                        )
                        yield Static("", id="schema-preview")
                        yield Input(placeholder="Filename (e.g. my.json)", id="fn")
                        yield Button("Save", id="btn-save-schema")

                with Horizontal(id="wizard-buttons"):
                    yield Button("Cancel", id="btn-cancel")
                    yield Button("Back", id="btn-back", disabled=True)
                    yield Button("Next", id="btn-next", variant="primary")

        def _update_view(self) -> None:
            """Update visibility based on current step."""
            for i in range(1, 5):
                container = self.query_one(f"#step{i}-content", Vertical)
                if i == self.step:
                    container.remove_class("hidden")
                else:
                    container.add_class("hidden")

            self.query_one("#wizard-step", Static).update(f"Step {self.step} of 4")
            self.query_one("#btn-back", Button).disabled = (self.step == 1)

            btn_next = self.query_one("#btn-next", Button)
            btn_next.label = "Finish" if self.step == 4 else "Next"

            # Update counts
            if self.step == 2:
                n = len(self.schema["nodes"])
                self.query_one("#nodes-count", Static).update(f"Nodes: {n}")
            elif self.step == 3:
                e = len(self.schema["edges"])
                self.query_one("#edges-count", Static).update(f"Edges: {e}")
            elif self.step == 4:
                import json as jm
                preview = jm.dumps(self.schema, indent=2)[:500]
                self.query_one("#schema-preview", Static).update(preview)

        def on_mount(self) -> None:
            self._update_view()

        def on_button_pressed(self, event: Button.Pressed) -> None:
            btn = event.button.id
            if btn == "btn-cancel":
                self.dismiss(None)
            elif btn == "btn-back" and self.step > 1:
                self.step -= 1
                self._update_view()
            elif btn == "btn-next":
                if self.step == 2 and not self.schema["nodes"]:
                    self.notify("Add at least one node", severity="warning")
                    return
                if self.step < 4:
                    self.step += 1
                    self._update_view()
                else:
                    self.dismiss(self.schema)
            elif btn == "btn-add-node":
                self._add_node()
            elif btn == "btn-add-edge":
                self._add_edge()
            elif btn == "btn-save-schema":
                self._save_schema()

        def _add_node(self) -> None:
            try:
                label = self.query_one("#nl", Input).value.strip()
                table = self.query_one("#nt", Input).value.strip()
                id_prop = self.query_one("#nid", Input).value.strip() or "id"
                props_str = self.query_one("#np", Input).value.strip()

                if not label or not table:
                    self.notify("Label and table required", severity="error")
                    return

                props = []
                if props_str:
                    for p in props_str.split(","):
                        if ":" in p:
                            nm, tp = p.split(":", 1)
                            props.append({"name": nm.strip(), "type": tp.strip()})

                self.schema["nodes"].append({
                    "name": label,
                    "tableName": table,
                    "idProperty": {"name": id_prop, "type": "int"},
                    "properties": props,
                })
                self.notify(f"Added: {label}")

                # Clear
                self.query_one("#nl", Input).value = ""
                self.query_one("#nt", Input).value = ""
                self.query_one("#nid", Input).value = ""
                self.query_one("#np", Input).value = ""
                self._update_view()
            except Exception as e:
                self.notify(f"Error: {e}", severity="error")

        def _add_edge(self) -> None:
            try:
                rel = self.query_one("#et", Input).value.strip()
                src = self.query_one("#es", Input).value.strip()
                tgt = self.query_one("#ed", Input).value.strip()
                tbl = self.query_one("#etbl", Input).value.strip()
                sfk = self.query_one("#esfk", Input).value.strip()
                tfk = self.query_one("#etfk", Input).value.strip()

                if not all([rel, src, tgt, tbl, sfk, tfk]):
                    self.notify("All fields required", severity="error")
                    return

                nodes = [n["name"] for n in self.schema["nodes"]]
                if src not in nodes or tgt not in nodes:
                    self.notify("Invalid node names", severity="error")
                    return

                self.schema["edges"].append({
                    "name": rel,
                    "sourceNode": src,
                    "sinkNode": tgt,
                    "tableName": tbl,
                    "sourceIdProperty": {"name": sfk, "type": "int"},
                    "sinkIdProperty": {"name": tfk, "type": "int"},
                    "properties": [],
                })
                self.notify(f"Added: {rel}")

                # Clear
                for fid in ["#et", "#es", "#ed", "#etbl", "#esfk", "#etfk"]:
                    self.query_one(fid, Input).value = ""
                self._update_view()
            except Exception as e:
                self.notify(f"Error: {e}", severity="error")

        def _save_schema(self) -> None:
            try:
                fn = self.query_one("#fn", Input).value.strip()
                if not fn:
                    self.notify("Enter filename", severity="error")
                    return
                if not fn.endswith(".json"):
                    fn += ".json"

                ex_dir = Path(__file__).parent.parent.parent / "examples"
                path = ex_dir / fn if ex_dir.exists() else Path(fn)

                import json as jm
                path.write_text(jm.dumps(self.schema, indent=2), encoding="utf-8")
                self.notify(f"Saved: {path}")
                self.dismiss(self.schema)
            except Exception as e:
                self.notify(f"Error: {e}", severity="error")

        def action_cancel(self) -> None:
            self.dismiss(None)

    class CypherTUI(App[None]):
        """Textual TUI for OpenCypher to SQL transpilation."""

        TITLE = "gsql2rsql - OpenCypher to Databricks SQL"
        ENABLE_COMMAND_PALETTE = False

        CSS = """
        Screen {
            layout: horizontal;
        }

        #left-panel {
            width: 45%;
            height: 100%;
            border: solid green;
        }

        #right-panel {
            width: 55%;
            height: 100%;
            border: solid blue;
        }

        #right-scroll {
            height: 1fr;
        }

        #examples-table {
            height: 1fr;
            margin: 1;
        }

        #filter-input {
            dock: top;
            margin: 1;
        }

        /* Section 1: Title + Description */
        #section-header {
            height: auto;
            padding: 1;
            border: solid cyan;
            margin: 0 1 1 1;
        }

        /* Section 2: OpenCypher Query */
        #section-cypher {
            height: auto;
            padding: 1;
            border: solid magenta;
            margin: 0 1 1 1;
        }

        /* Section 3: SQL Output */
        #section-sql {
            height: auto;
            padding: 1;
            border: solid green;
            margin: 0 1 1 1;
        }

        /* Section 4: Logical Plan */
        #section-plan {
            height: auto;
            padding: 1;
            border: solid cyan;
            margin: 0 1 1 1;
        }

        /* Section 5: AST Parse */
        #section-ast {
            height: auto;
            padding: 1;
            border: solid yellow;
            margin: 0 1 1 1;
        }



        #btn-copy-cypher {
            background: $primary;
        }

        #btn-copy-sql {
            background: $success;
        }

        #btn-copy-ast {
            background: $warning;
        }

        #btn-copy-plan {
            background: $secondary;
        }

        .btn-row {
            height: auto;
            align: right middle;
            padding: 0 1;
        }

        #title-left {
            dock: top;
            height: 3;
            content-align: center middle;
            background: $primary;
            color: $text;
            text-style: bold;
        }

        #title-right {
            dock: top;
            height: 3;
            content-align: center middle;
            background: $primary;
            color: $text;
            text-style: bold;
        }

        #status-bar {
            dock: bottom;
            height: 1;
            background: $surface;
            padding: 0 1;
        }

        .fraud-row {
            color: $error;
        }

        .credit-row {
            color: $success;
        }

        .features-row {
            color: $secondary;
        }

        .section-title {
            text-style: bold;
            background: $surface;
            padding: 0 1;
        }

        #progress-container {
            height: auto;
            margin: 0 1;
            display: none;
        }

        #progress-container.visible {
            display: block;
        }

        #progress-bar {
            width: 100%;
        }

        #progress-label {
            text-align: center;
            height: 1;
        }
        """

        BINDINGS = [
            Binding("q", "quit", "Quit", priority=True),
            Binding("ctrl+x", "execute", "Execute", priority=True),
            Binding("escape", "clear", "Clear", priority=True),
            Binding("f", "focus_filter", "Filter", priority=True),
            Binding("1", "filter_all", "All", priority=True),
            Binding("2", "filter_fraud", "Fraud", priority=True),
            Binding("3", "filter_credit", "Credit", priority=True),
            Binding("4", "filter_features", "Features", priority=True),
            Binding("5", "select_schema", "Schema", priority=True),
            Binding("ctrl+e", "edit_mode", "Edit", priority=True),
            Binding("r", "run_all", "Run All", priority=True),
            Binding("s", "copy_sql", "Copy SQL"),
            Binding("g", "copy_cypher", "Copy Cypher"),
            Binding("l", "copy_plan", "Copy Plan"),
            Binding("t", "copy_ast", "Copy AST"),
            Binding("a", "copy_all", "Copy All"),
        ]

        def __init__(
            self,
            examples: list[dict[str, Any]],
            schema: Any,
            status: str,
            cat_schemas: dict[str, Any],
            initial_schema_name: str | None = None,
            initial_schema_path: str | None = None,
        ) -> None:
            super().__init__()
            self.all_examples = examples
            self.graph_def = schema  # CLI-provided schema (fallback)
            self.category_schemas = cat_schemas  # Per-category schemas from YAML
            self.schema_status = status
            self.current_filter = "all"
            self.search_text = ""
            self.last_sql: str = ""
            self.last_query: str = ""
            self.last_plan: str = ""
            self.last_ast: str = ""
            self.current_category: str = ""
            self.last_example_idx: int | None = None
            # Track query execution status: idx -> "✅" | "❌" | ""
            self.query_status: dict[int, str] = {}
            # Flag to block shortcuts during run_all
            self._is_running: bool = False
            # Active schema tracking
            self.active_schema_name: str = initial_schema_name or "none"
            self.active_schema_path: str | None = initial_schema_path
            self.active_schema: Any = schema  # The actual schema provider

        def compose(self) -> ComposeResult:
            yield Header()

            with Horizontal():
                with Vertical(id="left-panel"):
                    yield Static(
                        f"📚 Examples ({len(self.all_examples)}) [1]All [2]Fraud [3]Credit [4]Features",
                        id="title-left",
                    )
                    yield Input(placeholder="🔍 Type to filter...", id="filter-input")
                    # Progress bar for run_all (hidden by default)
                    with Vertical(id="progress-container"):
                        yield Static("Running queries...", id="progress-label")
                        yield ProgressBar(id="progress-bar", total=100, show_eta=False)
                    yield DataTable(
                        id="examples-table", cursor_type="row", zebra_stripes=True
                    )

                with Vertical(id="right-panel"):

                    # Add Copy All button at the top
                    with Horizontal(classes="btn-row"):
                        yield Button(
                            label="📋 Copy All",
                            id="btn-copy-all-top",
                            variant="default",
                        )

                    with VerticalScroll(id="right-scroll"):
                        # Section 1: Title + Description
                        yield Static(
                            "[dim]Select an example from the list[/dim]",
                            id="section-header",
                        )

                        # Section 2: OpenCypher Query
                        yield Static(
                            "[bold magenta]═══ OpenCypher Query ═══[/bold magenta]\n"
                            "[dim]Query will appear here[/dim]",
                            id="section-cypher",
                        )
                        with Horizontal(classes="btn-row"):
                            yield Button(
                                label="📋 Copy Cypher",
                                id="btn-copy-cypher",
                                variant="primary",
                            )

                        # Query input for editing
                        yield TextArea(id="query-input", language="sql")

                        # Section 3: SQL Output
                        yield Static(
                            "[bold green]═══ Databricks SQL ═══[/bold green]\n"
                            "[dim]SQL output will appear here[/dim]",
                            id="section-sql",
                        )
                        with Horizontal(classes="btn-row"):
                            yield Button(
                                label="📋 Copy SQL",
                                id="btn-copy-sql",
                                variant="success",
                            )

                        # Section 4: Logical Plan
                        yield Static(
                            "[bold cyan]═══ Logical Plan ═══[/bold cyan]\n"
                            "[dim]Logical plan will appear here[/dim]",
                            id="section-plan",
                        )
                        with Horizontal(classes="btn-row"):
                            yield Button(
                                label="📋 Copy Plan",
                                id="btn-copy-plan",
                                variant="default",
                            )

                        # Section 5: AST Parse
                        yield Static(
                            "[bold yellow]═══ AST Parse ═══[/bold yellow]\n"
                            "[dim]AST will appear here[/dim]",
                            id="section-ast",
                        )
                        with Horizontal(classes="btn-row"):
                            yield Button(
                                label="📋 Copy AST",
                                id="btn-copy-ast",
                                variant="warning",
                            )

            yield Static(f" {self.schema_status}", id="status-bar")
            yield Footer()

        def on_mount(self) -> None:
            """Initialize the table."""
            self._refresh_table()
            self._update_status_bar()
            self.query_one("#examples-table", DataTable).focus()

        def _refresh_table(self) -> None:
            """Refresh the examples table."""
            table = self.query_one("#examples-table", DataTable)
            table.clear(columns=True)

            # Add columns with explicit keys for status updates
            table.add_column("#", width=4, key="num")
            table.add_column("St", width=3, key="status")  # Status column
            table.add_column("Cat", width=8, key="cat")
            table.add_column("Description", width=42, key="desc")
            table.add_column("Use Case", width=22, key="usecase")

            # Filter examples
            filtered = []
            search_lower = self.search_text.lower()

            for ex in self.all_examples:
                if (
                    self.current_filter != "all"
                    and ex["category"] != self.current_filter
                ):
                    continue
                if search_lower:
                    searchable = f"{ex['description']} {ex['application']} {ex['query']} {ex['notes']}".lower()
                    if search_lower not in searchable:
                        continue
                filtered.append(ex)

            # Add rows with category colors
            for ex in filtered:
                app_short = ex["application"]
                if ": " in app_short:
                    app_short = app_short.split(": ", 1)[1]

                desc = ex["description"]
                if len(desc) > 40:
                    desc = desc[:37] + "..."
                if len(app_short) > 20:
                    app_short = app_short[:17] + "..."

                # Get status emoji for this example
                status_emoji = self.query_status.get(ex["idx"], "")

                # Color based on category
                if ex["category"] == "fraud":
                    cat_text = Text("FRAUD", style="bold red")
                    desc_text = Text(desc, style="red")
                    app_text = Text(app_short, style="red")
                elif ex["category"] == "credit":
                    cat_text = Text("CREDIT", style="bold green")
                    desc_text = Text(desc, style="green")
                    app_text = Text(app_short, style="green")
                else:  # features or other
                    cat_text = Text("FEAT", style="bold cyan")
                    desc_text = Text(desc, style="cyan")
                    app_text = Text(app_short, style="cyan")

                table.add_row(
                    str(ex["idx"]),
                    status_emoji,
                    cat_text,
                    desc_text,
                    app_text,
                    key=str(ex["idx"]),
                )

            # Update title with run all hint
            title = self.query_one("#title-left", Static)
            cat_name = {
                "all": "All",
                "fraud": "Fraud 🔍",
                "credit": "Credit 💳",
                "features": "Features ⚡",
            }
            title.update(
                f"📚 {cat_name.get(self.current_filter, 'All')} "
                f"({len(filtered)}/{len(self.all_examples)}) [R]Run All"
            )

        def on_input_changed(self, event: Input.Changed) -> None:
            """Handle filter input."""
            if event.input.id == "filter-input":
                self.search_text = event.value
                self._refresh_table()

        def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
            """Handle row selection."""
            if self._is_running:
                return
            if event.row_key is None:
                return
            try:
                idx = int(str(event.row_key.value))
                for ex in self.all_examples:
                    if ex["idx"] == idx:
                        self._show_example(ex)
                        break
            except (ValueError, AttributeError):
                pass

        def _show_example(self, example: dict[str, Any]) -> None:
            """Display example and execute."""
            query_text = example["query"].strip()
            self.current_category = example["category"]
            self.last_query = query_text
            # remember which example is currently shown (used to mark errors)
            self.last_example_idx = example["idx"]

            cat_upper = example["category"].upper()
            cat_color = "red" if example["category"] == "fraud" else "green"

            # Section 1: Header (Title + Description only)
            header_section = self.query_one("#section-header", Static)
            notes_text = (
                f"\n\n[dim]📝 {example['notes'].strip()}[/dim]"
                if example.get("notes")
                else ""
            )
            header_content = (
                f"[bold {cat_color}]#{example['idx']} [{cat_upper}][/bold {cat_color}] "
                f"[bold white]{example['description']}[/bold white]\n"
                f"[cyan]{example.get('application', '')}[/cyan]"
                f"{notes_text}"
            )
            header_section.update(header_content)

            # Section 2: OpenCypher Query with syntax highlighting
            cypher_section = self.query_one("#section-cypher", Static)
            syntax = Syntax(query_text, "sql", theme="monokai", line_numbers=True)
            console_output = Console(record=True, force_terminal=True, width=80)
            console_output.print(syntax)
            highlighted = console_output.export_text()
            cypher_content = (
                f"[bold magenta]═══ OpenCypher Query ═══[/bold magenta] "
                f"[dim](G to copy)[/dim]\n{highlighted}"
            )
            cypher_section.update(cypher_content)

            # Update query input for editing
            query_input = self.query_one("#query-input", TextArea)
            query_input.load_text(query_text)

            # Auto-execute with category schema
            self._execute_query(query_text, example["category"])

        def _execute_query(self, query: str, category: str | None = None) -> None:
            """Execute query and show results."""
            if not query.strip():
                return

            self.last_query = query.strip()
            if category:
                self.current_category = category

            # Determine which schema to use:
            # 1. If in custom query mode (no category), use active_schema
            # 2. If viewing an example, use category-specific schema
            # 3. Fallback to CLI-provided schema
            if not self.current_category:
                # Custom query mode - use active schema
                schema_to_use = self.active_schema
            else:
                # Example mode - use category schema
                schema_to_use = self.category_schemas.get(
                    self.current_category, self.active_schema
                )
            if not schema_to_use:
                schema_to_use = self.graph_def

            sql_section = self.query_one("#section-sql", Static)
            plan_section = self.query_one("#section-plan", Static)
            ast_section = self.query_one("#section-ast", Static)
            result = _transpile_query(query, schema_to_use)

            # Update status and show notification based on result
            if result.get("transpile_error") or result.get("parse_error"):
                error_msg = result.get("transpile_error") or result.get("parse_error")
                # Mark as failed
                if self.last_example_idx is not None:
                    self.query_status[self.last_example_idx] = "❌"
                # Notify with error severity
                self.notify(f"Error: {error_msg}", severity="error")
                # Update table to show status
                self._update_row_status(self.last_example_idx, "❌")
            else:
                # Mark as success
                if self.last_example_idx is not None:
                    self.query_status[self.last_example_idx] = "✅"
                # Update table to show status
                self._update_row_status(self.last_example_idx, "✅")

            # Section 3: SQL Output
            if result["success"] and result["sql"]:
                self.last_sql = result["sql"]
                # Syntax highlight the SQL
                syntax = Syntax(
                    result["sql"], "sql", theme="monokai", line_numbers=True
                )
                console_output = Console(record=True, force_terminal=True, width=80)
                console_output.print(syntax)
                highlighted_sql = console_output.export_text()
                sql_content = (
                    f"[bold green]═══ Databricks SQL ═══[/bold green] "
                    f"[dim](S to copy)[/dim]\n{highlighted_sql}"
                )
                sql_section.update(sql_content)
            elif result["transpile_error"]:
                self.last_sql = result["transpile_error"]  # Allow copying the error
                sql_content = (
                    f"[bold yellow]═══ Databricks SQL ═══[/bold yellow]\n"
                    f"[yellow]⚠️  Transpile Error:[/yellow]\n{result['transpile_error']}"
                )
                sql_section.update(sql_content)
            else:
                self.last_sql = ""
                sql_section.update(
                    "[bold green]═══ Databricks SQL ═══[/bold green]\n"
                    "[dim]No SQL output[/dim]"
                )

            # Section 4: Logical Plan
            if result.get("logical_plan"):
                self.last_plan = result["logical_plan"]
                plan_content = (
                    f"[bold cyan]═══ Logical Plan ═══[/bold cyan] "
                    f"[dim](L to copy)[/dim]\n{result['logical_plan']}"
                )
                plan_section.update(plan_content)
            else:
                self.last_plan = ""
                plan_section.update(
                    "[bold cyan]═══ Logical Plan ═══[/bold cyan]\n"
                    "[dim]No logical plan (schema required)[/dim]"
                )

            # Section 5: AST Parse
            if result["parse_success"] and result["ast"]:
                self.last_ast = result["ast"]
                ast_content = (
                    f"[bold yellow]═══ AST Parse ═══[/bold yellow] "
                    f"[dim](T to copy)[/dim]\n{result['ast']}"
                )
                ast_section.update(ast_content)
            elif result["parse_error"]:
                self.last_ast = result["parse_error"]  # Allow copying the error
                ast_content = (
                    f"[bold red]═══ AST Parse ═══[/bold red]\n"
                    f"[red]✗ Parse Error:[/red]\n{result['parse_error']}"
                )
                ast_section.update(ast_content)
            else:
                self.last_ast = ""
                ast_section.update(
                    "[bold yellow]═══ AST Parse ═══[/bold yellow]\n"
                    "[dim]No AST output[/dim]"
                )

        def action_execute(self) -> None:
            """Execute query from input."""
            if self._is_running:
                return
            query_input = self.query_one("#query-input", TextArea)
            if query_input.text.strip():
                self._execute_query(query_input.text.strip())

        def action_clear(self) -> None:
            """Clear displays."""
            if self._is_running:
                return
            self.query_one("#query-input", TextArea).load_text("")
            self.query_one("#section-header", Static).update(
                "[dim]Select an example from the list[/dim]"
            )
            self.query_one("#section-cypher", Static).update(
                "[bold magenta]═══ OpenCypher Query ═══[/bold magenta]\n"
                "[dim]Query will appear here[/dim]"
            )
            self.query_one("#section-sql", Static).update(
                "[bold green]═══ Databricks SQL ═══[/bold green]\n"
                "[dim]SQL output will appear here[/dim]"
            )
            self.query_one("#section-plan", Static).update(
                "[bold cyan]═══ Logical Plan ═══[/bold cyan]\n"
                "[dim]Logical plan will appear here[/dim]"
            )
            self.query_one("#section-ast", Static).update(
                "[bold yellow]═══ AST Parse ═══[/bold yellow]\n"
                "[dim]AST will appear here[/dim]"
            )
            self.last_query = ""
            self.last_sql = ""
            self.last_plan = ""
            self.last_ast = ""

        def action_focus_filter(self) -> None:
            """Focus filter input."""
            if self._is_running:
                return
            self.query_one("#filter-input", Input).focus()

        def action_filter_all(self) -> None:
            """Show all."""
            if self._is_running:
                return
            self.current_filter = "all"
            self._refresh_table()

        def action_filter_fraud(self) -> None:
            """Show fraud only."""
            if self._is_running:
                return
            self.current_filter = "fraud"
            self._refresh_table()

        def action_filter_credit(self) -> None:
            """Show credit only."""
            if self._is_running:
                return
            self.current_filter = "credit"
            self._refresh_table()

        def action_filter_features(self) -> None:
            """Show features only."""
            if self._is_running:
                return
            self.current_filter = "features"
            self._refresh_table()

        def _update_row_status(self, idx: int | None, status: str) -> None:
            """Update the status column for a specific row."""
            if idx is None:
                return
            try:
                table = self.query_one("#examples-table", DataTable)
                # Use row_key (str(idx)) and column_key ("status")
                table.update_cell(str(idx), "status", status)
            except Exception:
                # Non-fatal: table might not have the row visible due to filter
                pass

        def action_run_all(self) -> None:
            """Run all queries sequentially to check status."""
            if self._has_text_input_focus():
                return
            if self._is_running:
                self.notify("Already running...", severity="warning")
                return

            # Start async worker
            self._run_all_queries()

        @work(thread=True)
        def _run_all_queries(self) -> None:
            """Worker to run all queries with progress updates."""
            self._is_running = True

            # Show progress bar
            self.call_from_thread(self._show_progress, True)

            total = len(self.all_examples)
            success_count = 0
            error_count = 0

            for i, ex in enumerate(self.all_examples):
                idx = ex["idx"]
                query = ex["query"].strip()
                category = ex["category"]

                # Update progress label and bar
                progress = int((i / total) * 100)
                self.call_from_thread(
                    self._update_progress, i + 1, total, progress, ex["description"][:30]
                )

                # Get schema for this category
                schema_to_use = self.category_schemas.get(category, self.graph_def)
                if not schema_to_use:
                    schema_to_use = self.graph_def

                # Transpile
                result = _transpile_query(query, schema_to_use)

                if result.get("transpile_error") or result.get("parse_error"):
                    self.query_status[idx] = "❌"
                    error_count += 1
                else:
                    self.query_status[idx] = "✅"
                    success_count += 1

                # Update row status in table
                self.call_from_thread(self._update_row_status, idx, self.query_status[idx])

            # Hide progress bar and refresh
            self.call_from_thread(self._show_progress, False)
            self.call_from_thread(self._refresh_table)

            # Show summary notification
            if error_count == 0:
                self.call_from_thread(
                    self.notify,
                    f"All {success_count} queries passed! ✅",
                    severity="information"
                )
            else:
                self.call_from_thread(
                    self.notify,
                    f"Results: {success_count} ✅ passed, {error_count} ❌ failed",
                    severity="warning" if error_count < success_count else "error"
                )

            self._is_running = False

        def _show_progress(self, show: bool) -> None:
            """Show or hide the progress container."""
            container = self.query_one("#progress-container", Vertical)
            if show:
                container.add_class("visible")
            else:
                container.remove_class("visible")

        def _update_progress(
            self, current: int, total: int, percent: int, desc: str
        ) -> None:
            """Update progress bar and label."""
            label = self.query_one("#progress-label", Static)
            bar = self.query_one("#progress-bar", ProgressBar)
            label.update(f"Running {current}/{total}: {desc}...")
            bar.update(progress=percent)

        def _has_text_input_focus(self) -> bool:
            """Check if any text input widget has focus."""
            try:
                if self.query_one("#filter-input", Input).has_focus:
                    return True
                if self.query_one("#query-input", TextArea).has_focus:
                    return True
            except Exception:
                pass
            return False

        def action_copy_cypher(self) -> None:
            """Copy Cypher query to clipboard."""
            if self._is_running or self._has_text_input_focus():
                return
            self._do_copy(self.last_query, "Cypher query")

        def action_copy_sql(self) -> None:
            """Copy SQL to clipboard."""
            if self._is_running or self._has_text_input_focus():
                return
            self._do_copy(self.last_sql, "SQL")

        def action_copy_plan(self) -> None:
            """Copy Logical Plan to clipboard."""
            if self._is_running or self._has_text_input_focus():
                return
            self._do_copy(self.last_plan, "Logical Plan")

        def action_copy_ast(self) -> None:
            """Copy AST to clipboard."""
            if self._is_running or self._has_text_input_focus():
                return
            self._do_copy(self.last_ast, "AST")

        def action_copy_all(self) -> None:
            """Copy all to clipboard."""
            if self._is_running or self._has_text_input_focus():
                return
            self._copy_all()

        def _copy_to_clipboard(self, text: str) -> tuple[bool, str]:
            """Copy text to clipboard. Returns (success, message)."""
            import shutil
            import subprocess

            if not text:
                return False, "Nothing to copy"

            # Build environment with DISPLAY for X11
            env = os.environ.copy()
            if "DISPLAY" not in env:
                env["DISPLAY"] = ":0"

            if sys.platform == "darwin":
                cmds = [["pbcopy"]]
            elif sys.platform == "win32":
                cmds = [["clip"]]
            else:
                # Linux: try xclip first (most reliable), then others
                cmds = [
                    ["xclip", "-selection", "clipboard"],
                    ["xsel", "--clipboard", "--input"],
                    ["wl-copy"],
                ]

            errors = []
            for cmd in cmds:
                if shutil.which(cmd[0]):
                    try:
                        proc = subprocess.run(
                            cmd,
                            input=text.encode("utf-8"),
                            capture_output=True,
                            timeout=3,
                            env=env,
                        )
                        if proc.returncode == 0:
                            return True, cmd[0]
                        errors.append(f"{cmd[0]}: rc={proc.returncode}")
                    except subprocess.TimeoutExpired:
                        errors.append(f"{cmd[0]}: timeout")
                    except Exception as e:
                        errors.append(f"{cmd[0]}: {e}")

            # Fallback: save to temp file
            try:
                temp_file = Path("/tmp/gsql2rsql_clipboard.txt")
                temp_file.write_text(text, encoding="utf-8")
                return True, f"file:{temp_file}"
            except Exception as e:
                return False, f"Failed: {errors}, file: {e}"

        def on_button_pressed(self, event: Button.Pressed) -> None:
            """Handle button clicks for copy actions."""
            button_id = event.button.id

            if button_id == "btn-copy-cypher":
                self._do_copy(self.last_query, "Cypher query")
            elif button_id == "btn-copy-sql":
                self._do_copy(self.last_sql, "SQL")
            elif button_id == "btn-copy-plan":
                self._do_copy(self.last_plan, "Logical Plan")
            elif button_id == "btn-copy-ast":
                self._do_copy(self.last_ast, "AST")
            elif button_id in ("btn-copy-all", "btn-copy-all-top"):
                self._copy_all()

        def _do_copy(self, content: str, label: str) -> None:
            """Perform copy operation and show notification."""
            if not content:
                self.notify(f"No {label} to copy", severity="warning")
                return

            success, msg = self._copy_to_clipboard(content)
            if success:
                if msg.startswith("file:"):
                    self.notify(f"{label} saved to {msg[5:]}", severity="information")
                else:
                    self.notify(f"{label} copied! ({msg})", severity="information")
            else:
                self.notify(f"Copy failed: {msg}", severity="error")

        def _copy_all(self) -> None:
            """Copy query + SQL + logical plan + AST to clipboard."""
            parts = []
            if self.last_query:
                parts.append(f"=== OpenCypher Query ===\n{self.last_query}")
            if self.last_sql:
                parts.append(f"\n=== Databricks SQL ===\n{self.last_sql}")
            if self.last_plan:
                parts.append(f"\n=== Logical Plan ===\n{self.last_plan}")
            if self.last_ast:
                parts.append(f"\n=== AST ===\n{self.last_ast}")

            if not parts:
                self.notify("Nothing to copy", severity="warning")
                return

            content = "\n".join(parts)
            success, msg = self._copy_to_clipboard(content)
            if success:
                if msg.startswith("file:"):
                    self.notify(f"All saved to {msg[5:]}", severity="information")
                else:
                    self.notify(f"All copied! ({msg})", severity="information")
            else:
                self.notify(f"Copy failed: {msg}", severity="error")

        def action_select_schema(self) -> None:
            """Open schema selector modal."""
            if self._is_running:
                return
            available = list(self.category_schemas.keys())
            modal = SchemaSelectorModal(self.active_schema_name, available)
            self.push_screen(modal, self._on_schema_selected)

        def _on_schema_selected(self, result: str | None) -> None:
            """Handle schema selection result."""
            if result is None:
                return  # Cancelled

            if result == "__file__":
                # Open file browser
                modal = FileBrowserModal(Path.home())
                self.push_screen(modal, self._on_file_selected)
            elif result == "__wizard__":
                # Open schema wizard
                modal = SchemaWizardModal()
                self.push_screen(modal, self._on_wizard_complete)
            elif result == "none":
                # No schema mode
                self.active_schema_name = "none"
                self.active_schema_path = None
                self.active_schema = None
                self._update_status_bar()
                self._save_schema_config()
                self.notify("Schema cleared - AST only mode", severity="information")
            elif result in self.category_schemas:
                # Built-in schema
                self.active_schema_name = result
                self.active_schema_path = None
                self.active_schema = self.category_schemas[result]
                self._update_status_bar()
                self._save_schema_config()
                self.notify(f"Switched to {result.title()} schema", severity="information")

        def _on_file_selected(self, result: Path | None) -> None:
            """Handle file selection result."""
            if result is None:
                return

            try:
                if result.suffix.lower() in (".yaml", ".yml"):
                    data = yaml.safe_load(result.read_text(encoding="utf-8"))
                    if "schema" in data:
                        schema_data = data["schema"]
                    else:
                        schema_data = data
                else:
                    schema_data = json.loads(result.read_text(encoding="utf-8"))

                loaded_schema = _load_schema_from_yaml(schema_data)
                self.active_schema_name = "custom"
                self.active_schema_path = str(result)
                self.active_schema = loaded_schema
                self._update_status_bar()
                self._save_schema_config()
                self.notify(f"Loaded schema from {result.name}", severity="information")
            except Exception as e:
                self.notify(f"Failed to load schema: {e}", severity="error")

        def _on_wizard_complete(self, result: dict | None) -> None:
            """Handle wizard completion."""
            if result is None:
                return

            try:
                loaded_schema = _load_schema_from_yaml(result)
                self.active_schema_name = "custom"
                self.active_schema_path = None
                self.active_schema = loaded_schema
                self._update_status_bar()
                self._save_schema_config()
                self.notify("Custom schema created and activated", severity="information")
            except Exception as e:
                self.notify(f"Failed to activate schema: {e}", severity="error")

        def _update_status_bar(self) -> None:
            """Update status bar with current schema info."""
            status_bar = self.query_one("#status-bar", Static)
            if self.active_schema_name == "none" or self.active_schema is None:
                status_bar.update(" [5]Schema: None (AST only)")
            elif self.active_schema_name == "custom" and self.active_schema_path:
                path = Path(self.active_schema_path)
                status_bar.update(f" [5]Schema: {path.name}")
            elif self.active_schema_name == "custom":
                status_bar.update(" [5]Schema: Custom (wizard)")
            else:
                status_bar.update(f" [5]Schema: {self.active_schema_name.title()}")

        def _save_schema_config(self) -> None:
            """Save current schema selection to config."""
            config = _load_config()
            config["last_schema"] = self.active_schema_name
            config["last_schema_path"] = self.active_schema_path
            _save_config(config)

        def action_edit_mode(self) -> None:
            """Enter edit mode for custom query input."""
            if self._is_running:
                return

            # Skip if an input or textarea already has focus (let them handle the key)
            if self._has_text_input_focus():
                return

            # Update header to show custom query mode
            header_section = self.query_one("#section-header", Static)
            schema_info = self.active_schema_name.title() if self.active_schema_name != "none" else "None (AST only)"
            header_section.update(
                f"[bold cyan]Custom Query Mode[/bold cyan]\n"
                f"Schema: {schema_info}\n"
                f"[dim]Type your query below, press Ctrl+X to execute[/dim]"
            )

            # Clear example tracking
            self.last_example_idx = None
            self.current_category = ""

            # Focus the TextArea
            query_input = self.query_one("#query-input", TextArea)
            query_input.focus()

    # Determine initial schema info
    initial_schema_name: str | None = None
    initial_schema_path: str | None = None
    if last_schema_name:
        initial_schema_name = last_schema_name
        initial_schema_path = last_schema_path

    app = CypherTUI(
        examples=examples_data,
        schema=graph_def,
        status=schema_status,
        cat_schemas=category_schemas,
        initial_schema_name=initial_schema_name,
        initial_schema_path=initial_schema_path,
    )
    app.run()


if __name__ == "__main__":
    main()
