"""Command-line interface for the openCypher transpiler."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import TextIO

import click

from gsql2rsql import __version__


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
def transpile(
    input_file: Path | None,
    output_file: Path | None,
    schema_file: Path,
    pretty: bool,
) -> None:
    """Transpile an openCypher query to Databricks SQL."""
    # Read the query
    if input_file:
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

        parser = OpenCypherParser()
        ast = parser.parse(query)
        plan = LogicalPlan.process_query_tree(ast, graph_def)
        renderer = SQLRenderer(graph_def)
        sql = renderer.render_plan(plan)
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
    if input_file:
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


def _load_schema(schema_data: dict) -> "ISQLDBSchemaProvider":
    """Load a graph schema from JSON data."""
    from gsql2rsql.common.schema import EntityProperty, NodeSchema, EdgeSchema
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


if __name__ == "__main__":
    main()
