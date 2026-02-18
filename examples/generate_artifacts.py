#!/usr/bin/env python3
"""Generate transpiler artifacts for all example queries.

This script executes the gsql2rsql transpiler for every openCypher query
found in the examples/ YAML files and persists artifacts to examples/out/.
"""

from __future__ import annotations

import json
import re
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


_VLP_PATTERN = re.compile(r'\*\d+\.\.\d+')


@dataclass
class TranspileResult:
    """Result of a transpilation attempt."""
    query_id: str
    description: str
    query: str
    application: str | None = None
    notes: str | None = None
    ast: str | None = None
    logical_plan: str | None = None
    sql: str | None = None
    procedural_databricks_sql: str | None = None
    procedural_pyspark_sql: str | None = None
    error: str | None = None
    success: bool = False


def load_schema_from_yaml(schema_data: dict) -> Any:
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
        table_filter = edge_data.get("filter")
        table_desc = SQLTableDescriptor(
            table_or_view_name=table_name,
            filter=table_filter,
        )

        provider.add_edge(edge_schema, table_desc)

    # Enable no-label support if configured
    no_label_config = schema_data.get("noLabelSupport")
    if no_label_config and no_label_config.get("enabled", False):
        table_name = no_label_config.get("tableName", "")
        node_id_columns = no_label_config.get("nodeIdColumns", ["id"])
        if table_name:
            # Collect properties from all nodes for wildcard
            all_properties: list[EntityProperty] = []
            seen_props: set[str] = set()
            for node_data in schema_data.get("nodes", []):
                for prop in node_data.get("properties", []):
                    if prop["name"] not in seen_props:
                        all_properties.append(
                            EntityProperty(
                                property_name=prop["name"],
                                data_type=type_mapping.get(prop.get("type", "string"), str),
                            )
                        )
                        seen_props.add(prop["name"])
            provider.enable_no_label_support(
                table_name=table_name,
                node_id_columns=node_id_columns,
                properties=all_properties,
            )

    return provider


def sanitize_name(name: str) -> str:
    """Create a safe directory/file name from a description."""
    # Remove special characters and replace spaces with underscores
    sanitized = re.sub(r'[^\w\s-]', '', name.lower())
    sanitized = re.sub(r'[\s-]+', '_', sanitized)
    return sanitized[:50]  # Limit length


def transpile_query(
    query: str,
    schema_provider: Any,
    description: str,
    query_id: str,
    application: str | None = None,
    notes: str | None = None,
) -> TranspileResult:
    """Transpile a single query and capture all artifacts."""
    from gsql2rsql import LogicalPlan, OpenCypherParser, SQLRenderer

    result = TranspileResult(
        query_id=query_id,
        description=description,
        query=query.strip(),
        application=application,
        notes=notes,
    )

    # Step 1: Parse to AST
    try:
        parser = OpenCypherParser()
        ast = parser.parse(query)
        result.ast = ast.dump_tree()
    except Exception as e:
        result.error = f"PARSER ERROR:\n{e}\n\n{traceback.format_exc()}"
        return result

    # Step 2: Generate Logical Plan
    try:
        from gsql2rsql.planner.subquery_optimizer import optimize_plan

        plan = LogicalPlan.process_query_tree(ast, schema_provider)
        # Apply optimizations (predicate pushdown, subquery flattening)
        optimize_plan(plan)
        # Use dump_graph() for structured plan output
        result.logical_plan = plan.dump_graph()
    except Exception as e:
        result.error = f"PLANNER ERROR:\n{e}\n\n{traceback.format_exc()}"
        result.ast = result.ast  # Keep AST even on planner failure
        return result

    # Step 3: Resolve columns
    try:
        plan.resolve(query)
    except Exception as e:
        result.error = f"RESOLVER ERROR:\n{e}\n\n{traceback.format_exc()}"
        return result

    # Step 4: Render SQL (CTE mode — default)
    try:
        renderer = SQLRenderer(schema_provider)
        sql = renderer.render_plan(plan)
        result.sql = sql
        result.success = True
    except Exception as e:
        result.error = f"RENDERER ERROR:\n{e}\n\n{traceback.format_exc()}"
        return result

    # Step 5: Procedural BFS variants for VLP queries
    if _VLP_PATTERN.search(query):
        for strategy, attr in [
            ("temp_tables", "procedural_databricks_sql"),
            ("numbered_views", "procedural_pyspark_sql"),
        ]:
            try:
                proc_ast = parser.parse(query)
                proc_plan = LogicalPlan.process_query_tree(
                    proc_ast, schema_provider,
                )
                optimize_plan(proc_plan)
                proc_plan.resolve(query)
                proc_renderer = SQLRenderer(
                    schema_provider,
                    vlp_rendering_mode="procedural",
                    materialization_strategy=strategy,
                )
                setattr(
                    result, attr,
                    proc_renderer.render_plan(proc_plan),
                )
            except Exception:
                pass  # procedural not supported for this query

    return result


def save_artifacts(result: TranspileResult, output_dir: Path) -> None:
    """Save transpilation artifacts to the output directory."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save query
    (output_dir / "query.cypher").write_text(result.query, encoding="utf-8")

    # Save AST
    if result.ast:
        (output_dir / "ast.txt").write_text(result.ast, encoding="utf-8")

    # Save logical plan
    if result.logical_plan:
        (output_dir / "logical_plan.txt").write_text(result.logical_plan, encoding="utf-8")

    # Save SQL (CTE mode)
    if result.sql:
        (output_dir / "databricks.sql").write_text(result.sql, encoding="utf-8")

    # Save procedural BFS SQL variants
    if result.procedural_databricks_sql:
        (output_dir / "procedural_databricks.sql").write_text(
            result.procedural_databricks_sql, encoding="utf-8",
        )
    if result.procedural_pyspark_sql:
        (output_dir / "procedural_pyspark.sql").write_text(
            result.procedural_pyspark_sql, encoding="utf-8",
        )

    # Save error if any
    if result.error:
        (output_dir / "error.txt").write_text(result.error, encoding="utf-8")

    # Save metadata
    metadata = {
        "query_id": result.query_id,
        "description": result.description,
        "application": result.application,
        "notes": result.notes,
        "success": result.success,
        "has_ast": result.ast is not None,
        "has_logical_plan": result.logical_plan is not None,
        "has_sql": result.sql is not None,
        "has_procedural_databricks": result.procedural_databricks_sql is not None,
        "has_procedural_pyspark": result.procedural_pyspark_sql is not None,
        "has_error": result.error is not None,
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2), encoding="utf-8"
    )


def process_yaml_file(yaml_path: Path, output_base: Path) -> list[TranspileResult]:
    """Process all queries in a YAML file."""
    print(f"\n{'='*60}")
    print(f"Processing: {yaml_path.name}")
    print(f"{'='*60}")

    with open(yaml_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    # Load schema from YAML
    schema_data = data.get("schema", {})
    schema_provider = load_schema_from_yaml(schema_data)

    # Get example name from file
    example_name = yaml_path.stem  # e.g., "fraud_queries"

    results = []
    examples = data.get("examples", [])

    for idx, example in enumerate(examples, 1):
        description = example.get("description", f"Query {idx}")
        query = example.get("query", "")
        application = example.get("application")
        notes = example.get("notes")

        if not query.strip():
            print(f"  [{idx}/{len(examples)}] SKIP: Empty query - {description[:50]}")
            continue

        # Create query ID
        query_id = f"{idx:02d}_{sanitize_name(description)}"

        print(f"  [{idx}/{len(examples)}] Processing: {description[:60]}...")

        # Transpile
        result = transpile_query(
            query,
            schema_provider,
            description,
            query_id,
            application=application,
            notes=notes,
        )

        # Save artifacts
        output_dir = output_base / example_name / query_id
        save_artifacts(result, output_dir)

        status = "OK" if result.success else "FAILED"
        print(f"           -> {status}")

        results.append(result)

    return results


def generate_summary(all_results: dict[str, list[TranspileResult]], output_base: Path) -> None:
    """Generate a summary of all transpilation results."""
    summary_lines = [
        "# Transpilation Artifacts Summary",
        "",
        f"Generated by: generate_artifacts.py",
        "",
        "## Results Overview",
        "",
    ]

    total_success = 0
    total_failed = 0

    for yaml_name, results in all_results.items():
        success = sum(1 for r in results if r.success)
        failed = len(results) - success
        total_success += success
        total_failed += failed

        summary_lines.append(f"### {yaml_name}")
        summary_lines.append(f"- Total: {len(results)}")
        summary_lines.append(f"- Success: {success}")
        summary_lines.append(f"- Failed: {failed}")
        summary_lines.append("")

        # List each query
        summary_lines.append("| # | Query ID | Status | Description |")
        summary_lines.append("|---|----------|--------|-------------|")

        for result in results:
            status = "OK" if result.success else "FAILED"
            desc = result.description[:50] + "..." if len(result.description) > 50 else result.description
            summary_lines.append(f"| {result.query_id[:2]} | {result.query_id} | {status} | {desc} |")

        summary_lines.append("")

    # Overall summary
    summary_lines.insert(4, f"- **Total Queries**: {total_success + total_failed}")
    summary_lines.insert(5, f"- **Successful**: {total_success}")
    summary_lines.insert(6, f"- **Failed**: {total_failed}")
    summary_lines.insert(7, "")

    summary_path = output_base / "SUMMARY.md"
    summary_path.write_text("\n".join(summary_lines), encoding="utf-8")
    print(f"\nSummary written to: {summary_path}")


def main() -> int:
    """Main entry point."""
    examples_dir = Path(__file__).parent
    output_base = examples_dir / "out"

    # Clean output directory
    if output_base.exists():
        import shutil
        shutil.rmtree(output_base)
    output_base.mkdir(parents=True)

    # Find all YAML files
    yaml_files = sorted(examples_dir.glob("*.yaml"))

    if not yaml_files:
        print("No YAML files found in examples/")
        return 1

    print(f"Found {len(yaml_files)} YAML files to process")

    all_results: dict[str, list[TranspileResult]] = {}

    for yaml_path in yaml_files:
        try:
            results = process_yaml_file(yaml_path, output_base)
            all_results[yaml_path.stem] = results
        except Exception as e:
            print(f"ERROR processing {yaml_path.name}: {e}")
            traceback.print_exc()

    # Generate summary
    generate_summary(all_results, output_base)

    # Print final statistics
    total = sum(len(r) for r in all_results.values())
    success = sum(1 for results in all_results.values() for r in results if r.success)

    print(f"\n{'='*60}")
    print(f"FINAL: {success}/{total} queries transpiled successfully")
    print(f"Output directory: {output_base}")
    print(f"{'='*60}")

    return 0 if success == total else 1


if __name__ == "__main__":
    sys.exit(main())
