#!/usr/bin/env python
"""Execute gsql2rsql transpiler for all YAML examples and persist artifacts."""
import json
import os
import re
import sys
import traceback
from pathlib import Path
import yaml

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import EdgeSchema, EntityProperty, NodeSchema
from gsql2rsql.planner.pass_manager import optimize_plan
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor


def load_yaml_schema(yaml_data: dict) -> SimpleSQLSchemaProvider:
    """Load schema from YAML data into a SimpleSQLSchemaProvider."""
    provider = SimpleSQLSchemaProvider()
    schema = yaml_data.get("schema", {})

    # Load nodes
    for node in schema.get("nodes", []):
        id_prop = node.get("idProperty", {})
        props = [
            EntityProperty(
                property_name=p["name"],
                data_type=get_python_type(p["type"])
            )
            for p in node.get("properties", [])
        ]

        node_schema = NodeSchema(
            name=node["name"],
            properties=props,
            node_id_property=EntityProperty(
                property_name=id_prop.get("name", "id"),
                data_type=get_python_type(id_prop.get("type", "int"))
            ),
        )
        provider.add_node(
            node_schema,
            SQLTableDescriptor(table_or_view_name=node["tableName"])
        )

    # Load edges
    for edge in schema.get("edges", []):
        source_id = edge.get("sourceIdProperty", {})
        sink_id = edge.get("sinkIdProperty", {})
        props = [
            EntityProperty(
                property_name=p["name"],
                data_type=get_python_type(p["type"])
            )
            for p in edge.get("properties", [])
        ]

        edge_schema = EdgeSchema(
            name=edge["name"],
            source_node_id=edge["sourceNode"],
            sink_node_id=edge["sinkNode"],
            source_id_property=EntityProperty(
                property_name=source_id.get("name", "source_id"),
                data_type=get_python_type(source_id.get("type", "int"))
            ),
            sink_id_property=EntityProperty(
                property_name=sink_id.get("name", "sink_id"),
                data_type=get_python_type(sink_id.get("type", "int"))
            ),
            properties=props,
        )
        provider.add_edge(
            edge_schema,
            SQLTableDescriptor(table_or_view_name=edge["tableName"])
        )

    return provider


def get_python_type(type_str: str):
    """Convert YAML type string to Python type."""
    type_map = {
        "int": int,
        "integer": int,
        "long": int,
        "float": float,
        "double": float,
        "string": str,
        "bool": bool,
        "boolean": bool,
    }
    return type_map.get(type_str.lower(), str)


def sanitize_filename(name: str) -> str:
    """Convert description to a valid filename."""
    # Remove special characters and replace spaces with underscores
    name = re.sub(r'[^\w\s-]', '', name.lower())
    name = re.sub(r'[\s]+', '_', name)
    return name[:60]  # Truncate to reasonable length


def transpile_query(query: str, provider: SimpleSQLSchemaProvider) -> dict:
    """Transpile a single query and return all artifacts."""
    result = {
        "ast": None,
        "ast_error": None,
        "logical_plan": None,
        "logical_plan_error": None,
        "sql": None,
        "sql_error": None,
        "success": True,
        "error_stage": None,
    }

    # Step 1: Parse to AST
    try:
        parser = OpenCypherParser()
        ast = parser.parse(query)
        result["ast"] = ast.dump_tree()
    except Exception as e:
        result["ast_error"] = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        result["success"] = False
        result["error_stage"] = "parser"
        return result

    # Step 2: Build Logical Plan
    try:
        plan = LogicalPlan.process_query_tree(ast, provider)
        # Apply optimizations (predicate pushdown, subquery flattening)
        optimize_plan(plan)
        result["logical_plan"] = plan.dump_graph()
    except Exception as e:
        result["logical_plan_error"] = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        result["success"] = False
        result["error_stage"] = "planner"
        return result

    # Step 3: Render SQL
    try:
        renderer = SQLRenderer(provider)
        sql = renderer.render_plan(plan)
        result["sql"] = sql
    except Exception as e:
        result["sql_error"] = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        result["success"] = False
        result["error_stage"] = "renderer"
        return result

    return result


def process_yaml_file(yaml_path: Path, out_dir: Path):
    """Process all examples in a YAML file."""
    print(f"\n{'='*80}")
    print(f"Processing: {yaml_path.name}")
    print(f"{'='*80}")

    with open(yaml_path, "r") as f:
        data = yaml.safe_load(f)

    # Load schema
    provider = load_yaml_schema(data)

    # Create output directory for this file
    file_out_dir = out_dir / yaml_path.stem
    file_out_dir.mkdir(parents=True, exist_ok=True)

    results = []
    examples = data.get("examples", [])

    for idx, example in enumerate(examples, 1):
        desc = example.get("description", f"query_{idx}")
        query = example.get("query", "").strip()
        app = example.get("application", "")
        notes = example.get("notes", "")

        query_id = f"{idx:02d}_{sanitize_filename(desc)}"
        query_dir = file_out_dir / query_id
        query_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n  [{idx:02d}] {desc[:60]}...")

        # Transpile
        result = transpile_query(query, provider)

        # Save metadata
        metadata = {
            "id": idx,
            "description": desc,
            "application": app,
            "query": query,
            "notes": notes,
            "success": result["success"],
            "error_stage": result["error_stage"],
        }
        with open(query_dir / "metadata.json", "w") as f:
            json.dump(metadata, f, indent=2)

        # Save AST
        if result["ast"]:
            with open(query_dir / "ast.txt", "w") as f:
                f.write(result["ast"])
            print(f"       ✓ AST generated")
        elif result["ast_error"]:
            with open(query_dir / "ast_error.txt", "w") as f:
                f.write(result["ast_error"])
            print(f"       ✗ AST failed")

        # Save Logical Plan
        if result["logical_plan"]:
            with open(query_dir / "logical_plan.txt", "w") as f:
                f.write(result["logical_plan"])
            print(f"       ✓ Logical plan generated")
        elif result["logical_plan_error"]:
            with open(query_dir / "logical_plan_error.txt", "w") as f:
                f.write(result["logical_plan_error"])
            print(f"       ✗ Logical plan failed")

        # Save SQL
        if result["sql"]:
            with open(query_dir / "databricks.sql", "w") as f:
                f.write(result["sql"])
            print(f"       ✓ SQL generated")
        elif result["sql_error"]:
            with open(query_dir / "sql_error.txt", "w") as f:
                f.write(result["sql_error"])
            print(f"       ✗ SQL failed")

        # Consolidate errors
        if not result["success"]:
            errors = []
            if result["ast_error"]:
                errors.append(f"=== AST Error ===\n{result['ast_error']}")
            if result["logical_plan_error"]:
                errors.append(f"=== Logical Plan Error ===\n{result['logical_plan_error']}")
            if result["sql_error"]:
                errors.append(f"=== SQL Error ===\n{result['sql_error']}")
            with open(query_dir / "error.txt", "w") as f:
                f.write("\n\n".join(errors))

        results.append({
            "id": idx,
            "description": desc,
            "application": app,
            "success": result["success"],
            "error_stage": result["error_stage"],
            "output_dir": str(query_dir),
        })

    # Save summary
    summary_path = file_out_dir / "summary.json"
    with open(summary_path, "w") as f:
        json.dump(results, f, indent=2)

    success_count = sum(1 for r in results if r["success"])
    print(f"\n  Summary: {success_count}/{len(results)} queries successful")

    return results


def main():
    examples_dir = Path(__file__).parent
    out_dir = examples_dir / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    yaml_files = list(examples_dir.glob("*_queries.yaml"))

    all_results = {}
    for yaml_path in sorted(yaml_files):
        results = process_yaml_file(yaml_path, out_dir)
        all_results[yaml_path.stem] = results

    # Save global summary
    global_summary = {
        "total_files": len(yaml_files),
        "files": list(all_results.keys()),
        "results": all_results,
        "totals": {
            "total_queries": sum(len(r) for r in all_results.values()),
            "successful": sum(
                sum(1 for q in r if q["success"])
                for r in all_results.values()
            ),
            "failed": sum(
                sum(1 for q in r if not q["success"])
                for r in all_results.values()
            ),
        }
    }

    with open(out_dir / "global_summary.json", "w") as f:
        json.dump(global_summary, f, indent=2)

    print(f"\n{'='*80}")
    print("GLOBAL SUMMARY")
    print(f"{'='*80}")
    print(f"Total YAML files: {global_summary['totals']['total_queries']}")
    print(f"Successful: {global_summary['totals']['successful']}")
    print(f"Failed: {global_summary['totals']['failed']}")
    print(f"\nArtifacts saved to: {out_dir}")


if __name__ == "__main__":
    main()
