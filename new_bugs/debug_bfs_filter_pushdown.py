"""Debug script for BFS filter pushdown optimization.

This script allows iterative testing of the transpiler to verify
that filters from previous MATCH clauses are pushed into the CTE base case.
"""

from __future__ import annotations

import re
import sys

from gsql2rsql import LogicalPlan, OpenCypherParser, SQLRenderer
from gsql2rsql.common.schema import EdgeSchema, EntityProperty, NodeSchema
from gsql2rsql.planner.subquery_optimizer import optimize_plan
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor


def create_test_schema() -> SimpleSQLSchemaProvider:
    """Create test schema provider with Person nodes and KNOWS edges."""
    provider = SimpleSQLSchemaProvider()

    # Add Person node
    person_props = [
        EntityProperty("id", "string"),
        EntityProperty("name", "string"),
        EntityProperty("age", "int"),
    ]
    person_schema = NodeSchema(
        name="Person",
        node_id_property=EntityProperty("id", "string"),
        properties=person_props,
    )
    provider.add_node(
        person_schema,
        SQLTableDescriptor("Person", "graph.Person"),
    )

    # Add KNOWS edge
    edge_props = [
        EntityProperty("src", "string"),
        EntityProperty("dst", "string"),
        EntityProperty("since", "int"),
    ]
    edge_schema = EdgeSchema(
        name="KNOWS",
        source_node_id="Person",
        sink_node_id="Person",
        source_id_property=EntityProperty("src", "string"),
        sink_id_property=EntityProperty("dst", "string"),
        properties=edge_props,
    )
    provider.add_edge(
        edge_schema,
        SQLTableDescriptor(edge_schema.id, "graph.Knows"),
    )

    return provider


def transpile_query(query: str, provider: SimpleSQLSchemaProvider) -> str:
    """Transpile a query using the full pipeline."""
    parser = OpenCypherParser()
    ast = parser.parse(query)
    plan = LogicalPlan.process_query_tree(ast, provider)
    optimize_plan(plan)
    plan.resolve(query)
    renderer = SQLRenderer(provider)
    return renderer.render_plan(plan)


def get_cte_base_case(sql: str) -> str | None:
    """Extract the CTE base case from SQL.

    The base case is the first SELECT in the WITH RECURSIVE ... AS (...).
    Everything from the first SELECT up to UNION ALL is the base case.
    """
    # Find the CTE definition - capture everything from first SELECT to UNION ALL
    cte_match = re.search(
        r"WITH RECURSIVE\s+\w+\s+AS\s*\(\s*(.*?)\s*UNION ALL",
        sql,
        re.DOTALL | re.IGNORECASE,
    )
    if cte_match:
        return cte_match.group(1).strip()
    return None


def check_filter_in_base_case(sql: str, filter_value: str) -> bool:
    """Check if a filter value appears in the CTE base case."""
    base_case = get_cte_base_case(sql)
    if base_case is None:
        print("  WARNING: No CTE base case found!")
        return False
    return filter_value in base_case


def run_test(name: str, query: str, expected_filter: str) -> bool:
    """Run a single test case."""
    print(f"\n{'='*60}")
    print(f"TEST: {name}")
    print(f"{'='*60}")
    print(f"Query:\n{query.strip()}")
    print()

    provider = create_test_schema()
    try:
        sql = transpile_query(query, provider)
    except Exception as e:
        import traceback
        print(f"  ERROR: Transpilation failed: {e}")
        traceback.print_exc()
        return False

    print(f"Generated SQL:\n{sql}")
    print()

    # Extract and show base case
    base_case = get_cte_base_case(sql)
    if base_case:
        print(f"CTE Base Case:\n{base_case}")
        print()

    # Check if filter is in base case
    filter_in_base = check_filter_in_base_case(sql, expected_filter)

    if filter_in_base:
        print(f"  PASS: Filter '{expected_filter}' found in CTE base case")
        return True
    else:
        print(f"  FAIL: Filter '{expected_filter}' NOT found in CTE base case")
        # Check if filter exists anywhere in the SQL
        if expected_filter in sql:
            print("  NOTE: Filter exists in SQL but AFTER the CTE (inefficient)")
        return False


def main() -> None:
    """Run all test cases."""
    print("BFS Filter Pushdown Debug Script")
    print("=" * 60)

    results: list[tuple[str, bool]] = []

    # Test 1: Two MATCH clauses with inline property filter
    # This is the main bug case
    results.append((
        "Two MATCH - Inline Property",
        run_test(
            "Two MATCH clauses with inline property filter",
            """
            MATCH (root:Person { id: "specific-id-123" })
            MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
            RETURN target.name
            """,
            "specific-id-123",
        ),
    ))

    # Test 2: Two MATCH clauses with WHERE filter
    results.append((
        "Two MATCH - WHERE clause",
        run_test(
            "Two MATCH clauses with WHERE filter",
            """
            MATCH (root:Person)
            WHERE root.id = "where-filter-456"
            MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
            RETURN target.name
            """,
            "where-filter-456",
        ),
    ))

    # Test 3: Single MATCH clause with inline filter (regression test)
    # This should already work
    results.append((
        "Single MATCH - Inline (regression)",
        run_test(
            "Single MATCH with inline property (regression test)",
            """
            MATCH p = (root:Person { id: "single-inline-789" })-[:KNOWS*1..3]->(target:Person)
            RETURN target.name
            """,
            "single-inline-789",
        ),
    ))

    # Test 4: Single MATCH clause with WHERE filter (regression test)
    results.append((
        "Single MATCH - WHERE (regression)",
        run_test(
            "Single MATCH with WHERE filter (regression test)",
            """
            MATCH p = (root:Person)-[:KNOWS*1..3]->(target:Person)
            WHERE root.id = "single-where-000"
            RETURN target.name
            """,
            "single-where-000",
        ),
    ))

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, p in results if p)
    total = len(results)

    for name, passed_test in results:
        status = "PASS" if passed_test else "FAIL"
        print(f"  [{status}] {name}")

    print()
    print(f"Passed: {passed}/{total}")

    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
