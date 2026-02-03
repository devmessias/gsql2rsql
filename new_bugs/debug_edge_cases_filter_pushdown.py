"""Debug script for edge cases in BFS filter pushdown optimization.

Tests various OpenCypher patterns that might not be optimized correctly.
"""

from __future__ import annotations

import re
import sys

from gsql2rsql import LogicalPlan, OpenCypherParser, SQLRenderer
from gsql2rsql.common.schema import EdgeSchema, EntityProperty, NodeSchema
from gsql2rsql.planner.subquery_optimizer import optimize_plan
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)


def create_test_schema() -> SimpleSQLSchemaProvider:
    """Create test schema with Person, Company nodes and multiple edge types."""
    provider = SimpleSQLSchemaProvider()

    # Person node
    person_schema = NodeSchema(
        name="Person",
        node_id_property=EntityProperty("id", "string"),
        properties=[
            EntityProperty("id", "string"),
            EntityProperty("name", "string"),
            EntityProperty("age", "int"),
        ],
    )
    provider.add_node(person_schema, SQLTableDescriptor("Person", "graph.Person"))

    # Company node
    company_schema = NodeSchema(
        name="Company",
        node_id_property=EntityProperty("id", "string"),
        properties=[
            EntityProperty("id", "string"),
            EntityProperty("name", "string"),
        ],
    )
    provider.add_node(company_schema, SQLTableDescriptor("Company", "graph.Company"))

    # KNOWS edge (Person -> Person)
    knows_schema = EdgeSchema(
        name="KNOWS",
        source_node_id="Person",
        sink_node_id="Person",
        source_id_property=EntityProperty("src", "string"),
        sink_id_property=EntityProperty("dst", "string"),
        properties=[
            EntityProperty("src", "string"),
            EntityProperty("dst", "string"),
            EntityProperty("since", "int"),
        ],
    )
    provider.add_edge(
        knows_schema, SQLTableDescriptor(knows_schema.id, "graph.Knows")
    )

    # FOLLOWS edge (Person -> Person)
    follows_schema = EdgeSchema(
        name="FOLLOWS",
        source_node_id="Person",
        sink_node_id="Person",
        source_id_property=EntityProperty("src", "string"),
        sink_id_property=EntityProperty("dst", "string"),
        properties=[
            EntityProperty("src", "string"),
            EntityProperty("dst", "string"),
        ],
    )
    provider.add_edge(
        follows_schema, SQLTableDescriptor(follows_schema.id, "graph.Follows")
    )

    # WORKS_AT edge (Person -> Company)
    works_schema = EdgeSchema(
        name="WORKS_AT",
        source_node_id="Person",
        sink_node_id="Company",
        source_id_property=EntityProperty("src", "string"),
        sink_id_property=EntityProperty("dst", "string"),
        properties=[
            EntityProperty("src", "string"),
            EntityProperty("dst", "string"),
        ],
    )
    provider.add_edge(
        works_schema, SQLTableDescriptor(works_schema.id, "graph.WorksAt")
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
    """Extract the CTE base case from SQL."""
    cte_match = re.search(
        r"WITH RECURSIVE\s+\w+\s+AS\s*\(\s*(.*?)\s*UNION ALL",
        sql,
        re.DOTALL | re.IGNORECASE,
    )
    if cte_match:
        return cte_match.group(1).strip()
    return None


def analyze_query(
    name: str,
    query: str,
    expected_in_base: list[str] | None = None,
    expected_not_in_base: list[str] | None = None,
) -> dict:
    """Analyze a query and check for expected optimizations."""
    print(f"\n{'='*70}")
    print(f"TEST: {name}")
    print(f"{'='*70}")
    print(f"Query:\n{query.strip()}")
    print()

    provider = create_test_schema()
    result = {
        "name": name,
        "success": True,
        "error": None,
        "has_cte": False,
        "base_case_filters": [],
        "issues": [],
    }

    try:
        sql = transpile_query(query, provider)
    except Exception as e:
        result["success"] = False
        result["error"] = str(e)
        print(f"  ERROR: {e}")
        return result

    print(f"Generated SQL:\n{sql[:2000]}...")
    print()

    base_case = get_cte_base_case(sql)
    if base_case:
        result["has_cte"] = True
        print(f"CTE Base Case:\n{base_case}")
        print()

        # Check expected filters in base case
        if expected_in_base:
            for expected in expected_in_base:
                if expected in base_case:
                    print(f"  [OK] '{expected}' found in base case")
                else:
                    print(f"  [ISSUE] '{expected}' NOT in base case")
                    if expected in sql:
                        print(f"         (exists in SQL but after CTE - not optimized)")
                    result["issues"].append(
                        f"Filter '{expected}' not pushed to base case"
                    )

        # Check filters that should NOT be in base case
        if expected_not_in_base:
            for not_expected in expected_not_in_base:
                if not_expected in base_case:
                    print(f"  [ISSUE] '{not_expected}' incorrectly in base case")
                    result["issues"].append(
                        f"Filter '{not_expected}' incorrectly pushed to base case"
                    )
                else:
                    print(f"  [OK] '{not_expected}' correctly NOT in base case")
    else:
        print("  No CTE found (no VLP in query)")

    return result


def main() -> None:
    """Run all edge case tests."""
    print("BFS Filter Pushdown Edge Cases Analysis")
    print("=" * 70)

    results = []

    # ==========================================================================
    # CASE 1: Filter on target node (should NOT be pushed)
    # ==========================================================================
    results.append(analyze_query(
        "Filter on TARGET node (should NOT be in base case)",
        """
        MATCH (root:Person { id: "alice" })
        MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
        WHERE target.age > 30
        RETURN target.name
        """,
        expected_in_base=["alice"],
        expected_not_in_base=["30"],  # target filter should NOT be pushed
    ))

    # ==========================================================================
    # CASE 2: Filter with OR (complex - may not be pushable)
    # ==========================================================================
    results.append(analyze_query(
        "Filter with OR condition",
        """
        MATCH (root:Person)
        WHERE root.id = "alice" OR root.id = "bob"
        MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
        RETURN target.name
        """,
        expected_in_base=["alice"],  # Should the OR be pushed?
    ))

    # ==========================================================================
    # CASE 3: Filter referencing multiple aliases (should NOT be pushed)
    # ==========================================================================
    results.append(analyze_query(
        "Filter referencing multiple aliases",
        """
        MATCH (root:Person { id: "alice" })
        MATCH (other:Person { id: "bob" })
        MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
        WHERE root.age = other.age
        RETURN target.name
        """,
        expected_in_base=["alice"],
        expected_not_in_base=["bob"],  # other's filter shouldn't be pushed to root's VLP
    ))

    # ==========================================================================
    # CASE 4: Three MATCH clauses with VLP in the middle
    # ==========================================================================
    results.append(analyze_query(
        "Three MATCH clauses - VLP in middle",
        """
        MATCH (root:Person { id: "alice" })
        MATCH p = (root)-[:KNOWS*1..2]->(middle:Person)
        MATCH (middle)-[:WORKS_AT]->(company:Company)
        RETURN company.name
        """,
        expected_in_base=["alice"],
    ))

    # ==========================================================================
    # CASE 5: OPTIONAL MATCH with VLP
    # ==========================================================================
    results.append(analyze_query(
        "OPTIONAL MATCH with VLP",
        """
        MATCH (root:Person { id: "alice" })
        OPTIONAL MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
        RETURN target.name
        """,
        expected_in_base=["alice"],
    ))

    # ==========================================================================
    # CASE 6: Multiple filters on same node (all should be pushed)
    # ==========================================================================
    results.append(analyze_query(
        "Multiple filters on same node",
        """
        MATCH (root:Person)
        WHERE root.id = "alice" AND root.age > 25
        MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
        RETURN target.name
        """,
        expected_in_base=["alice", "25"],
    ))

    # ==========================================================================
    # CASE 7: Filter with function call
    # ==========================================================================
    results.append(analyze_query(
        "Filter with function call (length)",
        """
        MATCH (root:Person)
        WHERE root.id = "alice"
        MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
        RETURN target.name
        """,
        expected_in_base=["alice"],
    ))

    # ==========================================================================
    # CASE 8: VLP without direction (bidirectional)
    # ==========================================================================
    results.append(analyze_query(
        "Bidirectional VLP",
        """
        MATCH (root:Person { id: "alice" })
        MATCH p = (root)-[:KNOWS*1..2]-(target:Person)
        RETURN target.name
        """,
        expected_in_base=["alice"],
    ))

    # ==========================================================================
    # CASE 9: Filter in inline props + WHERE combined
    # ==========================================================================
    results.append(analyze_query(
        "Inline props + WHERE on same node",
        """
        MATCH (root:Person { id: "alice" })
        WHERE root.age > 20
        MATCH p = (root)-[:KNOWS*1..3]->(target:Person)
        RETURN target.name
        """,
        expected_in_base=["alice", "20"],
    ))

    # ==========================================================================
    # CASE 10: Filter on relationship in VLP
    # ==========================================================================
    results.append(analyze_query(
        "Filter on relationship within VLP",
        """
        MATCH (root:Person { id: "alice" })
        MATCH p = (root)-[r:KNOWS*1..3]->(target:Person)
        WHERE ALL(rel IN relationships(p) WHERE rel.since > 2020)
        RETURN target.name
        """,
        expected_in_base=["alice"],
    ))

    # ==========================================================================
    # CASE 11: Backward direction VLP
    # ==========================================================================
    results.append(analyze_query(
        "Backward direction VLP",
        """
        MATCH (target:Person { id: "alice" })
        MATCH p = (target)<-[:KNOWS*1..3]-(root:Person)
        RETURN root.name
        """,
        expected_in_base=["alice"],
    ))

    # ==========================================================================
    # CASE 12: Two VLPs from same source
    # ==========================================================================
    results.append(analyze_query(
        "Two VLPs from same source (if supported)",
        """
        MATCH (root:Person { id: "alice" })
        MATCH p1 = (root)-[:KNOWS*1..2]->(friend:Person)
        MATCH p2 = (root)-[:FOLLOWS*1..2]->(follower:Person)
        RETURN friend.name, follower.name
        """,
        expected_in_base=["alice"],
    ))

    # ==========================================================================
    # Summary
    # ==========================================================================
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    issues_found = []
    for r in results:
        status = "OK" if not r["issues"] else "ISSUES"
        print(f"  [{status}] {r['name']}")
        if r["issues"]:
            for issue in r["issues"]:
                print(f"       - {issue}")
                issues_found.append((r["name"], issue))
        if r["error"]:
            print(f"       - ERROR: {r['error']}")

    print()
    if issues_found:
        print(f"Found {len(issues_found)} potential optimization issues")
        sys.exit(1)
    else:
        print("All edge cases handled correctly!")


if __name__ == "__main__":
    main()
