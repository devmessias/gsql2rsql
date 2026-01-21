#!/usr/bin/env python3
"""Generate example SQL for documentation.

This script generates the SQL output for the index.md example,
ensuring the documentation stays in sync with actual transpiler output.
"""

from gsql2rsql import GraphContext


def main():
    # Create GraphContext (same as in docs)
    graph = GraphContext(
        nodes_table="catalog.fraud.nodes",
        edges_table="catalog.fraud.edges",
        node_id_col="id",
        extra_node_attrs={"name": str, "risk_score": float},
        extra_edge_attrs={"amount": float, "timestamp": str},
    )

    graph.set_types(
        node_types=["Person", "Account", "Merchant"],
        edge_types=["TRANSACTION", "OWNS", "LOCATED_AT"],
    )

    # Query from docs
    query = """
    MATCH path = (origin:Person {id: 12345})-[:TRANSACTION*1..4]->(dest:Person)
    WHERE dest.risk_score > 0.8
    RETURN dest.id, dest.name, dest.risk_score, length(path) AS depth
    ORDER BY depth, dest.risk_score DESC
    LIMIT 100
    """

    sql = graph.transpile(query)

    print("=" * 80)
    print("GENERATED SQL (for docs/index.md collapsible)")
    print("=" * 80)
    print()
    print(sql)
    print()
    print("=" * 80)

    # Also generate simpler example for user guide
    simple_query = """
    MATCH (p:Person)-[:TRANSACTION]->(m:Merchant)
    WHERE m.risk_score > 0.5
    RETURN p.name, m.name AS merchant, m.risk_score
    ORDER BY m.risk_score DESC
    LIMIT 10
    """

    print()
    print("=" * 80)
    print("SIMPLE QUERY SQL (for user guide)")
    print("=" * 80)
    print()
    simple_sql = graph.transpile(simple_query)
    print(simple_sql)


if __name__ == "__main__":
    main()
