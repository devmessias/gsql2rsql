"""MkDocs macros for dynamic SQL generation in documentation.

This module provides macros that generate SQL examples dynamically,
ensuring documentation always matches the actual transpiler output.

Functions are defined at module level so they can be imported by other
scripts (e.g., generate_readme.py) and also registered as mkdocs macros.
"""

from __future__ import annotations

from textwrap import dedent


def transpile_cypher(
    cypher_query: str,
    nodes_table: str = "catalog.fraud.nodes",
    edges_table: str = "catalog.fraud.edges",
    node_types: list[str] | None = None,
    edge_types: list[str] | None = None,
    node_id_col: str = "id",
    edge_src_col: str = "src",
    edge_dst_col: str = "dst",
    node_type_col: str = "type",
    edge_type_col: str = "relationship_type",
    extra_node_attrs: dict[str, str] | None = None,
    extra_edge_attrs: dict[str, str] | None = None,
) -> str:
    """Transpile a Cypher query to SQL and return formatted output.

    Args:
        cypher_query: The OpenCypher query to transpile
        nodes_table: Fully qualified nodes table name
        edges_table: Fully qualified edges table name
        node_types: List of node type names
        edge_types: List of edge type names
        node_id_col: Column name for node ID
        edge_src_col: Column name for edge source
        edge_dst_col: Column name for edge destination
        node_type_col: Column name for node type
        edge_type_col: Column name for edge type
        extra_node_attrs: Additional node properties {name: type}
        extra_edge_attrs: Additional edge properties {name: type}

    Returns:
        Formatted SQL string
    """
    from gsql2rsql import GraphContext

    # Default types if not provided
    if node_types is None:
        node_types = ["Person"]
    if edge_types is None:
        edge_types = ["TRANSACTION"]

    # Convert string type names to Python types
    type_map = {"str": str, "int": int, "float": float, "bool": bool}

    node_attrs = {}
    if extra_node_attrs:
        for name, type_str in extra_node_attrs.items():
            node_attrs[name] = type_map.get(type_str, str)

    edge_attrs = {}
    if extra_edge_attrs:
        for name, type_str in extra_edge_attrs.items():
            edge_attrs[name] = type_map.get(type_str, str)

    # Create GraphContext
    graph = GraphContext(
        spark=None,
        nodes_table=nodes_table,
        edges_table=edges_table,
        node_type_col=node_type_col,
        edge_type_col=edge_type_col,
        node_id_col=node_id_col,
        edge_src_col=edge_src_col,
        edge_dst_col=edge_dst_col,
        extra_node_attrs=node_attrs,
        extra_edge_attrs=edge_attrs,
    )
    graph.set_types(node_types=node_types, edge_types=edge_types)

    # Transpile
    sql = graph.transpile(dedent(cypher_query).strip())

    return sql


def fraud_example_sql(indent: int = 0, include_fence: bool = False) -> str:
    """Generate the main fraud detection example SQL.

    This is the example shown on the homepage.

    Args:
        indent: Number of spaces to indent each line (for mkdocs collapsible blocks)
        include_fence: If True, include ```sql``` code fence markers
    """
    cypher = """
        MATCH path = (origin:Person {id: 12345})-[:TRANSACTION*1..4]->(dest:Person)
        WHERE dest.risk_score > 0.8
        RETURN dest.id, dest.name, dest.risk_score, length(path) AS depth
        ORDER BY depth, dest.risk_score DESC
        LIMIT 3
    """

    sql = transpile_cypher(
        cypher_query=cypher,
        nodes_table="catalog.fraud.nodes",
        edges_table="catalog.fraud.edges",
        node_types=["Person"],
        edge_types=["TRANSACTION"],
        extra_node_attrs={"name": "str", "risk_score": "float"},
        extra_edge_attrs={"amount": "float", "timestamp": "str"},
    )

    if include_fence:
        sql = f"```sql\n{sql}\n```"

    if indent > 0:
        prefix = " " * indent
        sql = "\n".join(prefix + line if line else line for line in sql.split("\n"))

    return sql


def simple_match_sql() -> str:
    """Generate a simple MATCH example."""
    cypher = """
        MATCH (p:Person)-[:KNOWS]->(f:Person)
        RETURN p.name, f.name
    """

    return transpile_cypher(
        cypher_query=cypher,
        nodes_table="people",
        edges_table="friendships",
        node_types=["Person"],
        edge_types=["KNOWS"],
        extra_node_attrs={"name": "str"},
    )


def define_env(env):
    """Register macros for mkdocs-macros plugin."""
    env.macro(transpile_cypher, "transpile_cypher")
    env.macro(fraud_example_sql, "fraud_example_sql")
    env.macro(simple_match_sql, "simple_match_sql")
