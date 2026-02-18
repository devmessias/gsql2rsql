"""Test: Verificar se caminhos variáveis geram filtros de tipo de nó.

NOTE: Some tests are skipped because they expose a pre-existing gap in VLP
rendering where type filters are not applied to source/sink nodes even when
labels are specified. This is a separate issue from no-label support.
"""

from gsql2rsql import GraphContext


def test_variable_path_with_labels_generates_type_filters():
    """Variable-length path with labels must generate type filters on source/sink JOIN."""
    graph = GraphContext(
        nodes_table="catalog.schema.nodes",
        edges_table="catalog.schema.edges",
        extra_node_attrs={"name": str, "age": int}
    )
    graph.set_types(
        node_types=["Person", "Company", "Device"],
        edge_types=["KNOWS", "WORKS_AT"]
    )

    query = """
    MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person)
    RETURN a.name, b.name, length(path) AS depth
    """

    sql = graph.transpile(query)

    # Type filters must be present in the final JOIN WHERE clause
    assert "node_type = 'Person'" in sql, \
        "Type filter 'Person' not found in generated SQL"

    # At least source + sink filters in the final SELECT
    person_filter_count = sql.count("node_type = 'Person'")
    assert person_filter_count >= 2, \
        f"Expected >= 2 Person type filters (source + sink), found {person_filter_count}"


def test_variable_path_shows_filter_locations():
    """Mostrar ONDE os filtros de tipo aparecem no SQL gerado."""
    graph = GraphContext(
        nodes_table="catalog.schema.nodes",
        edges_table="catalog.schema.edges",
        extra_node_attrs={"name": str}
    )
    graph.set_types(
        node_types=["Person", "Company"],
        edge_types=["KNOWS"]
    )

    query = """
    MATCH path = (a:Person)-[:KNOWS*1..2]->(b:Person)
    RETURN a.name, b.name
    """

    sql = graph.transpile(query)

    print("\n" + "=" * 80)
    print("ANÁLISE: Localização dos filtros no SQL")
    print("=" * 80)

    lines = sql.split('\n')
    for i, line in enumerate(lines, 1):
        if "node_type = 'Person'" in line:
            print(f"Linha {i:3d}: {line.strip()}")
        elif "relationship_type = 'KNOWS'" in line:
            print(f"Linha {i:3d}: {line.strip()}")
        elif "-- Base case" in line:
            print(f"\nLinha {i:3d}: *** BASE CASE ***")
        elif "UNION ALL" in line:
            print(f"\nLinha {i:3d}: *** UNION ALL (início do recursive case) ***")
        elif "-- Recursive case" in line:
            print(f"Linha {i:3d}: *** RECURSIVE CASE ***\n")

    print("\n" + "=" * 80)


def test_variable_path_with_one_untyped_node():
    """Variable-length path com UM nó sem label funciona (OpenCypher padrão).

    GraphContext (Triple Store) sempre habilita no-label support.
    """
    graph = GraphContext(
        nodes_table="catalog.schema.nodes",
        edges_table="catalog.schema.edges",
        extra_node_attrs={"name": str}
    )
    graph.set_types(
        node_types=["Person", "Company"],
        edge_types=["KNOWS"]
    )

    # Origem COM label, destino SEM label
    query = """
    MATCH path = (a:Person)-[:KNOWS*1..2]->(b)
    RETURN a.name, b.name
    """

    sql = graph.transpile(query)
    assert sql is not None
    assert "WITH RECURSIVE" in sql.upper()


def test_undirected_variable_path_type_filters():
    """Undirected variable-length path must generate type filters for both labels."""
    graph = GraphContext(
        nodes_table="catalog.schema.nodes",
        edges_table="catalog.schema.edges",
        extra_node_attrs={"name": str}
    )
    graph.set_types(
        node_types=["Person", "Device"],
        edge_types=["CONNECTED"]
    )

    query = """
    MATCH path = (a:Person)-[:CONNECTED*1..2]-(b:Device)
    RETURN a.name, b.name, length(path) AS depth
    """

    sql = graph.transpile(query)

    assert "node_type = 'Person'" in sql
    assert "node_type = 'Device'" in sql
    assert "relationship_type = 'CONNECTED'" in sql
