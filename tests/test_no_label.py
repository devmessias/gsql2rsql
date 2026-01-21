"""Test no-label node support in GraphContext (Triple Store)."""

from gsql2rsql import GraphContext


def test_no_label_after_with_works():
    """Nó SEM label funciona quando já foi definido em WITH anterior."""
    graph = GraphContext(
        nodes_table="catalog.schema.nodes",
        edges_table="catalog.schema.edges",
        extra_node_attrs={"name": str, "age": int}
    )
    graph.set_types(
        node_types=["Person", "Company"],
        edge_types=["WORKS_AT", "KNOWS"]
    )

    # ✅ FUNCIONA: Primeiro define o tipo com label, depois usa sem label
    query = """
    MATCH (a:Person)-[:WORKS_AT]->(b:Company)
    WITH a, COUNT(b) AS company_count
    MATCH (a)-[:KNOWS]->(friend:Person)
    RETURN a.name, company_count, friend.name
    """

    sql = graph.transpile(query)
    assert sql is not None
    assert "SELECT" in sql.upper()


def test_no_label_in_first_match_works():
    """Nó SEM label funciona em MATCH inicial (OpenCypher padrão).

    GraphContext (Triple Store) sempre habilita no-label support.
    """
    graph = GraphContext(
        nodes_table="catalog.schema.nodes",
        edges_table="catalog.schema.edges",
        extra_node_attrs={"name": str}
    )
    graph.set_types(
        node_types=["Person", "Company"],
        edge_types=["WORKS_AT"]
    )

    # Funciona: MATCH sem label gera SQL sem filtro de tipo
    query = """
    MATCH (a)-[:WORKS_AT]->(c:Company)
    RETURN a.name, c.name
    """

    sql = graph.transpile(query)
    assert sql is not None
    assert "SELECT" in sql.upper()
    # Company tem filtro, 'a' não tem
    assert "node_type = 'Company'" in sql


def test_with_label_generates_type_filter():
    """Quando especifica label, deve gerar filtro WHERE node_type = '...'"""
    graph = GraphContext(
        nodes_table="catalog.schema.nodes",
        edges_table="catalog.schema.edges",
        extra_node_attrs={"name": str}
    )
    graph.set_types(
        node_types=["Person", "Company"],
        edge_types=["WORKS_AT"]
    )

    # COM label → gera filtro de tipo
    query = """
    MATCH (a:Person)-[:WORKS_AT]->(c:Company)
    RETURN a.name, c.name
    """

    sql = graph.transpile(query)

    # Deve ter filtros para ambos os tipos
    assert "node_type = 'Person'" in sql
    assert "node_type = 'Company'" in sql

    print("\n" + "=" * 80)
    print("SQL gerado COM labels (comportamento atual correto):")
    print("=" * 80)
    print(sql)
    print("\n✅ Filtros de tipo presentes: node_type = 'Person' e node_type = 'Company'")
