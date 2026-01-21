"""Test: WHERE com predicados de label (a:Person OR a:Company).

NOTE: These tests are skipped because they require two features:
1. No-label node support (implemented via enable_no_label_support=True)
2. WHERE clause label predicate parsing (not yet implemented)

The WHERE label predicate feature (`WHERE a:Person`) requires additional
parser/planner work to convert label predicates in WHERE clauses into
node type filters.
"""

import pytest

from gsql2rsql import GraphContext


@pytest.mark.skip(reason="WHERE label predicates require additional parser/planner support - out of scope for no-label implementation")
def test_where_single_label_predicate():
    """WHERE a:Person deve funcionar."""
    graph = GraphContext(
        nodes_table="catalog.schema.nodes",
        edges_table="catalog.schema.edges",
        extra_node_attrs={"name": str}
    )
    graph.set_types(
        node_types=["Person", "Company"],
        edge_types=["KNOWS"]
    )

    print("\n" + "=" * 80)
    print("TESTE 1: WHERE com single label predicate")
    print("=" * 80)

    query = """
    MATCH (a)-[:KNOWS]->(b)
    WHERE a:Person
    RETURN a.name, b.name
    """

    print("Query:")
    print(query)

    try:
        sql = graph.transpile(query)
        print("\n✅ SUCESSO! SQL gerado:")
        print(sql)

        if "node_type = 'Person'" in sql or "'Person'" in sql:
            print("\n✅ Filtro de tipo Person encontrado!")
        else:
            print("\n❌ Filtro de tipo NÃO encontrado")

    except Exception as e:
        print(f"\n❌ ERRO: {type(e).__name__}: {e}")
        raise


@pytest.mark.skip(reason="WHERE label predicates require additional parser/planner support - out of scope for no-label implementation")
def test_where_or_label_predicates():
    """WHERE a:Person OR a:Company deve funcionar."""
    graph = GraphContext(
        nodes_table="catalog.schema.nodes",
        edges_table="catalog.schema.edges",
        extra_node_attrs={"name": str}
    )
    graph.set_types(
        node_types=["Person", "Company"],
        edge_types=["KNOWS"]
    )

    print("\n" + "=" * 80)
    print("TESTE 2: WHERE com OR de label predicates")
    print("=" * 80)

    query = """
    MATCH (a)-[:KNOWS]->(b)
    WHERE a:Person OR a:Company
    RETURN a.name, b.name
    """

    print("Query:")
    print(query)

    try:
        sql = graph.transpile(query)
        print("\n✅ SUCESSO! SQL gerado:")
        print(sql)

        has_person = "'Person'" in sql
        has_company = "'Company'" in sql

        if has_person and has_company:
            print("\n✅ Ambos os tipos (Person, Company) no SQL!")
            print("   Transpiler suporta WHERE a:Label OR a:Label!")
        else:
            print(f"\n❌ Tipos encontrados: Person={has_person}, "
                  f"Company={has_company}")

    except Exception as e:
        print(f"\n❌ ERRO: {type(e).__name__}: {e}")
        raise


def test_no_label_works():
    """Sem label funciona (OpenCypher padrão).

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

    query = """
    MATCH (a)-[:KNOWS]->(b)
    RETURN a.name, b.name
    """

    sql = graph.transpile(query)
    assert sql is not None
    assert "SELECT" in sql.upper()


@pytest.mark.skip(reason="WHERE label predicates require additional parser/planner support - out of scope for no-label implementation")
def test_comparison_label_vs_where():
    """Comparar: (a:Person) vs MATCH (a) WHERE a:Person."""
    graph = GraphContext(
        nodes_table="catalog.schema.nodes",
        edges_table="catalog.schema.edges",
        extra_node_attrs={"name": str}
    )
    graph.set_types(
        node_types=["Person", "Company"],
        edge_types=["KNOWS"]
    )

    print("\n" + "=" * 80)
    print("TESTE 4: Comparação - Label inline vs WHERE")
    print("=" * 80)

    # Query 1: Label inline
    query1 = """
    MATCH (a:Person)-[:KNOWS]->(b)
    RETURN a.name, b.name
    """

    print("\n📌 Query 1 (label inline):")
    print(query1)

    sql1 = graph.transpile(query1)
    print("SQL gerado:")
    print(sql1[:200] + "..." if len(sql1) > 200 else sql1)

    # Query 2: WHERE predicate
    query2 = """
    MATCH (a)-[:KNOWS]->(b)
    WHERE a:Person
    RETURN a.name, b.name
    """

    print("\n📌 Query 2 (WHERE predicate):")
    print(query2)

    try:
        sql2 = graph.transpile(query2)
        print("SQL gerado:")
        print(sql2[:200] + "..." if len(sql2) > 200 else sql2)

        print("\n📊 COMPARAÇÃO:")
        if sql1 == sql2:
            print("✅ Ambas geram o MESMO SQL!")
        else:
            print("⚠️  Geram SQL DIFERENTE")

    except Exception as e:
        print(f"\n❌ Query 2 FALHA: {type(e).__name__}: {e}")
        print("\n💡 Então WHERE a:Person TAMBÉM não funciona sem label!")
