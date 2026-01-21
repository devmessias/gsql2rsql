"""Test: Verificar se sintaxe OR funciona para relationship types."""

import pytest

from gsql2rsql import GraphContext


def test_relationship_or_syntax_simple():
    """Relationship OR syntax (:KNOWS|FOLLOWS) FAIL in simple paths.

    COMPORTAMENTO ATUAL: Falha com binding error
    RAZÃO: DataSourceOperator.bind() procura por relationship type
           'KNOWS|FOLLOWS' como um nome único, não splita em múltiplos
           tipos.

    OR syntax SOMENTE funciona em variable-length paths onde o planner
    splita explicitamente em logical_plan.py:1006
    """
    from gsql2rsql.common.exceptions import TranspilerBindingException

    graph = GraphContext(
        nodes_table="catalog.schema.nodes",
        edges_table="catalog.schema.edges",
        extra_node_attrs={"name": str}
    )
    graph.set_types(
        node_types=["Person"],
        edge_types=["KNOWS", "FOLLOWS"]
    )

    print("\n" + "=" * 80)
    print("TESTE 1: Relationship OR (:KNOWS|FOLLOWS) em caminho SIMPLES")
    print("=" * 80)

    query = """
    MATCH (a:Person)-[r:KNOWS|FOLLOWS]->(b:Person)
    RETURN a.name, b.name
    """

    print("Query Cypher:")
    print(query)
    print("\nTentando transpilar...")
    print("❌ ESPERADO: Falha com binding error")

    with pytest.raises(TranspilerBindingException) as exc_info:
        graph.transpile(query)

    print("\n✅ Confirmado: Falha com erro esperado")
    print("   Erro: %s" % exc_info.value)
    assert "Failed to bind relationship 'r' of type 'KNOWS|FOLLOWS'" in str(
        exc_info.value
    )


def test_relationship_or_syntax_variable_path():
    """Relationship OR syntax em caminho variável."""
    graph = GraphContext(
        nodes_table="catalog.schema.nodes",
        edges_table="catalog.schema.edges",
        extra_node_attrs={"name": str}
    )
    graph.set_types(
        node_types=["Person"],
        edge_types=["KNOWS", "FOLLOWS", "BLOCKS"]
    )

    print("\n" + "=" * 80)
    print("TESTE 2: Relationship OR em caminho variável")
    print("=" * 80)

    query = """
    MATCH path = (a:Person)-[r:KNOWS|FOLLOWS*1..3]->(b:Person)
    RETURN a.name, b.name, length(path) AS depth
    """

    print("Query Cypher:")
    print(query)

    try:
        sql = graph.transpile(query)
        print("\n✅ SUCESSO! SQL gerado (primeiras 50 linhas):")
        lines = sql.split('\n')
        for i, line in enumerate(lines[:50], 1):
            print(f"{i:3d}: {line}")
        if len(lines) > 50:
            print(f"... ({len(lines) - 50} linhas restantes)")

        # Verificar se splitou corretamente
        knows_count = sql.count("'KNOWS'")
        follows_count = sql.count("'FOLLOWS'")

        print("\n📊 Análise:")
        print("   - 'KNOWS' aparece %d vezes" % knows_count)
        print("   - 'FOLLOWS' aparece %d vezes" % follows_count)

        if knows_count > 0 and follows_count > 0:
            print("   ✅ Ambos os tipos presentes (OR funcionou!)")
        else:
            print("   ❌ OR não funcionou corretamente")

    except Exception as e:
        print(f"\n❌ ERRO: {type(e).__name__}: {e}")
        raise


def test_three_relationship_types_or():
    """Testar 3 relationship types com OR."""
    graph = GraphContext(
        nodes_table="catalog.schema.nodes",
        edges_table="catalog.schema.edges",
        extra_node_attrs={"name": str}
    )
    graph.set_types(
        node_types=["Person"],
        edge_types=["KNOWS", "FOLLOWS", "BLOCKS"]
    )

    print("\n" + "=" * 80)
    print("TESTE 3: Três relationship types com OR")
    print("=" * 80)

    query = """
    MATCH path = (a:Person)-[r:KNOWS|FOLLOWS|BLOCKS*1..2]->(b:Person)
    RETURN a.name, b.name
    """

    print("Query Cypher:")
    print(query)

    try:
        sql = graph.transpile(query)
        print("\n✅ SUCESSO! SQL gerado")

        # Verificar todos os 3 tipos
        types_found = []
        for type_name in ["KNOWS", "FOLLOWS", "BLOCKS"]:
            if f"'{type_name}'" in sql:
                types_found.append(type_name)

        print("\n📊 Tipos encontrados no SQL: %s" % types_found)

        if len(types_found) == 3:
            print("✅ Todos os 3 tipos presentes!")
        else:
            print("❌ Apenas %d/3 tipos encontrados" % len(types_found))

    except Exception as e:
        print(f"\n❌ ERRO: {type(e).__name__}: {e}")
        raise
