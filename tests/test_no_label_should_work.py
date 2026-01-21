"""
Demonstração: O transpiler DEVERIA aceitar nós sem label e gerar SQL sem filtro de tipo.

Em Cypher/Neo4j:
- MATCH (a:Person)-[:REL]->(b)      → Filtra b apenas se especificar label
- MATCH (a)-[:REL]->(b)             → TODOS os tipos de nós (sem filtro)

Comportamento atual do gsql2rsql:
- MATCH (a:Person)-[:REL]->(b:Company) → ✅ Gera WHERE type = 'Person' AND type = 'Company'
- MATCH (a)-[:REL]->(b)                → ❌ ERRO: "Failed to bind entity 'a' of type ''"

Comportamento ESPERADO:
- MATCH (a)-[:REL]->(b) → Deveria gerar SQL SEM filtro de tipo (ou com OR de todos os tipos)
"""

from gsql2rsql import GraphContext

graph = GraphContext(
    nodes_table="catalog.schema.nodes",
    edges_table="catalog.schema.edges",
    extra_node_attrs={"name": str, "amount": float}
)
graph.set_types(
    node_types=["Person", "Company", "Device"],
    edge_types=["WORKS_AT", "OWNS"]
)

print("=" * 80)
print("TESTE 1: COM labels (funciona atualmente)")
print("=" * 80)
query1 = """
MATCH (a:Person)-[:WORKS_AT]->(b:Company)
RETURN a.name, b.name
"""
print("Query:", query1)
sql1 = graph.transpile(query1)
print("\nSQL gerado:")
print(sql1)
print("\n✅ Note o filtro: WHERE node_type = 'Person' ... WHERE node_type = 'Company'")

print("\n" + "=" * 80)
print("TESTE 2: SEM label origem (DEVERIA funcionar mas FALHA)")
print("=" * 80)
query2 = """
MATCH (a)-[:WORKS_AT]->(b:Company)
RETURN a.name, b.name
"""
print("Query:", query2)
print("\n❌ FALHA com: TranspilerBindingException: Failed to bind entity 'a' of type ''")
print("\n💡 DEVERIA gerar SQL:")
print("   - Opção 1 (SEM filtro): SELECT ... FROM nodes a")
print("   - Opção 2 (OR): WHERE a.node_type IN ('Person', 'Company', 'Device')")

try:
    sql2 = graph.transpile(query2)
    print("\nSQL gerado:")
    print(sql2)
except Exception as e:
    print(f"\n❌ ERRO: {e}")

print("\n" + "=" * 80)
print("TESTE 3: SEM labels em ambos (DEVERIA funcionar mas FALHA)")
print("=" * 80)
query3 = """
MATCH (a)-[:OWNS]->(b)
RETURN a.name, b.name
"""
print("Query:", query3)
print("\n❌ FALHA com: TranspilerBindingException: Failed to bind entity 'a' of type ''")
print("\n💡 DEVERIA gerar SQL:")
print("   SELECT ... FROM nodes a JOIN edges e ... JOIN nodes b")
print("   -- SEM filtros WHERE node_type = ... (ou com OR de todos os tipos)")

try:
    sql3 = graph.transpile(query3)
    print("\nSQL gerado:")
    print(sql3)
except Exception as e:
    print(f"\n❌ ERRO: {e}")

print("\n" + "=" * 80)
print("CONCLUSÃO")
print("=" * 80)
print("""
O transpiler atualmente REQUER labels no primeiro MATCH.

Para seguir o padrão OpenCypher, deveria:

1. **Aceitar nós sem label** no MATCH inicial
2. **Gerar SQL sem filtro de tipo** (ou com OR de todos os tipos)
3. **Inferir tipo dinamicamente** com base nos relacionamentos possíveis

Exemplo de SQL esperado (Opção 1 - sem filtro):
    SELECT a.name, b.name
    FROM nodes a
    JOIN edges e ON e.src = a.node_id
    JOIN nodes b ON e.dst = b.node_id
    WHERE e.relationship_type = 'WORKS_AT'
    -- SEM filtro: AND a.node_type = '...'
    AND b.node_type = 'Company'

Exemplo de SQL esperado (Opção 2 - OR todos os tipos):
    SELECT a.name, b.name
    FROM nodes a
    JOIN edges e ON e.src = a.node_id
    JOIN nodes b ON e.dst = b.node_id
    WHERE e.relationship_type = 'WORKS_AT'
    AND a.node_type IN ('Person', 'Company', 'Device')  -- OR de todos
    AND b.node_type = 'Company'
""")
