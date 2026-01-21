# gsql2rsql

**Query your Delta Tables as a Graph**

No need for a separate graph database. Write intuitive OpenCypher queries, get optimized Spark SQL automatically.

---

## Why gsql2rsql?

| Challenge | Solution |
|-----------|----------|
| Graph queries require complex SQL with `WITH RECURSIVE` | Write 5 lines of Cypher instead |
| Need to maintain a separate graph database | Query Delta Lake directly |
| Hand-written recursive SQL is error-prone | Automatic cycle detection and path tracking |
| LLM-generated complex SQL is hard to audit | Human-readable Cypher + deterministic transpilation (optionally pass to LLM for final optimization) |

---

## See It in Action

```python
from gsql2rsql import GraphContext

# Point to your existing Delta tables - no migration needed
graph = GraphContext(
    nodes_table="catalog.fraud.nodes",
    edges_table="catalog.fraud.edges",
)

# Write graph queries with familiar Cypher syntax
sql = graph.transpile("""
    MATCH path = (origin:Person {id: 12345})-[:TRANSACTION*1..4]->(dest:Person)
    WHERE dest.risk_score > 0.8
    RETURN dest.id, dest.name, dest.risk_score, length(path) AS depth
    ORDER BY depth, dest.risk_score DESC
    LIMIT 100
""")

print(sql)  # Production-ready Spark SQL
```

**5 lines of Cypher → optimized Spark SQL with recursive CTEs**

??? example "Click to see the generated SQL"

    ```sql
    WITH RECURSIVE
      paths_1 AS (
        -- Base case: direct edges (depth = 1)
        SELECT
          e.src AS start_node,
          e.dst AS end_node,
          1 AS depth,
          ARRAY(e.src, e.dst) AS path,
          ARRAY(NAMED_STRUCT('src', e.src, 'dst', e.dst, 'amount', e.amount, 'timestamp', e.timestamp)) AS path_edges,
          ARRAY(e.src) AS visited
        FROM catalog.fraud.edges e
        JOIN catalog.fraud.nodes src ON src.id = e.src
        WHERE (relationship_type = 'TRANSACTION') AND (src.id) = (12345)

        UNION ALL

        -- Recursive case: extend paths
        SELECT
          p.start_node,
          e.dst AS end_node,
          p.depth + 1 AS depth,
          CONCAT(p.path, ARRAY(e.dst)) AS path,
          ARRAY_APPEND(p.path_edges, NAMED_STRUCT('src', e.src, 'dst', e.dst, 'amount', e.amount, 'timestamp', e.timestamp)) AS path_edges,
          CONCAT(p.visited, ARRAY(e.src)) AS visited
        FROM paths_1 p
        JOIN catalog.fraud.edges e
          ON p.end_node = e.src
        WHERE p.depth < 4
          AND NOT ARRAY_CONTAINS(p.visited, e.dst)
          AND (relationship_type = 'TRANSACTION')
      )
    SELECT
       _gsql2rsql_dest_id AS id
      ,_gsql2rsql_dest_name AS name
      ,_gsql2rsql_dest_risk_score AS risk_score
      ,(SIZE(_gsql2rsql_path_id) - 1) AS depth
    FROM (
      SELECT
         sink.id AS _gsql2rsql_dest_id
        ,sink.name AS _gsql2rsql_dest_name
        ,sink.risk_score AS _gsql2rsql_dest_risk_score
        ,source.id AS _gsql2rsql_origin_id
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.fraud.nodes sink ON sink.id = p.end_node
      JOIN catalog.fraud.nodes source ON source.id = p.start_node
      WHERE p.depth >= 1 AND p.depth <= 4 AND (sink.risk_score) > (0.8)
    ) AS _proj
    ORDER BY depth ASC, _gsql2rsql_dest_risk_score DESC
    LIMIT 100
    ```

---

## Why gsql2rsql?

| Problem | Solution |
|---------|----------|
| Graph queries are complex SQL | Write 5 lines of Cypher instead of 60+ |
| Need a separate graph database | Query Delta Lake directly |
| LLM-generated SQL is unauditable | Human-readable Cypher + deterministic transpilation |
| `WITH RECURSIVE` is error-prone | Automatically generated with cycle detection |

---

## Real-World Examples

=== "Fraud Detection"

    ```cypher
    -- Find fraud rings: accounts connected through suspicious transactions
    MATCH (a:Account)-[:TRANSFER*2..4]->(b:Account)
    WHERE a.flagged = true AND b.flagged = true
    RETURN DISTINCT a.id, b.id, length(path) AS hops
    ```

    [See more fraud detection queries →](examples/fraud.md)

=== "Credit Analysis"

    ```cypher
    -- Analyze credit exposure through guarantor chains
    MATCH path = (borrower:Customer)-[:GUARANTEES*1..3]->(guarantor:Customer)
    WHERE borrower.credit_score < 600
    RETURN borrower.id, COLLECT(guarantor.id) AS chain
    ```

    [See more credit analysis queries →](examples/credit.md)

=== "Social Network"

    ```cypher
    -- Friends of friends who work at tech companies
    MATCH (me:Person {id: 123})-[:KNOWS*1..2]->(friend)-[:WORKS_AT]->(c:Company)
    WHERE c.industry = 'Technology'
    RETURN DISTINCT friend.name, c.name
    ```

    [See all feature examples →](examples/features.md)

---

## Installation

```bash
pip install gsql2rsql
```

---

**That's it!** No schema boilerplate, no complex setup.

[Full User Guide →](user-guide.md)

---

## Low-Level API (Without GraphContext)

For advanced use cases or non-Triple-Store schemas, use the components directly:

```python
from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import SimpleGraphSchemaProvider, NodeSchema, EdgeSchema, EntityProperty
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor

# 1. Define graph schema
graph_schema = SimpleGraphSchemaProvider()
graph_schema.add_node(NodeSchema(
    name="Person",
    node_id_property=EntityProperty("id", int),
    properties=[EntityProperty("name", str)],
))
graph_schema.add_edge(EdgeSchema(
    name="KNOWS",
    source_node_id="Person",
    sink_node_id="Person",
))

# 2. Map to SQL tables
sql_schema = SimpleSQLSchemaProvider()
sql_schema.add_node(
    graph_schema.get_node_definition("Person"),
    SQLTableDescriptor(table_name="people", node_id_columns=["id"]),
)
sql_schema.add_edge(
    graph_schema.get_edge_definition("KNOWS", "Person", "Person"),
    SQLTableDescriptor(table_name="friendships"),
)

# 3. Transpile
parser = OpenCypherParser()
ast = parser.parse("MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name")
plan = LogicalPlan.process_query_tree(ast, graph_schema)
plan.resolve(original_query="...")

renderer = SQLRenderer(db_schema_provider=sql_schema)
sql = renderer.render_plan(plan)
```

API Reference →[](api-reference.md)

---

## Key Features

| Feature | Description |
|---------|-------------|
| **Variable-length paths** | `[:REL*1..5]` via `WITH RECURSIVE` |
| **Cycle detection** | Automatic `ARRAY_CONTAINS` checks |
| **Path functions** | `length(path)`, `nodes(path)`, `relationships(path)` |
| **Inline filters** | `(n:Person {id: 123})` pushes predicates to source |
| **Undirected edges** | `(a)-[:KNOWS]-(b)` via optimized UNION ALL |
| **Aggregations** | COUNT, SUM, AVG, COLLECT, etc. |
| **Type safety** | Schema validation before SQL generation |

---

## Architecture

gsql2rsql uses a **4-phase pipeline** for correctness:

```
OpenCypher → Parser → Planner → Resolver → Renderer → SQL
```

1. **Parser**: Cypher → AST (syntax only, no schema)
2. **Planner**: AST → Logical operators (semantics)
3. **Resolver**: Validate columns & types against schema
4. **Renderer**: Operators → Databricks SQL

This separation ensures each phase has clear responsibilities and can be tested independently.

[Architecture details →](architecture.md)

---

## Documentation

| Section | Description |
|---------|-------------|
| [**User Guide**](user-guide.md) | Getting started, GraphContext, schema setup |
| [**Examples**](examples/index.md) | 69 complete queries with generated SQL |
| [**API Reference**](api-reference.md) | Python API documentation |
| [**CLI Commands**](cli-commands.md) | Command-line interface |
| [**Architecture**](architecture.md) | How the transpiler works |

---

## Project Status

!!! info "Research Project"
    This is a hobby/research project with **682+ tests** and comprehensive
    PySpark validation. While not yet battle-tested at enterprise scale,
    it handles complex queries correctly. **Contributions welcome!**

- [GitHub Repository](https://github.com/devmessias/gsql2rsql)
- [Issue Tracker](https://github.com/devmessias/gsql2rsql/issues)
- [Contributing Guide](contributing.md)

---

## License

MIT License - see [LICENSE](https://github.com/devmessias/gsql2rsql/blob/main/LICENSE)
