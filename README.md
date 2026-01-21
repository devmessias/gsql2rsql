# gsql2rsql - OpenCypher to Databricks SQL Transpiler

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Documentation](https://img.shields.io/badge/docs-mkdocs-blue.svg)](https://devmessias.github.io/gsql2rsql)

**gsql2rsql** transpiles OpenCypher graph queries to Databricks SQL, enabling graph analytics on Delta Lake without a dedicated graph database.

> **Project Status**: This is a hobby/research project being developed towards production quality. While it handles complex queries and includes comprehensive tests, it's not yet  at enterprise scale. Contributions welcome!

!!! warning "Not for OLTP (obviously) or end-user queries"
    This transpiler is for **internal analytics and exploration** (data science, engineering, analysis). It obviously makes no sense for OLTP  ! If you plan to expose transpiled queries to end users, be careful: implement validation, rate limiting, and security. Use common sense.

## Why This Project?

### Inspiration: Microsoft's openCypherTranspiler

This project was inspired by Microsoft's [openCypherTranspiler](https://github.com/microsoft/openCypherTranspiler) (now **unmaintained**) which transpiled OpenCypher to T-SQL (SQL Server).

**Why a new transpiler?** Two reasons:

1. **Databricks SQL is fundamentally different** from T-SQL — WITH RECURSIVE, HOFs, and Delta Lake optimizations require different strategies
2. **Security-first architecture** — gsql2rsql uses strict [4-phase separation of concerns](docs/decision-log.md#decision-1-strict-4-phase-separation-of-concerns) for correctness:
   - **Parser**: Syntax only (no schema access)
   - **Planner**: Semantics only (builds logical operators)
   - **Resolver**: Validation only (schema checking, column resolution)
   - **Renderer**: Code generation only (**intentionally "dumb"** — no semantic decisions, just SQL generation)

This separation makes the transpiler **easier to audit, test, and trust**



**The game-changer**: Databricks recently added **WITH RECURSIVE** support, unlocking variable-leng

### Databricks SQL Higher-Order Functions (HOFs)

 Databricks SQL has **native array manipulation** via HOFs:

```sql
-- Transform array elements
SELECT transform(relationships, r -> r.amount) AS amounts
FROM fraud_paths

-- Filter complex conditions
SELECT filter(path, node -> node.risk_score > 0.8) AS risky_nodes
FROM customer_journeys

-- Aggregate with lambda
SELECT aggregate(
  transactions,
  0.0,
  (acc, t) -> acc + t.amount,
  acc -> acc
) AS total
FROM account_history
```

gsql2rsql leverages these HOFs for:
- **Path filtering**: `NONE(r IN relationships(path) WHERE r.suspicious)`
- **Path aggregations**: `SUM(r IN rels WHERE r.amount > 1000)`
- **Pattern matching**: Complex nested conditions

This makes Cypher → SQL transpilation **more natural**

## Why Graph Queries on Delta Lake?


```
Delta Lake (Single Source)
     ↓ OpenCypher (via gsql2rsql)
Databricks SQL
     ↓ Results
```

**Advantages**:
1. **No duplication**: Query source data directly
2. **Real-time**: Always fresh data
3. **No sync**: One less thing to break
4. **Cost-effective**: No second database
5. **Unified governance**: Single data platform

## Billion-Scale Relationships: Triple Stores in Delta

### The Problem with graph databases (oltp) at Scale

When you have **billions of relationships**:

- **Memory limits**: Graph must fit in RAM for good performance
- **Vertical scaling**: Limited by single-server resources
- **Cost**: Enterprise licenses + large EC2 instances = $$$$
- **Backup/Recovery**: GBs of graph data, long backup windows
- **Version upgrades**: Risky with large graphs


### Triple Store in Delta Lake

Model relationships as **triples** in Delta:

```sql
-- Nodes table (entities)
CREATE TABLE nodes (
  node_id STRING,
  type STRING,          -- Person, Account, Merchant, etc.
  properties MAP<STRING, STRING>,
  timestamp TIMESTAMP
) USING DELTA;

-- Edges table (relationships)
-- Option 1: Traditional partitioning (relationship_type + date)
CREATE TABLE edges (
  src STRING,           -- Source node_id
  relationship_type STRING,  -- TRANSACTION, OWNS, LOCATED_AT, etc.
  dst STRING,           -- Destination node_id
  properties MAP<STRING, STRING>,
  timestamp TIMESTAMP
) USING DELTA
PARTITIONED BY (relationship_type, DATE(timestamp));

-- Option 2: Liquid Clustering (DBR 13.3+, RECOMMENDED!)
-- Auto-tunes partitioning based on query patterns
CREATE TABLE edges (
  src STRING,
  relationship_type STRING,
  dst STRING,
  properties MAP<STRING, STRING>,
  timestamp TIMESTAMP
) USING DELTA
CLUSTER BY (relationship_type, src);

-- For traditional partitioning, optimize with Z-ordering
OPTIMIZE edges ZORDER BY (src, relationship_type, dst);
```

**Advantages**:
1. **Horizontal scale**: Petabytes, billions of rows, no problem
2. **Cost-effective**: S3 storage ($0.023/GB) vs RAM ($10+/GB)
3. **Time travel**: Delta Lake versioning = free audit trail
4. **Schema evolution**: Add properties without downtime
5. **ACID guarantees**: Delta Lake transactions
6. **Partition pruning**: `PARTITIONED BY (relationship_type, date)` → skip 90%+ of data
7. **Z-ordering**: `ZORDER BY (src, relationship_type, dst)` → sub-second lookups
8. **Liquid clustering**: Auto-tunes for query patterns (DBR 13.3+)

**Why partition by `relationship_type`?**
- Query: `MATCH (a)-[:TRANSACTION]->(b)` → Only scans TRANSACTION partition
- Without: Scans ALL relationships (TRANSACTION, OWNS, LOCATED_AT, etc.)
- Impact: **10-100x faster** for queries with specific relationship types

**This is why GraphContext API exists**: When your graph fits this pattern (nodes + edges tables), you don't need 100 lines of schema boilerplate — just 2 table paths and you're done.


## LLMs + Transpilers: Enterprise Governance

**The Problem**: In enterprise environments, **someone must be accountable** for queries before execution — even with LLM text-to-query.

### Why Transpilers Matter

**1. Reviewability**: Graph queries are **4-5 lines** vs **hundreds of SQL lines**
```cypher
# 5 lines in Cypher
MATCH (c:Customer)-[:TRANSACTION*1..3]->(m:Merchant)
WHERE m.risk_score > 0.9
RETURN c.id, COUNT(*) AS risky_tx
ORDER BY risky_tx DESC
LIMIT 100
```
vs 150+ lines of recursive SQL. Easier for humans to review and approve.


Transpilers turn LLM outputs into **governable, auditable, human-reviewable queries**.

## Quick Start

### Installation

```bash
pip install gsql2rsql
# Or from source:
git clone https://github.com/devmessias/gsql2rsql
cd gsql2rsql/python
uv pip install -e .
```

### Simplified API: GraphContext (Recommended for Triple Stores)

**Why Triple Stores + Delta Tables Scale**: Delta Lake's horizontal scaling, Z-ordering, and liquid clustering make **single triple store** architectures incredibly efficient — even at billions of edges. No need for complex multi-table schemas when Delta can handle everything.

**GraphContext API eliminates ~100 lines of boilerplate** for the common case: graph stored as two Delta tables (nodes + edges).

```python
from gsql2rsql import GraphContext

# 1. Create context (just 2 table paths!)
# Note: Table names without backticks - SQLRenderer adds them automatically
graph = GraphContext(
    nodes_table="catalog.fraud.nodes",
    edges_table="catalog.fraud.edges",
    extra_node_attrs={"name": str, "risk_score": float},
    extra_edge_attrs={"amount": float, "timestamp": str}
)

# 2. Set types (auto-discovered if spark session provided)
graph.set_types(
    node_types=["Person", "Account", "Merchant"],
    edge_types=["TRANSACTION", "OWNS", "LOCATED_AT"]
)

# 3. Query with inline filters (optimized!)
query = """
MATCH path = (origin:Person {id: 'alice'})-[:TRANSACTION*1..3]->(dest:Account)
WHERE dest.risk_score > 0.8
RETURN dest.id, dest.risk_score, length(path) AS depth
ORDER BY depth, dest.risk_score DESC
LIMIT 100
"""

sql = graph.transpile(query, optimize=True)  # Predicate pushdown enabled!

# 4. Execute on Databricks
# df = graph.execute(query)  # If spark session provided
# df.show()
```

**Performance tip**: For large graphs with many node/edge types, use `discover_edge_combinations=True` to only create schemas for **actual** edge combinations in your data:

```python
graph = GraphContext(
    spark=spark,  # Required for discovery
    nodes_table="catalog.fraud.nodes",
    edges_table="catalog.fraud.edges",
    discover_edge_combinations=True  # Query DB for real combinations
)
# If you have 10 node types × 5 edge types = 500 possible schemas
# But only 15 combinations exist → Creates only 15 schemas (33x faster!)
```

**Why this works at scale**:
- **Delta Z-ordering**: `OPTIMIZE edges ZORDER BY (src, edge_type)` → sub-second lookups
- **Liquid clustering**: Auto-tunes for your query patterns
- **Predicate pushdown**: Filters applied in DataSource (before joins)
- **Horizontal scale**: Billions of edges = more partitions = more parallelism

### Advanced: Manual Schema Setup (Full Control)

For multi-table schemas or when you need precise control over SQL table descriptors, use the manual setup:

**Example**: Find fraud networks using BFS (Breadth-First Search) up to depth 4, starting from a suspicious account and ignoring social relationships.

```python
from gsql2rsql.parser.opencypher_parser import OpenCypherParser
from gsql2rsql.planner.logical_plan import LogicalPlan
from gsql2rsql.renderer.sql_renderer import SQLRenderer
from gsql2rsql.common.schema import SimpleGraphSchemaProvider, NodeSchema, EdgeSchema, EntityProperty
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor

# 1. Define graph schema (for logical planner)
graph_schema = SimpleGraphSchemaProvider()

# Person node
person = NodeSchema(
    name="Person",
    properties=[
        EntityProperty(property_name="id", data_type=int),
        EntityProperty(property_name="name", data_type=str),
        EntityProperty(property_name="risk_score", data_type=float),
    ],
    node_id_property=EntityProperty(property_name="id", data_type=int)
)

graph_schema.add_node(person)

# Multiple edge types - we'll only query TRANSACAO_SUSPEITA
# AMIGOS and FAMILIARES are in the schema but ignored in the query
amigos = EdgeSchema(
    name="AMIGOS",
    source_node_id="Person",
    sink_node_id="Person",
    source_id_property=EntityProperty(property_name="person1_id", data_type=int),
    sink_id_property=EntityProperty(property_name="person2_id", data_type=int),
    properties=[]
)

familiares = EdgeSchema(
    name="FAMILIARES",
    source_node_id="Person",
    sink_node_id="Person",
    source_id_property=EntityProperty(property_name="person1_id", data_type=int),
    sink_id_property=EntityProperty(property_name="person2_id", data_type=int),
    properties=[]
)

transacao_suspeita = EdgeSchema(
    name="TRANSACAO_SUSPEITA",
    source_node_id="Person",
    sink_node_id="Person",
    source_id_property=EntityProperty(property_name="origem_id", data_type=int),
    sink_id_property=EntityProperty(property_name="destino_id", data_type=int),
    properties=[
        EntityProperty(property_name="valor", data_type=float),
        EntityProperty(property_name="timestamp", data_type=str),
    ]
)

graph_schema.add_edge(amigos)
graph_schema.add_edge(familiares)
graph_schema.add_edge(transacao_suspeita)

# 2. Define SQL schema (maps to Delta tables)
sql_schema = SimpleSQLSchemaProvider()

sql_schema.add_node(
    person,
    SQLTableDescriptor(
        table_name="fraud.person",  # Databricks catalog.schema.table
        node_id_columns=["id"],
    )
)

sql_schema.add_edge(
    amigos,
    SQLTableDescriptor(
        entity_id="Person@AMIGOS@Person",
        table_name="fraud.amigos",
    )
)

sql_schema.add_edge(
    familiares,
    SQLTableDescriptor(
        entity_id="Person@FAMILIARES@Person",
        table_name="fraud.familiares",
    )
)

sql_schema.add_edge(
    transacao_suspeita,
    SQLTableDescriptor(
        entity_id="Person@TRANSACAO_SUSPEITA@Person",
        table_name="fraud.transacao_suspeita",
    )
)

# 3. BFS Query: Find fraud network up to depth 4 from suspicious root account
# Only traverse TRANSACAO_SUSPEITA edges (ignore AMIGOS and FAMILIARES)
query = """
MATCH path = (origem:Person {id: 12345})-[:TRANSACAO_SUSPEITA*1..4]->(destino:Person)
RETURN
    origem.id AS origem_id,
    origem.name AS origem_name,
    destino.id AS destino_id,
    destino.name AS destino_name,
    destino.risk_score AS destino_risk_score,
    length(path) AS profundidade
ORDER BY profundidade, destino.risk_score DESC
LIMIT 100
"""

# 4. Transpile to SQL with WITH RECURSIVE (for BFS traversal)
parser = OpenCypherParser()
renderer = SQLRenderer(db_schema_provider=sql_schema)

ast = parser.parse(query)
plan = LogicalPlan.process_query_tree(ast, graph_schema)
plan.resolve(original_query=query)
sql = renderer.render_plan(plan)

print(sql)

# 5. Execute on Databricks
# df = spark.sql(sql)
# df.show(100, truncate=False)
```

**Output**: Databricks SQL with JOINs, WHERE filters, ORDER BY, and LIMIT — ready to execute on Delta Lake.

## Features

- ✅ **Variable-length paths** (`*1..N`) via `WITH RECURSIVE`
- ✅ **Undirected relationships** (`-[:REL]-`)
- ✅ **Path functions** (`length()`, `nodes()`, `relationships()`)
- ✅ **Aggregations** (`COUNT`, `SUM`, `COLLECT`, etc.)
- ✅ **Predicate pushdown** (filters applied in DataSource before joins)
- ✅ **Inline property filters** (`{name: 'Alice'}` → optimized WHERE clauses)
- ✅ **BFS source filter optimization** (inline filters applied in base case)
- ✅ **WITH clauses** (multi-stage composition)
- ✅ **UNION**, **OPTIONAL MATCH**, **CASE**, **DISTINCT**
- ✅ **GraphContext API** (simplified setup for Triple Stores)

See [full feature list](docs/index.md#features).

## Documentation

- 📘 [Installation & Quick Start](https://devmessias.github.io/gsql2rsql/installation/)
- 🎯 [Examples Gallery](https://devmessias.github.io/gsql2rsql/examples/) (69 queries)
  - [Fraud Detection](https://devmessias.github.io/gsql2rsql/examples/fraud/)
  - [Credit Risk](https://devmessias.github.io/gsql2rsql/examples/credit/)
  - [Feature Engineering](https://devmessias.github.io/gsql2rsql/examples/features/)
- 🏗️ [Architecture](https://devmessias.github.io/gsql2rsql/architecture/)
- 🤝 [Contributing](https://devmessias.github.io/gsql2rsql/contributing/)

## Development

```bash
# Setup
uv sync --extra dev
uv pip install -e ".[dev]"

# Tests
make test-no-pyspark   # Fast (no Spark dependency)
make test-pyspark      # Full validation with PySpark

# Lint & Format
make lint
make format
make typecheck
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for conventional commits and release process.

## Requirements

- **Python 3.12+**
- **Databricks Runtime 15.0+** (for `WITH RECURSIVE`)
- **PySpark** (optional, only for development/testing)



## Contributing

This is an **open hobby project** — contributions are very welcome!

- **Bugs**: [Open an issue](https://github.com/devmessias/gsql2rsql/issues)
- **Features**: Discuss in [Discussions](https://github.com/devmessias/gsql2rsql/discussions)
- **PRs**: Follow [conventional commits](CONTRIBUTING.md#commit-message-convention)

## License

MIT License - see [LICENSE](LICENSE).

## Acknowledgments

- Microsoft's [openCypherTranspiler](https://github.com/microsoft/openCypherTranspiler) (T-SQL) for inspiration
- [OpenCypher](https://opencypher.org/) community for the graph query language
- [ANTLR](https://www.antlr.org/) for parser generation
- [Databricks](https://databricks.com/) for Delta Lake + Spark SQL + `WITH RECURSIVE` support

## Author

**Bruno Messias**
[LinkedIn](https://www.linkedin.com/in/bruno-messias-510553193/) | [GitHub](https://github.com/devmessias)

---

**Status**: Active development | **Version**: 0.1.0 (Alpha) | **Python**: 3.12+
