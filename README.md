# gsql2rsql - OpenCypher to Databricks SQL Transpiler

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Documentation](https://img.shields.io/badge/docs-mkdocs-blue.svg)](https://devmessias.github.io/gsql2rsql)

**gsql2rsql** transpiles OpenCypher graph queries to Databricks SQL, enabling graph analytics on Delta Lake without a dedicated graph database.

> **Project Status**: This is a hobby/research project being developed towards production quality. While it handles complex queries and includes comprehensive tests, it's not yet  at enterprise scale. Contributions welcome!

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
CREATE TABLE relationships (
  subject_id STRING,    -- Source entity
  predicate STRING,     -- Relationship type
  object_id STRING,     -- Target entity
  properties MAP<STRING, STRING>,
  timestamp TIMESTAMP,
  _partition DATE GENERATED ALWAYS AS (DATE(timestamp))
) PARTITIONED BY (_partition);
```

**Advantages**:
1. **Horizontal scale**: Petabytes, billions of rows, no problem
2. **Cost-effective**: S3 storage ($0.023/GB) vs RAM ($10+/GB)
3. **Time travel**: Delta Lake versioning = free audit trail
4. **Schema evolution**: Add properties without downtime
5. **ACID guarantees**: Delta Lake transactions
6. **Z-ordering**: `OPTIMIZE table ZORDER BY (subject_id, predicate)` for fast lookups
7. **Liquid clustering**: Auto-optimize hot paths


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

### Your First Query

```python
from gsql2rsql.parser.opencypher_parser import OpenCypherParser
from gsql2rsql.planner.logical_plan import LogicalPlan
from gsql2rsql.renderer.sql_renderer import SQLRenderer
from gsql2rsql.planner.schema import DatabricksSchemaProvider, SimpleGraphSchemaProvider
from gsql2rsql.common.schema import NodeSchema, EdgeSchema, EntityProperty

# 1. Define schema (map graph to Delta tables)
schema = SimpleGraphSchemaProvider()

person = NodeSchema(
    name="Person",
    properties=[
        EntityProperty(property_name="id", data_type=int),
        EntityProperty(property_name="name", data_type=str),
        EntityProperty(property_name="age", data_type=int),
    ],
    node_id_property=EntityProperty(property_name="id", data_type=int)
)

company = NodeSchema(
    name="Company",
    properties=[
        EntityProperty(property_name="id", data_type=int),
        EntityProperty(property_name="name", data_type=str),
        EntityProperty(property_name="industry", data_type=str),
    ],
    node_id_property=EntityProperty(property_name="id", data_type=int)
)

works_at = EdgeSchema(
    name="WORKS_AT",
    source_node_id="Person",
    sink_node_id="Company",
    source_id_property=EntityProperty(property_name="person_id", data_type=int),
    sink_id_property=EntityProperty(property_name="company_id", data_type=int),
    properties=[EntityProperty(property_name="since", data_type=int)]
)

schema.add_node(person)
schema.add_node(company)
schema.add_edge(works_at)

# 2. Write Cypher query
query = """
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WHERE c.industry = 'Technology'
RETURN p.name, p.age, c.name AS company
ORDER BY p.age DESC
LIMIT 10
"""

# 3. Transpile to SQL
parser = OpenCypherParser()
schema_provider = DatabricksSchemaProvider(schema)
renderer = SQLRenderer(schema_provider)

ast = parser.parse(query)
plan = LogicalPlan.from_ast(ast, schema)
plan.resolve(query)
sql = renderer.render_plan(plan)

print(sql)

# 4. Execute on Databricks
# spark.sql(sql).show()
```

**Output**: Databricks SQL with JOINs, WHERE filters, ORDER BY, and LIMIT — ready to execute on Delta Lake.

## Features

- ✅ **Variable-length paths** (`*1..N`) via `WITH RECURSIVE`
- ✅ **Undirected relationships** (`-[:REL]-`)
- ✅ **Path functions** (`length()`, `nodes()`, `relationships()`)
- ✅ **Aggregations** (`COUNT`, `SUM`, `COLLECT`, etc.)
- ✅ **Filter pushdown** (optimizes Delta scans)
- ✅ **WITH clauses** (multi-stage composition)
- ✅ **UNION**, **OPTIONAL MATCH**, **CASE**, **DISTINCT**

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


See [full limitations](docs/limitations.md).

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
