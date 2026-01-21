# Examples Gallery

Real-world OpenCypher queries with generated Databricks SQL.

---

## Categories

<div class="grid cards" markdown>

-   :material-shield-alert:{ .lg .middle } **Fraud Detection**

    ---

    Detect fraud rings, money laundering patterns, and anomalous transactions using graph traversal.

    [:octicons-arrow-right-24: 17 queries](fraud.md)

-   :material-credit-card:{ .lg .middle } **Credit Analysis**

    ---

    Analyze credit risk through guarantor chains, co-borrower networks, and exposure analysis.

    [:octicons-arrow-right-24: 15 queries](credit.md)

-   :material-code-tags:{ .lg .middle } **All Features**

    ---

    Complete feature coverage: paths, aggregations, filters, UNION, CASE, and more.

    [:octicons-arrow-right-24: 69 queries](features.md)

</div>

---

## Quick Reference

| Category | Queries | Key Features |
|----------|---------|--------------|
| [Fraud Detection](fraud.md) | 17 | Variable-length paths, cycle detection, ring analysis |
| [Credit Analysis](credit.md) | 15 | Guarantor chains, exposure calculation, risk scoring |
| [All Features](features.md) | 69 | Complete feature coverage with generated SQL |

---

## Try It Yourself

### Using GraphContext (Recommended)

```python
from gsql2rsql import GraphContext

graph = GraphContext(
    nodes_table="catalog.schema.nodes",
    edges_table="catalog.schema.edges",
)
graph.set_types(
    node_types=["Person", "Account"],
    edge_types=["TRANSACTION", "OWNS"],
)

sql = graph.transpile("""
    MATCH (p:Person)-[:OWNS]->(a:Account)-[:TRANSACTION*1..3]->(target:Account)
    WHERE target.flagged = true
    RETURN p.name, target.id, length(path) AS hops
""")

print(sql)
```

### Using CLI

```bash
# From a file
gsql2rsql translate --schema examples/fraud_queries.yaml < query.cypher

# Interactive TUI
gsql2rsql tui --schema examples/fraud_queries.yaml
```

---

## About These Examples

All queries are:

- **Tested**: Validated against PySpark with real data
- **Production-ready**: Used in real fraud detection and credit analysis systems
- **Documented**: Each shows both OpenCypher input and SQL output

!!! tip "Request Examples"
    Need an example for a specific pattern? [Open an issue](https://github.com/devmessias/gsql2rsql/issues) with your use case.
