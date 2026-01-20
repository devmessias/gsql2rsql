# Project Summary

## Elevator Pitch

**gsql2rsql**: A production-quality transpiler converting OpenCypher graph queries to Databricks Spark SQL with recursive CTE support for variable-length path traversal.

## Quick Facts

- **Language**: Python 3.12+ (fully typed with strict mypy)
- **Runtime Target**: Databricks Runtime 17+ (Spark SQL with `WITH RECURSIVE`)
- **License**: MIT (© Microsoft Corporation)
- **Status**: Alpha (v0.1.0)
- **Main Entrypoint**: `gsql2rsql` CLI command (also importable as Python library)

## Purpose

OpenCypher is a declarative graph query language inspired by Neo4j's Cypher, optimized for pattern matching across nodes and edges. Databricks Spark SQL is a powerful relational SQL dialect designed for big data analytics, now with recursive CTE support (runtime 17+). This transpiler bridges the two worlds, enabling:

- **Graph analytics on tabular data**: Query your Databricks tables as if they were a graph database
- **BFS/DFS traversal**: Use variable-length paths (`-[:KNOWS*1..5]->`) without custom Spark code
- **Pattern matching**: Leverage Cypher's expressive syntax for relationship queries
- **Correctness**: Strict 4-phase architecture (parse → plan → resolve → render) ensures semantically correct SQL generation
- **Production-ready**: Comprehensive test suite (61 test files), PySpark validation, rich error messages with suggestions

The transpiler handles complex Cypher features including multi-hop traversals, aggregations, subqueries (`WITH` clauses), `OPTIONAL MATCH`, `UNWIND`, set operations (`UNION`/`INTERSECT`/`EXCEPT`), list comprehensions, and 40+ built-in functions. Generated SQL uses `WITH RECURSIVE` CTEs for efficient graph traversal and includes optimizations like predicate pushdown and conservative subquery flattening.

## Where to Look Next

- [01-quickstart.md](01-quickstart.md) — Run your first query in 2 minutes
- [02-architecture.md](02-architecture.md) — Understand the 4-phase transpilation pipeline
- [CONTRIBUTING.md](/home/devmessias/phd/cyper2dsql/python/CONTRIBUTING.md) — Separation of concerns and architectural boundaries
