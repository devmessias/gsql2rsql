# Quick Start Guide

## Installation

```bash
# Clone repository (INFERRED - replace with actual repo URL)
cd /path/to/cyper2dsql/python

# Create virtual environment and install dependencies
uv venv
uv sync --extra dev
uv pip install -e ".[dev]"
```

## Environment Requirements

- **Python**: 3.12 or 3.13
- **Target Runtime**: Databricks Runtime 17+ (requires `WITH RECURSIVE` CTE support)
- **Spark Version**: 3.5+ (for PySpark validation tests)
- **Key Dependencies**:
  - `antlr4-python3-runtime>=4.13.0` (parser)
  - `click>=8.1.0` (CLI)
  - `textual>=0.47.0` (interactive TUI)
  - `pyspark>=3.5.0` (dev only, for validation)

## Minimal Example: Simple Query

### 1. Create a Schema File

OpenCypher queries need a graph schema defining nodes and edges. Create `my_schema.json`:

```json
{
  "nodes": [
    {
      "name": "Person",
      "tableName": "graph.Person",
      "idProperty": {"name": "id", "type": "int"},
      "properties": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
      ]
    }
  ],
  "edges": [
    {
      "name": "KNOWS",
      "sourceNode": "Person",
      "sinkNode": "Person",
      "tableName": "graph.Knows",
      "sourceIdProperty": {"name": "source_id", "type": "int"},
      "sinkIdProperty": {"name": "target_id", "type": "int"},
      "properties": []
    }
  ]
}
```

### 2. Transpile a Query

**Cypher input:**
```cypher
MATCH (p:Person)-[:KNOWS]->(f:Person)
WHERE p.age > 30
RETURN p.name, f.name, f.age
```

**Command:**
```bash
echo "MATCH (p:Person)-[:KNOWS]->(f:Person) WHERE p.age > 30 RETURN p.name, f.name, f.age" | \
  uv run gsql2rsql transpile --schema my_schema.json
```

**Generated SQL:**
```sql
SELECT
  _gsql2rsql_p_name AS name,
  _gsql2rsql_f_name AS name,
  _gsql2rsql_f_age AS age
FROM (
  SELECT
    sink.id AS _gsql2rsql_f_id,
    sink.name AS _gsql2rsql_f_name,
    sink.age AS _gsql2rsql_f_age,
    source.id AS _gsql2rsql_p_id,
    source.name AS _gsql2rsql_p_name,
    source.age AS _gsql2rsql_p_age
  FROM
    graph.Knows AS edge
  INNER JOIN graph.Person AS source ON edge.source_id = source.id
  INNER JOIN graph.Person AS sink ON edge.target_id = sink.id
  WHERE source.age > 30
) AS _proj
```

### 3. Variable-Length Path Example

**Cypher input (BFS traversal):**
```cypher
MATCH (root:Person)-[:KNOWS*1..5]->(neighbor:Person)
WHERE root.id = 1
RETURN DISTINCT neighbor.id, neighbor.name
```

**Command:**
```bash
echo "MATCH (root:Person)-[:KNOWS*1..5]->(neighbor:Person) WHERE root.id = 1 RETURN DISTINCT neighbor.id, neighbor.name" | \
  uv run gsql2rsql transpile --schema my_schema.json
```

**Generated SQL (uses WITH RECURSIVE):**
```sql
WITH RECURSIVE
  paths_1 AS (
    -- Base case: direct edges (depth = 1)
    SELECT
      e.source_id AS start_node,
      e.target_id AS end_node,
      1 AS depth,
      ARRAY(e.source_id, e.target_id) AS path,
      ARRAY(e.source_id) AS visited
    FROM graph.Knows e
    UNION ALL
    -- Recursive case: extend paths
    SELECT
      p.start_node,
      e.target_id AS end_node,
      p.depth + 1 AS depth,
      CONCAT(p.path, ARRAY(e.target_id)) AS path,
      CONCAT(p.visited, ARRAY(e.source_id)) AS visited
    FROM paths_1 p
    JOIN graph.Knows e ON p.end_node = e.source_id
    WHERE p.depth < 5
      AND NOT array_contains(p.visited, e.target_id)
  )
SELECT DISTINCT
  _gsql2rsql_neighbor_id AS id,
  _gsql2rsql_neighbor_name AS name
FROM (
  SELECT
    sink.id AS _gsql2rsql_neighbor_id,
    sink.name AS _gsql2rsql_neighbor_name,
    source.id AS _gsql2rsql_root_id
  FROM paths_1 p
  JOIN graph.Person sink ON sink.id = p.end_node
  JOIN graph.Person source ON source.id = p.start_node
  WHERE p.depth >= 1 AND p.depth <= 5
    AND source.id = 1
) AS _proj
```

## Running Tests

```bash
# Run all tests (fast, excludes PySpark)
make test-no-pyspark

# Run all tests including PySpark validation (slower)
make test

# Run with coverage report
make test-cov

# Run specific feature test
uv run pytest tests/transpile_tests/test_01_simple_node_lookup.py -v
```

## Running Examples from YAML

The transpiler includes curated examples in `examples/*.yaml`:

```bash
# Interactive TUI mode (browse examples, live transpilation)
uv run gsql2rsql tui --examples examples/credit_queries.yaml

# Run PySpark validation on all examples
make test-pyspark-examples

# Run quick subset (first 5 credit queries)
make test-pyspark-quick
```

## CLI Commands Reference

```bash
# Show help
uv run gsql2rsql --help

# Transpile from stdin
echo "MATCH (n:Person) RETURN n" | uv run gsql2rsql transpile -s schema.json

# Transpile from file
uv run gsql2rsql transpile -s schema.json -i query.cypher

# Enable optimization (conservative subquery flattening)
uv run gsql2rsql transpile -s schema.json --optimize

# Show scope debugging information
uv run gsql2rsql transpile -s schema.json --explain-scopes

# Parse only (show AST without transpilation)
uv run gsql2rsql parse -i query.cypher

# Generate schema template
uv run gsql2rsql init-schema > my_schema.json
```

## Databricks Runtime Constraints

- **Minimum Runtime**: 17+ (requires `WITH RECURSIVE` support)
- **SQL Features Used**:
  - `WITH RECURSIVE` for variable-length paths
  - `ARRAY()` and `CONCAT()` for path tracking
  - `array_contains()` for cycle detection
  - `STRUCT()` for edge property collections
  - `COALESCE()` for `OPTIONAL MATCH` null handling
- **Known Issues**: Certain aggregation patterns may require runtime 18+ (INFERRED from test history)

## Where to Look Next

- [examples/credit_queries.yaml](../examples/credit_queries.yaml) — Credit analysis domain examples
- [examples/fraud_queries.yaml](../examples/fraud_queries.yaml) — Fraud detection patterns
- [examples/features_queries.yaml](../examples/features_queries.yaml) — Feature showcase
- [02-architecture.md](02-architecture.md) — Understand the transpilation pipeline
- [05-testing-and-examples.md](05-testing-and-examples.md) — Test structure and golden files
