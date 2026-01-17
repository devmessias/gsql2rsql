# gsql2rsql (Python)

A Python 3.12+ port of the openCypher to Databricks SQL transpiler.

This library helps you build an [openCypher](http://www.opencypher.org/) query layer on top of a relational database or structured data in data lakes. By leveraging this library, you can transpile openCypher queries into Databricks SQL with support for advanced features like `WITH RECURSIVE` for variable-length path traversals (BFS/DFS).

## Features

- **Databricks SQL compatible output**: Uses backticks for identifiers, `TRUE`/`FALSE` for booleans, `RLIKE` for regex, etc.
- **Variable-length path support**: Transpiles patterns like `(a)-[*1..5]->(b)` using `WITH RECURSIVE` CTEs
- **Cycle detection**: Automatic cycle prevention in recursive queries using `ARRAY_CONTAINS`
- **Clean syntax**: No T-SQL artifacts (brackets, `N''` strings, `PATINDEX`)

## Components

This library has three main components:

* **openCypher Parser**: Built on top of ANTLR4 and the official openCypher grammar to parse and create an AST (Abstract Syntax Tree) to abstract the syntactical structure of the graph query
* **Logical Planner**: Transforms the AST into a relational query logical plan similar to [Relational Algebra](https://en.wikipedia.org/wiki/Relational_algebra)
* **SQL Renderer**: Produces Databricks SQL from the logical plan with support for `WITH RECURSIVE` CTEs

## Requirements

- Python 3.12+
- uv (Python package manager)
- ANTLR4 (for grammar generation)

## Installation

### 1. Install ANTLR4 (for grammar generation)

```bash
# Download ANTLR4 jar
curl -O https://www.antlr.org/download/antlr-4.13.1-complete.jar

# Or install via package manager
# macOS:
brew install antlr

# Ubuntu/Debian:
```

### 2. Generate Grammar Files

Download the openCypher grammar from the official repository and generate Python parser:

```bash
# Download openCypher grammar
curl -O https://raw.githubusercontent.com/antlr/grammars-v4/refs/heads/master/cypher/CypherParser.g4

# Move to grammar directory
mkdir -p src/gsql2rsql/parser/grammar/
mv Cypher.g4 src/gsql2rsql/parser/grammar/

# Generate Python parser files
cd src/gsql2rsql/parser/grammar/
antlr4 -Dlanguage=Python3 -visitor Cypher.g4

# Or with jar:
java -jar /path/to/antlr-4.13.1-complete.jar -Dlanguage=Python3 -visitor Cypher.g4
```

This will generate:
- `CypherLexer.py`
- `CypherParser.py`
- `CypherVisitor.py`
- `CypherListener.py`

### 3. Install Python Dependencies

```bash
# Using uv (recommended)
uv venv
source .venv/bin/activate  # On Linux/macOS

uv pip install -e .

# Or with development dependencies
uv pip install -e ".[dev]"
```

## Quick Start

```python
from opencypher_transpiler import OpenCypherParser, LogicalPlan, SQLRenderer
from opencypher_transpiler.common.schema import SimpleGraphSchemaProvider, NodeSchema, EdgeSchema
from opencypher_transpiler.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor

cypher_query = """
    MATCH (d:device)-[:belongsTo]->(t:tenant)
    MATCH (d)-[:runs]->(a:app)
    RETURN t.id as TenantId, a.AppName as AppName, COUNT(d) as DeviceCount
"""

# Define your graph schema
graph_schema = SimpleGraphSchemaProvider()
graph_schema.add_node(NodeSchema(name="device"))
graph_schema.add_node(NodeSchema(name="tenant"))
graph_schema.add_node(NodeSchema(name="app"))
graph_schema.add_edge(EdgeSchema(name="belongsTo", source_node_id="device", sink_node_id="tenant"))
graph_schema.add_edge(EdgeSchema(name="runs", source_node_id="device", sink_node_id="app"))

# Define SQL schema mappings (Databricks format: catalog.schema.table)
sql_schema = SimpleSQLSchemaProvider()
sql_schema.add_table(SQLTableDescriptor(entity_id="device", table_name="catalog.schema.Device", node_id_columns=["id"]))
sql_schema.add_table(SQLTableDescriptor(entity_id="tenant", table_name="catalog.schema.Tenant", node_id_columns=["id"]))
sql_schema.add_table(SQLTableDescriptor(entity_id="app", table_name="catalog.schema.App", node_id_columns=["id"]))
sql_schema.add_table(SQLTableDescriptor(entity_id="device@belongsTo@tenant", table_name="catalog.schema.BelongsTo", node_id_columns=["device_id", "tenant_id"]))
sql_schema.add_table(SQLTableDescriptor(entity_id="device@runs@app", table_name="catalog.schema.Runs", node_id_columns=["device_id", "app_id"]))

# Parse, plan, and render
parser = OpenCypherParser()
ast = parser.parse(cypher_query)

plan = LogicalPlan.process_query_tree(ast, graph_schema)

renderer = SQLRenderer(db_schema_provider=sql_schema)
sql_query = renderer.render_plan(plan)

print("Transpiled Databricks SQL query:")
print(sql_query)
```

## Variable-Length Paths with WITH RECURSIVE (BFS)

The transpiler supports variable-length relationship patterns using `WITH RECURSIVE` CTEs for BFS traversal.

### Example: Find all neighbors up to 5 hops from a root vertex

**openCypher Query:**
```cypher
MATCH (root:Person {id: 42})-[:KNOWS*1..5]->(neighbor:Person)
RETURN DISTINCT neighbor.id, neighbor.name
```

**Generated Databricks SQL:**
```sql
WITH RECURSIVE paths AS (
    -- Base case: direct neighbors of root vertex (id=42)
    SELECT
        e.target_id AS current_node,
        1 AS depth,
        ARRAY(e.source_id) AS visited
    FROM `graph.Knows` e
    WHERE e.source_id = 42

    UNION ALL

    -- Recursive case: expand to next level neighbors
    SELECT
        e.target_id AS current_node,
        p.depth + 1 AS depth,
        ARRAY_APPEND(p.visited, e.source_id) AS visited
    FROM paths p
    JOIN `graph.Knows` e ON e.source_id = p.current_node
    WHERE p.depth < 5
      AND NOT ARRAY_CONTAINS(p.visited, e.target_id)  -- cycle detection
)
SELECT DISTINCT
    n.id,
    n.name
FROM paths p
JOIN `graph.Person` n ON n.id = p.current_node
WHERE p.depth >= 1
```

### Python Usage:

```python
from opencypher_transpiler import OpenCypherParser, LogicalPlan, SQLRenderer
from opencypher_transpiler.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor
from opencypher_transpiler.common.schema import (
    SimpleGraphSchemaProvider, NodeSchema, EdgeSchema, EntityProperty
)

# Define graph schema
graph_schema = SimpleGraphSchemaProvider()
graph_schema.add_node(NodeSchema(
    name="Person",
    properties=[
        EntityProperty("id", int),
        EntityProperty("name", str),
    ],
    node_id_property=EntityProperty("id", int),
))
graph_schema.add_edge(EdgeSchema(
    name="KNOWS",
    source_node_id="Person",
    sink_node_id="Person",
    source_id_property=EntityProperty("source_id", int),
    sink_id_property=EntityProperty("target_id", int),
))

# Define SQL schema (Databricks format)
sql_schema = SimpleSQLSchemaProvider()
sql_schema.add_node(
    NodeSchema(name="Person", node_id_property=EntityProperty("id", int)),
    SQLTableDescriptor(table_name="graph.Person", node_id_columns=["id"]),
)
sql_schema.add_edge(
    EdgeSchema(
        name="KNOWS",
        source_node_id="Person",
        sink_node_id="Person",
        source_id_property=EntityProperty("source_id", int),
        sink_id_property=EntityProperty("target_id", int),
    ),
    SQLTableDescriptor(table_name="graph.Knows"),
)

# BFS query: find neighbors up to 5 hops from vertex with id=42
cypher_query = """
MATCH (root:Person {id: 42})-[:KNOWS*1..5]->(neighbor:Person)
RETURN DISTINCT neighbor.id AS id, neighbor.name AS name
"""

# Transpile
parser = OpenCypherParser()
ast = parser.parse(cypher_query)
plan = LogicalPlan.process_query_tree(ast, graph_schema)
renderer = SQLRenderer(db_schema_provider=sql_schema)
sql = renderer.render_plan(plan)

print(sql)
```

### Key Features:

- **BFS Traversal**: Uses `WITH RECURSIVE` CTE to traverse the graph level by level
- **Cycle Detection**: `ARRAY_CONTAINS(visited, target_id)` prevents infinite loops
- **Bounded Depth**: `WHERE depth < max_hops` ensures traversal stops at specified depth
- **Min/Max Hops**: Pattern `*1..5` means at least 1 hop and at most 5 hops

## CLI Usage

```bash
# Transpile a query from stdin
echo "MATCH (n:Person) RETURN n.name" | gsql2rsql transpile --schema schema.json

# Transpile a query from file
gsql2rsql transpile --input query.cypher --schema schema.json --output query.sql

# Parse a query to AST
gsql2rsql parse --input query.cypher

# Initialize a schema template
gsql2rsql init-schema --output schema.json
```

## Schema File Format

```json
{
  "nodes": [
    {
      "name": "Person",
      "tableName": "catalog.schema.Person",
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
      "tableName": "catalog.schema.Knows",
      "sourceIdProperty": {"name": "source_id", "type": "int"},
      "sinkIdProperty": {"name": "target_id", "type": "int"},
      "properties": []
    }
  ]
}
```

## Development

```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Run tests
pytest

# Type checking
mypy src/opencypher_transpiler

# Linting
ruff check src/ tests/
ruff format src/ tests/
```

## Project Structure

```
python/
├── pyproject.toml          # Project configuration
├── README.md               # This file
└── src/
    └── opencypher_transpiler/
        ├── common/         # Shared utilities, exceptions, schemas
        │   ├── exceptions.py
        │   ├── logging.py
        │   ├── schema.py
        │   └── utils.py
        ├── parser/         # openCypher parser (ANTLR4)
        │   ├── grammar/    # ANTLR4 generated files
        │   ├── ast.py      # AST node definitions
        │   ├── operators.py # Operators and functions
        │   ├── opencypher_parser.py
        │   └── visitor.py  # Parse tree visitor
        ├── planner/        # Logical query planner
        │   ├── operators.py # Logical operators
        │   ├── schema.py   # Plan schema
        │   └── logical_plan.py
        ├── renderer/       # SQL rendering
        │   ├── schema_provider.py
        │   └── sql_renderer.py
        └── cli.py          # CLI interface
```

## License

MIT License - see LICENSE file for details.
