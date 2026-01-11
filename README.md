# openCypher Transpiler (Python)

A Python 3.12+ port of the openCypher to SQL transpiler.

This library helps you build an [openCypher](http://www.opencypher.org/) query layer on top of a relational database or structured data in data lakes. By leveraging this library, you can transpile openCypher queries into a target query language used by the relational database. We provide a sample target language renderer for [T-SQL](https://docs.microsoft.com/en-us/sql/t-sql/language-reference?view=sql-server-2017).

## Components

This library has three main components:

* **openCypher Parser**: Built on top of ANTLR4 and the official openCypher grammar to parse and create an AST (Abstract Syntax Tree) to abstract the syntactical structure of the graph query
* **Logical Planner**: Transforms the AST into a relational query logical plan similar to [Relational Algebra](https://en.wikipedia.org/wiki/Relational_algebra)
* **SQL Renderer**: Produces the actual query code from the logical plan (T-SQL renderer provided)

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
mkdir -p src/opencypher_transpiler/parser/grammar/
mv Cypher.g4 src/opencypher_transpiler/parser/grammar/

# Generate Python parser files
cd src/opencypher_transpiler/parser/grammar/
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

# Define SQL schema mappings
sql_schema = SimpleSQLSchemaProvider()
sql_schema.add_table(SQLTableDescriptor(entity_id="device", table_name="dbo.Device", node_id_columns=["id"]))
sql_schema.add_table(SQLTableDescriptor(entity_id="tenant", table_name="dbo.Tenant", node_id_columns=["id"]))
sql_schema.add_table(SQLTableDescriptor(entity_id="app", table_name="dbo.App", node_id_columns=["id"]))
sql_schema.add_table(SQLTableDescriptor(entity_id="device@belongsTo@tenant", table_name="dbo.BelongsTo", node_id_columns=["device_id", "tenant_id"]))
sql_schema.add_table(SQLTableDescriptor(entity_id="device@runs@app", table_name="dbo.Runs", node_id_columns=["device_id", "app_id"]))

# Parse, plan, and render
parser = OpenCypherParser()
ast = parser.parse(cypher_query)

plan = LogicalPlan.process_query_tree(ast, graph_schema)

renderer = SQLRenderer(db_schema_provider=sql_schema)
tsql_query = renderer.render_plan(plan)

print("Transpiled T-SQL query:")
print(tsql_query)
```

## CLI Usage

```bash
# Transpile a query
opencypher-transpiler transpile "MATCH (n:Person) RETURN n.name" --schema schema.json

# Parse a query to AST (JSON format)
opencypher-transpiler parse "MATCH (n:Person) RETURN n.name" --format json

# Initialize a schema template
opencypher-transpiler init-schema schema.json
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
