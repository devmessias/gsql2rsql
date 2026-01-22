# API Reference

This document provides a reference for the public API, CLI commands, and configuration options.

---

## Table of Contents

1. [Python API](#python-api)
2. [CLI Reference](#cli-reference)
3. [Schema Format](#schema-format)
4. [Configuration Options](#configuration-options)
5. [Exception Types](#exception-types)

---

## Python API

### Public Modules

The transpiler can be used as a Python library. Import from the top-level package:

```python
from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer, GraphContext
```

**Recommended for Triple Store architectures**: Use `GraphContext` for simplified setup when your graph data is stored in two Delta tables (nodes + edges).

### Core Classes

#### `OpenCypherParser`

**Module**: `gsql2rsql.parser.opencypher_parser`

**Purpose**: Parse OpenCypher query strings to AST.

```python
class OpenCypherParser:
    """Parser for OpenCypher queries."""

    def parse(self, query: str) -> QueryNode:
        """
        Parse OpenCypher query string to AST.

        Args:
            query: OpenCypher query string

        Returns:
            QueryNode: Root AST node

        Raises:
            TranspilerSyntaxErrorException: If query has syntax errors
        """
```

**Example Usage**:
```python
from gsql2rsql import OpenCypherParser

parser = OpenCypherParser()
ast = parser.parse("MATCH (n:Person) RETURN n")

print(ast.dump_tree())  # Visualize AST
```

---

#### `LogicalPlan`

**Module**: `gsql2rsql.planner.logical_plan`

**Purpose**: Convert AST to logical operator tree.

```python
class LogicalPlan:
    """Logical query plan with operator tree."""

    @staticmethod
    def process_query_tree(
        ast: QueryNode,
        schema: IGraphSchemaProvider
    ) -> "LogicalPlan":
        """
        Convert AST to logical plan.

        Args:
            ast: Abstract syntax tree from parser
            schema: Schema provider (SimpleSQLSchemaProvider implements IGraphSchemaProvider)

        Returns:
            LogicalPlan: Logical plan with operators

        Raises:
            TranspilerBindingException: If schema binding fails
        """

    def resolve(self, original_query: str) -> None:
        """
        Resolve column references and validate schema.

        Args:
            original_query: Original Cypher query (for error messages)

        Raises:
            ColumnResolutionError: If column resolution fails
        """

    def is_resolved(self) -> bool:
        """Check if plan has been resolved."""

    def dump_graph(self) -> str:
        """Generate text visualization of operator tree."""

    @property
    def starting_operators(self) -> list[LogicalOperator]:
        """Get starting operators (data sources)."""

    @property
    def terminal_operators(self) -> list[LogicalOperator]:
        """Get terminal operators (outputs)."""

    def all_operators(self) -> list[LogicalOperator]:
        """Get all operators in topological order."""
```

**Example Usage**:
```python
from gsql2rsql import OpenCypherParser, LogicalPlan
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider

# Parse
parser = OpenCypherParser()
ast = parser.parse("MATCH (n:Person) RETURN n")

# Build schema (SimpleSQLSchemaProvider)
schema = SimpleSQLSchemaProvider()
# ... add nodes and edges

# Create logical plan
plan = LogicalPlan.process_query_tree(ast, schema)

# Resolve column references
plan.resolve(original_query="MATCH (n:Person) RETURN n")

# Inspect plan
print(plan.dump_graph())
```

---

#### `SQLRenderer`

**Module**: `gsql2rsql.renderer.sql_renderer`

**Purpose**: Generate Databricks SQL from logical plan.

```python
class SQLRenderer:
    """SQL code generator for Databricks Spark SQL."""

    def __init__(
        self,
        db_schema_provider: ISQLDBSchemaProvider,
        enable_column_pruning: bool = True,
        config: dict[str, Any] | None = None
    ):
        """
        Initialize renderer.

        Args:
            db_schema_provider: SQL database schema provider
            enable_column_pruning: Enable column pruning optimization (default: True)
            config: Optional configuration dictionary for renderer behavior.
                Supported keys:
                - 'undirected_strategy': Strategy for undirected relationships.
                  Values: 'union_edges' (default) or 'or_join'
        """

    def render_plan(self, plan: LogicalPlan) -> str:
        """
        Generate SQL from logical plan.

        Args:
            plan: Logical plan (must be resolved)

        Returns:
            str: Databricks Spark SQL query

        Raises:
            RuntimeError: If plan is not resolved
            TranspilerNotSupportedException: If unsupported pattern
        """
```

**Example Usage (Default)**:
```python
from gsql2rsql import SQLRenderer
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor

# Create SQL schema provider
sql_schema = SimpleSQLSchemaProvider()
sql_schema.add_node(
    node_schema,
    SQLTableDescriptor(table_name="dbo.Person", node_id_columns=["id"])
)

# Render SQL with default optimizations
renderer = SQLRenderer(db_schema_provider=sql_schema)
sql = renderer.render_plan(plan)

print(sql)
```

**Example Usage (Custom Configuration)**:
```python
# Disable undirected relationship optimization (use legacy OR joins)
# Only recommended for debugging or very small datasets
renderer = SQLRenderer(
    db_schema_provider=sql_schema,
    config={"undirected_strategy": "or_join"}  # Default: "union_edges"
)
sql = renderer.render_plan(plan)
```

**Configuration Options**:

| Key | Values | Default | Description |
|-----|--------|---------|-------------|
| `undirected_strategy` | `"union_edges"` or `"or_join"` | `"union_edges"` | Strategy for undirected relationships (`-[:TYPE]-`). `"union_edges"` uses UNION ALL for O(n) performance. `"or_join"` uses OR conditions (legacy, slower). See performance documentation for details. |

---

### Schema Classes

#### `SimpleSQLSchemaProvider`

**Module**: `gsql2rsql.renderer.schema_provider`

**Purpose**: In-memory schema provider (unified graph + SQL schema).

```python
class SimpleSQLSchemaProvider(ISQLDBSchemaProvider):
    """Simple in-memory schema provider for graph and SQL mappings."""

    def add_node(self, node: NodeSchema, table_descriptor: SQLTableDescriptor) -> None:
        """Add node type to schema with SQL table mapping."""

    def add_edge(self, edge: EdgeSchema, table_descriptor: SQLTableDescriptor) -> None:
        """Add edge type to schema with SQL table mapping."""

    def get_node_schema(self, label: str) -> NodeSchema | None:
        """Get node schema by label."""

    def get_edge_schema(self, type_name: str) -> EdgeSchema | None:
        """Get edge schema by type name."""

    def all_nodes(self) -> list[NodeSchema]:
        """Get all node schemas."""

    def all_edges(self) -> list[EdgeSchema]:
        """Get all edge schemas."""
```

#### `NodeSchema`

**Module**: `gsql2rsql.common.schema`

```python
@dataclass
class NodeSchema(EntitySchema):
    """Schema for a node type."""

    name: str
    properties: list[EntityProperty]
    node_id_property: EntityProperty
```

#### `EdgeSchema`

**Module**: `gsql2rsql.common.schema`

```python
@dataclass
class EdgeSchema(EntitySchema):
    """Schema for an edge type."""

    name: str
    source_node: str
    sink_node: str
    properties: list[EntityProperty]
    source_id_property: EntityProperty
    sink_id_property: EntityProperty
```

#### `EntityProperty`

**Module**: `gsql2rsql.common.schema`

```python
@dataclass
class EntityProperty:
    """Property definition for node or edge."""

    name: str
    python_type: type  # int, str, float, bool, etc.
```

---

### `GraphContext` (Simplified API for Triple Stores)

**Module**: `gsql2rsql.graph_context`

**Purpose**: Simplified API for Triple Store architectures where graph data is stored in two Delta tables (nodes + edges). Eliminates ~100 lines of schema boilerplate.

```python
class GraphContext:
    """High-level API for Triple Store graph queries."""

    def __init__(
        self,
        spark: SparkSession | None = None,
        nodes_table: str | None = None,
        edges_table: str | None = None,
        node_type_col: str = "type",
        edge_type_col: str = "relationship_type",
        node_id_col: str = "node_id",
        edge_src_col: str = "src",
        edge_dst_col: str = "dst",
        extra_node_attrs: dict[str, type] | None = None,
        extra_edge_attrs: dict[str, type] | None = None,
        discover_edge_combinations: bool = False,
    ):
        """
        Initialize GraphContext with Triple Store tables.

        Args:
            spark: PySpark SparkSession (optional, only needed for execute())
            nodes_table: Fully qualified path to nodes table (e.g., "catalog.schema.nodes")
            edges_table: Fully qualified path to edges table
            node_type_col: Column name for node type (default: "type")
            edge_type_col: Column name for edge type (default: "relationship_type")
            node_id_col: Column name for node ID (default: "node_id")
            edge_src_col: Column name for edge source (default: "src")
            edge_dst_col: Column name for edge destination (default: "dst")
            extra_node_attrs: Additional node properties with types (default: auto-discover)
            extra_edge_attrs: Additional edge properties with types (default: auto-discover)
            discover_edge_combinations: If True, queries DB to find actual edge
                combinations instead of creating all possible combinations. Requires
                spark session. Default: False for backward compatibility.
        """

    def set_types(
        self,
        node_types: list[str],
        edge_types: list[str],
        edge_combinations: list[tuple[str, str, str]] | None = None,
    ) -> None:
        """
        Manually set node and edge types (for non-Spark usage).

        Args:
            node_types: List of node type names (e.g., ["Person", "Company"])
            edge_types: List of edge type names (e.g., ["KNOWS", "WORKS_AT"])
            edge_combinations: Optional list of actual edge combinations as
                (source_type, edge_type, sink_type) tuples.
        """

    def transpile(self, query: str, optimize: bool = True) -> str:
        """
        Transpile OpenCypher query to Databricks SQL.

        Args:
            query: OpenCypher query string
            optimize: Enable optimizations (predicate pushdown, flattening)

        Returns:
            Databricks SQL query string
        """

    def execute(self, query: str, optimize: bool = True) -> DataFrame:
        """
        Execute OpenCypher query and return results as DataFrame.

        Args:
            query: OpenCypher query string
            optimize: Enable optimizations

        Returns:
            PySpark DataFrame with query results

        Raises:
            RuntimeError: If spark session not provided
        """
```

**Example Usage (Basic)**:
```python
from gsql2rsql import GraphContext

# Create context (just 2 table paths!)
# Note: Table names without backticks - SQLRenderer adds them automatically
graph = GraphContext(
    nodes_table="catalog.fraud.nodes",
    edges_table="catalog.fraud.edges",
    extra_node_attrs={"name": str, "risk_score": float},
    extra_edge_attrs={"amount": float}
)

# Set types
graph.set_types(
    node_types=["Person", "Account", "Merchant"],
    edge_types=["TRANSACTION", "OWNS", "LOCATED_AT"]
)

# Transpile query
sql = graph.transpile("""
    MATCH (p:Person)-[:TRANSACTION]->(m:Merchant)
    WHERE p.risk_score > 0.8
    RETURN p.name, m.name
""")

print(sql)
```

**Example Usage (Auto-Discovery)**:
```python
# Auto-discover types and edge combinations from database
graph = GraphContext(
    spark=spark,  # Required for auto-discovery
    nodes_table="catalog.fraud.nodes",
    edges_table="catalog.fraud.edges",
    discover_edge_combinations=True  # Query DB for real combinations!
)

# Types auto-discovered - ready to use
df = graph.execute("""
    MATCH path = (a:Person)-[:TRANSACTION*1..3]->(b:Account)
    WHERE b.risk_score > 0.9
    RETURN b.id, length(path) AS depth
    ORDER BY depth
""")
df.show()
```

**Performance Tip**: For large graphs with many node/edge types, use `discover_edge_combinations=True` to only create schemas for **actual** edge combinations in your data. For example, if you have 10 node types × 5 edge types = 500 possible schemas, but only 15 combinations exist in your data, this creates only 15 schemas (33x faster!).

---

### Full Example: Transpilation Pipeline

```python
from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import NodeSchema, EdgeSchema, EntityProperty
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor

# 1. Define schema (SimpleSQLSchemaProvider)
schema = SimpleSQLSchemaProvider()

person = NodeSchema(
    name="Person",
    node_id_property=EntityProperty("id", int),
    properties=[
        EntityProperty("id", int),
        EntityProperty("name", str),
        EntityProperty("age", int),
    ],
)
schema.add_node(
    person,
    SQLTableDescriptor(table_name="graph.Person", node_id_columns=["id"]),
)

knows = EdgeSchema(
    name="KNOWS",
    source_node="Person",
    sink_node="Person",
    source_id_property=EntityProperty("source_id", int),
    sink_id_property=EntityProperty("target_id", int),
    properties=[],
)
schema.add_edge(
    knows,
    SQLTableDescriptor(
        table_name="graph.Knows",
        source_id_columns=["source_id"],
        sink_id_columns=["target_id"],
    ),
)

# 2. Parse query
query = "MATCH (p:Person)-[:KNOWS]->(f:Person) WHERE p.age > 30 RETURN p.name, f.name"
parser = OpenCypherParser()
ast = parser.parse(query)

# 3. Create logical plan
plan = LogicalPlan.process_query_tree(ast, schema)

# 4. Optimize (optional)
from gsql2rsql.planner.subquery_optimizer import SubqueryFlatteningOptimizer
optimizer = SubqueryFlatteningOptimizer(enabled=True)
optimizer.optimize(plan)

# 5. Resolve columns
plan.resolve(original_query=query)

# 6. Render SQL
renderer = SQLRenderer(db_schema_provider=schema)
sql = renderer.render_plan(plan)

print(sql)
```

---

## Schema Definition via Python API

You can define schemas programmatically using Python **dataclasses** instead of JSON files.

### Using Dataclasses (Recommended)

```python
from gsql2rsql.common.schema import NodeSchema, EdgeSchema, EntityProperty
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor

# Create schema provider (SimpleSQLSchemaProvider)
schema = SimpleSQLSchemaProvider()

# Define nodes
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

# Define edges
works_at = EdgeSchema(
    name="WORKS_AT",
    source_node_id="Person",
    sink_node_id="Company",
    properties=[
        EntityProperty(property_name="since", data_type=int),
    ],
    source_id_property=EntityProperty(property_name="person_id", data_type=int),
    sink_id_property=EntityProperty(property_name="company_id", data_type=int)
)

# Add to schema with SQL table mappings
schema.add_node(
    person,
    SQLTableDescriptor(
        table_name="catalog.mydb.Person",
        node_id_columns=["id"],
    )
)
schema.add_node(
    company,
    SQLTableDescriptor(
        table_name="catalog.mydb.Company",
        node_id_columns=["id"],
    )
)
schema.add_edge(
    works_at,
    SQLTableDescriptor(
        entity_id="Person@WORKS_AT@Company",
        table_name="catalog.mydb.PersonWorksAt",
    )
)

# Transpile
from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer

query = """
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WHERE c.industry = 'Technology'
RETURN p.name, c.name
"""

parser = OpenCypherParser()
ast = parser.parse(query)

plan = LogicalPlan.process_query_tree(ast, schema)
plan.resolve(original_query=query)

renderer = SQLRenderer(db_schema_provider=schema)
sql = renderer.render_plan(plan)
print(sql)
```

### Using Dictionary (Alternative)

You can also pass dictionaries directly:

```python
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider

schema_dict = {
    "nodes": [
        {
            "name": "Person",
            "idProperty": {"name": "id", "type": "int"},
            "tableName": "catalog.mydb.Person",
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
            "tableName": "catalog.mydb.Knows",
            "sourceIdProperty": {"name": "person_id", "type": "int"},
            "sinkIdProperty": {"name": "friend_id", "type": "int"},
            "properties": []
        }
    ]
}

# Convert to schema objects
# (Implementation depends on your schema loader)
```

### Benefits of Python API

- **Type Safety**: Catch errors at development time
- **IDE Support**: Autocompletion and type hints
- **Dynamic Generation**: Generate schemas from database introspection
- **Validation**: Built-in validation in dataclass `__post_init__`
- **Composability**: Reuse schema components across projects

### Pydantic Support (Future)

While we currently use Python's built-in `dataclasses`, we're considering adding Pydantic models for enhanced validation and serialization in future versions. The API would be similar:

```python
# Future Pydantic API (not yet implemented)
from gsql2rsql.schema import NodeModel, EdgeModel

person = NodeModel(
    name="Person",
    properties={"id": int, "name": str, "age": int},
    id_property="id"
)
```

---

## CLI Reference

### Installation

```bash
# Install package
pip install gsql2rsql  # (INFERRED - when published)

# Or install from source
cd cyper2dsql/python
pip install -e .

# Verify installation
gsql2rsql --version
```

### Main Command

```bash
gsql2rsql [OPTIONS] COMMAND [ARGS]...
```

**Global Options**:
- `--help` — Show help message
- `--version` — Show version information

---

### Command: `transpile`

**Purpose**: Transpile OpenCypher query to Databricks SQL

**Usage**:
```bash
gsql2rsql transpile [OPTIONS]
```

**Options**:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `-s, --schema PATH` | File path | *Required* | JSON schema file |
| `-i, --input PATH` | File path | stdin | Input Cypher query file |
| `-o, --output PATH` | File path | stdout | Output SQL file |
| `--optimize / --no-optimize` | Flag | `--optimize` | Enable/disable subquery optimization |
| `--resolve / --no-resolve` | Flag | `--resolve` | Enable/disable column resolution |
| `--explain-scopes` | Flag | Off | Show scope information in output |
| `--format` | Choice | `sql` | Output format: `sql`, `ast`, `plan` |

**Examples**:

```bash
# Transpile from stdin to stdout
echo "MATCH (n:Person) RETURN n" | gsql2rsql transpile -s schema.json

# Transpile from file
gsql2rsql transpile -s schema.json -i query.cypher -o output.sql

# Disable optimization
gsql2rsql transpile -s schema.json --no-optimize < query.cypher

# Show AST instead of SQL
gsql2rsql transpile -s schema.json --format ast < query.cypher

# Show scope debugging info
gsql2rsql transpile -s schema.json --explain-scopes < query.cypher
```

---

### Command: `parse`

**Purpose**: Parse OpenCypher query to AST (no transpilation)

**Usage**:
```bash
gsql2rsql parse [OPTIONS]
```

**Options**:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `-i, --input PATH` | File path | stdin | Input Cypher query file |
| `-o, --output PATH` | File path | stdout | Output AST file |
| `--format` | Choice | `tree` | Output format: `tree`, `json` |

**Examples**:

```bash
# Parse and show AST tree
echo "MATCH (n) RETURN n" | gsql2rsql parse

# Parse from file
gsql2rsql parse -i query.cypher

# Output as JSON
gsql2rsql parse --format json < query.cypher
```

---

### Command: `init-schema`

**Purpose**: Generate schema template

**Usage**:
```bash
gsql2rsql init-schema [OPTIONS]
```

**Options**:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `-o, --output PATH` | File path | stdout | Output schema file |
| `--example` | Choice | `simple` | Example type: `simple`, `movie`, `social` |

**Examples**:

```bash
# Generate simple schema template
gsql2rsql init-schema > my_schema.json

# Generate movie graph schema
gsql2rsql init-schema --example movie -o movie_schema.json
```

---

### Command: `tui`

**Purpose**: Launch interactive TUI (Text User Interface)

**Usage**:
```bash
gsql2rsql tui [OPTIONS]
```

**Options**:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--examples PATH` | File path | None | YAML examples file |
| `--schema PATH` | File path | None | Default JSON schema file |

**Examples**:

```bash
# Launch TUI with examples
gsql2rsql tui --examples examples/credit_queries.yaml

# Launch TUI with custom schema
gsql2rsql tui --schema my_schema.json --examples examples/my_queries.yaml
```

**TUI Features**:
- Browse curated examples from YAML files
- Live transpilation as you type
- Syntax highlighting for Cypher and SQL
- Copy SQL to clipboard
- Switch between schemas
- View AST, logical plan, and scope info
- Error highlighting with suggestions

---

## Schema Format

### JSON Schema Structure

```json
{
  "nodes": [
    {
      "name": "NodeLabel",
      "tableName": "catalog.schema.Table",
      "idProperty": {
        "name": "id_column",
        "type": "int"
      },
      "properties": [
        {"name": "property1", "type": "string"},
        {"name": "property2", "type": "int"}
      ]
    }
  ],
  "edges": [
    {
      "name": "RELATIONSHIP_TYPE",
      "sourceNode": "SourceLabel",
      "sinkNode": "SinkLabel",
      "tableName": "catalog.schema.EdgeTable",
      "sourceIdProperty": {
        "name": "source_id_column",
        "type": "int"
      },
      "sinkIdProperty": {
        "name": "sink_id_column",
        "type": "int"
      },
      "properties": [
        {"name": "edge_property", "type": "float"}
      ]
    }
  ]
}
```

### Supported Property Types

| Type | Python Type | Databricks SQL Type |
|------|-------------|---------------------|
| `"int"` | `int` | `BIGINT` |
| `"string"` | `str` | `STRING` |
| `"float"` | `float` | `DOUBLE` |
| `"boolean"` | `bool` | `BOOLEAN` |
| `"date"` (INFERRED) | Date | `DATE` |
| `"timestamp"` (INFERRED) | Datetime | `TIMESTAMP` |

### Example Schemas

**Simple Social Graph**:
```json
{
  "nodes": [
    {
      "name": "Person",
      "tableName": "social.Person",
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
      "tableName": "social.Knows",
      "sourceIdProperty": {"name": "person1_id", "type": "int"},
      "sinkIdProperty": {"name": "person2_id", "type": "int"},
      "properties": [
        {"name": "since", "type": "int"}
      ]
    }
  ]
}
```

**Multi-Node Graph (Credit Risk)**:
```json
{
  "nodes": [
    {
      "name": "Customer",
      "tableName": "credit.Customer",
      "idProperty": {"name": "id", "type": "int"},
      "properties": [
        {"name": "name", "type": "string"},
        {"name": "credit_score", "type": "int"}
      ]
    },
    {
      "name": "Account",
      "tableName": "credit.Account",
      "idProperty": {"name": "id", "type": "int"},
      "properties": [
        {"name": "balance", "type": "float"}
      ]
    }
  ],
  "edges": [
    {
      "name": "HAS_ACCOUNT",
      "sourceNode": "Customer",
      "sinkNode": "Account",
      "tableName": "credit.CustomerAccount",
      "sourceIdProperty": {"name": "customer_id", "type": "int"},
      "sinkIdProperty": {"name": "account_id", "type": "int"},
      "properties": []
    }
  ]
}
```

---

## Configuration Options

### pyproject.toml

**Package Metadata**: [pyproject.toml](https://github.com/devmessias/gsql2rsql/blob/main/python/pyproject.toml)

```toml
[project]
name = "gsql2rsql"
version = "0.1.0"
requires-python = ">=3.12"

[project.dependencies]
antlr4-python3-runtime = ">=4.13.0"
click = ">=8.1.0"
# ... more dependencies

[project.scripts]
gsql2rsql = "gsql2rsql.cli:main"
```

### Environment Variables

**INFERRED - Update if implemented**

| Variable | Purpose | Default |
|----------|---------|---------|
| `GSQL2RSQL_SCHEMA` | Default schema path | None |
| `GSQL2RSQL_LOG_LEVEL` | Logging level | `INFO` |
| `GSQL2RSQL_CACHE_DIR` | Cache directory | `~/.gsql2rsql/cache` |

---

## Exception Types

### `TranspilerSyntaxErrorException`

**Module**: `gsql2rsql.common.exceptions`

**Raised when**: Parser encounters syntax error in Cypher query

**Attributes**:
- `message: str` — Error description
- `line: int | None` — Line number in query
- `column: int | None` — Column number in query
- `query: str | None` — Original query

**Example**:
```python
from gsql2rsql import OpenCypherParser
from gsql2rsql.common.exceptions import TranspilerSyntaxErrorException

parser = OpenCypherParser()
try:
    ast = parser.parse("MATCH (n RETURN n")  # Missing closing paren
except TranspilerSyntaxErrorException as e:
    print(f"Syntax error at line {e.line}, column {e.column}: {e.message}")
```

---

### `TranspilerBindingException`

**Module**: `gsql2rsql.common.exceptions`

**Raised when**: Schema binding fails (e.g., undefined node label)

**Attributes**:
- `message: str` — Error description
- `entity_name: str | None` — Entity that failed to bind

**Example**:
```python
from gsql2rsql import LogicalPlan
from gsql2rsql.common.exceptions import TranspilerBindingException

try:
    plan = LogicalPlan.process_query_tree(ast, schema)
except TranspilerBindingException as e:
    print(f"Schema binding failed: {e.message}")
    print(f"Entity: {e.entity_name}")
```

---

### `ColumnResolutionError`

**Module**: `gsql2rsql.common.exceptions`

**Raised when**: Column reference cannot be resolved

**Attributes**:
- `message: str` — Error description
- `column_name: str | None` — Column that failed to resolve
- `suggestions: list[str]` — Suggested column names (Levenshtein distance)

**Example**:
```python
from gsql2rsql.common.exceptions import ColumnResolutionError

try:
    plan.resolve(original_query=query)
except ColumnResolutionError as e:
    print(f"Column resolution failed: {e.message}")
    if e.suggestions:
        print(f"Did you mean: {', '.join(e.suggestions)}?")
```

---

### `TranspilerNotSupportedException`

**Module**: `gsql2rsql.common.exceptions`

**Raised when**: Unsupported Cypher feature is used

**Attributes**:
- `message: str` — Error description
- `feature: str | None` — Unsupported feature name

**Example**:
```python
from gsql2rsql.common.exceptions import TranspilerNotSupportedException

try:
    sql = renderer.render_plan(plan)
except TranspilerNotSupportedException as e:
    print(f"Unsupported feature: {e.feature}")
    print(f"Details: {e.message}")
```

---

## Where to Look Next

- [user-guide.md](user-guide.md) — Usage examples
- [architecture.md](architecture.md) — Component details
- [contributing.md](contributing.md) — Extending the API
- [Examples](examples/index.md) — Example schemas and queries
