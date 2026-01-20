# Installation and Quick Start

This guide covers installing gsql2rsql and getting started with your first query.

---

## Requirements

- **Python**: 3.12 or later
- **Databricks Runtime**: 17+ (for executing generated SQL - only needed at runtime)

**Core Dependencies** (automatically installed):
- ANTLR4 runtime
- Click (CLI framework)
- Prompt Toolkit
- PyYAML
- Rich & Textual (TUI)

**Note**: PySpark is **NOT required** to use gsql2rsql. It's only a dev dependency for running validation tests.

---

## Install from PyPI

Once published, install using pip:

```bash
pip install gsql2rsql
```

---

## Install from Source

### Using uv (Recommended)

[uv](https://github.com/astral-sh/uv) is a fast Python package manager:

```bash
# Clone repository
git clone https://github.com/devmessias/gsql2rsql
cd gsql2rsql/python

# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install
uv sync
uv pip install -e .
```

### Using pip

```bash
# Clone repository
git clone https://github.com/devmessias/gsql2rsql
cd gsql2rsql/python

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in editable mode
pip install -e .
```

---

## Development Installation

For development with all testing dependencies:

```bash
# Using uv
uv sync --extra dev
uv pip install -e ".[dev]"

# Or using pip
pip install -e ".[dev]"
```

This installs additional tools:
- **pytest**: Testing framework
- **ruff**: Linting and formatting
- **mypy**: Type checking
- **PySpark**: For validation tests

---

## Verify Installation

Check that gsql2rsql is installed:

```bash
gsql2rsql --version
```

Test transpilation:

```bash
echo "MATCH (p:Person) RETURN p.name" | gsql2rsql translate --schema examples/schema.json
```

---

## Development Only: PySpark Setup

**PySpark is only needed if you're contributing to the project and want to run validation tests locally.**

Regular users do NOT need PySpark - the transpiler works without it.

### Install Development Dependencies

```bash
# Install all dev dependencies including PySpark
pip install -e ".[dev]"
```

### Install Java (PySpark requirement)

PySpark requires Java 8 or 11:

```bash
# Ubuntu/Debian
sudo apt-get install openjdk-11-jdk

# macOS
brew install openjdk@11
```

### Verify PySpark

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

### Run Validation Tests

```bash
# Run tests without PySpark (fast)
make test-no-pyspark

# Run PySpark validation tests (slower)
make test-pyspark
```

---

## Your First Query

This tutorial walks through transpiling your first OpenCypher query to SQL.

### Step 1: Create a Schema

gsql2rsql needs a schema that maps your graph to SQL tables. You can use Python dataclasses or JSON format.

#### Using Python (Recommended)

```python
from gsql2rsql.common.schema import SimpleGraphSchemaProvider, NodeSchema, EdgeSchema, EntityProperty
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor

# Create graph schema provider (for logical planner)
graph_schema = SimpleGraphSchemaProvider()

# Define Person node
person = NodeSchema(
    name="Person",
    properties=[
        EntityProperty(property_name="id", data_type=int),
        EntityProperty(property_name="name", data_type=str),
        EntityProperty(property_name="age", data_type=int),
    ],
    node_id_property=EntityProperty(property_name="id", data_type=int)
)

# Define Company node
company = NodeSchema(
    name="Company",
    properties=[
        EntityProperty(property_name="id", data_type=int),
        EntityProperty(property_name="name", data_type=str),
        EntityProperty(property_name="industry", data_type=str),
    ],
    node_id_property=EntityProperty(property_name="id", data_type=int)
)

# Define WORKS_AT relationship
works_at = EdgeSchema(
    name="WORKS_AT",
    source_node_id="Person",
    sink_node_id="Company",
    source_id_property=EntityProperty(property_name="person_id", data_type=int),
    sink_id_property=EntityProperty(property_name="company_id", data_type=int),
    properties=[
        EntityProperty(property_name="since", data_type=int)
    ]
)

# Add to graph schema
graph_schema.add_node(person)
graph_schema.add_node(company)
graph_schema.add_edge(works_at)

# Create SQL schema provider (maps to Delta tables)
sql_schema = SimpleSQLSchemaProvider()

sql_schema.add_node(
    person,
    SQLTableDescriptor(
        table_name="Person",  # or "catalog.schema.Person" for Databricks
        node_id_columns=["id"],
    )
)

sql_schema.add_node(
    company,
    SQLTableDescriptor(
        table_name="Company",
        node_id_columns=["id"],
    )
)

sql_schema.add_edge(
    works_at,
    SQLTableDescriptor(
        entity_id="Person@WORKS_AT@Company",
        table_name="WorksAt",
    )
)
```

#### Using JSON Schema

Alternatively, create `my_schema.json`:

```json
{
  "nodes": [
    {
      "name": "Person",
      "tableName": "catalog.mydb.Person",
      "idProperty": {"name": "id", "type": "int"},
      "properties": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
      ]
    },
    {
      "name": "Company",
      "tableName": "catalog.mydb.Company",
      "idProperty": {"name": "id", "type": "int"},
      "properties": [
        {"name": "name", "type": "string"},
        {"name": "industry", "type": "string"}
      ]
    }
  ],
  "edges": [
    {
      "name": "WORKS_AT",
      "sourceNode": "Person",
      "sinkNode": "Company",
      "tableName": "catalog.mydb.PersonWorksAt",
      "sourceIdProperty": {"name": "person_id", "type": "int"},
      "sinkIdProperty": {"name": "company_id", "type": "int"},
      "properties": [
        {"name": "since", "type": "int"}
      ]
    }
  ]
}
```

### Step 2: Write Your Query

Create a Cypher query in `my_query.cypher`:

```cypher
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WHERE c.industry = 'Technology'
RETURN p.name, p.age, c.name AS company
ORDER BY p.age DESC
LIMIT 10
```

### Step 3: Transpile to SQL

#### Using Python API

```python
from gsql2rsql.parser.opencypher_parser import OpenCypherParser
from gsql2rsql.planner.logical_plan import LogicalPlan
from gsql2rsql.renderer.sql_renderer import SQLRenderer

# Setup (using schemas from Step 1)
parser = OpenCypherParser()
renderer = SQLRenderer(db_schema_provider=sql_schema)

# Parse and transpile
query = """
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WHERE c.industry = 'Technology'
RETURN p.name, p.age, c.name AS company
ORDER BY p.age DESC
LIMIT 10
"""

ast = parser.parse(query)
plan = LogicalPlan.process_query_tree(ast, graph_schema)
plan.resolve(original_query=query)
sql = renderer.render_plan(plan)

print(sql)
```

#### Using CLI

```bash
gsql2rsql translate --schema my_schema.json < my_query.cypher
```

**Output:**
```sql
SELECT
   _gsql2rsql_p_name AS name
  ,_gsql2rsql_p_age AS age
  ,_gsql2rsql_c_name AS company
FROM (
  SELECT
     _left._gsql2rsql_p_id AS _gsql2rsql_p_id
    ,_left._gsql2rsql_p_name AS _gsql2rsql_p_name
    ,_left._gsql2rsql_p_age AS _gsql2rsql_p_age
    ,_left._gsql2rsql__anon1_person_id AS _gsql2rsql__anon1_person_id
    ,_left._gsql2rsql__anon1_company_id AS _gsql2rsql__anon1_company_id
    ,_right._gsql2rsql_c_id AS _gsql2rsql_c_id
    ,_right._gsql2rsql_c_name AS _gsql2rsql_c_name
    ,_right._gsql2rsql_c_industry AS _gsql2rsql_c_industry
  FROM (
    SELECT
       _left._gsql2rsql_p_id AS _gsql2rsql_p_id
      ,_left._gsql2rsql_p_name AS _gsql2rsql_p_name
      ,_left._gsql2rsql_p_age AS _gsql2rsql_p_age
      ,_right._gsql2rsql__anon1_person_id AS _gsql2rsql__anon1_person_id
      ,_right._gsql2rsql__anon1_company_id AS _gsql2rsql__anon1_company_id
    FROM (
      SELECT
         id AS _gsql2rsql_p_id
        ,name AS _gsql2rsql_p_name
        ,age AS _gsql2rsql_p_age
      FROM
        catalog.mydb.Person
    ) AS _left
    INNER JOIN (
      SELECT
         person_id AS _gsql2rsql__anon1_person_id
        ,company_id AS _gsql2rsql__anon1_company_id
      FROM
        catalog.mydb.PersonWorksAt
    ) AS _right
    ON (_left._gsql2rsql_p_id) = (_right._gsql2rsql__anon1_person_id)
  ) AS _left
  INNER JOIN (
    SELECT
       id AS _gsql2rsql_c_id
      ,name AS _gsql2rsql_c_name
      ,industry AS _gsql2rsql_c_industry
    FROM
      catalog.mydb.Company
  ) AS _right
  ON (_left._gsql2rsql__anon1_company_id) = (_right._gsql2rsql_c_id)
  WHERE (_gsql2rsql_c_industry) = ('Technology')
) AS _proj
ORDER BY _gsql2rsql_p_age DESC
LIMIT 10
```

### Step 4: Execute on Databricks

Save the SQL to a file and execute it:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("gsql2rsql").getOrCreate()

# Read generated SQL
with open("output.sql") as f:
    sql = f.read()

# Execute
result = spark.sql(sql)
result.show()
```

### Understanding the Output

The generated SQL:

1. **Reads from tables**: `catalog.mydb.Person`, `catalog.mydb.PersonWorksAt`, `catalog.mydb.Company`
2. **Projects columns** with prefixed names (e.g., `_gsql2rsql_p_name`)
3. **Joins tables** based on relationship IDs
4. **Applies WHERE filter** on `c.industry = 'Technology'`
5. **Orders and limits** results

The prefixed column names (`_gsql2rsql_*`) avoid collisions with user column names.

---

## Next Steps

- [**Examples Gallery**](examples/index.md): See real-world queries
- [**Python API Reference**](api-reference.md): Full API documentation
- [**CLI Commands**](cli-commands.md): Command reference
