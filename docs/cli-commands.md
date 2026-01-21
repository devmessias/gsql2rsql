# CLI Commands Reference

The `gsql2rsql` CLI provides commands for transpiling OpenCypher queries to SQL.

---

## Installation Verification

Check if gsql2rsql is installed correctly:

```bash
gsql2rsql --version
```

---

## Main Commands

### `translate`

Transpile an OpenCypher query to SQL.

**Usage:**

```bash
gsql2rsql translate --schema SCHEMA_FILE [OPTIONS] < query.cypher
```

**Arguments:**

- `--schema PATH` (required): Path to the schema JSON file that defines your graph-to-table mapping

**Options:**

- `--output PATH, -o PATH`: Write SQL output to a file instead of stdout
- `--format {spark,postgres,generic}`: Target SQL dialect (default: spark)
- `--indent N`: Number of spaces for SQL indentation (default: 2)
- `--debug`: Enable debug logging
- `--no-optimize`: Disable query optimization passes

**Examples:**

Basic translation:
```bash
gsql2rsql translate --schema my_schema.json < query.cypher
```

Save to file:
```bash
gsql2rsql translate --schema my_schema.json -o output.sql < query.cypher
```

From file with piping:
```bash
cat query.cypher | gsql2rsql translate --schema my_schema.json
```

With debug output:
```bash
gsql2rsql translate --schema my_schema.json --debug < query.cypher
```

---

### `validate`

Validate a schema file without transpiling.

**Usage:**

```bash
gsql2rsql validate --schema SCHEMA_FILE
```

**Arguments:**

- `--schema PATH` (required): Path to the schema JSON file to validate

**Example:**

```bash
gsql2rsql validate --schema my_schema.json
```

**Output:**
```
✓ Schema is valid
- 3 node types defined
- 2 edge types defined
- All references resolved
```

---

### `explain`

Show the query plan for an OpenCypher query.

**Usage:**

```bash
gsql2rsql explain --schema SCHEMA_FILE [OPTIONS] < query.cypher
```

**Arguments:**

- `--schema PATH` (required): Path to the schema JSON file

**Options:**

- `--format {text,json}`: Output format (default: text)
- `--verbose`: Show detailed operator information

**Example:**

```bash
gsql2rsql explain --schema my_schema.json < query.cypher
```

**Output:**
```
Query Plan:
├─ ProjectionOperator
│  └─ FilterOperator (condition: c.industry = 'Technology')
│     └─ JoinOperator (INNER)
│        ├─ JoinOperator (INNER)
│        │  ├─ DataSourceOperator (Person)
│        │  └─ DataSourceOperator (PersonWorksAt)
│        └─ DataSourceOperator (Company)
```

---

## Schema File Format

The schema file is a JSON document that maps your graph model to SQL tables.

**Structure:**

```json
{
  "nodes": [
    {
      "name": "NodeLabel",
      "tableName": "catalog.schema.table_name",
      "idProperty": {"name": "id_column", "type": "int"},
      "properties": [
        {"name": "prop_name", "type": "string"},
        {"name": "prop_name2", "type": "int"}
      ]
    }
  ],
  "edges": [
    {
      "name": "RELATIONSHIP_TYPE",
      "sourceNode": "SourceNodeLabel",
      "sinkNode": "SinkNodeLabel",
      "tableName": "catalog.schema.relationship_table",
      "sourceIdProperty": {"name": "source_id_column", "type": "int"},
      "sinkIdProperty": {"name": "sink_id_column", "type": "int"},
      "properties": [
        {"name": "prop_name", "type": "string"}
      ]
    }
  ]
}
```

**Property Types:**

Supported types for node and edge properties:

- `string` - Text data (maps to SQL VARCHAR/STRING)
- `int` - Integer numbers (maps to SQL INT/BIGINT)
- `long` - Large integers (maps to SQL BIGINT)
- `float` - Floating point numbers (maps to SQL FLOAT)
- `double` - Double precision floats (maps to SQL DOUBLE)
- `boolean` - True/false values (maps to SQL BOOLEAN)
- `date` - Date values (maps to SQL DATE)
- `timestamp` - Date and time (maps to SQL TIMESTAMP)

---

## Environment Variables

### `GSQL2RSQL_LOG_LEVEL`

Set the logging level.

**Values:** `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

**Default:** `INFO`

**Example:**

```bash
export GSQL2RSQL_LOG_LEVEL=DEBUG
gsql2rsql translate --schema my_schema.json < query.cypher
```

---

## Exit Codes

- `0` - Success
- `1` - General error
- `2` - Schema validation error
- `3` - Query parsing error
- `4` - Query planning error
- `5` - SQL rendering error

---

## Common Workflows

### Batch Processing

Process multiple queries:

```bash
for query in queries/*.cypher; do
  echo "Processing $query..."
  gsql2rsql translate --schema schema.json < "$query" > "sql/$(basename $query .cypher).sql"
done
```

### Integration with PySpark

```python
from pyspark.sql import SparkSession
import subprocess

spark = SparkSession.builder.appName("gsql2rsql").getOrCreate()

# Transpile query
cypher_query = "MATCH (p:Person) RETURN p.name"
result = subprocess.run(
    ["gsql2rsql", "translate", "--schema", "schema.json"],
    input=cypher_query.encode(),
    capture_output=True
)

sql_query = result.stdout.decode()

# Execute on Spark
df = spark.sql(sql_query)
df.show()
```

### Testing Generated SQL

Validate generated SQL syntax:

```bash
# Using Spark SQL parser
spark-sql -e "$(gsql2rsql translate --schema schema.json < query.cypher)" --dry-run

# Or save and execute
gsql2rsql translate --schema schema.json < query.cypher > output.sql
spark-sql -f output.sql
```

---

## Troubleshooting

### Schema Validation Errors

If you get schema validation errors:

1. Ensure all node/edge names are unique
2. Check that all edge source/sink nodes reference defined node types
3. Verify table names are fully qualified (catalog.schema.table)
4. Ensure ID property types match between nodes and edges

### Query Parsing Errors

If your OpenCypher query fails to parse:

1. Check that your query uses supported Cypher syntax
3. Use `--debug` flag to see detailed parsing information

### SQL Rendering Errors

If transpilation succeeds but SQL execution fails:

1. Verify table names in schema match actual tables in your database
2. Check that column names and types match
3. Ensure catalog/schema names are accessible
4. Review generated SQL for any obvious issues

---

## See Also

- [Query Translation Guide](architecture.md) - How transpilation works
- [API Reference](api-reference.md) - Python API documentation
- [Examples](examples/index.md) - Real-world query examples
