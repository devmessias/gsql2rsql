# Adding Examples to gsql2rsql TUI

This document explains how to add new query examples to the TUI (Text User Interface) example browser.

## YAML File Structure

Each example category is defined in a separate YAML file in the `examples/` directory. The file has two main sections:

1. **schema** - Defines the graph schema (nodes and edges) for the examples
2. **examples** - List of query examples with metadata

### Basic Structure

```yaml
# Category description comment
# These examples demonstrate [category purpose]

# Schema definition for this category
schema:
  nodes:
    - name: NodeLabel
      tableName: catalog.schema.TableName
      idProperty: { name: id, type: int }
      properties:
        - { name: propertyName, type: string }

  edges:
    - name: EDGE_NAME
      sourceNode: SourceNodeLabel
      sinkNode: SinkNodeLabel
      tableName: catalog.schema.EdgeTableName
      sourceIdProperty: { name: source_id, type: int }
      sinkIdProperty: { name: sink_id, type: int }
      properties:
        - { name: edgeProperty, type: float }

# Query examples
examples:
  - description: "Short description of what the query does"
    application: "Category: Use case name"
    query: |
      MATCH (n:Node)-[:REL]->(m:Other)
      WHERE n.property = 'value'
      RETURN n.id, m.name
    notes: |
      Detailed explanation of why this query is useful.
      Include computational complexity notes if relevant.
```

## Schema Section

### Node Definition

```yaml
nodes:
  - name: Person                          # Node label (used in MATCH)
    tableName: catalog.schema.Person      # Databricks table (catalog.schema.table)
    idProperty:                           # Primary key column
      name: id
      type: int                           # int, string, float, boolean
    properties:                           # Additional columns
      - { name: name, type: string }
      - { name: age, type: int }
      - { name: score, type: float }
      - { name: active, type: boolean }
```

### Edge Definition

```yaml
edges:
  - name: KNOWS                           # Relationship type (used in MATCH)
    sourceNode: Person                    # Source node label
    sinkNode: Person                      # Target node label
    tableName: catalog.schema.Knows       # Edge table in Databricks
    sourceIdProperty:                     # Foreign key to source node
      name: source_person_id
      type: int
    sinkIdProperty:                       # Foreign key to target node
      name: target_person_id
      type: int
    properties:                           # Edge properties (optional)
      - { name: since, type: int }
      - { name: strength, type: float }
```

### Supported Property Types

| Type | Description | SQL Equivalent |
|------|-------------|----------------|
| `int` | Integer numbers | `INT`, `BIGINT` |
| `integer` | Alias for int | `INT`, `BIGINT` |
| `long` | Large integers | `BIGINT` |
| `float` | Decimal numbers | `FLOAT`, `DOUBLE` |
| `double` | Alias for float | `DOUBLE` |
| `string` | Text values | `STRING`, `VARCHAR` |
| `bool` | Boolean values | `BOOLEAN` |
| `boolean` | Alias for bool | `BOOLEAN` |

## Examples Section

Each example requires these fields:

```yaml
examples:
  - description: "Brief title (shown in table)"     # Required: 1-2 lines
    application: "Category: Specific use case"      # Required: Domain label
    query: |                                        # Required: OpenCypher query
      MATCH (n:Node)
      RETURN n.id
    notes: |                                        # Optional: Detailed explanation
      Explain why this query pattern is useful.
      Include complexity notes if relevant.
```

### Field Guidelines

| Field | Max Length | Purpose |
|-------|------------|---------|
| `description` | ~60 chars | Short title for table display |
| `application` | ~40 chars | Category prefix + use case |
| `query` | No limit | Valid OpenCypher query |
| `notes` | No limit | Detailed explanation, complexity notes |

### Application Format Convention

Use this format: `"Category: Specific use case"`

Examples:
- `"Fraud: Co-shopper detection"`
- `"Credit: Risk scoring"`
- `"Features: Variable-length paths"`

## Creating a New Category

1. **Create the YAML file**:
   ```bash
   touch examples/my_category_queries.yaml
   ```

2. **Define the schema** appropriate for your examples

3. **Add examples** with increasing complexity

4. **Update the CLI** to load your category (in `cli.py`):

   ```python
   # In _load_examples() function:
   my_category_file = examples_dir / "my_category_queries.yaml"
   if my_category_file.exists():
       try:
           with my_category_file.open(encoding="utf-8") as f:
               data = yaml.safe_load(f)
               examples["my_category"] = data.get("examples", [])
               if "schema" in data:
                   schemas["my_category"] = data["schema"]
       except Exception:
           pass
   ```

## Example: Complete YAML File

```yaml
# Simple Graph Examples for Learning
# These examples demonstrate basic OpenCypher patterns

schema:
  nodes:
    - name: Person
      tableName: catalog.demo.Person
      idProperty: { name: id, type: int }
      properties:
        - { name: name, type: string }
        - { name: age, type: int }

    - name: City
      tableName: catalog.demo.City
      idProperty: { name: id, type: int }
      properties:
        - { name: name, type: string }
        - { name: population, type: int }

  edges:
    - name: LIVES_IN
      sourceNode: Person
      sinkNode: City
      tableName: catalog.demo.LivesIn
      sourceIdProperty: { name: person_id, type: int }
      sinkIdProperty: { name: city_id, type: int }

    - name: KNOWS
      sourceNode: Person
      sinkNode: Person
      tableName: catalog.demo.Knows
      sourceIdProperty: { name: person_id, type: int }
      sinkIdProperty: { name: friend_id, type: int }
      properties:
        - { name: since, type: int }

examples:
  - description: "Find all people"
    application: "Basics: Simple node lookup"
    query: |
      MATCH (p:Person)
      RETURN p.name, p.age
    notes: |
      The simplest possible query - retrieves all nodes of a type.
      Complexity: O(n) where n is the number of Person nodes.
      Cost: Single table scan, very efficient.

  - description: "Find people by name filter"
    application: "Basics: Property filtering"
    query: |
      MATCH (p:Person)
      WHERE p.name = 'Alice'
      RETURN p.name, p.age
    notes: |
      Filters nodes by property value.
      Complexity: O(n) without index, O(log n) with index.
      Cost: Low - uses WHERE clause pushdown.

  - description: "Find friends of friends"
    application: "Basics: Multi-hop traversal"
    query: |
      MATCH (p:Person)-[:KNOWS]->(friend:Person)-[:KNOWS]->(fof:Person)
      WHERE p.name = 'Alice' AND p <> fof
      RETURN DISTINCT fof.name
    notes: |
      Two-hop graph traversal with cycle prevention.
      Complexity: O(k^2) where k is average degree.
      Cost: Multiple JOINs - use indexes on foreign keys.
```

## Best Practices

1. **Order examples by complexity**: Start with simple queries, progress to complex ones

2. **Use realistic scenarios**: Examples should represent real-world use cases

3. **Include complexity notes**: Help users understand performance implications

4. **Test all queries**: Ensure every query transpiles correctly before adding

5. **Use consistent naming**: Follow the existing naming conventions in the codebase

6. **Document edge cases**: Note any limitations or special behaviors

## Testing Your Examples

Run the TUI to verify your examples work:

```bash
gsql2rsql tui
```

Then press `R` to run all queries and verify they transpile without errors.

## Existing Categories

| Category | File | Examples | Focus |
|----------|------|----------|-------|
| Fraud | `fraud_queries.yaml` | 17 | Fraud detection patterns |
| Credit | `credit_queries.yaml` | 15 | Credit risk analysis |
| Features | `features_queries.yaml` | 19 | Library feature showcase |
