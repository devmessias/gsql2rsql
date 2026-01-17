# 15 – DISTINCT Rows

## Cypher

```cypher
MATCH (p:Person)-[:KNOWS]->(f:Person)
RETURN DISTINCT f.name
```

## What this query does

This query finds all unique friend names in the graph, removing duplicates. If multiple persons know someone with the same name, that name appears only once in the results.

Key aspects:
- Matches `KNOWS` relationships
- Projects friend names
- `DISTINCT` removes duplicate rows
- No aggregation or GROUP BY involved

## Expected Databricks SQL

```sql
SELECT DISTINCT
  f.name AS name
FROM graph.Person p
INNER JOIN graph.Knows r ON p.id = r.source_id
INNER JOIN graph.Person f ON r.target_id = f.id
```

## Additional Test Cases

### DISTINCT with multiple columns

```cypher
MATCH (p:Person)-[:KNOWS]->(f:Person)
RETURN DISTINCT f.name, f.age
```

Expected SQL:
```sql
SELECT DISTINCT
  f.name AS name,
  f.age AS age
FROM graph.Person p
INNER JOIN graph.Knows r ON p.id = r.source_id
INNER JOIN graph.Person f ON r.target_id = f.id
```

Distinctness is based on the combination of all returned columns.

### DISTINCT with ORDER BY

```cypher
MATCH (p:Person)-[:KNOWS]->(f:Person)
RETURN DISTINCT f.name
ORDER BY f.name ASC
```

Expected SQL:
```sql
SELECT DISTINCT
  f.name AS name
FROM graph.Person p
INNER JOIN graph.Knows r ON p.id = r.source_id
INNER JOIN graph.Person f ON r.target_id = f.id
ORDER BY f.name ASC
```

### DISTINCT with LIMIT

```cypher
MATCH (p:Person)-[:KNOWS]->(f:Person)
RETURN DISTINCT f.name
LIMIT 10
```

Combines deduplication with result limiting.

### Multi-edge-type test

```cypher
MATCH (p:Person)-[:KNOWS|FOLLOWS]->(f:Person)
RETURN DISTINCT f.name
```

## Operator Analysis

**Current operators are sufficient:**
- ✅ `ProjectionOperator` has `is_distinct` flag
- ✅ Renders as `SELECT DISTINCT` in SQL

**No new operator needed.** The existing `ProjectionOperator` already supports DISTINCT:
```python
@dataclass
class ProjectionOperator(UnaryLogicalOperator):
    projections: list[tuple[str, QueryExpression]]
    is_distinct: bool = False  # ← Already exists!
    order_by: list[tuple[QueryExpression, bool]]
    limit: int | None
    skip: int | None
```

## Notes

- **Recursion required**: No
- **Ordering semantics**: No implicit ordering; use ORDER BY for deterministic results
- **Null handling**: NULL values are considered distinct values
- **Multi-edge applicability**: Yes
- **Performance consideration**:
  - DISTINCT requires sorting or hashing
  - For large result sets, consider if deduplication is necessary
  - DISTINCT on entire rows is less expensive than on computed expressions
- **Key semantics**:
  - DISTINCT applies to the entire result row
  - Two rows are duplicates if ALL columns match
  - DISTINCT can be combined with ORDER BY, LIMIT, SKIP
