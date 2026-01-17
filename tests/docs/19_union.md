# 19 – UNION

## Cypher

```cypher
MATCH (p:Person)
RETURN p.name AS name
UNION
MATCH (c:City)
RETURN c.name AS name
```

## What this query does

This query combines results from two separate queries using UNION, which removes duplicates. It returns names from both Person and City nodes, deduplicated.

Key aspects:
- Two independent MATCH clauses
- UNION combines results and removes duplicates
- Both queries must return same number and types of columns
- Column names from first query are used

## Expected Databricks SQL

```sql
SELECT
  p.name AS name
FROM graph.Person p

UNION

SELECT
  c.name AS name
FROM graph.City c
```

## Additional Test Cases

### UNION ALL (keeps duplicates)

```cypher
MATCH (p:Person)
RETURN p.name AS name
UNION ALL
MATCH (c:City)
RETURN c.name AS name
```

Expected SQL:
```sql
SELECT
  p.name AS name
FROM graph.Person p

UNION ALL

SELECT
  c.name AS name
FROM graph.City c
```

**UNION ALL** is faster (no deduplication needed).

### UNION with multiple columns

```cypher
MATCH (p:Person)
RETURN p.name AS name, 'Person' AS type
UNION
MATCH (c:City)
RETURN c.name AS name, 'City' AS type
```

Expected SQL:
```sql
SELECT
  p.name AS name,
  'Person' AS type
FROM graph.Person p

UNION

SELECT
  c.name AS name,
  'City' AS type
FROM graph.City c
```

### UNION with ORDER BY and LIMIT

```cypher
MATCH (p:Person)
RETURN p.name AS name
UNION
MATCH (c:City)
RETURN c.name AS name
ORDER BY name
LIMIT 10
```

Expected SQL:
```sql
(
  SELECT
    p.name AS name
  FROM graph.Person p

  UNION

  SELECT
    c.name AS name
  FROM graph.City c
)
ORDER BY name
LIMIT 10
```

**Important**: ORDER BY and LIMIT apply to entire UNION result, must wrap in parentheses.

### Multiple UNION

```cypher
MATCH (p:Person)
RETURN p.name
UNION
MATCH (c:City)
RETURN c.name
UNION
MATCH (m:Movie)
RETURN m.title
```

Chains multiple UNIONs.

## Operator Analysis

**Current operator status:**
- ✅ `SetOperator` exists with `SetOperationType.UNION` and `UNION_ALL`
- ✅ Already handles this pattern

**From operators.py:**
```python
class SetOperator(BinaryLogicalOperator):
    operation: SetOperationType  # UNION, UNION_ALL, INTERSECT, EXCEPT
```

**No new operator needed!** SetOperator already supports UNION.

**Potential optimization issues:**

### 1. ⚠️ Schema Compatibility Check
Renderer must verify:
- Same number of columns
- Compatible types
- Use first query's column names

### 2. ⚠️ ORDER BY/LIMIT Handling
When UNION has ORDER BY/LIMIT:
- Must wrap UNION in subquery
- Apply ORDER BY/LIMIT to outer query
- Example:
  ```sql
  SELECT * FROM (
    SELECT ... UNION SELECT ...
  ) ORDER BY col LIMIT 10
  ```

### 3. Performance Consideration
- UNION = UNION + DISTINCT (expensive)
- UNION ALL = no deduplication (fast)
- Prefer UNION ALL when duplicates don't matter

## Notes

- **Recursion required**: No
- **Ordering semantics**:
  - No implicit ordering in UNION
  - Use ORDER BY after UNION for deterministic order
  - ORDER BY applies to entire result set
- **Null handling**: NULLs are treated as equal in UNION deduplication
- **Multi-edge applicability**: N/A (query-level combinator)
- **Performance considerations**:
  - UNION requires sorting/hashing for deduplication
  - UNION ALL is O(n+m), UNION is O((n+m)log(n+m))
  - Large UNIONs can be expensive
  - Consider if deduplication is necessary
- **Key semantics**:
  - UNION = deduplicated combination
  - UNION ALL = keep all rows
  - Column counts must match
  - Column types must be compatible
  - First query determines column names
  - NULL = NULL for deduplication

## Implementation Notes

Existing SetOperator should handle this, but verify:
1. ✅ UNION vs UNION ALL distinction
2. ⚠️ ORDER BY/LIMIT scoping (may need fix in renderer)
3. ⚠️ Schema validation (type compatibility)
4. ⚠️ Column aliasing from first query
