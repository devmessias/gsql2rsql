# 20 – COALESCE Function

## Cypher

```cypher
MATCH (p:Person)
RETURN COALESCE(p.nickname, p.name) AS displayName
```

## What this query does

COALESCE returns the first non-null value from its argument list. Common use cases:
- Providing default values for nullable properties
- Combining multiple fallback options
- Null-safe property access

Key aspects:
- Takes 1+ arguments (variadic)
- Returns first non-null value
- Returns NULL if all arguments are NULL
- Type is determined by argument types

## Expected Databricks SQL

```sql
SELECT
  COALESCE(__p_nickname, __p_name) AS displayName
FROM (
  SELECT
     id AS __p_id
    ,name AS __p_name
    ,nickname AS __p_nickname
  FROM
    `graph`.`Person`
) AS _proj
```

## Additional Test Cases

### COALESCE with literal default

```cypher
MATCH (p:Person)
RETURN COALESCE(p.nickname, 'Unknown') AS displayName
```

Expected SQL:
```sql
SELECT
  COALESCE(__p_nickname, 'Unknown') AS displayName
FROM ...
```

### COALESCE with multiple arguments

```cypher
MATCH (p:Person)
RETURN COALESCE(p.nickname, p.alias, p.name, 'Anonymous') AS displayName
```

Expected SQL:
```sql
SELECT
  COALESCE(__p_nickname, __p_alias, __p_name, 'Anonymous') AS displayName
FROM ...
```

### COALESCE in WHERE clause

```cypher
MATCH (p:Person)
WHERE COALESCE(p.age, 0) > 18
RETURN p.name
```

Expected SQL:
```sql
SELECT __p_name AS name
FROM (
  SELECT * FROM (...)
  WHERE COALESCE(__p_age, 0) > 18
) AS _proj
```

### COALESCE with aggregation

```cypher
MATCH (p:Person)
RETURN COALESCE(p.city, 'Unknown') AS city, COUNT(p) AS count
```

Expected SQL:
```sql
SELECT
  COALESCE(__p_city, 'Unknown') AS city,
  COUNT(*) AS count
FROM ...
GROUP BY COALESCE(__p_city, 'Unknown')
```

## Operator Analysis

**Current operator status:**
- `Function` enum in operators.py - needs COALESCE entry
- `FUNCTIONS` dict - needs mapping
- `_render_function` in sql_renderer.py - needs COALESCE case

**Implementation:**
- COALESCE is variadic (1+ args)
- Databricks SQL supports COALESCE natively
- Simple passthrough rendering: `COALESCE(arg1, arg2, ...)`

**No new operator needed!** Just add to existing function infrastructure.

## Notes

- **Recursion required**: No
- **Performance considerations**:
  - COALESCE is evaluated left-to-right
  - Short-circuit evaluation (stops at first non-null)
  - Very efficient in Databricks
- **Key semantics**:
  - First non-null value wins
  - NULL if all NULL
  - Arguments evaluated left-to-right
  - Type coercion follows SQL rules
