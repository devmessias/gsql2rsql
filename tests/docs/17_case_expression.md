# 17 – CASE Expression

## Cypher

```cypher
MATCH (p:Person)
RETURN p.name,
       CASE
         WHEN p.age < 18 THEN 'minor'
         WHEN p.age >= 18 AND p.age < 65 THEN 'adult'
         ELSE 'senior'
       END AS ageGroup
```

## What this query does

This query categorizes persons into age groups using a CASE expression. CASE is a conditional expression that evaluates conditions sequentially and returns the first matching result.

Key aspects:
- Matches all Person nodes
- Uses CASE WHEN THEN ELSE END for conditional logic
- Multiple conditions with AND logic
- Projects original name and computed ageGroup

## Expected Databricks SQL

```sql
SELECT
  p.name AS name,
  CASE
    WHEN p.age < 18 THEN 'minor'
    WHEN p.age >= 18 AND p.age < 65 THEN 'adult'
    ELSE 'senior'
  END AS ageGroup
FROM graph.Person p
```

## Additional Test Cases

### CASE without ELSE

```cypher
MATCH (p:Person)
RETURN p.name,
       CASE WHEN p.age >= 18 THEN 'adult' END AS status
```

If no condition matches and ELSE is omitted, result is NULL.

### CASE with aggregation

```cypher
MATCH (p:Person)
RETURN
  CASE
    WHEN p.age < 18 THEN 'minor'
    ELSE 'adult'
  END AS ageGroup,
  COUNT(p) AS count
```

Expected SQL:
```sql
SELECT
  CASE
    WHEN p.age < 18 THEN 'minor'
    ELSE 'adult'
  END AS ageGroup,
  COUNT(*) AS count
FROM graph.Person p
GROUP BY CASE WHEN p.age < 18 THEN 'minor' ELSE 'adult' END
```

**Important**: GROUP BY must include the CASE expression, not just the alias.

### CASE in WHERE clause

```cypher
MATCH (p:Person)
WHERE CASE WHEN p.status IS NULL THEN 'unknown' ELSE p.status END = 'active'
RETURN p.name
```

### Nested CASE

```cypher
MATCH (p:Person)
RETURN p.name,
       CASE
         WHEN p.age < 18 THEN 'minor'
         ELSE CASE
           WHEN p.age >= 65 THEN 'senior'
           ELSE 'adult'
         END
       END AS ageGroup
```

## Operator Analysis

**Current operators are sufficient:**
- ✅ CASE expressions are handled as `QueryExpression` types
- ✅ Rendered directly as CASE WHEN in SQL

**Potential optimization issue:**
- ⚠️ **Column pruning**: If CASE references columns not in final projection, ensure they're available but not over-projected
- ⚠️ **GROUP BY with CASE**: Must repeat full CASE expression in GROUP BY clause, not use alias

**No new operator needed**, but renderer must:
1. Properly translate CASE AST to SQL CASE
2. Handle CASE in GROUP BY (must use expression, not alias)
3. Preserve evaluation order of WHEN clauses

## Notes

- **Recursion required**: No
- **Ordering semantics**: CASE evaluation is sequential (first match wins)
- **Null handling**:
  - Unmatched conditions with no ELSE return NULL
  - NULL in condition expressions follows SQL three-valued logic
- **Multi-edge applicability**: N/A (expression-level feature)
- **Performance considerations**:
  - CASE evaluation is lazy (stops at first match)
  - Complex CASE in GROUP BY can be expensive
  - Consider materialization for repeated CASE expressions
- **Key semantics**:
  - CASE WHEN ... THEN ... ELSE ... END
  - Sequential evaluation
  - First matching condition wins
  - ELSE is optional (defaults to NULL)
  - Can appear in SELECT, WHERE, HAVING, ORDER BY
