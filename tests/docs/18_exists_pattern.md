# 18 – EXISTS Pattern Predicate

## Cypher

```cypher
MATCH (p:Person)
WHERE EXISTS { (p)-[:ACTED_IN]->(:Movie) }
RETURN p.name
```

## What this query does

This query finds persons who have acted in at least one movie using the EXISTS pattern predicate. EXISTS checks if a pattern has at least one match without materializing all matches.

Key aspects:
- Matches all Person nodes
- Uses EXISTS subquery to check for ACTED_IN relationships
- Returns persons who satisfy the existence condition
- Does NOT return the movies (only checks existence)

## Expected Databricks SQL

```sql
SELECT
  p.name AS name
FROM graph.Person p
WHERE EXISTS (
  SELECT 1
  FROM graph.ActedIn r
  JOIN graph.Movie m ON r.target_id = m.id
  WHERE r.source_id = p.id
)
```

Alternative using LEFT JOIN with NULL check:
```sql
SELECT DISTINCT
  p.name AS name
FROM graph.Person p
LEFT JOIN graph.ActedIn r ON p.id = r.source_id
LEFT JOIN graph.Movie m ON r.target_id = m.id
WHERE r.source_id IS NOT NULL
```

**Prefer EXISTS subquery** for better performance and semantics.

## Additional Test Cases

### EXISTS with NOT

```cypher
MATCH (p:Person)
WHERE NOT EXISTS { (p)-[:ACTED_IN]->(:Movie) }
RETURN p.name
```

Find persons who have NOT acted in any movie.

Expected SQL:
```sql
SELECT
  p.name AS name
FROM graph.Person p
WHERE NOT EXISTS (
  SELECT 1
  FROM graph.ActedIn r
  WHERE r.source_id = p.id
)
```

### EXISTS with property filter in pattern

```cypher
MATCH (p:Person)
WHERE EXISTS { (p)-[:ACTED_IN]->(m:Movie) WHERE m.year > 2020 }
RETURN p.name
```

Expected SQL:
```sql
SELECT
  p.name AS name
FROM graph.Person p
WHERE EXISTS (
  SELECT 1
  FROM graph.ActedIn r
  JOIN graph.Movie m ON r.target_id = m.id
  WHERE r.source_id = p.id
    AND m.year > 2020
)
```

### Multiple EXISTS conditions

```cypher
MATCH (p:Person)
WHERE EXISTS { (p)-[:ACTED_IN]->(:Movie) }
  AND EXISTS { (p)-[:DIRECTED]->(:Movie) }
RETURN p.name
```

Finds persons who both acted AND directed.

### EXISTS with multi-edge types

```cypher
MATCH (p:Person)
WHERE EXISTS { (p)-[:ACTED_IN|DIRECTED]->(:Movie) }
RETURN p.name
```

Expected SQL must filter edge types in the EXISTS subquery:
```sql
WHERE EXISTS (
  SELECT 1
  FROM graph.Relationships r
  WHERE r.source_id = p.id
    AND r.type IN ('ACTED_IN', 'DIRECTED')
)
```

## Operator Analysis

**Current operator gap:**
- ❌ **ExistsOperator or SubqueryOperator might be needed**
- EXISTS is fundamentally different from JoinOperator:
  - Does NOT materialize matches
  - Only checks boolean existence
  - Should generate subquery, not join

**Implementation options:**

### Option 1: New ExistsPredicateOperator
```python
class ExistsPredicateOperator(UnaryLogicalOperator):
    pattern: LogicalOperator  # The pattern to check
    is_negated: bool = False  # For NOT EXISTS
```

Renders as:
```sql
WHERE [NOT] EXISTS (
  SELECT 1 FROM <pattern>
)
```

### Option 2: Extend SelectionOperator
Add EXISTS as a predicate type in SelectionOperator expressions.

**Recommendation**: **Option 1 - Dedicated operator** because:
- EXISTS has different semantics (boolean test, not data flow)
- Needs correlated subquery support
- Optimization opportunities differ from regular predicates

**Optimization notes:**
- ⚠️ **Semi-join**: EXISTS is a semi-join, not inner join
- ⚠️ **Early termination**: EXISTS can stop after first match
- ⚠️ **Correlated subquery**: Must properly handle outer reference (e.g., `p.id`)

## Notes

- **Recursion required**: No (but subquery required)
- **Ordering semantics**: N/A (boolean predicate)
- **Null handling**: EXISTS never returns NULL (always TRUE or FALSE)
- **Multi-edge applicability**: ✅ Yes - critical for edge type filtering in EXISTS
- **Performance considerations**:
  - EXISTS is typically faster than JOIN + DISTINCT
  - Query optimizer can use semi-join algorithms
  - Early termination after first match
  - Correlated subqueries can be expensive without proper indexing
- **Key semantics**:
  - EXISTS returns TRUE if pattern matches at least once
  - NOT EXISTS returns TRUE if pattern never matches
  - Subquery should SELECT 1 (not materialized data)
  - Correlated subquery references outer query variables

## Implementation Priority

⚠️ **HIGH PRIORITY** - EXISTS is fundamental for:
- Filtering by relationship existence
- NOT EXISTS for absence checks
- Common pattern in real queries
- Better performance than JOIN alternatives
