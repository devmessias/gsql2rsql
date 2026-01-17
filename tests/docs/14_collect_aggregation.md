# 14 – Collect Aggregation

## Cypher

```cypher
MATCH (c:City)<-[:LIVES_IN]-(p:Person)
RETURN c.name AS city, COLLECT(p.name) AS residents
```

## What this query does

This query groups persons by their city and collects all person names into a list (array) for each city. COLLECT is an aggregation function that creates arrays from grouped values.

Key aspects:
- Matches `LIVES_IN` relationships (backward direction)
- Groups by city name (implicit GROUP BY)
- Aggregates person names into an array using `COLLECT()`
- Returns one row per city with an array of resident names

## Expected Databricks SQL

```sql
SELECT
  c.name AS city,
  COLLECT_LIST(p.name) AS residents
FROM graph.City c
INNER JOIN graph.LivesIn r ON c.id = r.target_id
INNER JOIN graph.Person p ON r.source_id = p.id
GROUP BY c.name
```

## Additional Test Cases

### Collect with multiple aggregations

```cypher
MATCH (c:City)<-[:LIVES_IN]-(p:Person)
RETURN c.name AS city, COLLECT(p.name) AS residents, COUNT(p) AS population
```

Expected SQL:
```sql
SELECT
  c.name AS city,
  COLLECT_LIST(p.name) AS residents,
  COUNT(*) AS population
FROM graph.City c
INNER JOIN graph.LivesIn r ON c.id = r.target_id
INNER JOIN graph.Person p ON r.source_id = p.id
GROUP BY c.name
```

### Collect IDs

```cypher
MATCH (c:City)<-[:LIVES_IN]-(p:Person)
RETURN c.name AS city, COLLECT(p.id) AS residentIds
```

### Multi-edge-type test

```cypher
MATCH (c:City)<-[:LIVES_IN|WORKS_IN]-(p:Person)
RETURN c.name AS city, COLLECT(p.name) AS people
```

This ensures COLLECT works correctly with multiple relationship types.

## Operator Analysis

**Current operators are sufficient:**
- ✅ `ProjectionOperator` already handles aggregation functions
- ✅ `COLLECT` is implemented as `AggregationFunction.COLLECT`
- ✅ Rendered as `COLLECT_LIST()` in Databricks SQL

**No new operator needed.** The existing query algebra supports this via:
1. JoinOperator for relationship traversal
2. ProjectionOperator with aggregation expression

## Notes

- **Recursion required**: No
- **Ordering semantics**: Array element order is non-deterministic unless ORDER BY is used
- **Null handling**: COLLECT includes NULL values in the array
- **Multi-edge applicability**: Yes - works with `[:LIVES_IN|WORKS_IN]`
- **Performance consideration**: COLLECT can produce large arrays; ensure appropriate LIMIT
- **Key semantic**:
  - `COLLECT(p.name)` → `COLLECT_LIST(p.name)` in Databricks
  - Empty groups produce empty arrays `[]`
  - Single element groups produce `[value]`
