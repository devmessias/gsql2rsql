# 11 – Aggregation with GROUP BY

## Cypher

```cypher
MATCH (p:Person)-[:LIVES_IN]->(c:City)
RETURN c.name AS city, COUNT(p) AS population
```

## What this query does

This query groups persons by their city and counts how many people live in each city. In Cypher, aggregation functions like `COUNT()` automatically create implicit groupings based on non-aggregated return expressions.

Key aspects:
- Matches directed `LIVES_IN` relationships from `Person` to `City` nodes
- Groups results by `c.name` (the city name)
- Counts persons per group using `COUNT(p)`
- Returns one row per distinct city with its population count

## Expected Databricks SQL

```sql
SELECT
  c.name AS city,
  COUNT(*) AS population
FROM graph.Person p
INNER JOIN graph.LivesIn r ON p.id = r.source_id
INNER JOIN graph.City c ON r.target_id = c.id
GROUP BY c.name
```

## Additional Test Cases

### Multiple aggregations per group

```cypher
MATCH (p:Person)-[:LIVES_IN]->(c:City)
RETURN c.name AS city, COUNT(p) AS population, AVG(p.age) AS avgAge
```

Expected SQL:
```sql
SELECT
  c.name AS city,
  COUNT(*) AS population,
  AVG(CAST(p.age AS DOUBLE)) AS avgAge
FROM graph.Person p
INNER JOIN graph.LivesIn r ON p.id = r.source_id
INNER JOIN graph.City c ON r.target_id = c.id
GROUP BY c.name
```

### Multiple grouping keys

```cypher
MATCH (p:Person)-[:LIVES_IN]->(c:City)
RETURN c.name AS city, p.status AS status, COUNT(p) AS count
```

Expected SQL:
```sql
SELECT
  c.name AS city,
  p.status AS status,
  COUNT(*) AS count
FROM graph.Person p
INNER JOIN graph.LivesIn r ON p.id = r.source_id
INNER JOIN graph.City c ON r.target_id = c.id
GROUP BY c.name, p.status
```

### Multi-edge-type test

```cypher
MATCH (p:Person)-[:LIVES_IN|WORKS_IN]->(c:City)
RETURN c.name AS city, COUNT(p) AS count
```

This tests aggregation with multiple relationship types, ensuring proper edge type filtering in the JOIN.

## Notes

- **Recursion required**: No
- **Ordering semantics**: Results are unordered (no ORDER BY)
- **Null handling**: `COUNT(p)` counts all matching rows; NULLs in grouped columns create separate groups
- **Multi-edge applicability**: Yes - `[:LIVES_IN|WORKS_IN]` should work with aggregation
- **Key semantic**: Cypher uses implicit GROUP BY based on non-aggregated return expressions
- **Aggregation rules**:
  - Non-aggregated expressions in RETURN become GROUP BY columns
  - `COUNT(p)` on an entity translates to `COUNT(*)`
  - `COUNT(p.property)` counts non-NULL values of that property
  - Multiple aggregations per group are supported
