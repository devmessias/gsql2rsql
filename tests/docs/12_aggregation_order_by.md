# 12 – Aggregation with ORDER BY

## Cypher

```cypher
MATCH (p:Person)-[:LIVES_IN]->(c:City)
RETURN c.name AS city, COUNT(p) AS population
ORDER BY population DESC
```

## What this query does

This query groups persons by their city, counts the population, and orders results by population in descending order. This demonstrates the combination of:
- Aggregation (COUNT)
- Implicit GROUP BY
- ORDER BY on an aggregated value

Key aspects:
- Matches `LIVES_IN` relationships from `Person` to `City`
- Groups by city name
- Counts persons per city
- Orders by the aggregated count descending (most populous first)

## Expected Databricks SQL

```sql
SELECT
  c.name AS city,
  COUNT(*) AS population
FROM graph.Person p
INNER JOIN graph.LivesIn r ON p.id = r.source_id
INNER JOIN graph.City c ON r.target_id = c.id
GROUP BY c.name
ORDER BY population DESC
```

## Additional Test Cases

### Order by grouping column ascending

```cypher
MATCH (p:Person)-[:LIVES_IN]->(c:City)
RETURN c.name AS city, COUNT(p) AS population
ORDER BY city ASC
```

Expected SQL:
```sql
SELECT
  c.name AS city,
  COUNT(*) AS population
FROM graph.Person p
INNER JOIN graph.LivesIn r ON p.id = r.source_id
INNER JOIN graph.City c ON r.target_id = c.id
GROUP BY c.name
ORDER BY city ASC
```

### Multiple ORDER BY columns

```cypher
MATCH (p:Person)-[:LIVES_IN]->(c:City)
RETURN c.name AS city, COUNT(p) AS population, AVG(p.age) AS avgAge
ORDER BY population DESC, avgAge ASC
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
ORDER BY population DESC, avgAge ASC
```

### Order by with LIMIT

```cypher
MATCH (p:Person)-[:LIVES_IN]->(c:City)
RETURN c.name AS city, COUNT(p) AS population
ORDER BY population DESC
LIMIT 10
```

Expected SQL adds:
```sql
... ORDER BY population DESC
LIMIT 10
```

## Notes

- **Recursion required**: No
- **Ordering semantics**: Explicit ordering by aggregated or grouped columns
- **Null handling**: NULLs in ORDER BY columns follow Databricks default behavior (NULLS LAST for ASC)
- **Multi-edge applicability**: Yes - ordering works with any edge type combination
- **Key semantics**:
  - ORDER BY can reference result aliases (e.g., `population`)
  - ORDER BY can reference grouping columns (e.g., `city`)
  - Multiple ORDER BY columns are supported
  - ASC/DESC modifiers apply per column
  - LIMIT can be combined with ORDER BY
