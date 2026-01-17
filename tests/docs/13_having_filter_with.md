# 13 – HAVING-like Aggregation Filter using WITH

## Cypher

```cypher
MATCH (p:Person)-[:LIVES_IN]->(c:City)
WITH c.name AS city, COUNT(p) AS population
WHERE population > 1000
RETURN city, population
```

## What this query does

This query demonstrates Cypher's pattern for filtering on aggregated values, equivalent to SQL's HAVING clause. The pattern is:
1. MATCH the pattern
2. WITH creates an intermediate result with aggregated values
3. WHERE on the WITH filters the aggregated results
4. RETURN projects the final output

Key aspects:
- WITH acts as a pipeline stage, materializing the aggregation
- WHERE after WITH filters on aggregated columns
- This is semantically equivalent to SQL's GROUP BY ... HAVING

## Expected Databricks SQL

```sql
SELECT
  city,
  population
FROM (
  SELECT
    c.name AS city,
    COUNT(*) AS population
  FROM graph.Person p
  INNER JOIN graph.LivesIn r ON p.id = r.source_id
  INNER JOIN graph.City c ON r.target_id = c.id
  GROUP BY c.name
) subquery
WHERE population > 1000
```

Alternative using HAVING directly:
```sql
SELECT
  c.name AS city,
  COUNT(*) AS population
FROM graph.Person p
INNER JOIN graph.LivesIn r ON p.id = r.source_id
INNER JOIN graph.City c ON r.target_id = c.id
GROUP BY c.name
HAVING COUNT(*) > 1000
```

## Additional Test Cases

### Multiple filter conditions on aggregations

```cypher
MATCH (p:Person)-[:LIVES_IN]->(c:City)
WITH c.name AS city, COUNT(p) AS population, AVG(p.age) AS avgAge
WHERE population > 100 AND avgAge > 25
RETURN city, population, avgAge
```

### WITH followed by ORDER BY

```cypher
MATCH (p:Person)-[:LIVES_IN]->(c:City)
WITH c.name AS city, COUNT(p) AS population
WHERE population > 500
RETURN city, population
ORDER BY population DESC
```

### Chained WITH clauses

```cypher
MATCH (p:Person)-[:LIVES_IN]->(c:City)
WITH c.name AS city, COUNT(p) AS population
WHERE population > 100
WITH city, population * 2 AS doubledPop
RETURN city, doubledPop
```

## Notes

- **Recursion required**: No
- **Ordering semantics**: No implicit ordering; use ORDER BY if needed
- **Null handling**: Aggregated NULLs follow standard SQL semantics
- **Multi-edge applicability**: Yes - works with any relationship pattern
- **Key semantics**:
  - WITH creates a materialization point in the query
  - WHERE after WITH filters the WITH output (like HAVING)
  - WITH can appear multiple times for multi-stage pipelines
  - Aliases defined in WITH are visible in subsequent clauses
- **Implementation options**:
  - Subquery with outer WHERE
  - Direct HAVING clause
  - Both are semantically equivalent
