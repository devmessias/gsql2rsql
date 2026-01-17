# 09 – OPTIONAL MATCH (Left Join Semantics)

## Cypher

```cypher
MATCH (p:Person)
OPTIONAL MATCH (p)-[:ACTED_IN]->(m:Movie)
RETURN p.name, m.title
```

## What this query does

This query:
1. First matches all `Person` nodes (MATCH)
2. Then optionally matches any `ACTED_IN` relationships to `Movie` nodes (OPTIONAL MATCH)
3. Returns person names and movie titles

The key semantic difference from a regular MATCH is that OPTIONAL MATCH uses **left join semantics**:
- All persons are returned, even if they haven't acted in any movie
- For persons without movie relationships, `m.title` will be NULL
- This is equivalent to a SQL LEFT JOIN

## Expected Databricks SQL

```sql
SELECT
  p.name AS name,
  m.title AS title
FROM graph.Person p
LEFT JOIN graph.ActedIn r ON p.id = r.person_id
LEFT JOIN graph.Movie m ON r.movie_id = m.id
```

Note: Current implementation correctly uses LEFT JOIN but with `ON TRUE` condition.

## Notes

- **Recursion required**: No
- **Ordering semantics**: Natural table order
- **Null handling**: Critical - m.title is NULL when no ACTED_IN relationship exists
- **Multi-edge applicability**: Yes - `OPTIONAL MATCH (p)-[:ACTED_IN|DIRECTED]->(m:Movie)` would match either relationship type
- **Key assertion**: Must verify LEFT JOIN is used, not INNER JOIN
