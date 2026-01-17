# 07 – Relationship Match with Property Filter

## Cypher

```cypher
MATCH (p:Person)-[r:KNOWS]->(f:Person)
WHERE r.since > 2020
RETURN p.name, f.name, r.since
```

## What this query does

This query matches all `KNOWS` relationships between `Person` nodes where the relationship has a `since` property greater than 2020. The relationship is given an alias `r` to allow filtering and projecting its properties.

Key aspects:
- Matches directed relationships from `p` to `f`
- The relationship `r` has properties (`since`, etc.)
- Filters based on relationship property `r.since > 2020`
- Returns properties from both nodes and the relationship

## Expected Databricks SQL

```sql
SELECT
  p.name AS name,
  f.name AS name,
  r.since AS since
FROM graph.Person p
INNER JOIN graph.Knows r ON p.id = r.source_id
INNER JOIN graph.Person f ON r.target_id = f.id
WHERE r.since > 2020
```

Note: Current implementation uses cartesian joins with `ON TRUE` and filters in WHERE, which is functionally equivalent but less efficient.

## Notes

- **Recursion required**: No
- **Ordering semantics**: Natural table order
- **Null handling**: Only relationships with non-NULL `since > 2020` are matched
- **Multi-edge applicability**: Yes - could test with `[:KNOWS|FOLLOWS]` if both have `since` property
- **Known limitation**: Current implementation uses `ON TRUE` joins instead of proper foreign key conditions
