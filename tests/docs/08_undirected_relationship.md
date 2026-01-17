# 08 – Undirected Relationship Match

## Cypher

```cypher
MATCH (p:Person)-[:KNOWS]-(f:Person)
RETURN p.name, f.name
```

## What this query does

This query matches `KNOWS` relationships between `Person` nodes in **either direction**. The relationship pattern `-[:KNOWS]-` (without arrows) matches both:
- `(p)-[:KNOWS]->(f)` - p knows f
- `(p)<-[:KNOWS]-(f)` - f knows p (equivalently, p is known by f)

This is a bidirectional match, meaning if only `(A)-[:KNOWS]->(B)` exists in the graph, both `(p=A, f=B)` and `(p=B, f=A)` would be valid matches.

## Expected Databricks SQL

For undirected matching, the SQL should ideally use a UNION or OR condition:

```sql
SELECT p.name, f.name
FROM (
  -- Forward direction
  SELECT p.id, p.name, f.id, f.name
  FROM graph.Person p
  JOIN graph.Knows r ON p.id = r.source_id
  JOIN graph.Person f ON r.target_id = f.id

  UNION ALL

  -- Backward direction
  SELECT p.id, p.name, f.id, f.name
  FROM graph.Person p
  JOIN graph.Knows r ON p.id = r.target_id
  JOIN graph.Person f ON r.source_id = f.id
) subquery
```

Note: Current implementation may not fully implement bidirectional semantics.

## Notes

- **Recursion required**: No
- **Ordering semantics**: Natural table order, duplicates possible from both directions
- **Null handling**: Standard - only matched nodes/relationships returned
- **Multi-edge applicability**: Yes - `[:KNOWS|FOLLOWS]-` tests bidirectional with multiple types
- **Known limitation**: Verify if current implementation truly matches both directions or only one
