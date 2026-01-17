# 05 – Count Nodes by Label

## Cypher

```cypher
MATCH (p:Person)
RETURN COUNT(p) AS totalPeople
```

## What this query does

This query counts all nodes labeled `Person` and returns the count as a single row with the alias `totalPeople`. The `COUNT(p)` aggregation function counts the number of matched nodes. Since there is no GROUP BY, this is a global aggregation returning one row.

## Expected Databricks SQL

```sql
SELECT
  COUNT(*) AS `totalPeople`
FROM `dbo.Person` AS p
```

## Notes

- **Recursion required**: No
- **Ordering semantics**: Not applicable (single aggregated row)
- **Null handling**: `COUNT(p)` counts all rows, equivalent to `COUNT(*)` in SQL
- **Ambiguity resolved**:
  - `COUNT(p)` where `p` is a node/entity translates to `COUNT(*)`
  - `COUNT(p.property)` would count non-NULL values of that property
- Returns exactly one row with the count value
- The result is always a non-negative integer
