# 03 – Property Projection with Aliases

## Cypher

```cypher
MATCH (p:Person)
RETURN p.name AS personName, p.id AS personId
```

## What this query does

This query retrieves all `Person` nodes and projects only two specific properties: `name` and `id`. Each property is given an alias (`personName` and `personId`) that becomes the column name in the result set. This is standard SQL-like projection with aliasing.

## Expected Databricks SQL

```sql
SELECT
  p.name AS `personName`,
  p.id AS `personId`
FROM `dbo.Person` AS p
```

## Notes

- **Recursion required**: No
- **Ordering semantics**: Natural table order (no ORDER BY clause)
- **Null handling**: Properties are projected as-is, including NULL values
- **Ambiguity resolved**: Property access `p.name` maps directly to column access in SQL
- The aliases are quoted with backticks in Databricks SQL
- Unlike returning the full entity `p`, this projects only specified properties
