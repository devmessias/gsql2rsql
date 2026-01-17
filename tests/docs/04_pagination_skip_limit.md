# 04 – Pagination with SKIP and LIMIT

## Cypher

```cypher
MATCH (p:Person)
RETURN p.name
ORDER BY p.name
SKIP 10
LIMIT 5
```

## What this query does

This query retrieves `Person` nodes, orders them by the `name` property in ascending order, skips the first 10 results, and returns the next 5 results. This is standard pagination: starting at offset 10, return 5 rows. The ORDER BY is required for deterministic pagination.

## Expected Databricks SQL

```sql
SELECT
  p.name AS `p.name`
FROM `dbo.Person` AS p
ORDER BY p.name ASC
LIMIT 5 OFFSET 10
```

## Notes

- **Recursion required**: No
- **Ordering semantics**: ORDER BY `p.name` ASC (ascending is default)
- **Null handling**: NULLs are sorted first in Databricks (NULL-first semantics)
- **Ambiguity resolved**:
  - SKIP N maps to OFFSET N in Databricks SQL
  - LIMIT N maps to LIMIT N
  - ORDER BY is essential for deterministic results across pages
- Databricks SQL uses `LIMIT n OFFSET m` syntax (not `OFFSET m ROWS FETCH NEXT n ROWS`)
