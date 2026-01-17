# 02 – Node Lookup with Property Filter

## Cypher

```cypher
MATCH (p:Person)
WHERE p.name = 'Alice'
RETURN p
```

## What this query does

This query retrieves all nodes labeled `Person` where the `name` property equals `'Alice'`. The WHERE clause filters nodes after the MATCH. Only nodes matching both the label and the property condition are returned.

## Expected Databricks SQL

```sql
SELECT
  __p_id AS `p.id`,
  __p_name AS `p.name`
FROM `dbo.Person` AS p
WHERE
  p.name = 'Alice'
```

## Notes

- **Recursion required**: No
- **Ordering semantics**: Natural table order (no ORDER BY clause)
- **Null handling**: Only nodes with non-NULL `name` equal to 'Alice' are returned
- **Ambiguity resolved**: The WHERE clause translates directly to SQL WHERE clause
- Equality comparison uses `=` operator in SQL
- String literals are quoted with single quotes and escaped properly
- **Note**: Inline property map syntax `MATCH (p:Person {name: 'Alice'})` may not be supported. Use explicit WHERE clause.
