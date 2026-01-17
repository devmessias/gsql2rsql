# 01 – Simple Node Lookup by Label

## Cypher

```cypher
MATCH (p:Person)
RETURN p
```

## What this query does

This query retrieves all nodes labeled `Person` from the graph. It does not apply any filtering, ordering, or pagination. The result includes all properties of each Person node found in the graph.

## Expected Databricks SQL

```sql
SELECT
  __p_id AS `p.id`,
  __p_name AS `p.name`
FROM `dbo.Person` AS p
```

## Notes

- **Recursion required**: No
- **Ordering semantics**: Natural table order (no ORDER BY clause)
- **Null handling**: All Person nodes are returned, regardless of whether properties are NULL
- **Ambiguity resolved**: When returning an entity (not specific properties), all properties defined in the schema are projected
- The field names are prefixed with `__` and the entity alias, following the transpiler's internal naming convention
- Databricks SQL uses backticks for identifier quoting
