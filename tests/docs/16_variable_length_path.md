# 16 – Variable-Length Path (Bounded)

## Cypher

```cypher
MATCH (start:Person)-[:KNOWS*1..3]->(end:Person)
WHERE start.name = 'Alice'
RETURN DISTINCT end.name
```

## What this query does

This query finds all persons reachable from Alice within 1 to 3 KNOWS relationships (hops). This requires graph traversal with bounded depth.

Key aspects:
- `*1..3` means 1 to 3 hops (min=1, max=3)
- Traverses `KNOWS` relationships recursively
- Starts from a specific person (Alice)
- Returns distinct names of reachable persons
- **Requires WITH RECURSIVE in SQL**

## Expected Databricks SQL

```sql
WITH RECURSIVE
  paths AS (
    -- Base case: direct edges (depth = 1)
    SELECT
      e.source_id AS start_node,
      e.target_id AS end_node,
      1 AS depth,
      ARRAY(e.source_id, e.target_id) AS path,
      ARRAY(e.source_id) AS visited
    FROM graph.Knows e
    JOIN graph.Person start ON e.source_id = start.id
    WHERE start.name = 'Alice'

    UNION ALL

    -- Recursive case: extend paths
    SELECT
      p.start_node,
      e.target_id AS end_node,
      p.depth + 1 AS depth,
      CONCAT(p.path, ARRAY(e.target_id)) AS path,
      CONCAT(p.visited, ARRAY(e.source_id)) AS visited
    FROM paths p
    JOIN graph.Knows e ON p.end_node = e.source_id
    WHERE p.depth < 3
      AND NOT ARRAY_CONTAINS(p.visited, e.target_id)
  )
SELECT DISTINCT
  end_person.name AS name
FROM paths p
JOIN graph.Person end_person ON p.end_node = end_person.id
WHERE p.depth >= 1
```

## Additional Test Cases

### Without start filter (all pairs)

```cypher
MATCH (start:Person)-[:KNOWS*1..2]->(end:Person)
RETURN DISTINCT start.name, end.name
```

This finds all person pairs connected within 2 hops.

### Exact length

```cypher
MATCH (start:Person)-[:KNOWS*2]->(end:Person)
RETURN DISTINCT end.name
```

Only paths of exactly 2 hops (min=2, max=2).

### Unbounded depth (dangerous!)

```cypher
MATCH (start:Person)-[:KNOWS*1..]->(end:Person)
WHERE start.name = 'Alice'
RETURN DISTINCT end.name
```

No upper bound - requires careful max_depth limit in implementation.

### Multi-edge-type test

```cypher
MATCH (start:Person)-[:KNOWS|FOLLOWS*1..3]->(end:Person)
WHERE start.name = 'Alice'
RETURN DISTINCT end.name
```

**Critical**: Edge type filter must appear in BOTH base case and recursive step:
```sql
-- Base case
FROM graph.Knows e WHERE e.type IN ('KNOWS', 'FOLLOWS')

-- Recursive case
FROM paths p JOIN graph.Knows e ON ... WHERE e.type IN ('KNOWS', 'FOLLOWS')
```

## Operator Analysis

**Current operators are sufficient:**
- ✅ `RecursiveTraversalOperator` exists and handles this
- ✅ Supports `min_hops` and `max_hops`
- ✅ Generates `WITH RECURSIVE` CTEs
- ✅ Implements cycle detection via `visited` array
- ✅ Supports multiple edge types

**From operators.py:**
```python
class RecursiveTraversalOperator(LogicalOperator):
    def __init__(
        self,
        edge_types: list[str],
        source_node_type: str,
        target_node_type: str,
        min_hops: int,
        max_hops: int | None = None,
        ...
    )
```

**No new operator needed!** The existing architecture already supports this perfectly.

## Notes

- **Recursion required**: ✅ YES - Uses `WITH RECURSIVE`
- **Ordering semantics**: No implicit ordering on paths
- **Null handling**: Paths with NULL endpoints are excluded
- **Multi-edge applicability**: ✅ YES - Critical for correctness
- **Cycle detection**:
  - Uses `visited` array to track nodes in current path
  - Prevents infinite loops
  - `NOT ARRAY_CONTAINS(p.visited, e.target_id)`
- **Performance considerations**:
  - Unbounded depth can be expensive - always set `max_hops`
  - Default max depth should be reasonable (10 or less)
  - Cycle detection adds overhead but is necessary
- **Key semantics**:
  - `*1..3` → min=1, max=3
  - `*2` → min=2, max=2 (exact)
  - `*1..` → min=1, max=default (dangerous)
  - `*0..3` → includes zero-length paths (identity)
- **Implementation notes**:
  - Base case generates depth=1 paths
  - Recursive step extends until max_depth
  - Final SELECT filters by min_depth
  - Join with target node table to project properties
