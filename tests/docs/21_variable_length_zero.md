# 21 – Variable-Length Path *0..N (Zero-Length Paths)

## Cypher

```cypher
MATCH (p:Person)-[:KNOWS*0..2]->(f:Person)
RETURN DISTINCT f.name
```

## What this query does

Variable-length paths with `*0..N` syntax allow matching paths that include zero-length paths (identity paths). This means the starting node itself is included in the results.

Key aspects:
- `*0..2` means: depth 0 (the node itself), depth 1 (direct relationships), depth 2 (two hops)
- Depth 0 represents an "identity path" - the starting node with no traversal
- Commonly used for "node and its reachable nodes" queries
- Must generate WITH RECURSIVE CTE with a depth=0 base case

**Semantic difference**:
- `*1..2`: Only friends and friends-of-friends (excludes the person themselves)
- `*0..2`: The person, their friends, and friends-of-friends (includes the person)

## Expected Databricks SQL

```sql
WITH RECURSIVE _recursive_cte AS (
  -- Base case: Zero-length paths (depth = 0)
  SELECT
     __p_id AS start_node
    ,__p_id AS end_node
    ,0 AS depth
    ,ARRAY(__p_id) AS path
  FROM (
    SELECT
       id AS __p_id
      ,name AS __p_name
    FROM
      `graph`.`Person`
  ) AS _source

  UNION ALL

  -- Base case: Direct edges (depth = 1)
  SELECT
     __p_id AS start_node
    ,_edge.target_id AS end_node
    ,1 AS depth
    ,ARRAY(__p_id, _edge.target_id) AS path
  FROM (
    SELECT
       id AS __p_id
      ,name AS __p_name
    FROM
      `graph`.`Person`
  ) AS _source
  INNER JOIN `graph`.`Knows` AS _edge
    ON __p_id = _edge.source_id

  UNION ALL

  -- Recursive case: Extend paths (depth + 1)
  SELECT
     _cte.start_node
    ,_edge.target_id AS end_node
    ,_cte.depth + 1 AS depth
    ,ARRAY_APPEND(_cte.path, _edge.target_id) AS path
  FROM _recursive_cte AS _cte
  INNER JOIN `graph`.`Knows` AS _edge
    ON _cte.end_node = _edge.source_id
  WHERE _cte.depth < 2
    AND NOT ARRAY_CONTAINS(_cte.path, _edge.target_id)
)
SELECT DISTINCT
   __f_name AS name
FROM _recursive_cte AS _cte
INNER JOIN (
  SELECT
     id AS __f_id
    ,name AS __f_name
  FROM
    `graph`.`Person`
) AS _target
  ON _cte.end_node = __f_id
WHERE _cte.depth >= 0
  AND _cte.depth <= 2
```

**Key SQL features**:
- **Three parts in CTE**: depth=0 base case, depth=1 base case, recursive case
- **Depth=0 base case**: SELECT same id as start_node and end_node (identity)
- **No joins for depth=0**: Just the source table with id aliased twice
- **WHERE depth >= 0**: Includes all depths from 0 to max_hops

## Additional Test Cases

### Pure zero-length path (*0..0)

```cypher
MATCH (p:Person {name: 'Alice'})-[:KNOWS*0..0]->(f:Person)
RETURN f.name
```

**Expected behavior**: Returns only 'Alice' (the starting node itself).

**Expected SQL**:
```sql
WITH RECURSIVE _recursive_cte AS (
  -- Only depth=0 base case
  SELECT __p_id AS start_node, __p_id AS end_node, 0 AS depth, ...
  FROM ... WHERE name = 'Alice'
  -- No depth=1 or recursive case needed since max_hops=0
)
SELECT __f_name AS name
FROM _recursive_cte
JOIN Person f ON _cte.end_node = __f_id
WHERE _cte.depth >= 0 AND _cte.depth <= 0  -- Only depth=0
```

### Unbounded from zero (*0..)

```cypher
MATCH (p:Person {name: 'Alice'})-[:KNOWS*0..]->(f:Person)
RETURN DISTINCT f.name
```

**Expected behavior**: Returns Alice and all transitively reachable people (unlimited depth).

**Expected SQL**:
```sql
WITH RECURSIVE _recursive_cte AS (
  -- Base case: depth=0
  SELECT __p_id AS start_node, __p_id AS end_node, 0 AS depth, ...
  UNION ALL
  -- Base case: depth=1
  SELECT ...
  UNION ALL
  -- Recursive case with cycle detection but no max depth limit
  SELECT ...
  WHERE NOT ARRAY_CONTAINS(_cte.path, _edge.target_id)  -- Only cycle check, no depth < N
)
SELECT DISTINCT __f_name AS name
FROM _recursive_cte
WHERE _cte.depth >= 0  -- No upper bound
```

### Multiple edge types with zero-length

```cypher
MATCH (p:Person)-[:KNOWS|FOLLOWS*0..2]->(f:Person)
RETURN DISTINCT f.name
```

**Expected behavior**: Include starting person, then people reachable via KNOWS or FOLLOWS (or both) up to 2 hops.

**Expected SQL**:
```sql
WITH RECURSIVE _recursive_cte AS (
  -- Base case: depth=0 (same as single edge type)
  SELECT __p_id AS start_node, __p_id AS end_node, 0 AS depth, ...

  UNION ALL

  -- Base case: depth=1 with multiple edge tables
  SELECT __p_id AS start_node, _edge.target_id AS end_node, 1 AS depth, ...
  FROM _source
  INNER JOIN `graph`.`Knows` AS _edge ON ...

  UNION ALL

  SELECT __p_id AS start_node, _edge.target_id AS end_node, 1 AS depth, ...
  FROM _source
  INNER JOIN `graph`.`Follows` AS _edge ON ...

  UNION ALL

  -- Recursive case with both edge types
  SELECT _cte.start_node, _edge.target_id, _cte.depth + 1, ...
  FROM _recursive_cte AS _cte
  INNER JOIN `graph`.`Knows` AS _edge ON ...
  WHERE _cte.depth < 2 AND cycle_detection

  UNION ALL

  SELECT _cte.start_node, _edge.target_id, _cte.depth + 1, ...
  FROM _recursive_cte AS _cte
  INNER JOIN `graph`.`Follows` AS _edge ON ...
  WHERE _cte.depth < 2 AND cycle_detection
)
SELECT DISTINCT __f_name AS name
FROM _recursive_cte
WHERE _cte.depth >= 0 AND _cte.depth <= 2
```

### With WHERE clause on starting node

```cypher
MATCH (p:Person {name: 'Alice'})-[:KNOWS*0..2]->(f:Person)
RETURN DISTINCT f.name
```

**Expected behavior**: Filter applied to source table, depth=0 base case only includes Alice.

**Expected SQL**:
```sql
WITH RECURSIVE _recursive_cte AS (
  -- Depth=0: Only Alice (filter applied to source)
  SELECT __p_id AS start_node, __p_id AS end_node, 0 AS depth, ...
  FROM (
    SELECT id AS __p_id, name AS __p_name
    FROM `graph`.`Person`
    WHERE name = 'Alice'  -- ← Filter here
  ) AS _source

  UNION ALL

  -- Depth=1: Alice's direct friends
  SELECT __p_id AS start_node, _edge.target_id, 1 AS depth, ...
  FROM (
    SELECT id AS __p_id, name AS __p_name
    FROM `graph`.`Person`
    WHERE name = 'Alice'  -- ← Filter here too
  ) AS _source
  JOIN `graph`.`Knows` _edge ON ...

  UNION ALL

  -- Recursive case: Friends of Alice's friends
  SELECT ...
)
```

### With WHERE clause on target node

```cypher
MATCH (p:Person)-[:KNOWS*0..2]->(f:Person)
WHERE f.age > 30
RETURN DISTINCT f.name
```

**Expected behavior**: Filter applied after CTE, only return people over 30.

**Expected SQL**:
```sql
WITH RECURSIVE _recursive_cte AS (
  -- Depth=0, depth=1, recursive cases (no filter here)
  ...
)
SELECT DISTINCT __f_name AS name
FROM _recursive_cte
JOIN (
  SELECT id AS __f_id, name AS __f_name, age AS __f_age
  FROM `graph`.`Person`
) AS _target ON _cte.end_node = __f_id
WHERE _cte.depth >= 0 AND _cte.depth <= 2
  AND __f_age > 30  -- ← Filter on final results
```

## Operator Analysis

**Current operator status:**
- `RecursiveTraversalOperator` already exists in [operators.py:451-488](../../src/gsql2rsql/planner/operators.py)
- Has `min_hops` and `max_hops` fields
- Parser already correctly parses `*0..N` syntax

**Bugs to fix:**

1. **Planner bug** ([logical_plan.py:365](../../src/gsql2rsql/planner/logical_plan.py)):
   ```python
   # Current (treats 0 as falsy):
   min_hops=rel.min_hops or 1  # BUG: 0 or 1 → 1

   # Fix:
   min_hops=rel.min_hops if rel.min_hops is not None else 1
   ```

2. **Renderer min_depth bug** ([sql_renderer.py:369](../../src/gsql2rsql/renderer/sql_renderer.py)):
   ```python
   # Current:
   min_depth = op.min_hops or 1

   # Fix:
   min_depth = op.min_hops if op.min_hops is not None else 1
   ```

3. **Missing depth=0 base case** ([sql_renderer.py:398+](../../src/gsql2rsql/renderer/sql_renderer.py)):
   - Current implementation always starts CTE with depth=1
   - Need to add conditional depth=0 base case when `min_hops == 0`
   - Add UNION ALL before depth=1 case

**Implementation strategy:**
- Parser: ✅ Already works correctly
- Planner: Fix falsy value bug (1 line change)
- Renderer: Fix min_depth bug + add depth=0 base case generation (~50 lines)

**No new operators needed!** Just bug fixes to existing RecursiveTraversalOperator rendering.

## Notes

- **Recursion required**: YES - WITH RECURSIVE CTE
- **Performance considerations**:
  - **Depth=0 base case is cheap**: O(n) single table scan, no joins
  - **Column pruning applies**: Only select required columns for depth=0 case
  - **UNION ALL is efficient**: Databricks optimizes UNION ALL well
  - **Cycle detection**: depth=0 has no edges, so cycle detection only applies to depth≥1
  - **Large graphs**: Use LIMIT or max_hops to prevent unbounded recursion
- **Key semantics**:
  - **Depth=0 = identity**: start_node = end_node, no relationship traversed
  - **Path array at depth=0**: Contains only the starting node id
  - **No edge for depth=0**: Depth=0 base case doesn't join to edge table
  - **Cycle detection unaffected**: Depth=0 paths have no cycles (just the node itself)
- **Edge cases**:
  - `*0..0`: Only depth=0 base case, no recursive case needed
  - `*0..`: Depth=0 + unbounded recursion with cycle detection
  - `[:REL*0..N]` with multiple edge types: Depth=0 same for all types
  - WHERE clause on relationship properties: Only affects depth≥1 (no edge at depth=0)
- **Correctness**:
  - **Must include starting node** when min_hops=0
  - **DISTINCT usually needed** to avoid duplicate paths to same node
  - **depth >= 0 in WHERE**: Critical for including depth=0 results
- **Testing strategy**:
  - Verify depth=0 rows in CTE output
  - Verify starting node appears in final results
  - Verify *0..0 returns only starting node
  - Verify column pruning works for depth=0 base case
  - Verify multiple edge types work with *0..N
  - Verify no cartesian joins
