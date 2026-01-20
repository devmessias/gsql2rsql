# Limitations and Unsupported Features

This document lists known limitations, unsupported OpenCypher features, runtime caveats, and recommended workarounds.

---

## Runtime Requirements

### Databricks Runtime 17+ Required

**Requirement**: Databricks Runtime 17 or higher

**Reason**: The transpiler generates `WITH RECURSIVE` CTEs for variable-length paths. This feature was added in Databricks Runtime 17.

**Affected Features**:
- Variable-length paths: `-[:TYPE*1..N]->`
- Any query with `*` in relationship patterns

**Workaround**: Upgrade to Databricks Runtime 17+. No workaround for older runtimes (would require complete rewrite to PySpark DataFrame code).

### Spark SQL Limitations

**ARRAY Operations**: Generated SQL uses `ARRAY()`, `CONCAT()`, `array_contains()` functions.
- These require Spark SQL, not standard ANSI SQL
- Older Spark versions may have incomplete array support

**STRUCT for Edge Collections**: Edge property collections use `COLLECT(STRUCT(...))`.
- Requires Spark SQL struct support
- May have performance implications on very large result sets

### Undirected Relationship Performance (OPTIMIZED)

**Status**: ✅ **Optimized** (as of 2026-01-19)

**Default Behavior**: Undirected relationships (`-[:TYPE]-`) now use UNION ALL edge expansion for optimal performance.

**Example Query**:
```cypher
-- Undirected relationship (fast with optimization)
MATCH (a:Person)-[:KNOWS]-(b:Person)
WHERE a.name = 'Alice'
RETURN b.name
```

**Generated SQL (Optimized - Default)**:
```sql
-- Edges expanded bidirectionally before joining
JOIN (
  SELECT source_id AS node_id, target_id AS other_id, props FROM Knows
  UNION ALL
  SELECT target_id AS node_id, source_id AS other_id, props FROM Knows
) k ON person.id = k.node_id
```

**Performance**:
- Small datasets (< 1000 rows): Fast (hash join)
- Medium datasets (1K-100K rows): Fast (hash join with indexes)
- Large datasets (> 100K rows): Optimized (O(n) instead of O(n²))

**Known Limitation**: Self-loops (e.g., `(a)-[:KNOWS]-(a)`) may appear twice in results.
- **Workaround**: Add `WHERE a.id <> b.id` or use `DISTINCT`

**Disabling Optimization (for debugging or compatibility)**:
```python
from gsql2rsql import SQLRenderer

# Use legacy OR join strategy (slower but simpler SQL)
renderer = SQLRenderer(
    db_schema_provider=schema,
    config={"undirected_strategy": "or_join"}  # Not recommended
)
```

**Generated SQL (Legacy - OR Join)**:
```sql
-- Only use for small datasets or debugging
ON (person.id = knows.source_id OR person.id = knows.target_id)
```

**Learn More**: See [UNDIRECTED_OPTIMIZATION_IMPLEMENTATION.md](development/UNDIRECTED_OPTIMIZATION_IMPLEMENTATION.md) for implementation details and trade-off analysis.

---

## Unsupported OpenCypher Features

### 1. MERGE (Upsert Operations)

**Status**: ❌ Not Supported

**Reason**: `MERGE` requires write operations. The transpiler generates **read-only** SQL queries.

**Example (unsupported)**:
```cypher
MERGE (p:Person {id: 123})
ON CREATE SET p.created_at = timestamp()
ON MATCH SET p.last_seen = timestamp()
```

**Workaround**: Use Databricks `MERGE INTO` statement directly (not via transpiler).

### 2. CREATE, DELETE, SET, REMOVE (Write Operations)

**Status**: ❌ Not Supported

**Reason**: Transpiler is **read-only**. No support for graph mutations.

**Examples (all unsupported)**:
```cypher
CREATE (p:Person {name: 'Alice'})
DELETE p
SET p.age = 30
REMOVE p.age
```

**Workaround**: Use standard SQL `INSERT`, `UPDATE`, `DELETE` statements on your Databricks tables.

### 3. Shortest Path (shortestPath, allShortestPaths)

**Status**: ⚠️ Partial Support (INFERRED)

**Supported**: BFS traversal with depth tracking via `-[:TYPE*1..N]->`
**Not Supported**: Built-in `shortestPath()` and `allShortestPaths()` functions

**Example**:
```cypher
-- ❌ Not supported
MATCH p = shortestPath((a:Person)-[:KNOWS*]-(b:Person))
RETURN p

-- ✅ Workaround: use bounded BFS with ORDER BY depth LIMIT 1
MATCH (a:Person)-[:KNOWS*1..10]-(b:Person)
WHERE a.id = 1 AND b.id = 100
RETURN a, b
ORDER BY length(relationships(path)) ASC
LIMIT 1
```

**Limitation**: Workaround requires explicit max depth and may be inefficient for large graphs.

### 4. FOREACH (Iteration with Side Effects)

**Status**: ❌ Not Supported

**Reason**: `FOREACH` is for mutations (side effects). Transpiler is read-only.

**Example (unsupported)**:
```cypher
MATCH p = (a)-[:KNOWS*]-(b)
FOREACH (n IN nodes(p) | SET n.visited = true)
```

**Workaround**: None (fundamentally incompatible with read-only SQL).

### 5. CALL Procedures (APOC, Custom Procedures)

**Status**: ❌ Not Supported

**Reason**: No concept of stored procedures in the transpilation model. APOC functions are Neo4j-specific.

**Example (unsupported)**:
```cypher
CALL apoc.periodic.iterate(...)
CALL db.stats.retrieve(...)
```

**Workaround**: Use Databricks SQL UDFs or built-in functions where equivalent functionality exists.

### 6. Pattern Comprehension with WHERE

**Status**: ⚠️ Partial Support (INFERRED)

**Supported**: Simple pattern comprehension `[(n)-[:KNOWS]->(f) | f.name]`
**Not Supported**: Pattern comprehension with complex `WHERE` clauses (INFERRED from lack of tests)

**Example**:
```cypher
-- ⚠️ May not be fully supported
RETURN [(n)-[:KNOWS]->(f) WHERE f.age > 30 | f.name] AS friends
```

**Workaround**: Use standard `MATCH` with `WITH` clause instead:
```cypher
MATCH (n)-[:KNOWS]->(f)
WHERE f.age > 30
WITH n, COLLECT(f.name) AS friends
RETURN n, friends
```

### 7. Multiple MATCH Patterns in Single Clause

**Status**: ⚠️ Limited Support

**Supported**: Single pattern per `MATCH` clause
**Not Supported**: Comma-separated patterns in one `MATCH` (INFERRED)

**Example**:
```cypher
-- ⚠️ May not work
MATCH (a)-[:KNOWS]->(b), (b)-[:LIKES]->(c)
RETURN a, b, c

-- ✅ Workaround: use multiple MATCH clauses
MATCH (a)-[:KNOWS]->(b)
MATCH (b)-[:LIKES]->(c)
RETURN a, b, c
```

### 8. Map Projections

**Status**: ⚠️ Partial Support

**Supported**: Basic map literals `{key: value}` (test: [test_35_map_literals.py](../tests/transpile_tests/test_35_map_literals.py))
**Not Supported**: Map projections with property selectors `n{.id, .name}` (INFERRED)

**Example**:
```cypher
-- ⚠️ May not work
RETURN n{.id, .name, .age} AS person

-- ✅ Workaround: explicit map construction
RETURN {id: n.id, name: n.name, age: n.age} AS person
```

### 9. Temporal Types (Duration, Temporal Arithmetic)

**Status**: ⚠️ Partial Support

**Supported**: Basic datetime functions (test: [test_34_datetime.py](../tests/transpile_tests/test_34_datetime.py))
**Not Supported**: `duration()`, temporal arithmetic (INFERRED)

**Example**:
```cypher
-- ⚠️ May not work
RETURN datetime() - duration({days: 7}) AS last_week

-- ✅ Workaround: use Databricks date functions
RETURN date_sub(current_timestamp(), 7) AS last_week
```

### 10. Geospatial Functions

**Status**: ❌ Not Supported (INFERRED - no tests found)

**Reason**: No OpenCypher geospatial support in transpiler.

**Example (unsupported)**:
```cypher
RETURN distance(point({x: 0, y: 0}), point({x: 3, y: 4})) AS dist
```

**Workaround**: Use Databricks Geospatial functions directly in SQL.

---

## Known Correctness Caveats

### 1. Aggregation After Aggregation

**Status**: ⚠️ Under Investigation

**Issue**: Recent commits mention fixes for "aggregation entity projection" bugs (see commit: `155da2f`).

**Potentially Problematic Pattern**:
```cypher
MATCH (p:Person)-[:BOUGHT]->(product)
WITH p, COUNT(*) AS purchases
WITH p.name, SUM(purchases) AS total  -- Aggregation after aggregation
RETURN p.name, total
```

**Related Test**: [tests/test_aggregation_entity_projection.py](../tests/test_aggregation_entity_projection.py)

**Workaround**: If you encounter `UNRESOLVED_COLUMN` errors, try flattening the aggregation:
```cypher
MATCH (p:Person)-[:BOUGHT]->(product)
WITH p.name, COUNT(*) AS total
RETURN p.name, total
```

### 2. Multi-WITH Entity Continuation

**Status**: ⚠️ Fixed Recently

**Issue**: Recent tests added for "multi-WITH entity continuation bug" (commit: `7ec0add`).

**Affected Pattern**:
```cypher
MATCH (n:Node)
WITH n
WITH n, n.property AS prop
RETURN n, prop
```

**Related Test**: [tests/test_multi_with_entity_continuation.py](../tests/test_multi_with_entity_continuation.py)

**Status**: Should be fixed in latest version. If you encounter issues, report with minimal repro case.

### 3. UNRESOLVED_COLUMN Errors with Aggregations

**Status**: ⚠️ Edge Cases Remain (INFERRED)

**Context**: Column resolution after aggregation boundaries is complex. Some edge cases may trigger Databricks `UNRESOLVED_COLUMN` errors.

**Symptom**: Transpilation succeeds, but Databricks SQL execution fails with "column not found".

**Workaround**:
1. Simplify `WITH` clauses (fewer intermediate steps)
2. Explicitly alias all aggregated columns
3. Avoid referencing entity properties after aggregation (use aliases instead)

**Related Commits**:
- `155da2f`: "preserve full column names for entity projections after aggregation"
- `6388679`: "use the AggregationBoundaryOperator itself"

---

## Performance Caveats

### 1. Deep Variable-Length Paths

**Issue**: `WITH RECURSIVE` CTEs for deep paths (e.g., `-[:TYPE*1..20]->`) can be slow or hit Spark recursion limits.

**Recommendation**:
- Keep max depth ≤ 10 for most queries
- Use `LIMIT` to reduce result set size
- Consider pre-computing transitive closures for frequent deep traversals

**Example**:
```cypher
-- ⚠️ May be slow
MATCH (a)-[:FOLLOWS*1..20]->(b)
RETURN DISTINCT b

-- ✅ Better: bounded depth with LIMIT
MATCH (a)-[:FOLLOWS*1..5]->(b)
RETURN DISTINCT b
LIMIT 100
```

### 2. Cartesian Products

**Issue**: Certain query patterns generate cartesian products (cross joins without predicates).

**Detection**: Transpiler attempts to avoid cartesian products (test: [test_10_relationship_join_no_cartesian.py](../tests/transpile_tests/test_10_relationship_join_no_cartesian.py))

**Recommendation**: Always connect patterns with shared variables or predicates.

**Example**:
```cypher
-- ❌ Cartesian product (no connection between patterns)
MATCH (p:Person), (c:Company)
RETURN p, c

-- ✅ Better: connected patterns
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
RETURN p, c
```

### 3. Large COLLECT Aggregations

**Issue**: `COLLECT()` aggregations create arrays in memory. Very large collections can cause OOM.

**Affected Pattern**:
```cypher
MATCH (p:Person)-[:BOUGHT]->(product)
RETURN p.name, COLLECT(product) AS all_products  -- Could be millions
```

**Recommendation**:
- Use `LIMIT` within aggregation: `COLLECT(product)[0..100]` (if supported)
- Filter before aggregation: `WHERE product.category = 'Electronics'`
- Use `COUNT()` instead of `COLLECT()` when possible

### 4. Edge Collection in Recursive CTEs

**Issue**: Path analyzer optimizes away edge collection when not needed (see [Decision 8 in decision log](03-decision-log.md#decision-8-path-analyzer-for-edge-optimization)).

**Impact**: If you access `relationships(path)`, the transpiler collects edges in recursive CTE (expensive).

**Recommendation**: Only use `relationships(path)` when necessary. Use `length(path)` for simple path length checks.

---

## SQL Dialect Constraints

### 1. Databricks-Specific Functions

The transpiler generates SQL using Databricks-specific functions:

| Function | Purpose | Databricks-Specific? |
|----------|---------|---------------------|
| `array_contains()` | Cycle detection | Yes (some DBs use `ANY()`) |
| `CONCAT(array1, array2)` | Array concatenation | Yes (PostgreSQL uses `||`) |
| `COLLECT()` | Aggregation to array | No (SQL standard `ARRAY_AGG()`) |
| `STRUCT()` | Named tuple | Yes (PostgreSQL uses `ROW()`) |

**Implication**: Generated SQL is **not portable** to other databases without modification.

**Related Decision**: [Decision 4 in decision log](03-decision-log.md#decision-4-alternative-sql-backends)

### 2. Recursive CTE Limitations

**Databricks Specifics**:
- No `CYCLE` clause (must manually track visited nodes with array)
- No `SEARCH` clause (must manually compute depth)
- Performance: Recursive CTEs may not be optimized as well as native graph traversal

---

## Open Questions / TODOs

### 1. Full List of Unsupported Functions

**Status**: Documentation incomplete

**TODO**: Audit all OpenCypher functions and document which are unsupported.

**Known gaps**:
- ❌ Geospatial: `distance()`, `point()`, `withinBBox()`
- ❌ Graph algorithms: `pageRank()`, `betweenness()`, etc. (Neo4j GDS)
- ❌ APOC: All `apoc.*` functions
- ⚠️ Date/time: Partial support (needs audit)

**Where to add**: This document (section above)

### 2. Neo4j Compatibility Matrix

**Status**: No formal compatibility documentation

**TODO**: Create a compatibility matrix showing which Neo4j features work vs. don't work.

**Format**:
```
| Feature              | Neo4j | gsql2rsql | Notes |
|----------------------|-------|-----------|-------|
| MATCH pattern        | ✅    | ✅        | Full  |
| Variable-length path | ✅    | ✅        | Max depth 10 recommended |
| shortestPath()       | ✅    | ❌        | Use workaround |
| ...                  | ...   | ...       | ...   |
```

**Where to add**: New doc `docs/09-neo4j-compatibility.md`

### 3. PySpark Test Coverage

**Status**: Not all tests have PySpark validation

**Context**: Golden file tests (44 tests) validate SQL output, but not all are executed on PySpark.

**TODO**: Identify which patterns are **not** covered by PySpark tests.

**Command**: Compare test counts:
```bash
# Golden file tests
ls tests/transpile_tests/*.py | wc -l  # 44+

# PySpark examples
ls examples/*.yaml  # 3 files (credit, fraud, features)
```

**Related Files**:
- [tests/test_examples_with_pyspark.py](../tests/test_examples_with_pyspark.py)
- [examples/](../examples/)

### 4. Aggregation Semantics Edge Cases

**Status**: Recent bug fixes suggest edge cases remain

**Context**: Commits `155da2f`, `6388679`, `7ec0add` all relate to aggregation bugs.

**TODO**: Document known patterns that may still have issues.

**Investigation needed**:
1. Review all recent aggregation-related commits
2. Document specific patterns that were buggy
3. Add regression tests for each pattern

**Related Tests**:
- [tests/test_aggregation_entity_projection.py](../tests/test_aggregation_entity_projection.py)
- [tests/test_multi_with_entity_continuation.py](../tests/test_multi_with_entity_continuation.py)
- [tests/transpile_tests/test_41_column_projection_through_aggregation.py](../tests/transpile_tests/test_41_column_projection_through_aggregation.py)

### 5. Subquery Optimizer Completeness

**Status**: Conservative optimizer only handles simple cases

**TODO**: Document all patterns that **could** be flattened but aren't (yet).

**Examples**:
- Multiple consecutive `SelectionOperator` (already handled)
- `SelectionOperator` → `ProjectionOperator` (already handled)
- `ProjectionOperator` → `SelectionOperator` (NOT handled - could be safe)

**Related File**: [src/gsql2rsql/planner/subquery_optimizer.py](../src/gsql2rsql/planner/subquery_optimizer.py)

### 6. Error Message Quality

**Status**: Resolver provides good suggestions, but other phases may not

**TODO**: Audit error messages across all phases:
- Parser: ANTLR errors (often cryptic)
- Planner: Schema binding errors
- Resolver: Column resolution errors (✅ good)
- Renderer: SQL generation errors

**Where to improve**: [src/gsql2rsql/common/exceptions.py](../src/gsql2rsql/common/exceptions.py)

---

## Recommended Workarounds Summary

| Limitation | Workaround |
|------------|------------|
| MERGE / CREATE / DELETE | Use native Databricks SQL statements |
| shortestPath() | Use bounded BFS with `ORDER BY depth LIMIT 1` |
| Deep paths (>10 hops) | Pre-compute transitive closure or use external graph DB |
| Large COLLECT() | Filter before aggregation, use LIMIT |
| APOC functions | Find equivalent Databricks SQL function or UDF |
| Geospatial | Use Databricks Geospatial functions directly |
| Pattern comprehension with WHERE | Use explicit `MATCH` + `WITH` + `COLLECT()` |
| Cartesian products | Always connect patterns with shared variables |
| UNRESOLVED_COLUMN errors | Simplify `WITH` clauses, use explicit aliases |

---

## Where to Look Next

- [05-testing-and-examples.md](05-testing-and-examples.md) — How to add tests for new patterns
- [07-developer-guide.md](07-developer-guide.md) — How to extend support for new features
- [03-decision-log.md](03-decision-log.md) — Why certain features are not supported
