# Architectural Decision Log

This document captures important design decisions, their rationale, and where they're implemented in the codebase.

---

## Decision 1: Strict 4-Phase Separation of Concerns

**Status**: ✅ Adopted (documented in [CONTRIBUTING.md](../CONTRIBUTING.md))

### Context
Transpilers often mix parsing, semantic analysis, and code generation, leading to tight coupling and hard-to-debug issues.

### Decision
Enforce strict separation across 4 phases:
1. **Parser**: Syntax only (no schema access)
2. **Planner**: Semantics only (no column validation)
3. **Resolver**: Validation only (no SQL generation)
4. **Renderer**: Code generation only (no semantic decisions)

### Rationale
- **Correctness**: Each phase has clear invariants and responsibilities
- **Testability**: Can test each phase independently with mocked inputs
- **Debuggability**: Can inspect intermediate representations (AST, operators, resolution results)
- **Maintainability**: Changes to one phase don't cascade to others

### Trade-offs
- **More code**: Separate data structures for each phase (AST, operators, resolved refs)
- **More memory**: Keep AST + operators + resolution in memory
- **Performance**: Multiple passes over the data structure (acceptable for query compilation)

### Implementation
- Parser: [src/gsql2rsql/parser/](../src/gsql2rsql/parser/)
- Planner: [src/gsql2rsql/planner/logical_plan.py](../src/gsql2rsql/planner/logical_plan.py)
- Resolver: [src/gsql2rsql/planner/column_resolver.py](../src/gsql2rsql/planner/column_resolver.py)
- Renderer: [src/gsql2rsql/renderer/sql_renderer.py](../src/gsql2rsql/renderer/sql_renderer.py)

---

## Decision 2: ANTLR for Parsing (vs. Hand-Written Parser)

**Status**: ✅ Adopted

### Context
OpenCypher grammar is complex (multiple expression types, operator precedence, pattern matching). Need robust error recovery.

### Decision
Use ANTLR 4.13+ parser generator with visitor pattern to build AST.

### Rationale
- **Correctness**: Proven grammar, handles precedence and associativity correctly
- **Maintainability**: Grammar is declarative and separate from code
- **Error recovery**: ANTLR has excellent error reporting and recovery
- **Standards**: OpenCypher community provides reference ANTLR grammars

### Trade-offs
- **Dependency**: Requires ANTLR runtime (`antlr4-python3-runtime>=4.13.0`)
- **Build step**: Must regenerate parser when grammar changes (`make grammar`)
- **Generated code**: ~8000 lines of generated parser code in repo

### Implementation
- Grammar: [CypherParser.g4](../CypherParser.g4) (INFERRED: root-level grammar file)
- Generated files: [src/gsql2rsql/parser/grammar/](../src/gsql2rsql/parser/grammar/)
- Visitor: [src/gsql2rsql/parser/visitor.py](../src/gsql2rsql/parser/visitor.py)
- Entry point: [src/gsql2rsql/parser/opencypher_parser.py](../src/gsql2rsql/parser/opencypher_parser.py)

---

## Decision 3: Logical Operators (vs. Direct AST → SQL)

**Status**: ✅ Adopted

### Context
Direct AST-to-SQL translation is fragile and hard to optimize. SQL generation logic becomes deeply nested.

### Decision
Introduce a logical operator layer (relational algebra) between AST and SQL.

### Rationale
- **Optimization**: Can apply transformations on operators before SQL generation
- **Clarity**: Operators have clear semantics (DataSource, Join, Selection, Projection, etc.)
- **Correctness**: Easier to reason about query semantics in relational algebra terms
- **Extensibility**: New SQL backends can reuse operator layer

### Trade-offs
- **Complexity**: Another intermediate representation
- **Memory**: Operator tree + AST in memory simultaneously during planning

### Implementation
- Operators: [src/gsql2rsql/planner/operators.py](../src/gsql2rsql/planner/operators.py)
- Conversion: [src/gsql2rsql/planner/logical_plan.py:LogicalPlan.process_query_tree()](../src/gsql2rsql/planner/logical_plan.py) (~line 100+)
- Rendering: [src/gsql2rsql/renderer/sql_renderer.py](../src/gsql2rsql/renderer/sql_renderer.py)

---

## Decision 4: Symbol Table with Nested Scopes

**Status**: ✅ Adopted

### Context
Cypher `WITH` clauses create new scopes. Variables can be redefined. Need to track what's available at each point in the query.

### Decision
Build a symbol table during planning with support for nested scopes. Each `WITH` creates a new scope.

### Rationale
- **Correctness**: Prevents variable name collisions
- **Error messages**: Can show available variables when reference is invalid
- **Type tracking**: Track entity vs. value types for each variable

### Trade-offs
- **Complexity**: Scope management adds cognitive overhead
- **Edge cases**: Handling variable shadowing, scope boundaries

### Implementation
- Symbol table: [src/gsql2rsql/planner/symbol_table.py](../src/gsql2rsql/planner/symbol_table.py)
- Usage in planner: [src/gsql2rsql/planner/logical_plan.py](../src/gsql2rsql/planner/logical_plan.py) (built during AST traversal)
- Scope tracking: `SymbolTable.enter_scope()` / `SymbolTable.exit_scope()` methods

---

## Decision 5: Separate Resolution Phase (vs. Resolving During Planning)

**Status**: ✅ Adopted (documented in [CONTRIBUTING.md](../CONTRIBUTING.md))

### Context
Column references can't be validated during planning because:
1. Schema propagation happens bottom-up
2. Aggregation boundaries affect column availability
3. Need full operator graph to compute scopes

### Decision
Add a separate resolution phase after planning. Walk operator graph, build scopes, resolve all column references.

### Rationale
- **Correctness**: Full context available for validation
- **Error messages**: Can provide suggestions using Levenshtein distance
- **Separation**: Renderer doesn't need to make semantic decisions

### Trade-offs
- **Extra pass**: Must visit operator graph twice (once for planning, once for resolution)
- **Memory**: Store ResolutionResult alongside LogicalPlan

### Implementation
- Resolver: [src/gsql2rsql/planner/column_resolver.py:ColumnResolver.resolve()](../src/gsql2rsql/planner/column_resolver.py) (~line 50+)
- Invocation: [src/gsql2rsql/planner/logical_plan.py:LogicalPlan.resolve()](../src/gsql2rsql/planner/logical_plan.py)
- Resolution data: [src/gsql2rsql/planner/column_ref.py](../src/gsql2rsql/planner/column_ref.py)

---

## Decision 6: Conservative Optimizer (vs. Aggressive Flattening)

**Status**: ✅ Adopted

### Context
SQL subquery nesting can be deep, affecting readability and performance. But aggressive flattening can change semantics.

### Decision
Only flatten patterns **proven safe**:
- ✅ Selection → Projection (WHERE before SELECT)
- ✅ Selection → Selection (merge WHERE clauses)
- ❌ Projection → Projection (aliases must stay in separate subqueries)
- ❌ Anything involving aggregation

### Rationale
- **Safety first**: Wrong results are worse than slower SQL
- **Transparency**: User can disable with `--no-optimize`
- **Debuggability**: Easier to debug generated SQL when flattening is conservative

### Trade-offs
- **Performance**: May generate more subqueries than necessary
- **SQL readability**: Nested queries can be hard to read

### Implementation
- Optimizer: [src/gsql2rsql/planner/subquery_optimizer.py:SubqueryFlatteningOptimizer](../src/gsql2rsql/planner/subquery_optimizer.py)
- Invocation: CLI flag `--optimize` (enabled by default, can disable with `--no-optimize`)
- Rules: `_can_flatten_selection_projection()`, `_can_flatten_selection_selection()` methods

---

## Decision 7: WITH RECURSIVE for Variable-Length Paths

**Status**: ✅ Adopted

### Context
Variable-length paths (`-[:KNOWS*1..5]->`) require graph traversal. Options:
1. Generate PySpark DataFrame code (requires runtime, not pure SQL)
2. Use `WITH RECURSIVE` CTE (SQL standard, Databricks 17+)
3. Expand paths at transpile time (exponential blowup)

### Decision
Use `WITH RECURSIVE` CTEs for variable-length paths. Generate BFS/DFS traversal in SQL.

### Rationale
- **Pure SQL**: No runtime dependencies beyond Databricks SQL
- **Scalability**: Recursive CTE scales to large graphs
- **Correctness**: Cycle detection with `visited` array
- **Standard**: SQL standard feature, widely supported

### Trade-offs
- **Databricks 17+ only**: Not supported in older runtimes
- **Performance**: Recursive CTEs can be slow for deep/wide graphs
- **Complexity**: Generated SQL is verbose

### Implementation
- Operator: [src/gsql2rsql/planner/operators.py:RecursiveTraversalOperator](../src/gsql2rsql/planner/operators.py)
- Rendering: [src/gsql2rsql/renderer/sql_renderer.py:_render_recursive()](../src/gsql2rsql/renderer/sql_renderer.py) (~line 800+)
- Example: See [tests/output/expected/21_variable_length_zero.sql](../tests/output/expected/21_variable_length_zero.sql)

---

## Decision 8: Path Analyzer for Edge Optimization

**Status**: ✅ Adopted

### Context
Variable-length paths generate edge collections by default. But if the user doesn't access `relationships(path)`, we're collecting edges for nothing.

### Decision
Analyze usage of `relationships(path)` function. If not used, skip edge collection in recursive CTE.

### Rationale
- **Performance**: Collecting edges is expensive (ARRAY CONCAT on every recursion)
- **Memory**: Edge arrays grow linearly with path depth
- **Optimization**: Zero-cost abstraction when edges aren't needed

### Trade-offs
- **Complexity**: Must analyze expression trees to detect usage
- **Correctness**: Must handle ALL/ANY predicates that access edges

### Implementation
- Analyzer: [src/gsql2rsql/planner/path_analyzer.py:PathAnalyzer](../src/gsql2rsql/planner/path_analyzer.py)
- Usage: Called during planning for `RecursiveTraversalOperator` construction
- Optimization: `needs_edge_collection` flag in `RecursiveTraversalOperator`

---

## Decision 9: Structured SQL Column Naming

**Status**: ✅ Adopted

### Context
Cypher variables (`p`, `f`) must map to SQL columns. Need to avoid collisions and track entity vs. property.

### Decision
Use structured naming convention: `_gsql2rsql_{variable}_{property}`
- Entity projection: `_gsql2rsql_p_id AS p`
- Property projection: `_gsql2rsql_p_name AS name`

### Rationale
- **Collision avoidance**: Prefix prevents conflicts with user column names
- **Tracking**: Can parse column name to extract variable and property
- **Debugging**: Clear provenance in generated SQL

### Trade-offs
- **Verbosity**: Column names are long
- **SQL readability**: Generated SQL is harder to read for humans

### Implementation
- Naming: [src/gsql2rsql/planner/column_ref.py:compute_sql_column_name()](../src/gsql2rsql/planner/column_ref.py)
- Usage: Throughout [src/gsql2rsql/renderer/sql_renderer.py](../src/gsql2rsql/renderer/sql_renderer.py)

---

## Decision 10: Schema as JSON (vs. Code or YAML)

**Status**: ✅ Adopted (JSON), ⚠️ YAML for examples

### Context
Graph schema defines nodes, edges, properties, and their SQL table mappings. Need a format that's:
- Human-readable for editing
- Machine-parseable for validation
- Version-controllable

### Decision
Use JSON for schema definitions. YAML for example collections (with embedded schemas).

### Rationale
- **JSON**: Strict, widely supported, easy to validate with JSON Schema
- **YAML**: More readable for large example collections with comments
- **Separation**: Schema is separate from queries (vs. inline)

### Trade-offs
- **JSON verbosity**: More braces and quotes than YAML
- **No comments**: JSON doesn't support comments (use description fields)
- **Two formats**: Inconsistency between schema.json and examples/*.yaml

### Implementation
- JSON schema: [examples/schema.json](../examples/schema.json)
- YAML examples: [examples/credit_queries.yaml](../examples/credit_queries.yaml)
- Schema loading: [src/gsql2rsql/common/schema.py](../src/gsql2rsql/common/schema.py)
- YAML loading: [src/gsql2rsql/pyspark_executor.py:load_schema_from_yaml()](../src/gsql2rsql/pyspark_executor.py)

---

## Decision 11: Golden File Testing

**Status**: ✅ Adopted

### Context
Need to validate that transpiler generates correct SQL. Can't compare generated SQL directly (whitespace, formatting differences).

### Decision
Use golden file testing: store expected SQL in `tests/output/expected/`, compare normalized SQL.

### Rationale
- **Regression detection**: Any change to generated SQL is caught
- **Visual diff**: Can use standard diff tools to review changes
- **Documentation**: Expected files serve as examples

### Trade-offs
- **Maintenance**: Must update expected files when SQL format changes
- **False positives**: Formatting changes trigger test failures

### Implementation
- Expected SQL: [tests/output/expected/](../tests/output/expected/)
- Comparison: [tests/utils/sql_test_utils.py:assert_sql_equal()](../tests/utils/sql_test_utils.py)
- Diff output: [tests/output/diff/](../tests/output/diff/)
- Update command: `make dump-sql-save ID=01 NAME=simple_node_lookup`

---

## Decision 12: PySpark Validation Tests

**Status**: ✅ Adopted

### Context
Generated SQL might be syntactically correct but semantically wrong. Need to validate against real execution.

### Decision
Run transpiled SQL on PySpark DataFrames, compare results against expected outcomes.

### Rationale
- **Correctness**: Catches semantic bugs that golden files miss
- **Integration**: Validates entire pipeline end-to-end
- **Realistic**: Uses actual Spark SQL engine

### Trade-offs
- **Slow**: PySpark tests take 10-20x longer than unit tests
- **Dependency**: Requires PySpark (only for dev, not for users)
- **Flakiness**: Spark startup/shutdown can be flaky

### Implementation
- Test file: [tests/test_examples_with_pyspark.py](../tests/test_examples_with_pyspark.py)
- Executor: [src/gsql2rsql/pyspark_executor.py](../src/gsql2rsql/pyspark_executor.py)
- Run command: `make test-pyspark-examples`
- Quick validation: `make test-pyspark-quick` (first 5 examples)

---

## Decision 13: Textual TUI for Interactive Development

**Status**: ✅ Adopted

### Context
Developers need to iterate on queries quickly. Command-line round-trip is slow. Need visual feedback for errors.

### Decision
Build interactive TUI using Textual framework with live transpilation, schema switching, and clipboard support.

### Rationale
- **Productivity**: Edit query, see SQL immediately
- **Discoverability**: Browse curated examples from YAML files
- **Debugging**: See AST, operators, scope info in UI
- **UX**: Rich formatting, syntax highlighting, copy-paste support

### Trade-offs
- **Dependency**: Requires `textual>=0.47.0` (heavy framework)
- **Maintenance**: TUI code is complex (2200 lines in cli.py)

### Implementation
- TUI command: `gsql2rsql tui --examples examples/credit_queries.yaml`
- Implementation: [src/gsql2rsql/cli.py:tui()](../src/gsql2rsql/cli.py) (~line 1500+)
- Dependencies: `textual`, `rich`, `prompt-toolkit`

---

## Decision 14: Defensive Handling of Pre-Rendered Field Names

**Status**: ✅ Adopted (2026-01-19)

### Context
Different operators produce entity fields with different naming states:
- **DataSourceOperator**: `field_name = "id"` (simple property name)
- **RecursiveTraversalOperator**: `field_name = "_gsql2rsql_peer_id"` (full SQL name)

Renderer methods assumed all `field_name` values were simple property names, causing double-prefixing for recursive traversal outputs.

### Decision
Add defensive checks in renderer to detect pre-rendered SQL column names:
```python
if field.field_name and field.field_name.startswith(COLUMN_PREFIX):
    # Already a full SQL name - use as-is
    key = field.field_name
else:
    # Simple property name - construct full SQL name
    key = self._get_field_name(entity_alias, field.field_alias)
```

### Rationale
- **Correctness**: Prevents double-prefixing (`_gsql2rsql_peer_peer_id` ❌)
- **Backward compatible**: Doesn't break existing DataSourceOperator behavior
- **Minimal changes**: Only affects renderer, no planner changes needed
- **Preserves information**: Keeps pre-rendered names for debugging

### Trade-offs
- **More complex**: Requires checks in 10+ locations across renderer
- **Implicit contract**: Relies on `COLUMN_PREFIX` to detect pre-rendered names
- **Alternative rejected**: Could have cleared `field_name` after recursive traversal, but loses debugging information

### Implementation
- Renderer: [src/gsql2rsql/renderer/sql_renderer.py](../src/gsql2rsql/renderer/sql_renderer.py)
  - `_collect_required_columns()` (~lines 447-490)
  - `_render_join_conditions()` (~lines 2311-2394)
  - `_render_join_projection()` (~lines 1941-2049)
  - `_extract_columns_from_schema()` (~lines 2166-2200)
- Tests: [tests/test_variable_length_path_field_naming.py](../tests/test_variable_length_path_field_naming.py)
- Documentation: [docs/development/VARLEN_PATH_FIELD_NAMING_FIX.md](development/VARLEN_PATH_FIELD_NAMING_FIX.md)

---

## Open Questions / TODOs

### 1. Support for Neo4j-Specific Functions
**Question**: Should we add support for Neo4j-specific functions (e.g., `apoc.*`)?
**Trade-off**: Would require custom UDFs in Databricks, breaking pure SQL generation.
**Decision needed**: User feedback on priority of Neo4j compatibility.

### 2. Multi-Graph Support
**Question**: Should schema support multiple named graphs (vs. single graph per schema)?
**Current**: Single graph per schema file.
**Trade-off**: More complex schema format, but enables multi-tenancy patterns.

### 3. Query Planner Hints
**Question**: Should we allow users to hint join order or index usage in Cypher comments?
**Current**: No hint support, Databricks optimizer makes all decisions.
**Trade-off**: Power users want control, but hints are non-portable.

### 4. Alternative SQL Backends
**Question**: Should we support other SQL dialects (PostgreSQL, DuckDB, SQLite)?
**Current**: Databricks-only.
**Trade-off**: Broader adoption vs. maintenance burden. Need to abstract SQL dialect differences.
**File to modify**: [src/gsql2rsql/renderer/sql_renderer.py](../src/gsql2rsql/renderer/sql_renderer.py) (add dialect parameter)

### 5. Property Graph Schema Import
**Question**: Should we auto-generate schema from existing Databricks tables using `DESCRIBE TABLE`?
**Current**: Manual JSON schema creation.
**Trade-off**: Convenience vs. requiring database connection at transpile time.
**Related command**: `gsql2rsql init-schema` (currently generates template, could introspect DB)

### 6. Incremental Compilation
**Question**: Should we cache AST/operators for repeated queries?
**Current**: Full transpilation on every invocation.
**Trade-off**: Performance vs. complexity. Only matters for hot-path query generation.

### 7. Aggregation After Aggregation
**Question**: Current architecture allows aggregation after aggregation. Is this correct?
**Context**: Recent commits mention "aggregation entity projection" bugs.
**Investigation needed**: Review [src/gsql2rsql/planner/operators.py:AggregationBoundaryOperator](../src/gsql2rsql/planner/operators.py) (~line 600+)
**Related test**: [tests/test_aggregation_entity_projection.py](../tests/test_aggregation_entity_projection.py)

### 8. Predicate Pushdown Completeness
**Question**: Are all pushable predicates being pushed to recursive CTEs?
**Context**: PathAnalyzer handles some cases, but complex predicates might not be covered.
**Investigation needed**: Review [src/gsql2rsql/planner/path_analyzer.py](../src/gsql2rsql/planner/path_analyzer.py)
**Related tests**: `test_37_source_node_filter_pushdown.py`, `test_39_recursive_sink_filter_pushdown.py`

---

## Where to Look Next

- [04-limitations.md](04-limitations.md) — Known limitations and unsupported features
- [CONTRIBUTING.md](../CONTRIBUTING.md) — Detailed phase boundaries
- [02-architecture.md](02-architecture.md) — Component breakdown
