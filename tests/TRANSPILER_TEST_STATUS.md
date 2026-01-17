# gsql2rsql Transpiler Test Coverage Status

## Overview

This document tracks the implementation status of automated transpiler tests for read-only OpenCypher queries. Each test validates that the transpiler correctly converts OpenCypher to Databricks SQL.

## Test Infrastructure

The testing framework uses a **spec-driven golden file** approach with **structural SQL assertions**:

### Directory Structure

```
tests/
├── docs/                           # Markdown specifications
│   ├── 01_simple_node_lookup.md
│   ├── 02_node_with_property_filter.md
│   └── ...
├── output/                         # Test artifacts
│   ├── expected/                   # Golden files (canonical SQL)
│   │   ├── 01_simple_node_lookup.sql
│   │   └── ...
│   ├── actual/                     # Generated SQL on failure
│   └── diff/                       # Unified diffs on failure
├── transpile_tests/                # Automated tests
│   ├── test_01_simple_node_lookup.py
│   └── ...
├── utils/                          # Test utilities
│   ├── sql_test_utils.py           # Golden file comparison
│   └── sql_assertions.py           # Structural assertions
└── TRANSPILER_TEST_STATUS.md       # This file
```

### Key Commands

```bash
# Run all transpiler tests
make test-transpile

# Run only golden file tests
make test-transpile-golden

# Dump SQL for specific test with diff
make dump-sql-01
make dump-sql-02
make dump-sql-03

# Dump SQL for custom query
make dump-sql ID=01 NAME=simple_node_lookup
make dump-sql-custom CYPHER="MATCH (n:Person) RETURN n.name"

# Show all diffs
make diff-all
```

## Test Status

### ✅ Completed Tests (4 queries)

| ID | Query Type | Status | Golden File | Test |
|----|------------|--------|-------------|------|
| 01 | Simple node lookup by label | ✅ PASS | [01_simple_node_lookup.sql](output/expected/01_simple_node_lookup.sql) | test_01_simple_node_lookup.py |
| 02 | Node lookup with property filter | ✅ PASS | [02_node_with_property_filter.sql](output/expected/02_node_with_property_filter.sql) | test_02_node_with_property_filter.py |
| 03 | Property projection with aliases | ✅ PASS | [03_property_projection_aliases.sql](output/expected/03_property_projection_aliases.sql) | test_03_property_projection_aliases.py |
| 06 | Single-hop directed relationship match | ✅ PASS | - | test_06_single_hop_relationship.py |

**Total: 4/35 (11%)**

### 📝 Pending Tests (31 queries)

See full list at the bottom of this document.

## Test Framework Features

### 1. Golden File Comparison

Each test compares transpiler output against a canonical expected SQL file:

```python
def test_golden_file_match(self) -> None:
    actual_sql = self._transpile(cypher)
    expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
    assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)
```

On failure, artifacts are written to:
- `tests/output/actual/<id>_<name>.sql` - Generated SQL
- `tests/output/diff/<id>_<name>.diff` - Unified diff

### 2. SQL Normalization

Before comparison, SQL is normalized:
- Remove backticks
- Lowercase keywords
- Normalize whitespace
- Collapse empty lines

### 3. Structural Assertions

Available assertions in `tests/utils/sql_assertions.py`:

```python
# Basic structure
assert_has_select(sql)
assert_has_from_table(sql, "Person")
assert_has_where(sql, condition="name = 'Alice'")

# Joins
assert_has_join(sql, join_type="LEFT", count=2)
assert_no_join(sql)
assert_no_cartesian_join(sql)

# Clauses
assert_has_order_by(sql, column="name", direction="ASC")
assert_has_limit_offset(sql, limit=10, offset=5)
assert_has_group_by(sql, columns=["id"])
assert_has_having(sql)

# Aggregation
assert_has_aggregation(sql, function="COUNT")
assert_has_array_agg(sql)

# Recursion (for variable-length paths)
assert_has_recursive_cte(sql, cte_name="paths")
assert_cycle_detection(sql)
assert_depth_limit(sql, max_depth=5)

# Projection
assert_projected_columns(sql, ["id", "name"])
assert_has_distinct(sql)

# OPTIONAL MATCH
assert_left_join_for_optional(sql)
```

### 4. SQLStructure Parser

For programmatic SQL analysis:

```python
from tests.utils.sql_assertions import SQLStructure

structure = SQLStructure(raw_sql=sql)
assert structure.has_where
assert structure.has_recursive_cte
assert structure.limit_value == 10
assert len(structure.join_clauses) == 2
```

## Creating New Tests

### Step 1: Create Golden File

```bash
# Generate and inspect SQL
make dump-sql-custom CYPHER="MATCH (p:Person) WHERE p.age > 21 RETURN p.name"

# Save as golden file
# Edit tests/output/expected/<id>_<name>.sql
```

### Step 2: Create Test File

```python
"""Test NN: Description."""

from tests.utils.sql_test_utils import assert_sql_equal, load_expected_sql
from tests.utils.sql_assertions import (
    assert_has_select,
    assert_has_where,
    SQLStructure,
)

class TestQueryName:
    TEST_ID = "NN"
    TEST_NAME = "query_name"

    def setup_method(self) -> None:
        # Schema setup...
        pass

    def _transpile(self, cypher: str) -> str:
        # Transpilation logic...
        pass

    def test_golden_file_match(self) -> None:
        """Test transpiled SQL matches golden file."""
        actual_sql = self._transpile(cypher)
        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_structural_has_where(self) -> None:
        """Test SQL has WHERE clause."""
        sql = self._transpile(cypher)
        assert_has_where(sql)
```

### Step 3: Run and Iterate

```bash
# Run test
make test-transpile

# On failure, inspect diff
cat tests/output/diff/<id>_<name>.diff

# Fix transpiler or update golden file
```

## Known Limitations

1. **Inline property maps**: `MATCH (p:Person {name: 'Alice'})` not supported
   - **Workaround**: Use `WHERE p.name = 'Alice'`

2. **Join conditions**: Uses `ON TRUE` cartesian joins
   - Functionally correct but inefficient

3. **Entity projection**: `RETURN p` projects all schema properties

## Pending Tests

### Basic Queries
- 04: Pagination with SKIP and LIMIT
- 05: Count nodes by label

### Relationship Queries
- 07: Relationship match with property filter
- 08: Undirected relationship match
- 09: OPTIONAL MATCH (left join semantics)

### Aggregation Queries
- 10: Aggregation with GROUP BY
- 11: Aggregation with ORDER BY
- 12: HAVING-like aggregation filter using WITH

### Variable-Length Path Queries
- 13: Variable-length path (bounded) - `[:KNOWS*1..5]`
- 14: Variable-length path including zero-length - `[:KNOWS*0..3]`

### Collection Operations
- 15: Collect aggregation
- 16: Collect with DISTINCT
- 17: Ordered collect

### List Operations
- 18: UNWIND list
- 19: List comprehension
- 20: Pattern comprehension

### Conditional Expressions
- 21: CASE expression
- 22: COALESCE function

### Predicates
- 23: EXISTS pattern predicate
- 24: ALL / ANY predicate on path
- 25: REDUCE over path relationships

### Path Functions
- 26: Path functions (nodes, relationships, length)
- 27: Map projection

### Advanced Queries
- 28: DISTINCT rows
- 29: UNION
- 30: Multiple MATCH (cartesian product)
- 31: Subquery with CALL
- 32: Correlated subquery returning list
- 33: Parameterized IN list
- 34: Property existence check (IS NULL)
- 35: Backtick-escaped labels and relationships
