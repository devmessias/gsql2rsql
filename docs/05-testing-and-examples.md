# Testing and Examples Guide

This document explains the test suite organization, how tests map to features, and how to add new tests or examples.

---

## Test Suite Overview

The test suite has **61 test files** organized into categories:

```
tests/
├── conftest.py                           # Pytest fixtures (shared test data)
│
├── transpile_tests/                      # Feature tests (44 golden file tests)
│   ├── test_01_simple_node_lookup.py
│   ├── test_06_single_hop_relationship.py
│   ├── test_11_aggregation_group_by.py
│   ├── test_16_variable_length_path.py  # (INFERRED - not found in listing)
│   └── ... (44 total)
│
├── test_parser.py                        # Unit: AST construction
├── test_planner.py                       # Unit: Logical plan generation
├── test_symbol_table.py                  # Unit: Variable tracking
├── test_column_resolver.py               # Unit: Column resolution
├── test_renderer.py                      # Unit: SQL rendering
│
├── test_column_resolution_pipeline.py    # Integration: Full resolution flow
├── test_renderer_resolution_integration.py  # Integration: Resolution + rendering
├── test_schema_propagation.py            # Integration: Scope propagation
│
├── test_pyspark_basic.py                 # PySpark infrastructure tests
├── test_examples_with_pyspark.py         # PySpark validation on examples
│
├── test_aggregation_entity_projection.py # Regression: Aggregation bugs
├── test_multi_with_entity_continuation.py  # Regression: Multi-WITH bugs
└── ... (other regression tests)
```

---

## Test Categories

### 1. Golden File Tests (Feature Validation)

**Location**: [tests/transpile_tests/](../tests/transpile_tests/)

**Count**: 44 tests

**Purpose**: Validate that specific Cypher patterns transpile to expected SQL.

**Pattern**:
1. Define schema (nodes, edges)
2. Transpile Cypher query
3. Compare generated SQL to golden file in [tests/output/expected/](../tests/output/expected/)
4. Structural assertions (has SELECT, has JOIN, etc.)

**Example**: [tests/transpile_tests/test_01_simple_node_lookup.py](../tests/transpile_tests/test_01_simple_node_lookup.py)

```python
class TestSimpleNodeLookup:
    TEST_ID = "01"
    TEST_NAME = "simple_node_lookup"

    def test_golden_file_match(self) -> None:
        cypher = "MATCH (p:Person) RETURN p"
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)
```

**Golden File**: [tests/output/expected/01_simple_node_lookup.sql](../tests/output/expected/01_simple_node_lookup.sql)

### 2. Unit Tests (Component Isolation)

**Purpose**: Test individual components (parser, planner, resolver, renderer) in isolation.

**Examples**:

| File | Component Tested |
|------|------------------|
| [test_parser.py](../tests/test_parser.py) | AST construction from Cypher |
| [test_planner.py](../tests/test_planner.py) | Logical operator tree generation |
| [test_symbol_table.py](../tests/test_symbol_table.py) | Variable scoping and tracking |
| [test_column_resolver.py](../tests/test_column_resolver.py) | Column reference resolution |
| [test_renderer.py](../tests/test_renderer.py) | SQL string generation |

**Pattern**:
```python
def test_parser_simple_match():
    parser = OpenCypherParser()
    ast = parser.parse("MATCH (n:Node) RETURN n")

    assert isinstance(ast, QueryNode)
    assert len(ast.single_queries) == 1
    # ... more assertions
```

### 3. Integration Tests (Multi-Phase)

**Purpose**: Test interactions between phases (e.g., resolution + rendering).

**Examples**:

| File | Phases Tested |
|------|---------------|
| [test_column_resolution_pipeline.py](../tests/test_column_resolution_pipeline.py) | Parser → Planner → Resolver |
| [test_renderer_resolution_integration.py](../tests/test_renderer_resolution_integration.py) | Resolver → Renderer |
| [test_schema_propagation.py](../tests/test_schema_propagation.py) | Planner scope propagation |

### 4. PySpark Validation Tests (End-to-End)

**Purpose**: Execute transpiled SQL on actual PySpark DataFrames to validate correctness.

**Files**:
- [tests/test_pyspark_basic.py](../tests/test_pyspark_basic.py) — Infrastructure tests (Spark session, data generation)
- [tests/test_examples_with_pyspark.py](../tests/test_examples_with_pyspark.py) — Run examples from YAML files

**Pattern**:
1. Load example YAML (schema + queries)
2. Generate sample data as PySpark DataFrames
3. Transpile Cypher query
4. Execute SQL on DataFrames
5. Validate results (non-empty, correct structure)

**Example Command**:
```bash
# Run all PySpark tests (slow)
make test-pyspark

# Run quick subset (first 5 credit queries)
make test-pyspark-quick

# Run specific domain
make test-pyspark-credit
```

### 5. Regression Tests (Bug Prevention)

**Purpose**: Ensure fixed bugs don't reoccur.

**Examples**:
- [test_aggregation_entity_projection.py](../tests/test_aggregation_entity_projection.py) — Fix for aggregation column name bug (commit `155da2f`)
- [test_multi_with_entity_continuation.py](../tests/test_multi_with_entity_continuation.py) — Fix for multi-WITH entity bug (commit `7ec0add`)
- [test_symbol_duplication_fix.py](../tests/test_symbol_duplication_fix.py) — Fix for symbol table duplication (INFERRED)
- [test_error_position_tracking.py](../tests/test_error_position_tracking.py) — Error message line/column accuracy (INFERRED)

---

## Test File Naming Convention

**Golden File Tests**: `test_{ID}_{feature_name}.py`
- **ID**: 2-digit number (01-99) indicating test order/complexity
- **feature_name**: Descriptive snake_case name

**Examples**:
- `test_01_simple_node_lookup.py` — Basic node matching
- `test_06_single_hop_relationship.py` — Single-edge relationship
- `test_11_aggregation_group_by.py` — GROUP BY aggregation
- `test_25_unwind.py` — UNWIND clause
- `test_36_path_functions.py` — Path manipulation functions

**Unit/Integration Tests**: `test_{component}.py`
- `test_parser.py`
- `test_renderer.py`
- `test_column_resolution_pipeline.py`

**Regression Tests**: `test_{bug_description}.py`
- `test_aggregation_entity_projection.py`
- `test_multi_with_entity_continuation.py`

---

## Golden File Structure

### Directory Layout

```
tests/output/
├── expected/                  # Golden reference SQL files
│   ├── 01_simple_node_lookup.sql
│   ├── 11_aggregation_group_by.sql
│   └── ...
│
├── actual/                    # Generated SQL from latest test run
│   ├── 01_simple_node_lookup.sql
│   └── ...
│
├── diff/                      # Diff files when actual != expected
│   ├── 01_simple_node_lookup.diff
│   └── ...
│
└── pyspark/                   # PySpark execution results (INFERRED)
    └── ...
```

### Updating Golden Files

**When to Update**:
- Intentional change to SQL generation logic
- Improved SQL output (more efficient, more readable)
- Bug fix that changes correct output

**Command**:
```bash
# Generate and save new golden file for test 01
make dump-sql-save ID=01 NAME=simple_node_lookup

# View diff between actual and expected
make dump-sql ID=01 NAME=simple_node_lookup --diff

# View all diffs
make diff-all
```

**Workflow**:
1. Make code change
2. Run test: `pytest tests/transpile_tests/test_01_simple_node_lookup.py`
3. Test fails (SQL doesn't match golden file)
4. Review diff: `make dump-sql ID=01 NAME=simple_node_lookup --diff`
5. If correct, update golden file: `make dump-sql-save ID=01 NAME=simple_node_lookup`
6. Re-run test to confirm

---

## Example Collections (YAML)

### Overview

Example collections are YAML files containing:
- Schema definition (nodes, edges, properties)
- Multiple queries with descriptions
- Domain-specific patterns (credit, fraud, features)

**Location**: [examples/](../examples/)

**Files**:
- [credit_queries.yaml](../examples/credit_queries.yaml) — Credit risk analysis (customer, loans, transactions)
- [fraud_queries.yaml](../examples/fraud_queries.yaml) — Fraud detection patterns
- [features_queries.yaml](../examples/features_queries.yaml) — Feature showcase (all supported Cypher constructs)

### YAML Format

```yaml
# Schema definition
schema:
  nodes:
    - name: Customer
      tableName: catalog.credit.Customer
      idProperty: { name: id, type: int }
      properties:
        - { name: name, type: string }
        - { name: status, type: string }

  edges:
    - name: HAS_ACCOUNT
      sourceNode: Customer
      sinkNode: Account
      tableName: catalog.credit.CustomerAccount
      sourceIdProperty: { name: customer_id, type: int }
      sinkIdProperty: { name: account_id, type: int }

# Example queries
examples:
  - description: "Find high-risk customers with multiple late payments"
    application: "Credit Risk Assessment"
    query: |
      MATCH (c:Customer)-[:HAS_LOAN]->(loan:Loan)-[:PAYMENT]->(p:Payment)
      WHERE p.on_time = false
      WITH c, COUNT(p) AS late_payments
      WHERE late_payments >= 3
      RETURN c.name, late_payments
      ORDER BY late_payments DESC
    notes: "Identifies customers with 3+ late payments for risk modeling"

  - description: "Calculate customer credit utilization"
    application: "Credit Monitoring"
    query: |
      MATCH (c:Customer)-[:HAS_CARD]->(card:CreditCard)
      WITH c, SUM(card.balance) AS total_balance, SUM(card.credit_limit) AS total_limit
      RETURN c.name, total_balance, total_limit,
             (total_balance / total_limit) AS utilization_ratio
      WHERE utilization_ratio > 0.8
    notes: "High utilization (>80%) indicates financial stress"
```

### Using Examples

**Interactive TUI**:
```bash
# Browse examples with live transpilation
uv run gsql2rsql tui --examples examples/credit_queries.yaml
```

**PySpark Validation**:
```bash
# Run all examples on PySpark
make test-pyspark-examples

# Run specific domain
make test-pyspark-credit
make test-pyspark-fraud
make test-pyspark-features
```

**Command-Line**:
```bash
# Manually transpile an example (extract query from YAML)
cat examples/credit_queries.yaml | grep "query:" | head -1 | \
  uv run gsql2rsql transpile --schema examples/credit_queries.yaml
```

---

## Adding a New Golden File Test

### Checklist

- [ ] 1. Determine test ID (next available number, e.g., 45)
- [ ] 2. Choose descriptive name (e.g., `optional_match_chained`)
- [ ] 3. Create test file: `tests/transpile_tests/test_45_optional_match_chained.py`
- [ ] 4. Define schema (reuse fixtures from `conftest.py` if possible)
- [ ] 5. Write Cypher query
- [ ] 6. Implement `_transpile()` helper
- [ ] 7. Add `test_golden_file_match()` method
- [ ] 8. Add structural assertion tests (optional but recommended)
- [ ] 9. Run test (will fail - no golden file yet)
- [ ] 10. Generate golden file: `make dump-sql-save ID=45 NAME=optional_match_chained`
- [ ] 11. Review generated SQL for correctness
- [ ] 12. Re-run test (should pass)
- [ ] 13. Add documentation (optional): `tests/docs/45_optional_match_chained.md`

### Template

```python
"""Test 45: Chained OPTIONAL MATCH patterns.

Validates that multiple OPTIONAL MATCH clauses correctly generate LEFT JOINs
with proper null handling.
"""

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    SimpleGraphSchemaProvider,
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)

from tests.utils.sql_test_utils import (
    assert_sql_equal,
    load_expected_sql,
)
from tests.utils.sql_assertions import (
    assert_has_left_join,
    assert_has_coalesce,
)


class TestOptionalMatchChained:
    """Test chained OPTIONAL MATCH with multiple LEFT JOINs."""

    TEST_ID = "45"
    TEST_NAME = "optional_match_chained"

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Define schema (reuse fixture or create inline)
        self.graph_schema = SimpleGraphSchemaProvider()
        # ... add nodes and edges

        self.sql_schema = SimpleSQLSchemaProvider()
        # ... add table descriptors

    def _transpile(self, cypher: str) -> str:
        """Helper to transpile a Cypher query."""
        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.graph_schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=self.sql_schema)
        return renderer.render_plan(plan)

    def test_golden_file_match(self) -> None:
        """Test that transpiled SQL matches golden file."""
        cypher = """
            MATCH (p:Person)
            OPTIONAL MATCH (p)-[:KNOWS]->(f:Person)
            OPTIONAL MATCH (f)-[:LIKES]->(h:Hobby)
            RETURN p.name, f.name, h.name
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        if expected_sql is None:
            from tests.utils.sql_test_utils import EXPECTED_DIR
            raise AssertionError(
                f"No golden file found. Create: "
                f"{EXPECTED_DIR}/{self.TEST_ID}_{self.TEST_NAME}.sql"
            )

        assert_sql_equal(
            expected_sql,
            actual_sql,
            self.TEST_ID,
            self.TEST_NAME,
        )

    def test_structural_has_left_joins(self) -> None:
        """Test that SQL uses LEFT JOIN for OPTIONAL MATCH."""
        cypher = """
            MATCH (p:Person)
            OPTIONAL MATCH (p)-[:KNOWS]->(f:Person)
            RETURN p.name, f.name
        """
        sql = self._transpile(cypher)

        assert_has_left_join(sql)

    def test_structural_has_coalesce(self) -> None:
        """Test that optional properties use COALESCE for null handling."""
        cypher = """
            MATCH (p:Person)
            OPTIONAL MATCH (p)-[:KNOWS]->(f:Person)
            RETURN p.name, COALESCE(f.name, 'Unknown') AS friend_name
        """
        sql = self._transpile(cypher)

        assert_has_coalesce(sql)
```

### Running Your New Test

```bash
# Run test (will fail initially)
pytest tests/transpile_tests/test_45_optional_match_chained.py -v

# Generate golden file
make dump-sql-save ID=45 NAME=optional_match_chained

# Re-run test (should pass)
pytest tests/transpile_tests/test_45_optional_match_chained.py -v

# Run all golden file tests
make test-transpile
```

---

## Adding a New PySpark Example

### Checklist

- [ ] 1. Choose domain (credit, fraud, features, or create new)
- [ ] 2. Add query to appropriate YAML file (e.g., `examples/credit_queries.yaml`)
- [ ] 3. Ensure schema is defined in YAML (or reuse existing)
- [ ] 4. Add `description`, `application`, `query`, and `notes` fields
- [ ] 5. Run PySpark validation: `make test-pyspark-credit`
- [ ] 6. Fix any errors (schema issues, unsupported features, etc.)
- [ ] 7. Document in YAML `notes` field

### YAML Template

```yaml
examples:
  - description: "Short one-line description of what query does"
    application: "Use case category (e.g., Fraud Detection, Risk Assessment)"
    query: |
      MATCH (n:Node)-[:EDGE]->(m:Node)
      WHERE n.property > 100
      RETURN n.name, COUNT(m) AS count
      ORDER BY count DESC
      LIMIT 10
    notes: "Additional context: why this pattern is useful, edge cases, performance notes"
```

### Example: Adding a Fraud Detection Query

**File**: [examples/fraud_queries.yaml](../examples/fraud_queries.yaml)

```yaml
examples:
  - description: "Detect circular money transfers (potential laundering)"
    application: "Anti-Money Laundering (AML)"
    query: |
      MATCH path = (a:Account)-[:TRANSFER*3..5]->(a)
      WHERE ALL(t IN relationships(path) WHERE t.amount > 10000)
      RETURN a.id, length(path) AS cycle_length,
             [t IN relationships(path) | t.amount] AS transfer_amounts
    notes: |
      Finds cycles of 3-5 transfers that return to the same account, all with
      amounts >$10k. Suspicious pattern for money laundering detection.
```

### Running PySpark Examples

```bash
# Run all fraud queries
make test-pyspark-fraud

# Run with verbose output (see DataFrames)
make test-pyspark-verbose

# Run with timeout (prevent hanging)
make test-pyspark-timeout
```

---

## Test Utilities Reference

### SQL Assertion Helpers

**Location**: [tests/utils/sql_assertions.py](../tests/utils/sql_assertions.py)

```python
from tests.utils.sql_assertions import (
    assert_has_select,        # Check for SELECT clause
    assert_has_from_table,    # Check for specific table reference
    assert_has_join,          # Check for any JOIN
    assert_has_left_join,     # Check for LEFT JOIN (OPTIONAL MATCH)
    assert_has_where,         # Check for WHERE clause
    assert_has_group_by,      # Check for GROUP BY
    assert_has_recursive_cte, # Check for WITH RECURSIVE
    assert_has_coalesce,      # Check for COALESCE (null handling)
    assert_no_join,           # Verify no JOIN present
    SQLStructure,             # Parse SQL structure
)
```

### SQL Test Utilities

**Location**: [tests/utils/sql_test_utils.py](../tests/utils/sql_test_utils.py)

```python
from tests.utils.sql_test_utils import (
    load_expected_sql,        # Load golden file
    assert_sql_equal,         # Compare with normalization
    EXPECTED_DIR,             # Path to expected/ directory
    ACTUAL_DIR,               # Path to actual/ directory
    DIFF_DIR,                 # Path to diff/ directory
)
```

### Fixtures (conftest.py)

**Location**: [tests/conftest.py](../tests/conftest.py)

```python
@pytest.fixture
def movie_graph_schema():
    """Standard test schema: Person, Movie, ACTED_IN, DIRECTED."""
    # Returns SimpleGraphSchemaProvider with movie domain

@pytest.fixture
def simple_sql_schema():
    """SQL schema provider for movie graph."""
    # Returns SimpleSQLSchemaProvider

# ... more fixtures
```

---

## Running Tests

### Fast Tests (No PySpark)

```bash
# All tests except PySpark (recommended for development)
make test-no-pyspark

# Specific golden file test
pytest tests/transpile_tests/test_01_simple_node_lookup.py -v

# All golden file tests
make test-transpile

# Unit tests only
pytest tests/test_parser.py tests/test_planner.py -v
```

### PySpark Tests (Slow)

```bash
# All PySpark tests
make test-pyspark

# Quick subset (first 5 examples)
make test-pyspark-quick

# Specific domain
make test-pyspark-credit
make test-pyspark-fraud
make test-pyspark-features

# Basic infrastructure tests only
make test-pyspark-basic
```

### Coverage

```bash
# Run with coverage report
make test-cov

# View HTML report (generates htmlcov/)
open htmlcov/index.html
```

### Debugging

```bash
# Verbose output with full tracebacks
make test-verbose

# Run single test with pdb
pytest tests/transpile_tests/test_01_simple_node_lookup.py::TestSimpleNodeLookup::test_golden_file_match -v --pdb

# Show SQL diffs
make diff-all
```

---

## Where to Look Next

- [07-developer-guide.md](07-developer-guide.md) — How to extend the transpiler
- [tests/](../tests/) — Browse existing tests for patterns
- [Makefile](../Makefile) — All test commands
- [tests/utils/](../tests/utils/) — Test utilities and helpers
