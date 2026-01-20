# Developer Guide: Extending the Transpiler

This guide provides detailed instructions for extending the transpiler with new features, operators, and SQL targets.

---

## Table of Contents

1. [Adding New Functions](#adding-new-functions)
2. [Adding New Operators](#adding-new-operators)
3. [Adding New AST Transforms](#adding-new-ast-transforms)
4. [Adding New Codegen Rules](#adding-new-codegen-rules)
5. [Supporting New SQL Targets](#supporting-new-sql-targets)
6. [Debugging Checklist](#debugging-checklist)
7. [Common Pitfalls](#common-pitfalls)

---

## Adding New Functions

**Use Case**: Add support for a new Cypher function (e.g., `toUpper()`, `sqrt()`, `split()`)

### Step-by-Step Guide

#### 1. Add Function to Enum

**File**: [src/gsql2rsql/parser/operators.py](../src/gsql2rsql/parser/operators.py)

**Location**: `Function` enum (~line 50+)

```python
class Function(Enum):
    """Built-in Cypher functions."""
    # ... existing functions

    # String functions
    TOUPPER = "toUpper"
    TOLOWER = "toLower"
    SPLIT = "split"  # ✅ Add new function here

    # Math functions
    SQRT = "sqrt"
    POW = "pow"

    # ... more functions
```

#### 2. Add Type Evaluation (Optional)

**File**: [src/gsql2rsql/parser/ast.py](../src/gsql2rsql/parser/ast.py)

**Location**: `QueryExpressionFunction.evaluate_type()` method (~line 800+)

```python
class QueryExpressionFunction(QueryExpression):
    def evaluate_type(self, symbol_table: SymbolTable) -> DataType:
        # Evaluate argument types
        arg_types = [arg.evaluate_type(symbol_table) for arg in self.arguments]

        # ✅ Add type rule for new function
        if self.function == Function.SPLIT:
            # split(string, delimiter) -> list[string]
            return DataType.LIST_STRING

        if self.function == Function.SQRT:
            # sqrt(number) -> float
            return DataType.FLOAT

        # ... existing type rules
```

#### 3. Add SQL Rendering

**File**: [src/gsql2rsql/renderer/sql_renderer.py](../src/gsql2rsql/renderer/sql_renderer.py)

**Location**: `SQLRenderer._render_function()` method (~line 1500+)

```python
class SQLRenderer:
    def _render_function(
        self,
        func: Function,
        args: list[str],
        context: RenderContext
    ) -> str:
        """Render Cypher function to SQL."""

        # ✅ Add SQL mapping for new function
        if func == Function.SPLIT:
            # Cypher: split(string, delimiter)
            # Databricks SQL: split(string, delimiter)
            return f"split({args[0]}, {args[1]})"

        if func == Function.SQRT:
            # Cypher: sqrt(x)
            # Databricks SQL: sqrt(x)
            return f"sqrt({args[0]})"

        if func == Function.TOUPPER:
            # Cypher: toUpper(string)
            # Databricks SQL: UPPER(string)
            return f"UPPER({args[0]})"

        # ... existing function mappings
```

#### 4. Add Tests

**Create Golden File Test**: [tests/transpile_tests/test_47_string_functions.py](../tests/transpile_tests/)

```python
"""Test 47: String manipulation functions."""

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from tests.utils.sql_test_utils import assert_sql_equal, load_expected_sql


class TestStringFunctions:
    TEST_ID = "47"
    TEST_NAME = "string_functions"

    def test_split_function(self) -> None:
        """Test split() function transpilation."""
        cypher = """
            MATCH (p:Person)
            RETURN split(p.name, ' ') AS name_parts
        """
        actual_sql = self._transpile(cypher)

        expected_sql = load_expected_sql(self.TEST_ID, self.TEST_NAME)
        assert_sql_equal(expected_sql, actual_sql, self.TEST_ID, self.TEST_NAME)

    def test_split_function_has_split_call(self) -> None:
        """Test that SQL contains split() function call."""
        cypher = "MATCH (p:Person) RETURN split(p.name, ' ') AS parts"
        sql = self._transpile(cypher)

        assert "split(" in sql.lower()
        assert "p.name" in sql or "_gsql2rsql_p_name" in sql
```

**Generate Golden File**:
```bash
make dump-sql-save ID=47 NAME=string_functions
```

**Run Tests**:
```bash
pytest tests/transpile_tests/test_47_string_functions.py -v
make test-no-pyspark
```

### Function Categories

| Category | Examples | SQL Mapping Notes |
|----------|----------|-------------------|
| String | `toUpper()`, `toLower()`, `trim()`, `split()` | Usually 1:1 with SQL functions |
| Math | `sqrt()`, `pow()`, `round()`, `abs()` | Usually 1:1 with SQL functions |
| List | `size()`, `reverse()`, `head()`, `tail()` | May need SQL array functions |
| Aggregation | `collect()`, `count()`, `sum()`, `avg()` | Handled separately (see below) |
| Date/Time | `datetime()`, `date()`, `timestamp()` | Databricks-specific functions |

---

## Adding New Operators

**Use Case**: Add support for a new binary operator (e.g., `=~` for regex matching)

### Step-by-Step Guide

#### 1. Add Operator to Enum

**File**: [src/gsql2rsql/parser/operators.py](../src/gsql2rsql/parser/operators.py)

**Location**: `BinaryOperator` enum (~line 20+)

```python
class BinaryOperator(Enum):
    """Binary operators in Cypher."""
    # ... existing operators

    # Comparison operators
    REGEX_MATCH = "=~"  # ✅ Add new operator
    NOT_REGEX_MATCH = "!=~"  # INFERRED - may not exist

    # ... more operators
```

#### 2. Update Grammar (if needed)

**File**: [CypherParser.g4](../CypherParser.g4) (INFERRED)

**Location**: Expression rules

If the operator is not already in the grammar, add it:
```antlr
comparisonExpression
    : additiveExpression (
        ('=' | '<>' | '!=' | '<' | '<=' | '>' | '>=' | '=~' | '!=~') additiveExpression
      )*
    ;
```

**Regenerate Parser**:
```bash
make grammar
```

#### 3. Update Visitor (if needed)

**File**: [src/gsql2rsql/parser/visitor.py](../src/gsql2rsql/parser/visitor.py)

**Location**: `visitComparisonExpression()` method

```python
class CypherVisitor:
    def visitComparisonExpression(self, ctx):
        # ... existing logic

        # ✅ Map operator string to enum
        operator_map = {
            "=": BinaryOperator.EQUALS,
            "<>": BinaryOperator.NOT_EQUALS,
            "=~": BinaryOperator.REGEX_MATCH,  # Add mapping
            # ... more mappings
        }
```

#### 4. Add SQL Rendering

**File**: [src/gsql2rsql/renderer/sql_renderer.py](../src/gsql2rsql/renderer/sql_renderer.py)

**Location**: `_render_binary_operator()` method (~line 1200+)

```python
class SQLRenderer:
    def _render_binary_operator(
        self,
        operator: BinaryOperator,
        left: str,
        right: str
    ) -> str:
        """Render binary operator to SQL."""

        # ✅ Add SQL mapping
        if operator == BinaryOperator.REGEX_MATCH:
            # Databricks SQL: RLIKE or REGEXP
            return f"({left} RLIKE {right})"

        if operator == BinaryOperator.NOT_REGEX_MATCH:
            return f"(NOT {left} RLIKE {right})"

        # ... existing operators
        if operator == BinaryOperator.EQUALS:
            return f"({left} = {right})"
```

#### 5. Add Tests

```python
def test_regex_match_operator(self) -> None:
    """Test =~ operator transpilation."""
    cypher = """
        MATCH (p:Person)
        WHERE p.name =~ 'J.*'
        RETURN p.name
    """
    sql = self._transpile(cypher)

    assert "RLIKE" in sql or "REGEXP" in sql
```

---

## Adding New AST Transforms

**Use Case**: Add a new optimization or transformation that modifies the AST or logical plan

### Example: Constant Folding Optimization

**Goal**: Evaluate constant expressions at transpile time (e.g., `2 + 3` → `5`)

#### 1. Create Transform Class

**File**: [src/gsql2rsql/planner/constant_folder.py](../src/gsql2rsql/planner/) (new file)

```python
"""Constant folding optimization for expressions."""

from gsql2rsql.parser.ast import (
    QueryExpression,
    QueryExpressionBinary,
    QueryExpressionValue,
)
from gsql2rsql.parser.operators import BinaryOperator


class ConstantFolder:
    """Fold constant expressions into single values."""

    def fold_expression(self, expr: QueryExpression) -> QueryExpression:
        """Recursively fold constants in expression tree."""

        if isinstance(expr, QueryExpressionBinary):
            # Fold children first
            left = self.fold_expression(expr.left)
            right = self.fold_expression(expr.right)

            # Check if both sides are constants
            if isinstance(left, QueryExpressionValue) and isinstance(right, QueryExpressionValue):
                # Evaluate constant operation
                result = self._evaluate_constant_binary(expr.operator, left.value, right.value)
                return QueryExpressionValue(value=result)

            # Return with folded children
            return QueryExpressionBinary(operator=expr.operator, left=left, right=right)

        return expr

    def _evaluate_constant_binary(self, op: BinaryOperator, left: Any, right: Any) -> Any:
        """Evaluate binary operation on constants."""
        if op == BinaryOperator.PLUS:
            return left + right
        if op == BinaryOperator.MINUS:
            return left - right
        if op == BinaryOperator.MULTIPLY:
            return left * right
        if op == BinaryOperator.DIVIDE:
            return left / right
        # ... more operators

        # Can't fold, return None (caller will keep original)
        return None
```

#### 2. Integrate into Pipeline

**File**: [src/gsql2rsql/planner/logical_plan.py](../src/gsql2rsql/planner/logical_plan.py)

**Location**: `process_query_tree()` method

```python
from gsql2rsql.planner.constant_folder import ConstantFolder

class LogicalPlan:
    @staticmethod
    def process_query_tree(ast: QueryNode, schema: IGraphSchemaProvider) -> "LogicalPlan":
        # ... existing logic

        # ✅ Apply constant folding (optional optimization)
        if enable_constant_folding:
            folder = ConstantFolder()
            ast = folder.fold_query(ast)  # Transform AST

        # Continue with operator construction
        # ...
```

#### 3. Add CLI Flag (Optional)

**File**: [src/gsql2rsql/cli.py](../src/gsql2rsql/cli.py)

```python
@click.option(
    "--fold-constants",
    is_flag=True,
    default=False,
    help="Enable constant folding optimization"
)
def transpile(schema, optimize, fold_constants, ...):
    # ... pass flag to planner
```

#### 4. Add Tests

```python
def test_constant_folding_addition(self) -> None:
    """Test that 2 + 3 is folded to 5."""
    cypher = "MATCH (n:Node) WHERE n.age > 2 + 3 RETURN n"
    sql = self._transpile(cypher, fold_constants=True)

    # Should contain 5, not 2 + 3
    assert "> 5" in sql
    assert "+ 3" not in sql
```

---

## Adding New Codegen Rules

**Use Case**: Generate more efficient SQL for specific patterns

### Example: IN Clause Optimization

**Goal**: Convert `x = 1 OR x = 2 OR x = 3` to `x IN (1, 2, 3)`

#### 1. Add Pattern Detection

**File**: [src/gsql2rsql/renderer/sql_renderer.py](../src/gsql2rsql/renderer/sql_renderer.py)

**Location**: New method

```python
class SQLRenderer:
    def _detect_in_pattern(self, expr: QueryExpressionBinary) -> Optional[tuple[str, list[Any]]]:
        """Detect x = A OR x = B OR x = C pattern."""

        if expr.operator != BinaryOperator.OR:
            return None

        # Collect all OR-connected EQUALS comparisons
        equals_exprs = self._collect_or_equals(expr)

        if not equals_exprs:
            return None

        # Check if all comparisons reference same column
        columns = [e.left for e in equals_exprs]
        if not all(self._same_column(columns[0], col) for col in columns):
            return None

        # Extract values
        values = [e.right for e in equals_exprs]
        return (columns[0], values)

    def _render_expression(self, expr: QueryExpression, context: RenderContext) -> str:
        """Render expression with IN optimization."""

        if isinstance(expr, QueryExpressionBinary):
            # ✅ Try to optimize to IN clause
            in_pattern = self._detect_in_pattern(expr)
            if in_pattern:
                column, values = in_pattern
                column_sql = self._render_expression(column, context)
                values_sql = ", ".join(self._render_expression(v, context) for v in values)
                return f"{column_sql} IN ({values_sql})"

        # ... existing rendering logic
```

#### 2. Add Tests

```python
def test_or_optimized_to_in_clause(self) -> None:
    """Test that x = 1 OR x = 2 OR x = 3 becomes x IN (1, 2, 3)."""
    cypher = """
        MATCH (n:Node)
        WHERE n.age = 10 OR n.age = 20 OR n.age = 30
        RETURN n
    """
    sql = self._transpile(cypher)

    assert "IN (10, 20, 30)" in sql
    assert "OR" not in sql  # Should be optimized away
```

---

## Supporting New SQL Targets

**Use Case**: Generate SQL for PostgreSQL, DuckDB, or other databases instead of Databricks

### Architecture for Multi-Target Support

#### 1. Create SQL Dialect Interface

**File**: [src/gsql2rsql/renderer/sql_dialect.py](../src/gsql2rsql/renderer/) (new file)

```python
"""SQL dialect abstraction for multi-target support."""

from abc import ABC, abstractmethod
from enum import Enum


class SQLDialect(Enum):
    """Supported SQL dialects."""
    DATABRICKS = "databricks"
    POSTGRESQL = "postgresql"
    DUCKDB = "duckdb"
    SQLITE = "sqlite"


class ISQLDialectProvider(ABC):
    """Interface for SQL dialect-specific code generation."""

    @abstractmethod
    def render_recursive_cte(self, ...params...) -> str:
        """Generate WITH RECURSIVE CTE (syntax varies by dialect)."""

    @abstractmethod
    def render_array_concat(self, left: str, right: str) -> str:
        """Generate array concatenation (CONCAT vs ||)."""

    @abstractmethod
    def render_array_contains(self, array: str, element: str) -> str:
        """Generate array containment check (array_contains vs ANY)."""

    @abstractmethod
    def render_struct(self, fields: dict[str, str]) -> str:
        """Generate struct/row literal (STRUCT vs ROW)."""

    @abstractmethod
    def supports_recursive_cte(self) -> bool:
        """Check if dialect supports WITH RECURSIVE."""


class DatabricksDialect(ISQLDialectProvider):
    """Databricks Spark SQL dialect."""

    def render_recursive_cte(self, ...params...) -> str:
        # Current implementation
        return f"WITH RECURSIVE {name} AS (...)"

    def render_array_concat(self, left: str, right: str) -> str:
        return f"CONCAT({left}, {right})"

    def render_array_contains(self, array: str, element: str) -> str:
        return f"array_contains({array}, {element})"

    def render_struct(self, fields: dict[str, str]) -> str:
        field_list = ", ".join(f"{v} AS {k}" for k, v in fields.items())
        return f"STRUCT({field_list})"

    def supports_recursive_cte(self) -> bool:
        return True


class PostgreSQLDialect(ISQLDialectProvider):
    """PostgreSQL dialect."""

    def render_array_concat(self, left: str, right: str) -> str:
        return f"({left} || {right})"  # PostgreSQL uses || for arrays

    def render_array_contains(self, array: str, element: str) -> str:
        return f"({element} = ANY({array}))"  # PostgreSQL uses ANY()

    def render_struct(self, fields: dict[str, str]) -> str:
        field_list = ", ".join(f"{v}" for v in fields.values())
        return f"ROW({field_list})"  # PostgreSQL uses ROW()

    def supports_recursive_cte(self) -> bool:
        return True
```

#### 2. Refactor Renderer to Use Dialect

**File**: [src/gsql2rsql/renderer/sql_renderer.py](../src/gsql2rsql/renderer/sql_renderer.py)

```python
from gsql2rsql.renderer.sql_dialect import ISQLDialectProvider, DatabricksDialect


class SQLRenderer:
    def __init__(
        self,
        db_schema_provider: ISQLDBSchemaProvider,
        dialect: ISQLDialectProvider = None  # ✅ Add dialect parameter
    ):
        self.db_schema = db_schema_provider
        self.dialect = dialect or DatabricksDialect()  # Default to Databricks

    def _render_recursive(self, op: RecursiveTraversalOperator) -> str:
        """Render WITH RECURSIVE using dialect-specific syntax."""
        # ✅ Delegate to dialect
        return self.dialect.render_recursive_cte(...)

    def _render_array_concat(self, left: str, right: str) -> str:
        """Render array concatenation using dialect-specific syntax."""
        return self.dialect.render_array_concat(left, right)
```

#### 3. Add CLI Flag for Dialect Selection

**File**: [src/gsql2rsql/cli.py](../src/gsql2rsql/cli.py)

```python
@click.option(
    "--dialect",
    type=click.Choice(["databricks", "postgresql", "duckdb"]),
    default="databricks",
    help="Target SQL dialect"
)
def transpile(schema, dialect, ...):
    # ... load schema

    # ✅ Create dialect provider
    if dialect == "postgresql":
        from gsql2rsql.renderer.sql_dialect import PostgreSQLDialect
        dialect_provider = PostgreSQLDialect()
    elif dialect == "duckdb":
        dialect_provider = DuckDBDialect()
    else:
        dialect_provider = DatabricksDialect()

    # Pass to renderer
    renderer = SQLRenderer(db_schema_provider=sql_schema, dialect=dialect_provider)
```

#### 4. Add Tests for Each Dialect

```python
class TestPostgreSQLDialect:
    """Test PostgreSQL-specific SQL generation."""

    def test_array_concat_uses_pipe_operator(self):
        cypher = "MATCH (n)-[:REL*1..3]->(m) RETURN n"
        sql = self._transpile(cypher, dialect="postgresql")

        # PostgreSQL uses || for array concat
        assert "||" in sql
        assert "CONCAT(" not in sql

    def test_array_contains_uses_any(self):
        cypher = "MATCH (n)-[r*]-(m) WHERE 'foo' IN relationships(path) RETURN n"
        sql = self._transpile(cypher, dialect="postgresql")

        assert "ANY(" in sql
        assert "array_contains(" not in sql
```

---

## Debugging Checklist

### When SQL Generation is Wrong

**Step 1: Verify AST is Correct**
```bash
# Parse only, inspect AST
uv run gsql2rsql parse -i query.cypher

# Look for:
# - Correct node types
# - Correct operator precedence
# - All expressions parsed
```

**Step 2: Verify Logical Plan is Correct**
```python
# In Python REPL
from gsql2rsql import OpenCypherParser, LogicalPlan

ast = parser.parse(query)
plan = LogicalPlan.process_query_tree(ast, schema)

print(plan.dump_graph())  # Visualize operator tree

# Look for:
# - Correct operator types (Join, Selection, Projection, etc.)
# - Correct operator ordering
# - All predicates captured
```

**Step 3: Verify Symbol Table is Correct**
```bash
# Show scope information
uv run gsql2rsql transpile -s schema.json -i query.cypher --explain-scopes

# Look for:
# - All variables defined
# - Correct scopes (WITH boundaries)
# - No missing definitions
```

**Step 4: Verify Column Resolution is Correct**
```python
# Check ResolutionResult
plan.resolve(original_query=query)

if not plan.is_resolved:
    print("Resolution failed!")

# Inspect resolved columns
for proj_name, resolved_proj in plan.resolution_result.resolved_projections.items():
    print(f"{proj_name}: {resolved_proj.column_refs}")
```

**Step 5: Inspect Generated SQL**
```bash
# Generate SQL
uv run gsql2rsql transpile -s schema.json -i query.cypher

# Compare with golden file
make dump-sql ID=XX NAME=test_name --diff
```

### When Tests are Failing

**Checklist**:
- [ ] Did you run `make format` and `make check`?
- [ ] Are all imports correct (no circular dependencies)?
- [ ] Did you update golden files if SQL output changed?
- [ ] Are type hints correct (run `make typecheck`)?
- [ ] Did you add unit tests and integration tests?
- [ ] Does the change respect phase boundaries (see [06-contributing.md](06-contributing.md))?

### When PySpark Tests are Failing

**Common Issues**:

1. **Schema mismatch**: Generated data doesn't match schema definition
   - Check: [src/gsql2rsql/pyspark_executor.py](../src/gsql2rsql/pyspark_executor.py) data generation
   - Fix: Update sample data generator

2. **SQL syntax error**: Generated SQL is invalid for Spark
   - Check: Run SQL directly in Databricks SQL editor
   - Fix: Update renderer to use correct Spark SQL syntax

3. **Result mismatch**: Query returns wrong results
   - Check: Add `print(df.show())` to see actual results
   - Fix: Likely a semantic bug in planner or renderer

4. **Timeout**: Query takes too long
   - Check: Is the query generating cartesian product?
   - Fix: Add proper join conditions

---

## Common Pitfalls

### 1. Violating Phase Boundaries

**❌ Wrong**:
```python
# Parser accessing schema
class CypherVisitor:
    def visitPropertyExpression(self, ctx):
        if not self.schema.has_property(...):  # ❌ Parser shouldn't access schema
            raise Exception()
```

**✅ Correct**:
```python
# Resolver validating properties
class ColumnResolver:
    def resolve_property(self, entity, property):
        if not self.schema.has_property(...):  # ✅ Resolver's responsibility
            raise ColumnResolutionError()
```

### 2. Forgetting to Update Golden Files

**Problem**: Test fails after legitimate SQL change

**Solution**:
```bash
# Review change
make dump-sql ID=XX NAME=test_name --diff

# If correct, update golden file
make dump-sql-save ID=XX NAME=test_name
```

### 3. Not Handling NULL Values

**Problem**: OPTIONAL MATCH generates incorrect SQL (no null handling)

**Solution**: Always use `COALESCE()` for optional properties
```python
if column_ref.is_from_optional_match:
    return f"COALESCE({sql_column}, NULL)"
```

### 4. Incorrect Operator Precedence

**Problem**: `a + b * c` rendered as `(a + b) * c` instead of `a + (b * c)`

**Solution**: Ensure parentheses in renderer respect precedence
```python
def _render_binary(self, expr):
    left = self._render_with_parens_if_needed(expr.left, expr.operator)
    right = self._render_with_parens_if_needed(expr.right, expr.operator)
    return f"{left} {op} {right}"
```

### 5. Missing Type Annotations

**Problem**: Mypy errors in CI

**Solution**: Add type hints to all functions
```python
# ✅ All parameters and return types annotated
def render_expression(self, expr: QueryExpression, ctx: RenderContext) -> str:
    ...
```

### 6. Circular Imports

**Problem**: `ImportError: cannot import name 'X' from partially initialized module`

**Solution**: Use forward references and import inside functions
```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gsql2rsql.planner.operators import LogicalOperator

def process(operator: "LogicalOperator") -> None:
    ...
```

### 7. Mutable Default Arguments

**Problem**: Default list/dict arguments are shared across calls

**❌ Wrong**:
```python
def process(items: list[str] = []) -> None:  # ❌ Mutable default
    items.append("new")
```

**✅ Correct**:
```python
def process(items: list[str] | None = None) -> None:  # ✅ Use None
    if items is None:
        items = []
    items.append("new")
```

---

## Where to Look Next

- [06-contributing.md](06-contributing.md) — Code style and PR workflow
- [02-architecture.md](02-architecture.md) — Phase details and boundaries
- [05-testing-and-examples.md](05-testing-and-examples.md) — Testing patterns
- [08-api-reference.md](08-api-reference.md) — Public API reference
