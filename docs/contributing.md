# Contributing Guide

This document combines the architectural boundaries from [CONTRIBUTING.md](contributing.md) with practical developer workflow guidance.

---

## Quick Start for Contributors

### 1. Set Up Development Environment

```bash
# Clone repository (INFERRED - update with actual URL)
git clone https://github.com/your-org/cyper2dsql.git
cd cyper2dsql/python

# Create virtual environment with uv
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies (dev mode)
uv sync --extra dev
uv pip install -e ".[dev]"
```

### 2. Verify Installation

```bash
# Run fast test suite
make test-no-pyspark

# Run linter and type checker
make check

# Transpile example query
echo "MATCH (n:Person) RETURN n" | uv run gsql2rsql transpile -s examples/schema.json
```

### 3. Make Changes

**Before coding**:
1. Read [architectural boundaries](#architectural-separation-of-concerns) (critical!)
2. Identify which phase your change affects (parser, planner, resolver, or renderer)
3. Add tests first (TDD recommended)

**Development loop**:
1. Make changes to `src/`
2. Run `make test-no-pyspark` (fast feedback)
3. Run `make check` (lint + typecheck)
4. Add/update golden files if SQL output changes
5. Run full test suite: `make test`

### 4. Submit Changes

```bash
# Format code
make format

# Run all checks
make check

# Run full test suite (including PySpark)
make test

# Commit with descriptive message
git add .
git commit -m "feat: add support for map projection in RETURN clause"

# Push and create PR
git push origin feature/map-projection
```

---

## Architectural Separation of Concerns

**⚠️ CRITICAL: Read this before making any changes!**

The transpiler enforces strict separation of concerns across 4 phases. Violating these boundaries will cause architectural degradation and hard-to-debug issues.

This section is derived from [CONTRIBUTING.md](contributing.md).

### Phase 1: Parser (OpenCypherParser)

**Location**: [src/gsql2rsql/parser/](https://github.com/devmessias/gsql2rsql/tree/main/python/src/gsql2rsql/parser/)

**Input**: Cypher query string
**Output**: Abstract Syntax Tree (AST)

**Responsibility**: Lexical/syntactic analysis only

**Does NOT**:
- ❌ Validate semantics
- ❌ Resolve references
- ❌ Access schema
- ❌ Perform type checking
- ❌ Validate property names

**Rules**:
- Parser MUST NOT import from `planner/`, `renderer/`, or `common/schema.py`
- Parser MUST NOT call graph schema provider
- Parser MUST only validate syntax (grammar rules)

**Example Valid Change**:
```python
# ✅ Adding a new AST node type for a new Cypher construct
class QueryExpressionPatternComprehension(QueryExpression):
    pattern: QueryPattern
    where_clause: Optional[WhereClause]
    projection: QueryExpression
```

**Example Invalid Change**:
```python
# ❌ WRONG: Parser accessing schema
class CypherVisitor:
    def visitPropertyExpression(self, ctx):
        entity_name = self._get_entity_name(ctx)
        # ❌ WRONG: Don't validate property existence here
        if not self.schema.has_property(entity_name, property_name):
            raise Exception("Property not found")
```

### Phase 2: Planning (LogicalPlan)

**Location**: [src/gsql2rsql/planner/logical_plan.py](https://github.com/devmessias/gsql2rsql/tree/main/python/src/gsql2rsql/planner/logical_plan.py)

**Input**: AST + GraphSchema
**Output**: Logical operator tree + SymbolTable

**Responsibility**:
- ✅ Convert AST to logical operators
- ✅ Build symbol table (variable definitions, scopes)
- ✅ Track entity/value types
- ✅ Handle WITH boundaries, MATCH patterns, aggregations

**Does NOT**:
- ❌ Resolve column references
- ❌ Validate property access
- ❌ Generate SQL
- ❌ Query database schema

**Rules**:
- Planner CAN import from `parser/` (uses AST)
- Planner CAN import from `common/schema.py` (uses GraphSchema)
- Planner MUST NOT import from `renderer/`
- Planner MUST NOT perform column resolution (that's Phase 4)

**Example Valid Change**:
```python
# ✅ Adding a new logical operator
class WindowOperator(LogicalOperator):
    """Represents a window function (OVER clause)."""
    partition_by: list[str]
    order_by: list[OrderByItem]
    window_function: WindowFunction
```

**Example Invalid Change**:
```python
# ❌ WRONG: Planner resolving column references
class LogicalPlan:
    def _process_projection(self, projection: ProjectionItem):
        # ❌ WRONG: Don't resolve column refs during planning
        resolved_ref = self._resolve_column_reference(projection.expression)
        # Column resolution belongs in Phase 4 (Resolver)
```

### Phase 3: Optimization (SubqueryFlatteningOptimizer)

**Location**: [src/gsql2rsql/planner/subquery_optimizer.py](https://github.com/devmessias/gsql2rsql/tree/main/python/src/gsql2rsql/planner/subquery_optimizer.py)

**Input**: LogicalPlan
**Output**: Optimized LogicalPlan (modified in-place)

**Responsibility**:
- ✅ Apply conservative transformations
- ✅ Only flatten proven-safe patterns

**Does NOT**:
- ❌ Change query semantics
- ❌ Resolve columns
- ❌ Generate SQL

**Rules**:
- Optimizer MUST be conservative (safety first)
- Optimizer MUST NOT flatten patterns that could change semantics
- Optimizer CAN be disabled by user (`--no-optimize`)

### Phase 4: Resolution (ColumnResolver)

**Location**: [src/gsql2rsql/planner/column_resolver.py](https://github.com/devmessias/gsql2rsql/tree/main/python/src/gsql2rsql/planner/column_resolver.py)

**Input**: LogicalPlan + AST + GraphSchema
**Output**: ResolutionResult (resolved column refs, expressions, projections)

**Responsibility**:
- ✅ Validate ALL column references against symbol table
- ✅ Query schema for entity properties
- ✅ Detect entity returns vs property returns
- ✅ Track property availability across boundaries
- ✅ Build ResolvedColumnRef/ResolvedExpression structures

**Does NOT**:
- ❌ Generate SQL
- ❌ Modify logical plan structure
- ❌ Perform optimizations

**Rules**:
- Resolver CAN import from `parser/`, `planner/`, `common/`
- Resolver MUST NOT import from `renderer/`
- Resolver MUST validate ALL column refs before SQL generation
- Resolver MUST provide rich error messages with suggestions

**Example Valid Change**:
```python
# ✅ Improving error messages with better suggestions
class ColumnResolver:
    def _suggest_similar_columns(self, invalid_name: str, available: list[str]) -> list[str]:
        # Use Levenshtein distance to suggest typo fixes
        distances = [(name, levenshtein(invalid_name, name)) for name in available]
        return [name for name, dist in sorted(distances, key=lambda x: x[1]) if dist <= 2]
```

### Phase 5: Rendering (SQLRenderer)

**Location**: [src/gsql2rsql/renderer/sql_renderer.py](https://github.com/devmessias/gsql2rsql/tree/main/python/src/gsql2rsql/renderer/sql_renderer.py)

**Input**: LogicalPlan + ResolutionResult + GraphSchema
**Output**: SQL string

**Responsibility**:
- ✅ Generate SQL from logical plan
- ✅ Use pre-resolved column references
- ✅ Handle SQL dialect specifics

**Does NOT**:
- ❌ Resolve columns
- ❌ Validate references
- ❌ Make semantic decisions

**Rules**:
- Renderer CAN import from all phases (uses everything)
- Renderer MUST use `ResolutionResult` for all column refs
- Renderer MUST NOT resolve columns itself
- Renderer MUST NOT perform semantic validation

**Example Valid Change**:
```python
# ✅ Adding support for new SQL function
class SQLRenderer:
    def _render_function(self, func: Function, args: list[str]) -> str:
        if func == Function.RTRIM:
            # New function mapping
            return f"RTRIM({', '.join(args)})"
        # ... existing functions
```

**Example Invalid Change**:
```python
# ❌ WRONG: Renderer resolving columns
class SQLRenderer:
    def _render_property_access(self, entity: str, property: str) -> str:
        # ❌ WRONG: Don't resolve property here
        if not self.schema.has_property(entity, property):
            raise Exception("Property not found")
        # Resolution should already be done in Phase 4
```

---

## Branch and PR Workflow

### Branch Naming

Use descriptive branch names with prefixes:

- `feat/` — New feature (e.g., `feat/add-shortest-path`)
- `fix/` — Bug fix (e.g., `fix/aggregation-column-names`)
- `refactor/` — Code refactoring (e.g., `refactor/simplify-resolver`)
- `test/` — Test additions (e.g., `test/add-optional-match-tests`)
- `docs/` — Documentation (e.g., `docs/update-quickstart`)
- `chore/` — Maintenance (e.g., `chore/update-dependencies`)

### Commit Message Format

Use conventional commits format:

```
<type>: <short description>

<optional longer description>

<optional footer>
```

**Types**: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `perf`, `style`

**Examples**:
```
feat: add support for CASE expression in RETURN clause

Implements CASE WHEN ... THEN ... ELSE ... END syntax in projections.
Adds new AST node QueryExpressionCaseExpression and rendering logic.

Closes #42
```

```
fix: preserve column names after aggregation boundaries

Column names were being lost when entity properties were projected after
GROUP BY. Now uses full qualified names (_gsql2rsql_entity_property).

Fixes #51
```

### PR Checklist

Before submitting a PR:

- [ ] All tests pass: `make test`
- [ ] Code is formatted: `make format`
- [ ] No lint errors: `make lint`
- [ ] Type checking passes: `make typecheck`
- [ ] Added tests for new features
- [ ] Updated golden files if SQL output changed
- [ ] Added/updated documentation
- [ ] Commit messages follow conventional format
- [ ] PR description explains the change and motivation

---

## Code Style

### Python Style Guide

**Formatter**: Ruff (configured in [pyproject.toml](https://github.com/devmessias/gsql2rsql/blob/main/python/pyproject.toml))

**Line length**: 100 characters

**Imports**: Sorted with isort (part of Ruff)

**Type hints**: Required for all functions (strict mypy)

**Docstrings**: Google style (recommended, not enforced)

### Running Formatters and Linters

```bash
# Auto-format code
make format

# Check formatting (CI mode)
make format-check

# Run linter
make lint

# Auto-fix linting issues
make lint-fix

# Run type checker
make typecheck

# Run all checks
make check
```

### Type Hints

**Required**: All function signatures must have type hints

**Example**:
```python
# ✅ Good
def render_expression(
    self,
    expr: QueryExpression,
    context: RenderContext
) -> str:
    """Render an expression to SQL string."""
    ...

# ❌ Bad (missing type hints)
def render_expression(self, expr, context):
    ...
```

**Mypy Configuration**: See [pyproject.toml](https://github.com/devmessias/gsql2rsql/blob/main/python/pyproject.toml) `[tool.mypy]` section

- `strict = true` (all strict checks enabled)
- `disallow_untyped_defs = true` (no untyped functions)
- `warn_return_any = true` (warn on `Any` returns)

### Naming Conventions

| Item | Convention | Example |
|------|------------|---------|
| Classes | PascalCase | `LogicalOperator`, `ColumnResolver` |
| Functions | snake_case | `render_plan()`, `resolve_column()` |
| Constants | UPPER_SNAKE_CASE | `MAX_DEPTH`, `DEFAULT_TIMEOUT` |
| Private methods | `_snake_case` | `_render_helper()` |
| Type aliases | PascalCase | `EntityMap`, `ColumnMapping` |

### File Organization

Within each module file:
1. Module docstring
2. Imports (stdlib, third-party, local)
3. Constants
4. Type aliases
5. Helper functions
6. Main classes
7. Module-level functions (if any)

**Example**:
```python
"""Module for SQL rendering logic.

This module contains the SQLRenderer class which converts logical plans
to Databricks Spark SQL.
"""

# Standard library
from typing import Any, Optional

# Third-party
from antlr4 import InputStream

# Local
from gsql2rsql.planner.operators import LogicalOperator
from gsql2rsql.planner.column_ref import ResolvedColumnRef

# Constants
MAX_RECURSION_DEPTH = 100
DEFAULT_INDENT = "  "

# Type aliases
OperatorMap = dict[str, LogicalOperator]

# Classes
class SQLRenderer:
    ...
```

---

## Common Development Tasks

### Adding a New Cypher Feature

**Example**: Add support for `range()` function

1. **Phase 1 (Parser)**: Add to grammar or AST if needed
   ```python
   # If new function, add to Function enum in operators.py
   class Function(Enum):
       RANGE = "range"  # Generate sequence of integers
   ```

2. **Phase 2 (Planner)**: Handle in operator construction (if needed)
   ```python
   # Usually functions are just expressions, no special operator needed
   ```

3. **Phase 4 (Resolver)**: Type checking (if needed)
   ```python
   # Add type evaluation rule
   def _evaluate_function_type(self, func: Function, args: list[DataType]) -> DataType:
       if func == Function.RANGE:
           return DataType.LIST_INT
   ```

4. **Phase 5 (Renderer)**: Add SQL generation
   ```python
   class SQLRenderer:
       def _render_function(self, func: Function, args: list[str]) -> str:
           if func == Function.RANGE:
               # Databricks: sequence(start, stop, step)
               return f"sequence({args[0]}, {args[1]})"
   ```

5. **Add Tests**: Golden file test + unit tests
   ```bash
   # Create test_46_range_function.py
   # Generate golden file
   make dump-sql-save ID=46 NAME=range_function
   ```

### Fixing a Bug

**Example**: Fix incorrect null handling in OPTIONAL MATCH

1. **Write failing test first** (TDD)
   ```python
   # tests/test_optional_match_null_bug.py
   def test_optional_match_null_handling():
       cypher = "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p.name, f.name"
       sql = transpile(cypher)
       # Should use COALESCE for f.name
       assert "COALESCE" in sql
   ```

2. **Run test** (should fail)
   ```bash
   pytest tests/test_optional_match_null_bug.py -v
   ```

3. **Identify the phase** where the bug is (use `--explain-scopes` for debugging)
   ```bash
   echo "MATCH (p) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p.name, f.name" | \
     uv run gsql2rsql transpile -s examples/schema.json --explain-scopes
   ```

4. **Fix the bug** in the appropriate phase
   ```python
   # src/gsql2rsql/renderer/sql_renderer.py
   def _render_optional_property(self, ref: ResolvedColumnRef) -> str:
       if ref.is_from_optional_match:
           # ✅ Add COALESCE for null handling
           return f"COALESCE({ref.sql_column_name}, NULL)"
       return ref.sql_column_name
   ```

5. **Run tests** (should pass now)
   ```bash
   pytest tests/test_optional_match_null_bug.py -v
   make test-no-pyspark
   ```

6. **Update golden files** if SQL output changed
   ```bash
   make diff-all  # Review changes
   make dump-sql-save ID=09 NAME=optional_match  # Update if correct
   ```

### Debugging Transpilation Issues

**Step 1: Isolate the query**
```bash
# Save problematic query to file
echo "MATCH (n:Node) WHERE n.prop > 10 RETURN n" > debug_query.cypher
```

**Step 2: Inspect AST**
```bash
uv run gsql2rsql parse -i debug_query.cypher
```

**Step 3: Inspect logical plan**
```python
# In Python REPL or script
from gsql2rsql import OpenCypherParser, LogicalPlan
from gsql2rsql.common.schema import SimpleGraphSchemaProvider

parser = OpenCypherParser()
ast = parser.parse(open("debug_query.cypher").read())

schema = SimpleGraphSchemaProvider()
# ... add schema

plan = LogicalPlan.process_query_tree(ast, schema)
print(plan.dump_graph())  # Visualize operator tree
```

**Step 4: Check scopes**
```bash
uv run gsql2rsql transpile -s examples/schema.json -i debug_query.cypher --explain-scopes
```

**Step 5: Enable verbose logging** (INFERRED - check if implemented)
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

---

## Grammar Changes

### When to Modify Grammar

**Rare**: Grammar changes are needed only for:
- New Cypher syntax not currently supported
- Parser error recovery improvements
- Performance optimizations

**Not needed for**:
- New functions (add to `Function` enum in `operators.py`)
- New operators (add to `BinaryOperator` enum)
- Semantic changes (those belong in planner/renderer)

### Modifying the Grammar

**File**: [CypherParser.g4](https://github.com/devmessias/gsql2rsql/blob/main/python/CypherParser.g4) (INFERRED - root level)

**After changes**:
```bash
# Regenerate parser
make grammar

# Verify grammar compiles
javac -version  # Ensure Java is installed

# Run parser tests
make test-parser  # (INFERRED command)
```

**Note**: Generated files in `src/gsql2rsql/parser/grammar/` will change. Commit them.

---

## Release Process

**INFERRED - Update with actual process**

### Versioning

Follow Semantic Versioning (SemVer): `MAJOR.MINOR.PATCH`

- **MAJOR**: Breaking changes (API, CLI, SQL output incompatibility)
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

### Release Checklist

- [ ] All tests pass on main branch
- [ ] CHANGELOG.md updated (INFERRED - if exists)
- [ ] Version bumped in [pyproject.toml](https://github.com/devmessias/gsql2rsql/blob/main/python/pyproject.toml)
- [ ] Documentation updated
- [ ] Tag created: `git tag -a v0.2.0 -m "Release v0.2.0"`
- [ ] Build package: `make build`
- [ ] Publish to PyPI: `make publish` (or `make publish-test` for TestPyPI)

---

## Getting Help

### Resources

- **Architecture**: [architecture.md](architecture.md)
- **Developer Guide**: [contributing.md](contributing.md)

### Communication

**INFERRED - Update with actual channels**

- **Issues**: GitHub Issues (bug reports, feature requests)
- **Discussions**: GitHub Discussions (questions, ideas)
- **Slack/Discord**: (if available)

### Reporting Bugs

**Include**:
1. Cypher query that causes the issue
2. Schema definition (JSON)
3. Expected SQL output (or behavior)
4. Actual SQL output (or error message)
5. Transpiler version: `uv run gsql2rsql --version`
6. Python version: `python --version`

**Template**:
```markdown
## Bug Report

**Cypher Query:**
```cypher
MATCH (n:Node) WHERE n.prop > 10 RETURN n
```

**Schema:**
```json
{ "nodes": [ ... ] }
```

**Expected SQL:**
```sql
SELECT * FROM Node WHERE prop > 10
```

**Actual SQL:**
```sql
SELECT * FROM Node  -- WHERE clause missing!
```

**Environment:**
- Transpiler version: 0.1.0
- Python version: 3.12.1
- OS: Ubuntu 22.04
```

---

--8<-- "inspiration.md"

---

## Where to Look Next

- [contributing.md](contributing.md) — Detailed extension guide
- [examples/index.md](examples/index.md) — Testing patterns
- [architecture.md](architecture.md) — Component details
