Current Separation of Concerns
Before proceeding, let me restate the current architectural boundaries established through Issues #1 and #2:

1. Parser Phase (OpenCypherParser)
Input: Cypher query string
Output: Abstract Syntax Tree (AST)
Responsibility: Lexical/syntactic analysis only
Does NOT: Validate semantics, resolve references, access schema
2. Planning Phase (LogicalPlan)
Input: AST + GraphSchema
Output: Logical operator tree + SymbolTable
Responsibility:
Convert AST to logical operators
Build symbol table (variable definitions, scopes)
Track entity/value types
Handle WITH boundaries, MATCH patterns, aggregations
Does NOT: Resolve column references, validate property access
3. Resolution Phase (ColumnResolver)
Input: LogicalPlan + AST + GraphSchema
Output: ResolutionResult (resolved column refs, expressions, projections)
Responsibility:
Validate ALL column references against symbol table
Query schema for entity properties
Detect entity returns vs property returns
Track property availability across boundaries
Build ResolvedColumnRef/ResolvedExpression structures
Does NOT: Generate SQL, modify logical plan structure
4. Rendering Phase (SQLRenderer)
Input: LogicalPlan + ResolutionResult + GraphSchema
Output: SQL string
Responsibility:
Generate SQL from logical plan
Use pre-resolved column references
Handle SQL dialect specifics
Does NOT: Resolve columns, validate references, make semantic decisions

---

# Contributing Guide

## Development Setup

1. **Clone the repository**
```bash
git clone https://github.com/devmessias/gsql2rsql
cd gsql2rsql/python
```

2. **Install dependencies**
```bash
# Using uv (recommended)
uv sync --extra dev
uv pip install -e ".[dev]"

# Or using pip
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

3. **Run tests**
```bash
make test-no-pyspark  # Fast unit tests
make test-pyspark     # Full validation (requires Java)
```

## Commit Message Convention

We use [Conventional Commits](https://www.conventionalcommits.org/) for automated versioning and changelog generation.

### Format

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Types

- **feat**: New feature (triggers **minor** version bump: 0.1.0 → 0.2.0)
- **fix**: Bug fix (triggers **patch** version bump: 0.1.0 → 0.1.1)
- **perf**: Performance improvement (triggers **patch** version bump)
- **docs**: Documentation changes (no version bump)
- **style**: Code style/formatting (no version bump)
- **refactor**: Code refactoring (no version bump)
- **test**: Adding/updating tests (no version bump)
- **chore**: Maintenance tasks (no version bump)
- **ci**: CI/CD changes (no version bump)

### Examples

```bash
# Feature (minor bump)
git commit -m "feat: add support for UNION queries"
git commit -m "feat(parser): support CASE expressions"

# Bug fix (patch bump)
git commit -m "fix: resolve column names in nested WITH clauses"
git commit -m "fix(renderer): escape special characters in strings"

# Performance improvement (patch bump)
git commit -m "perf: optimize join order for large graphs"

# Documentation (no bump)
git commit -m "docs: update installation guide"

# Tests (no bump)
git commit -m "test: add PySpark validation for UNION queries"
```

### Breaking Changes

For breaking changes, add `!` after the type or include `BREAKING CHANGE:` in the footer:

```bash
git commit -m "feat!: remove deprecated schema format"

# Or with footer
git commit -m "feat: redesign schema API

BREAKING CHANGE: Schema API now requires explicit table mappings"
```

### Makefile Helpers

```bash
make commit-feat       # Interactive feature commit
make commit-fix        # Interactive fix commit
make commit-docs       # Interactive docs commit
make commit-test       # Interactive test commit
```

## Pull Request Process

1. **Create a feature branch**
```bash
git checkout -b feat/my-feature
# or
git checkout -b fix/issue-123
```

2. **Make your changes**
- Follow the Separation of Concerns (see above)
- Add tests for new functionality
- Update documentation if needed

3. **Run tests and linting**
```bash
make test-no-pyspark  # Unit tests
make lint             # Ruff linting
make typecheck        # MyPy type checking
```

4. **Commit using conventional commits**
```bash
git add -A
git commit -m "feat: add LIMIT support for aggregated queries"
```

5. **Push and create PR**
```bash
git push origin feat/my-feature
```

6. **PR Review**
- CI will run tests automatically
- At least one approval required
- Squash and merge when ready

## Release Process

Releases are **fully automated** via GitHub Actions:

1. **Push commits to main** (with conventional commit messages)
2. **GitHub Actions** will:
   - Analyze commits since last release
   - Determine next version (patch/minor/major)
   - Generate CHANGELOG.md
   - Create GitHub Release
   - Publish to PyPI (via OIDC)
   - Deploy documentation

### Manual Release (Not Recommended)

```bash
make release-dry-run  # Preview what would be released
make release          # Create release locally
```

### Version Bumping

```bash
make version-bump-patch  # 0.1.0 → 0.1.1
make version-bump-minor  # 0.1.0 → 0.2.0
make version-bump-major  # 0.1.0 → 1.0.0
```

## Testing Guidelines

### Unit Tests (Fast)
```bash
make test-no-pyspark
```
- Run these during development
- Cover parser, planner, resolver, renderer
- No external dependencies

### PySpark Tests (Slow)
```bash
make test-pyspark          # All PySpark tests
make test-pyspark-fraud    # Fraud queries only
make test-pyspark-credit   # Credit queries only
```
- Run before creating PR
- Validate SQL on real Spark DataFrames
- Requires Java 11

### Test Coverage
```bash
make test-cov  # Generate HTML coverage report
```

## Code Style

- **Formatting**: Ruff (automatic)
- **Type Hints**: Required for all public APIs
- **Line Length**: 100 characters
- **Imports**: isort via Ruff

```bash
make format    # Auto-format code
make lint      # Check linting
make typecheck # Run MyPy
```

## Documentation

### Building Docs Locally
```bash
make docs-generate  # Generate artifacts and pages
make docs-build     # Build MkDocs site
make docs-serve     # Serve at http://localhost:8000
```

### Adding Examples
1. Add query to `examples/{category}_queries.yaml`
2. Run `make docs-generate-artifacts`
3. Run `make docs-generate-pages`
4. Verify with `make docs-build`

## Getting Help

- **Issues**: [GitHub Issues](https://github.com/devmessias/gsql2rsql/issues)
- **Discussions**: [GitHub Discussions](https://github.com/devmessias/gsql2rsql/discussions)
- **Documentation**: [gsql2rsql.dev](https://devmessias.github.io/gsql2rsql)
