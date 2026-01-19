.PHONY: help install install-dev test test-cov lint format typecheck grammar clean build publish \
       test-pyspark test-pyspark-basic test-pyspark-examples test-pyspark-quick test-pyspark-verbose

PYTHON := .venv/bin/python3
UV := uv
ANTLR_JAR := antlr-4.13.1-complete.jar
GRAMMAR_DIR := src/gsql2rsql/parser/grammar

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─────────────────────────────────────────────────────────────────────────────
# Installation
# ─────────────────────────────────────────────────────────────────────────────

install:  ## Install dependencies
	$(UV) sync

install-dev:  ## Install with dev dependencies
	$(UV) sync --extra dev
	$(UV) pip install -e ".[dev]"
venv:  ## Create virtual environment
	$(UV) venv

# ─────────────────────────────────────────────────────────────────────────────
# Testing
# ─────────────────────────────────────────────────────────────────────────────

test:  ## Run tests
	$(UV) run pytest tests/

test-cov:  ## Run tests with coverage
	$(UV) run pytest tests/ --cov=src/gsql2rsql --cov-report=term-missing --cov-report=html

test-no-pyspark:  ## Run all tests except PySpark tests
	$(UV) run pytest tests/ -v --ignore=tests/test_examples_with_pyspark.py --ignore=tests/test_pyspark_basic.py

test-bfs:  ## Run BFS/recursive tests only
	$(UV) run pytest tests/test_renderer.py::TestBFSWithRecursive -v

test-verbose:  ## Run tests with verbose output
	$(UV) run pytest tests/ -v --tb=long

# ─────────────────────────────────────────────────────────────────────────────
# PySpark Testing
# ─────────────────────────────────────────────────────────────────────────────

test-pyspark:  ## Run all PySpark tests
	$(UV) run pytest tests/test_pyspark_basic.py tests/test_examples_with_pyspark.py -v

test-pyspark-timeout:  ## Run PySpark tests with timeout (60s per test)
	$(UV) run pytest tests/test_pyspark_basic.py tests/test_examples_with_pyspark.py -v --timeout=60 --timeout-method=thread

test-pyspark-quick:  ## Run a quick subset of PySpark tests (first 5 examples)
	$(UV) run pytest tests/test_examples_with_pyspark.py::TestExampleExecution::test_example -k "credit_queries.yaml and (00 or 01 or 02 or 03 or 04)" -v --timeout=30

test-pyspark-basic:  ## Run basic PySpark infrastructure tests
	$(UV) run pytest tests/test_pyspark_basic.py -v

test-pyspark-examples:  ## Run PySpark tests on curated examples
	$(UV) run pytest tests/test_examples_with_pyspark.py -v

test-pyspark-quick:  ## Run quick PySpark validation (direct script execution)
	$(UV) run python tests/test_examples_with_pyspark.py

test-pyspark-verbose:  ## Run PySpark tests with detailed output
	$(UV) run pytest tests/test_pyspark_basic.py tests/test_examples_with_pyspark.py -v --tb=long -s

test-pyspark-features:  ## Run PySpark tests only for features_queries.yaml
	$(UV) run pytest tests/test_examples_with_pyspark.py -v -k "features_queries"

test-pyspark-fraud:  ## Run PySpark tests only for fraud_queries.yaml
	$(UV) run pytest tests/test_examples_with_pyspark.py -v -k "fraud_queries"

test-pyspark-credit:  ## Run PySpark tests only for credit_queries.yaml
	$(UV) run pytest tests/test_examples_with_pyspark.py -v -k "credit_queries"

test-pyspark-summary:  ## Generate PySpark test summary report
	$(UV) run pytest tests/test_examples_with_pyspark.py::TestExamplesSummary -v -s

# ─────────────────────────────────────────────────────────────────────────────
# Testing & Validation
# ─────────────────────────────────────────────────────────────────────────────

test-equivalence:  ## Test SQL equivalence for generated queries
	$(UV) run python scripts/test_equivalence.py

generate-test-queries:  ## Generate comprehensive OpenCypher test queries
	$(UV) run python scripts/generate_queries.py

validate-sql-syntax:  ## Validate generated SQL syntax
	$(UV) run python scripts/validate_sql.py

benchmark-queries:  ## Benchmark query transpilation performance
	$(UV) run python scripts/benchmark.py

test-recursive-query:  ## Test variable-length path query with custom schema
	@echo "Testing recursive query transpilation..."
	@echo "MATCH path = (root:Vertex)-[rels:REL*1..5]-(n:Vertex) WHERE root.node_id = '1234_algo' AND n.node_type = 'node_type' AND NONE(r IN rels WHERE r.relationship_type IN ['a', 'b']) RETURN rels AS edges, n AS vertex_info" | $(UV) run gsql2rsql transpile --schema tests/schemas/recursive_test_schema.json

# ─────────────────────────────────────────────────────────────────────────────
# Code Quality
# ─────────────────────────────────────────────────────────────────────────────

lint:  ## Run linter (ruff)
	$(UV) run ruff check src/ tests/

lint-fix:  ## Run linter and fix issues
	$(UV) run ruff check src/ tests/ --fix

format:  ## Format code (ruff)
	$(UV) run ruff format src/ tests/

format-check:  ## Check code formatting
	$(UV) run ruff format src/ tests/ --check

typecheck:  ## Run type checker (mypy)
	$(UV) run mypy src/

check: lint format-check typecheck  ## Run all checks (lint, format, typecheck)

# ─────────────────────────────────────────────────────────────────────────────
# Grammar
# ─────────────────────────────────────────────────────────────────────────────

grammar:  ## Generate ANTLR parser from grammar
	java -jar $(ANTLR_JAR) -Dlanguage=Python3 -visitor -o $(GRAMMAR_DIR) $(GRAMMAR_DIR)/Cypher.g4

grammar-check:  ## Check if ANTLR jar exists
	@test -f $(ANTLR_JAR) || (echo "Error: $(ANTLR_JAR) not found. Download from https://www.antlr.org/download.html" && exit 1)

# ─────────────────────────────────────────────────────────────────────────────
# Build & Publish
# ─────────────────────────────────────────────────────────────────────────────

build:  ## Build package
	$(UV) build

publish-test:  ## Publish to TestPyPI
	$(UV) publish --publish-url https://test.pypi.org/legacy/

publish:  ## Publish to PyPI
	$(UV) publish

# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

EXAMPLE_SCHEMA := examples/schema.json
EXAMPLE_SCHEMA_SINGLE_TABLE := examples/schema_single_edge_table.json

cli-help:  ## Show CLI help
	$(UV) run gsql2rsql --help

cli-transpile-help:  ## Show transpile command help
	$(UV) run gsql2rsql transpile --help

cli-example:  ## Run example query
	@echo "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name" | $(UV) run gsql2rsql transpile -s $(EXAMPLE_SCHEMA)

cli-bfs-example:  ## Run BFS example query
	@echo "MATCH (root:Person)-[:KNOWS*1..5]->(neighbor:Person) RETURN DISTINCT neighbor.id, neighbor.name" | $(UV) run gsql2rsql transpile -s $(EXAMPLE_SCHEMA)

cli-bfs-from-root:  ## Run BFS from specific root node
	@echo "MATCH (root:Person)-[:KNOWS*1..5]->(neighbor:Person) WHERE root.id = 1 RETURN DISTINCT neighbor.id, neighbor.name" | $(UV) run gsql2rsql transpile -s $(EXAMPLE_SCHEMA)

cli-bfs-multi-edge:  ## Run BFS with multiple edge types (single table with filter)
	@echo "MATCH (p:Person)-[:KNOWS|FOLLOWS*1..3]->(f:Person) WHERE p.id = 1 RETURN DISTINCT f.id, f.name" | $(UV) run gsql2rsql transpile -s $(EXAMPLE_SCHEMA_SINGLE_TABLE)

# ─────────────────────────────────────────────────────────────────────────────
# Per-Query SQL Dump & Diff (for human validation)
# ─────────────────────────────────────────────────────────────────────────────

dump-sql-01:  ## Dump SQL for test 01 (simple node lookup)
	@$(UV) run python scripts/dump_query_sql.py 01 simple_node_lookup --diff

dump-sql-02:  ## Dump SQL for test 02 (node with property filter)
	@$(UV) run python scripts/dump_query_sql.py 02 node_with_property_filter --diff

dump-sql-03:  ## Dump SQL for test 03 (property projection)
	@$(UV) run python scripts/dump_query_sql.py 03 property_projection_aliases --diff

dump-sql-06:  ## Dump SQL for test 06 (single-hop relationship)
	@$(UV) run python scripts/dump_query_sql.py 06 single_hop_relationship --diff

dump-sql-11:  ## Dump SQL for test 11 (aggregation with GROUP BY)
	@$(UV) run python scripts/dump_query_sql.py 11 aggregation_group_by --diff

dump-sql-12:  ## Dump SQL for test 12 (aggregation with ORDER BY)
	@$(UV) run python scripts/dump_query_sql.py 12 aggregation_order_by --diff

dump-sql-14:  ## Dump SQL for test 14 (COLLECT aggregation)
	@$(UV) run python scripts/dump_query_sql.py 14 collect_aggregation --diff

dump-sql-15:  ## Dump SQL for test 15 (DISTINCT rows)
	@$(UV) run python scripts/dump_query_sql.py 15 distinct_rows --diff

dump-sql-17:  ## Dump SQL for test 17 (CASE expression)
	@$(UV) run python scripts/dump_query_sql.py 17 case_expression --diff

dump-sql-18:  ## Dump SQL for test 18 (EXISTS pattern)
	@$(UV) run python scripts/dump_query_sql.py 18 exists_pattern --diff

dump-sql-19:  ## Dump SQL for test 19 (UNION)
	@$(UV) run python scripts/dump_query_sql.py 19 union --diff

dump-sql-20:  ## Dump SQL for test 20 (COALESCE)
	@$(UV) run python scripts/dump_query_sql.py 20 coalesce --diff

dump-sql-21:  ## Dump SQL for test 21 (Variable-length *0..N)
	@$(UV) run python scripts/dump_query_sql.py 21 variable_length_zero --diff

dump-sql:  ## Dump SQL for a specific test (usage: make dump-sql ID=01 NAME=simple_node_lookup)
	@$(UV) run python scripts/dump_query_sql.py $(ID) $(NAME) --diff

dump-sql-save:  ## Dump and save SQL to actual/ (usage: make dump-sql-save ID=01 NAME=simple_node_lookup)
	@$(UV) run python scripts/dump_query_sql.py $(ID) $(NAME) --save --diff

dump-sql-custom:  ## Dump SQL for custom Cypher (usage: make dump-sql-custom CYPHER="MATCH (n) RETURN n")
	@$(UV) run python scripts/dump_query_sql.py 00 custom --cypher "$(CYPHER)"

test-transpile:  ## Run transpiler tests only
	$(UV) run pytest tests/transpile_tests/ -v

test-transpile-golden:  ## Run only golden file tests
	$(UV) run pytest tests/transpile_tests/ -v -k "golden"

diff-all:  ## Show all diffs between actual and expected SQL
	@for f in tests/output/diff/*.diff; do \
		if [ -f "$$f" ]; then \
			echo "=== $$f ==="; \
			cat "$$f"; \
			echo ""; \
		fi; \
	done

# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

clean:  ## Clean build artifacts
	rm -rf build/ dist/ *.egg-info .pytest_cache/ .mypy_cache/ .ruff_cache/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

clean-all: clean  ## Clean everything including venv
	rm -rf .venv/

tree:  ## Show project structure
	@tree -I '__pycache__|.venv|.git|*.pyc|.pytest_cache|.mypy_cache|.ruff_cache' --dirsfirst

watch-test:  ## Run tests on file change (requires entr)
	@find src tests -name "*.py" | entr -c make test
