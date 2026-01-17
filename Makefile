.PHONY: help install install-dev test test-cov lint format typecheck grammar clean build publish

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

test-bfs:  ## Run BFS/recursive tests only
	$(UV) run pytest tests/test_renderer.py::TestBFSWithRecursive -v

test-verbose:  ## Run tests with verbose output
	$(UV) run pytest tests/ -v --tb=long

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
