# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and uses [Conventional Commits](https://www.conventionalcommits.org/).

## [Unreleased]

### Added
- Initial release
- OpenCypher to Databricks SQL transpiler
- Support for MATCH, WHERE, RETURN, WITH clauses
- Variable-length path traversal with `*1..N` syntax
- Undirected relationship support
- Filter pushdown optimizations
- Comprehensive test suite (682+ tests)
- PySpark validation tests
- Documentation with MkDocs
- Examples for fraud detection, credit analysis, and feature engineering

### Features
- 4-phase transpilation pipeline (Parser → Planner → Resolver → Renderer)
- Schema definition via Python dataclasses or JSON
- CLI with interactive mode
- TUI for query editing and visualization

## [0.1.0] - 2026-01-19

Initial development release.
