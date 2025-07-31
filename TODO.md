# BigQuery AST Library - TODO List

This file tracks all planned improvements and enhancements for the BigQuery AST library.

## High Priority

### 1. Type System Improvements
- [ ] Add `from __future__ import annotations` to all modules for cleaner type hints
- [ ] Use stricter type annotations (e.g., `List[Expression]` instead of `list`)
- [ ] Fix circular type references with proper forward declarations
- **Files**: `builders.py`, `ast_types.py`, `parser.py`
- **Related Issue**: #2

### 2. Validation and Error Handling
- [ ] Add validation to builder methods (e.g., valid join types, non-empty SELECT)
- [ ] Implement proper error messages with context
- [ ] Add runtime type checking for builder inputs
- **Files**: `builders.py`
- **Related Issue**: #3

### 3. Testing
- [ ] Create comprehensive test suite for all builder methods
- [ ] Add edge case tests (empty arrays, deeply nested expressions)
- [ ] Add integration tests for complex query generation
- [ ] Add property-based tests using hypothesis
- **Files**: `tests/test_builders.py`, `tests/test_parser.py`
- **Related Issue**: #5

## Medium Priority

### 4. SQL Serialization
- [ ] Create dedicated SQL serializer with proper formatting
- [ ] Add indentation and pretty-printing support
- [ ] Implement proper identifier quoting for BigQuery
- [ ] Handle all BigQuery-specific syntax correctly
- **Files**: Create new `ast/serializer.py`
- **Related Issue**: #6

### 5. Documentation
- [ ] Expand docstrings with usage examples for each builder method
- [ ] Create comprehensive README with getting started guide
- [ ] Add architecture documentation
- [ ] Create cookbook with common patterns
- **Files**: `README.md`, all source files
- **Related Issue**: #4

## Low Priority

### 6. Additional Features
- [ ] Add support for all BigQuery literal types (BYTES, DATE, TIMESTAMP, etc.)
- [ ] Implement pattern matching similar to ast-types
- [ ] Add transformation utilities for common refactorings
- [ ] Create visualization tools for AST debugging

### 7. Performance
- [ ] Add caching for frequently accessed node properties
- [ ] Optimize tree traversal algorithms
- [ ] Profile and optimize memory usage for large queries

## Completed
- [x] Initial AST node definitions
- [x] Basic builder API
- [x] Collection API for node manipulation
- [x] Enhanced NodePath with scope tracking

---

## How to Contribute

1. Pick a TODO item and create a GitHub issue if one doesn't exist
2. Create a feature branch: `feature/issue-number-description`
3. Make your changes with tests
4. Submit a PR referencing the issue

## Notes

- Each TODO should reference a GitHub issue for tracking
- Keep PRs focused on a single concern
- All changes should include appropriate tests
- Update this file as items are completed or new ones are identified