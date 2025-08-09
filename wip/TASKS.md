# Implementation Tasks

This document lists outstanding tasks to bring the BigQuery AST transformation module closer to feature parity with jscodeshift.

## 1. Statement and Clause Transformations
- [ ] Implement transformation helpers for `GROUP BY` clauses.
- [ ] Implement `HAVING` clause transformations.
- [ ] Implement `ORDER BY` clause transformations.
- [ ] Implement `LIMIT`/`OFFSET` clause transformations.
- [ ] Add support for `WITH` (CTE) clause transformations.
- [ ] Handle set operations (`UNION`, `INTERSECT`, `EXCEPT`).
- [ ] Complete transformation logic for `MERGE` statements.

## 2. Literal Parsing and Builders
- [ ] Parse and build `GEOGRAPHY` literals.
- [ ] Parse and build `STRUCT` literals.
- [ ] Parse and build `RANGE` literals.
- [ ] Provide builders for `TIME`, `DATETIME`, `TIMESTAMP`, and `INTERVAL` literals.

## 3. Collection and NodePath Utilities
- [ ] Add `paths()` method for iterating `NodePath` objects within a collection.
- [ ] Add `to_source()` utility to emit SQL from transformed ASTs.
- [ ] Expose collection-level `insert_before()` and `insert_after()` helpers.
- [ ] Introduce additional builder helpers for common BigQuery transformations.

## 4. Testing and Documentation
- [ ] Add unit tests covering each new transformation and literal handler.
- [ ] Update documentation to describe new APIs and supported syntax.

