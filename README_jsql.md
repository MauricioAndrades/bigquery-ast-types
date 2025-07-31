# symbo/jsql: Ergonomic SQL AST Manipulation for Data Pipelines

## What is this?

**jsql.py** is a wrapper around sqlglot that lets you traverse and transform SQL ASTs programmatically and ergonomically—like [jscodeshift](https://github.com/facebook/jscodeshift) for SQL.

**alib.py** is a library for composing query templates programmatically using the `j` API.

---

## Quickstart

```python
from symbo import j

root = j.parse("SELECT order_id, quantity, price FROM orders")

# Traverse with Iterator!
for n in j.Iterator(root, j.Column, lambda n: n.name == "quantity"):
    n.replaceWith(j.Column(j("SAFE_CAST(quantity AS INT64)", "quantity")))

print(root.sql())
```

## Builder API

- `j.Select(...)`, `j.Column("col")`, `j.Expression(...)`
- `j("SQL_EXPR", "alias")` — alias builder
- `j.parse(sql_string)` — parse SQL to JSQLNode
- `j.Iterator(root, node_type, predicate)` — walk the AST, chainable

## Programmatic Query Patterns

Use `alib.py` for reusable builders:

```python
from symbo import alib

root = alib.order_3cte_pipeline("orders", ["order_id", "customer_id", "amount"])
print(root.sql())
```

## How to Extend

- Add methods to `Iterator` for more power (`map`, `filter`, etc.)
- Add more builders to `alib.py` for new patterns
- Document your patterns and add tests!

## Why use this?

- No more string hacking for SQL pipelines
- Easy, composable AST mutation
- Supports all BigQuery features via sqlglot
- Designed for DAGs, batch jobs, complex data engineering tasks

---