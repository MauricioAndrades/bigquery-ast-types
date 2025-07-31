# BQAST - BigQuery AST Transformation Library 🐕

A jQuery-like AST transformation library for BigQuery SQL, enabling developers to write simple queries that get automatically transformed into complex, standardized patterns.

## Overview

BQAST provides:
- **jsql**: jQuery-like wrapper around sqlglot for ergonomic AST traversal
- **alib**: Reusable query patterns (order merges, deduplication, standardization)
- **Pure AST transformations**: No regex, no string manipulation

## Installation

```bash
cd apps/composer
source venv/bin/activate
pip install sqlglot
```

## Quick Start

### Transform Simple Query to Order Merge

```python
from bqast.alib import OrderMergeBuilder

# Developer writes simple query
simple_sql = """
SELECT order_id, product_id, visitor_id, quantity
FROM `my-project.raw_data.orders`
WHERE order_date >= '2024-01-01'
"""

# Transform to standardized 3-CTE merge
builder = OrderMergeBuilder("my-project", "event_analytics")
merge_sql = builder.build_3cte_merge(
    source_sql=simple_sql,
    retailer_id=12345,
    datetime_threshold="2024-01-01 00:00:00"
)
```

### Direct AST Manipulation

```python
from bqast.jsql import j

# Parse SQL
query = j.parse("SELECT a, b, c FROM table1")

# Find all columns
columns = query.find(exp.Column).toList()

# Replace column 'b' with 'new_b'
query.find(exp.Column, 
    predicate=lambda n: n.name == "b"
).replaceWith(exp.Column(this="new_b"))

# Get transformed SQL
print(query.sql(pretty=True))
```

## Core Components

### jsql - jQuery-like AST Interface

```python
# Parsing
query = j.parse("SELECT * FROM orders")

# Finding nodes
query.find(exp.Column)                    # Find all columns
query.find(exp.Table)                     # Find all tables
query.find(exp.Select)                    # Find SELECT statements

# Filtering
query.find(exp.Column).filter(
    lambda n: n.name.startswith('order_')
)

# Transforming
query.find(exp.Column).replaceWith(
    lambda n: standardize_string_id(n.node)
)

# Iterating
query.find(exp.Column).forEach(
    lambda node, index: print(f"{index}: {node.name}")
)
```

### alib - Reusable Patterns

#### Order Merge Builder
```python
from bqast.alib import OrderMergeBuilder

builder = OrderMergeBuilder("project", "dataset")
merge_sql = builder.build_3cte_merge(source_sql, retailer_id, threshold)
```

#### Deduplication Patterns
```python
from bqast.alib import DedupPatterns

# Simple dedup
sql = DedupPatterns.simple_dedup(
    partition_by=['order_id', 'product_id'],
    order_by=[('timestamp', 'DESC')]
)

# Comprehensive dedup (partition by ALL columns)
sql = DedupPatterns.comprehensive_dedup(all_columns)
```

#### Standardization Patterns
```python
from bqast.alib import StandardizationPatterns

columns = {
    'visitor_id': 'string_id',    # NULLIF(TRIM(col), '')
    'quantity': 'integer',        # SAFE_CAST(col AS INT64)
    'price': 'numeric',          # COALESCE(SAFE_CAST(col AS NUMERIC), 0)
    'is_active': 'boolean'       # COALESCE(col, FALSE)
}

standardized = StandardizationPatterns.standardize_columns(columns)
```

## Examples

See the `examples/` directory for:
- `order_merge_demo.py` - Complete order merge transformations
- `test_jsql.py` - jsql API examples
- `test_serializer.py` - AST to SQL serialization

## Testing

```bash
python test_runner.py
```

## Architecture

```
bqast/
├── jsql.py          # jQuery-like wrapper around sqlglot
├── alib.py          # Reusable query patterns
├── ast/
│   ├── bq_ast_types.py   # AST node definitions
│   ├── bq_builders.py    # Fluent builder API
│   ├── bq_parser.py      # SQL parser
│   └── serializer.py     # AST to SQL
└── examples/
    ├── order_merge_demo.py
    ├── test_jsql.py
    └── test_serializer.py
```

## Design Principles

1. **Pure AST Transformations**: No regex, no string manipulation
2. **Ergonomic API**: jQuery-like interface for intuitive AST traversal
3. **Composable Patterns**: Mix and match transformation patterns
4. **Type Safety**: Leverages sqlglot's type system
5. **BigQuery Native**: Handles all BigQuery-specific syntax

## Contract

Developers write simple, readable SQL → We transform using AST → Output optimized, standardized code

---

Built with ❤️ by Little Bow Wow 🐕
