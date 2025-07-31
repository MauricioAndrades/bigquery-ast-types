# BQAST Architecture - Post Merge

After consolidating duplicate implementations, here's the final architecture:

## Core Modules

### 1. **ast/** - AST Core Implementation
- `ast_types.py` - Complete AST node definitions for BigQuery
- `builders.py` - Fluent builder API with validation
- `parser.py` - BigQuery SQL parser using bigquery-sql-parser
- `serializer.py` - AST to SQL serialization with formatting
- `visitor.py` - Visitor pattern implementation
- `collection.py` - AST node collections with jQuery-like API
- `node_path.py` & `enhanced_node_path.py` - AST traversal utilities
- `scope.py` - Scope management for AST transformations

### 2. **jsql.py** - jQuery-like SQL Transformation API
- Wrapper around sqlglot for ergonomic AST manipulation
- Iterator pattern with find(), filter(), replaceWith(), forEach()
- Pattern matching capabilities
- Helper functions for common transformations

### 3. **alib.py** - Reusable Query Patterns Library
- `OrderMergeBuilder` - 3-CTE merge pattern generation
- `DedupPatterns` - Deduplication strategies
- `StandardizationPatterns` - Column type standardization
- `RetailerPatterns` - Retailer-specific patterns (e.g., Chewy)
- `QueryAnalyzer` - Query complexity analysis

## Key Design Decisions

### 1. Pure AST Transformations
- NO regex or string manipulation
- All transformations via AST node manipulation
- Type-safe builders with validation

### 2. Dual Parser Strategy
- **bigquery-sql-parser** for initial prototyping (ast/parser.py)
- **sqlglot** for production use (jsql.py)
- Migration path from one to the other

### 3. jQuery-inspired API
```python
# Find and transform pattern
query.find(exp.Column)
     .filter(lambda n: n.name.startswith('order_'))
     .replaceWith(lambda n: standardize_string_id(n.node))
```

### 4. Builder Pattern with Validation
```python
# Type-safe builders
b.col('order_id')  # Returns Identifier
b.lit(42)          # Returns IntegerLiteral
b.date('2024-01-01')  # Validates format
```

## Usage Flow

1. **Simple SQL Input** → Parser → AST
2. **AST Transformation** → jsql Iterator API → Modified AST
3. **Pattern Application** → alib patterns → Standardized AST
4. **Serialization** → Serializer → BigQuery SQL Output

## Example: Order Merge Transformation

```python
from bqast.alib import OrderMergeBuilder

# Input: Simple SQL
simple = "SELECT * FROM orders WHERE date >= '2024-01-01'"

# Transform: Apply 3-CTE pattern
builder = OrderMergeBuilder("project", "dataset")
complex = builder.build_3cte_merge(
    source_sql=simple,
    retailer_id=123,
    datetime_threshold="2024-01-01"
)

# Output: Complex MERGE with CTEs, deduplication, standardization
```

## Testing

- `tests/test_builders.py` - Builder validation tests
- `test_runner.py` - Integration tests
- `examples/` - Working examples and demos

## Next Steps

1. Complete sqlglot migration for all components
2. Add more retailer-specific patterns
3. Build VS Code extension using AST capabilities
4. Create automated migration tools for legacy SQL

---

Built with ❤️ by Little Bow Wow 🐕
