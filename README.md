# BigQuery AST Types

A comprehensive AST (Abstract Syntax Tree) library for BigQuery SQL manipulation, following the ast-types pattern.

## Installation

```bash
pip install -e .
```

## Quick Start

```python
import bigquery_ast_types as ast

# Parse SQL
query = ast.parse("SELECT * FROM my_table WHERE id = 1")

# Build AST programmatically
from bigquery_ast_types import b

query = b.select(
    b.col("id"),
    b.col("name")
).from_table("users")

# Serialize to SQL
sql = ast.to_sql(query)
print(sql)  # SELECT id, name FROM users

# Pretty print
print(ast.pretty_print(query))
```

## Features

- **Type-safe AST nodes** - Strongly typed node definitions for BigQuery SQL
- **NodePath traversal** - jQuery-like traversal with parent tracking
- **Visitor pattern** - Transform AST with visitor classes  
- **Builder API** - Fluent API for constructing AST nodes
- **SQL serialization** - Convert AST back to formatted SQL
- **Multiple parsers** - SQLGlot parser included, extensible for others

## Structure

```
bigquery-ast-types/
├── lib/              # Core library modules
│   ├── types.py      # AST node type definitions
│   ├── node_path.py  # Path-based traversal
│   ├── visitor.py    # Visitor pattern implementation
│   ├── builders.py   # Fluent builder API
│   ├── scope.py      # Scope management
│   └── serializer.py # SQL serialization
├── parsers/          # SQL parsers
│   └── sqlglot.py    # SQLGlot-based parser
├── examples/         # Usage examples
├── tests/            # Test suite
└── docs/             # Documentation
```

## Documentation

See the [docs/](docs/) directory for detailed documentation.

## License

MIT