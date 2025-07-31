# ZetaSQL AST Traversal & Manipulation Library (Python)

This project provides an ast-types/jscodeshift-style abstraction for traversing, analyzing, and editing BigQuery SQL ASTs using ZetaSQL's Python bindings.

## Project Structure

```
zsql_ast_transformer/
│
├── zsql_ast_transformer/
│   ├── __init__.py
│   ├── parser.py
│   ├── node_path.py
│   ├── visitor.py
│   ├── scope.py
│   ├── builders.py
│   └── types.py
│
├── tests/
│   ├── __init__.py
│   ├── test_parser.py
│   ├── test_traversal.py
│   ├── test_editing.py
│   ├── test_scope.py
│   └── test_builders.py
│
├── examples/
│   └── find_tables.py
│
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Instructions

1. **Install ZetaSQL Python Bindings**  
   See: [ZetaSQL Python Bindings](https://github.com/google/zetasql/tree/master/python)
   ```
   pip install zetasql-python
   ```

2. **Clone & Install**
   ```
   git clone <this-repo-url>
   cd zsql_ast_transformer
   pip install -r requirements.txt
   ```

3. **Run Tests**
   ```
   pytest tests/
   ```

4. **Example Usage**
   See `examples/find_tables.py` for a sample visitor.

## Extending

- Add more builder functions in `builders.py` for new AST node types.
- Enhance `scope.py` as needed for more advanced name resolution.
- Expand the visitor API with more node type handlers.

---

**PRs and issues are welcome!**