# The jscodeshift Pattern in BigQuery AST Collection

## What is jscodeshift?

jscodeshift is Facebook's JavaScript codemod toolkit that revolutionized AST transformations by applying jQuery's philosophy to code manipulation. Instead of manually traversing trees with visitors, it provides a fluent, chainable API that feels natural to developers familiar with DOM manipulation.

## Core Philosophy: jQuery for ASTs

Just as jQuery transformed DOM manipulation from this:
```javascript
// Vanilla DOM
var elements = document.getElementsByClassName('user');
for (var i = 0; i < elements.length; i++) {
    elements[i].style.color = 'red';
}
```

To this:
```javascript
// jQuery
$('.user').css('color', 'red');
```

jscodeshift transforms AST manipulation from this:
```python
# Traditional Visitor Pattern
class RenameVisitor(BaseVisitor):
    def visit_Identifier(self, node):
        if node.name == 'oldName':
            node.name = 'newName'
        self.generic_visit(node)

visitor = RenameVisitor()
visitor.visit(ast)
```

To this:
```python
# jscodeshift Pattern (your collection.py)
astCollection(ast).find(Identifier).filter(
    lambda p: p.node.name == 'oldName'
).replaceWith(
    lambda p, i: b.col('newName')
)
```

## The Pattern's Key Components

### 1. **Collection Wrapper**
The foundation is wrapping nodes in a collection that provides the chainable API:

```python
# Your implementation
class Collection(Generic[T]):
    def __init__(self, paths: List[NodePath]):
        self.paths = paths
```

This is like jQuery's `$()` - it turns raw nodes into something you can chain methods on.

### 2. **Chainable Finding Methods**
Methods that search the tree and return new collections:

```python
# Find all descendants of a type
collection.find(BinaryOp)

# Filter current collection
collection.filter(lambda p: p.node.operator == '=')

# Find closest ancestor
collection.closest(Select)

# Get parents/children
collection.parent()
collection.children()
```

**Key Principle**: Each method returns a `Collection`, enabling chaining.

### 3. **Transformation Methods**
Methods that modify the AST:

```python
# Replace nodes
collection.replaceWith(new_node)
collection.replaceWith(lambda path, index: generate_node(path))

# Insert nodes
collection.insertBefore(new_node)
collection.insertAfter(new_node)

# Remove nodes
collection.remove()
```

### 4. **Iteration Methods**
Methods for working with the collection as a set:

```python
# Execute function for each
collection.forEach(lambda path, i: print(path.node))

# Map to new values
names = collection.map(lambda path, i: path.node.name)

# Test conditions
has_nulls = collection.some(lambda p: isinstance(p.node, NullLiteral))
all_strings = collection.every(lambda p: isinstance(p.node, StringLiteral))
```

### 5. **Inspection Methods**
Methods to understand what's in the collection:

```python
collection.size()           # How many nodes
collection.isEmpty()        # Is it empty?
collection.getTypes()       # What types are present?
collection.hasClass(Select) # Does it contain this type?
```

## Real-World Example: Transforming a Query

Let's say you want to:
1. Find all column references to "user_id"
2. Add a table prefix "u"
3. Wrap them in LOWER()

### Traditional Visitor Approach
```python
class TransformVisitor(BaseVisitor):
    def visit_Identifier(self, node):
        if node.name == 'user_id' and not node.table:
            # Create new function call node
            new_node = FunctionCall(
                function_name='LOWER',
                arguments=[Identifier('user_id', table='u')]
            )
            # Complex parent replacement logic needed here
            # ...
        self.generic_visit(node)
```

### jscodeshift Pattern Approach
```python
from lib.collection import astCollection
from lib.builders import b

astCollection(query)
    .find(Identifier)
    .filter(lambda p: p.node.name == 'user_id' and not p.node.table)
    .replaceWith(lambda p, i: 
        b.func('LOWER', b.col('user_id', table='u'))
    )
```

## Advanced Patterns in Your Implementation

### 1. **Path-Based Navigation**
Your implementation wraps nodes in `NodePath`, preserving parent relationships:

```python
collection.getPath('body.statements.0')  # Navigate by path
collection.getProp('name')               # Extract property values
```

### 2. **Bulk Operations with Index Safety**
Operations like `insertBefore` process in reverse order to avoid index shifting:

```python
def insertBefore(self, node):
    new_paths = []
    # Process in reverse to avoid index issues
    for i, path in enumerate(reversed(self.paths)):
        # ... insertion logic
    return Collection(list(reversed(new_paths)))
```

### 3. **Lazy vs Eager Evaluation**
The collection is eager (holds a list), but finding is lazy via visitors:

```python
def find(self, node_type, predicate=None):
    class FindVisitor(BaseVisitor):
        def __init__(self):
            self.found = []
        
        def visit(self, path):
            # Only collect matching nodes
            if type_match and (predicate is None or predicate(path)):
                self.found.append(path)
    
    # Visit happens here, building results
    visitor = FindVisitor()
    for path in self.paths:
        visit(path, visitor)
    
    return Collection(visitor.found)
```

## Why This Pattern is Powerful

### 1. **Declarative over Imperative**
You describe WHAT you want, not HOW to traverse:
```python
# Declarative: "Find all identifiers named 'x' and rename them"
col.find(Identifier, lambda p: p.node.name == 'x').replaceWith(b.col('y'))

# vs Imperative: "Walk the tree, check each node, if it's an identifier..."
```

### 2. **Composable Transformations**
Chain multiple operations naturally:
```python
col.find(Select)
   .find(Identifier)      # Find identifiers within SELECTs
   .filter(is_user_table)  # Only user tables
   .closest(Join)          # Go up to their JOIN
   .remove()               # Remove those JOINs
```

### 3. **Familiar to Web Developers**
Anyone who's used jQuery immediately understands:
```python
# jQuery
$('.old-class').removeClass('old-class').addClass('new-class')

# jscodeshift pattern
col.find(OldNode).replaceWith(NewNode)
```

### 4. **Handles Multiple Nodes Naturally**
Operations apply to all matched nodes without explicit loops:
```python
# This updates ALL matching identifiers, not just one
col.find(Identifier, match_condition).replaceWith(new_value)
```

## Comparison: Your Implementation vs Original jscodeshift

### Your Strengths
1. **Type Safety** - Generic types and type hints
2. **NodePath Integration** - Preserves parent relationships
3. **Scope Awareness** - Integrated scope tracking

### jscodeshift Advantages
1. **Recast Integration** - Preserves formatting/comments
2. **Dry Run Mode** - Test transformations without applying
3. **Statistics** - Built-in transformation statistics

### Different Design Choices

| Feature | jscodeshift | Your Implementation |
|---------|------------|-------------------|
| Node Wrapper | AST nodes directly | NodePath with parent tracking |
| Method Names | jQuery-style (e.g., `at()`) | More explicit (e.g., `at()` and `get()`) |
| Transform Application | Can be dry-run | Always applies |
| Return Values | Always collections | Mixed (some return lists) |

## Best Practices When Using This Pattern

### 1. **Start Broad, Then Narrow**
```python
col.find(Select)           # All SELECTs
   .find(Join)             # JOINs within those
   .filter(is_left_join)   # Only LEFT JOINs
```

### 2. **Use Predicates for Complex Matching**
```python
def is_aggregate_function(path):
    return (isinstance(path.node, FunctionCall) and 
            path.node.name in ['COUNT', 'SUM', 'AVG'])

col.find(FunctionCall, is_aggregate_function)
```

### 3. **Combine with Builders**
```python
col.find(Literal).replaceWith(
    lambda p, i: b.cast(p.node, 'STRING')  # Wrap all literals in CAST
)
```

### 4. **Use Index Parameter for Conditional Logic**
```python
col.find(SelectColumn).replaceWith(
    lambda p, i: b.select_col(
        p.node.expression,
        alias=f"col_{i}"  # Generate unique aliases
    )
)
```

## Example: Complex Query Transformation

Here's a real-world example that showcases the pattern's power:

```python
# Task: Sanitize a query by:
# 1. Adding table prefixes to all unqualified columns
# 2. Wrapping string comparisons in LOWER()
# 3. Converting = NULL to IS NULL

from lib.collection import astCollection
from lib.builders import b

def sanitize_query(query):
    col = astCollection(query)
    
    # Add table prefixes
    col.find(Identifier).filter(
        lambda p: not p.node.table
    ).forEach(
        lambda p, i: setattr(p.node, 'table', infer_table(p))
    )
    
    # Wrap string comparisons in LOWER()
    col.find(BinaryOp).filter(
        lambda p: (p.node.operator in ['=', '!='] and
                  any(isinstance(child.node, StringLiteral) 
                      for child in p.get_children()))
    ).replaceWith(
        lambda p, i: b.eq(
            b.func('LOWER', p.node.left),
            b.func('LOWER', p.node.right)
        ) if p.node.operator == '=' else b.neq(
            b.func('LOWER', p.node.left),
            b.func('LOWER', p.node.right)
        )
    )
    
    # Convert = NULL to IS NULL
    col.find(BinaryOp).filter(
        lambda p: (p.node.operator == '=' and
                  isinstance(p.node.right, NullLiteral))
    ).replaceWith(
        lambda p, i: b.is_null(p.node.left)
    )
    
    return query
```

# Complete BigQuery AST Library Architecture

## Overview

This library provides two parallel approaches for BigQuery SQL manipulation:
1. **Native AST System** - Custom-built AST with type safety and visitor pattern
2. **SQLGlot Wrapper** - jQuery-like wrapper around sqlglot for rapid transformations

## Core Architecture Layers

```
┌─────────────────────────────────────────────────────────┐
│                    User Interface                        │
│  builders.py / bsql.py / collection.py (jQuery-like)    │
└─────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────┐
│                    AST Manipulation                      │
│     node_path.py / visitor.py / scope.py               │
└─────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────┐
│                  Core Type System                        │
│                      types.py                           │
└─────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────┐
│                    Serialization                         │
│                    serializer.py                         │
└─────────────────────────────────────────────────────────┘
```

## Module Breakdown

### 1. **types.py** - Core Type System
The foundation containing all AST node definitions with proper inheritance:

- **Base Classes**: `ASTNode`, `Expression`, `Statement`
- **Literal Types**: `StringLiteral`, `IntegerLiteral`, `BooleanLiteral`, etc.
- **Expression Types**: `BinaryOp`, `UnaryOp`, `FunctionCall`, `Cast`, `Case`
- **Statement Types**: `Select`, `Insert`, `Update`, `Merge`, `CreateTable`
- **BigQuery Types**: `BQDataType`, `BigQueryType`, `TypeParser`, `TypeValidator`
- **Enums**: `JoinType`, `ComparisonOp`, `OrderDirection`

**Key Features:**
- All expressions inherit from `Expression`
- Visitor pattern support via `accept()` method
- Type validation and parsing for BigQuery data types
- Compatibility aliases for flexible field access

### 2. **builders.py** - Fluent Builder API
Provides ergonomic construction of AST nodes:

```python
from lib.builders import b

# Instead of: BinaryOp(Identifier("age"), ">=", IntegerLiteral(18))
expr = b.gte(b.col("age"), b.lit(18))

# Complex expressions
condition = b.null_safe_eq(
    b.col("user_id"),
    b.col("owner_id")
)
```

**Issue Found**: Still contains `ExpressionTypes` tuple that should be removed (relies on inheritance instead)

### 3. **node_path.py** - Tree Navigation & Manipulation
Wraps AST nodes with traversal capabilities:

```python
path = NodePath(ast_root)
for child in path.walk():
    if isinstance(child.node, Identifier):
        print(f"Found: {child.node.name} at {child.path}")
```

**Key Features:**
- Parent/child relationship tracking
- Scope management for name resolution
- Tree modification (insert_before, insert_after, replace, remove)
- Cached properties for performance

### 4. **visitor.py** - Visitor Pattern Implementation
Simple visitor pattern for tree traversal:

```python
class MyVisitor(BaseVisitor):
    def visit_Identifier(self, path):
        print(f"Visiting identifier: {path.node.name}")

visit(ast_root, MyVisitor())
```

**Design Note**: Uses method name convention `visit_{ClassName}` for dispatch

### 5. **collection.py** - jQuery-like Collection API
Chainable API for working with sets of nodes:

```python
from lib.collection import astCollection

collection = astCollection(ast_root)
identifiers = collection.find(Identifier).filter(
    lambda p: p.node.name.startswith("user_")
)
identifiers.replaceWith(lambda p, i: b.col(f"u_{p.node.name}"))
```

**Key Methods:**
- `find()`, `filter()`, `closest()` - Navigation
- `replaceWith()`, `insertBefore()`, `insertAfter()` - Manipulation
- `forEach()`, `map()`, `some()`, `every()` - Iteration

### 6. **bsql.py** - SQLGlot Integration
Alternative approach using sqlglot as the backend:

```python
from lib.bsql import j

ast = j.parse("SELECT * FROM users WHERE age >= 18")
ast.find(exp.Column).replaceWith(
    lambda n: j.Func("LOWER", n.node)
)
print(ast.sql())
```

**Key Features:**
- Wraps sqlglot expressions in `SQLNode`
- jQuery-like `Iterator` for traversal
- Pattern matching utilities
- Transformation helpers

### 7. **serializer.py** - SQL Generation
Converts AST back to formatted SQL:

```python
from lib.serializer import to_sql, pretty_print

sql = pretty_print(ast_node)  # Formatted SQL
sql = compact_print(ast_node)  # Compact SQL
```

**Options:**
- Indentation control
- Keyword capitalization
- Quote handling
- Format styles (expanded/compact)

### 8. **scope.py** - Name Resolution
Tracks variable/table visibility:

```python
scope = Scope()
scope.declare("users", table_ref)
resolved = scope.lookup("users")  # Returns table_ref
```

**Scope Creation Points:**
- CTEs create new scope
- Subqueries create new scope
- Table functions create new scope

## Design Patterns in Use

### 1. Visitor Pattern
- Each node has `accept(visitor)`
- Visitor has type-specific methods
- Enables tree traversal without modifying nodes

### 2. Builder Pattern
- Fluent API hides complex construction
- Validation at build time
- Type inference (e.g., `lit()` creates appropriate literal type)

### 3. Composite Pattern
- Expressions contain other expressions
- Uniform treatment of leaf and composite nodes

### 4. Facade Pattern
- `bsql.py` provides simplified interface to sqlglot
- `builders.py` simplifies AST construction

### 5. Iterator Pattern
- `Collection` and `Iterator` classes for traversal
- jQuery-inspired chaining

## Two Approaches Comparison

### Native AST (`types.py` + `builders.py`)
**Pros:**
- Full control over type system
- Type safety with dataclasses
- Extensible visitor pattern
- Clean separation of concerns

**Cons:**
- More code to maintain
- Need to implement SQL generation
- Manual parser implementation needed

### SQLGlot Wrapper (`bsql.py`)
**Pros:**
- Leverages mature sqlglot library
- Built-in SQL parsing/generation
- Handles edge cases

**Cons:**
- External dependency
- Less type safety
- Limited to sqlglot's capabilities

## Usage Patterns

### Pattern 1: Build and Transform
```python
# Build AST using builders
from lib.builders import b

query = b.select(
    columns=[b.col("name"), b.col("age")],
    from_table="users",
    where=b.gte(b.col("age"), b.lit(18))
)

# Transform using visitor
class UppercaseColumns(BaseVisitor):
    def visit_Identifier(self, path):
        path.node.name = path.node.name.upper()

visit(query, UppercaseColumns())
```

### Pattern 2: Find and Replace
```python
# Using collections
from lib.collection import astCollection

col = astCollection(query)
col.find(Identifier, lambda p: p.node.name == "age").replaceWith(
    b.col("user_age")
)
```

### Pattern 3: Parse and Modify
```python
# Using bsql
from lib.bsql import j

query = j.parse("SELECT * FROM users")
query.find(exp.Star).replaceWith(
    j.Column("user_id")
)
```

## Key Design Decisions

### 1. Inheritance-based Type Checking
All expression types inherit from `Expression`, eliminating the need for type tuples. Simply use:
```python
isinstance(node, Expression)
```

### 2. Field Name Compatibility
The types module provides property aliases for compatibility:
- `function_name` with alias `name`
- `arguments` with alias `args`

This allows both naming conventions to work seamlessly.

### 3. Import Management
The `__init__.py` uses try/except blocks to handle optional dependencies and avoid circular imports between modules.

## Best Practices

### For Library Users

1. **Choose Your Approach**
   - Use native AST for type safety and control
   - Use bsql for quick transformations with existing SQL

2. **Start with Builders**
   ```python
   from lib.builders import b
   # Build your AST incrementally
   ```

3. **Use Collections for Bulk Operations**
   ```python
   from lib.collection import astCollection
   # Find and transform multiple nodes at once
   ```

4. **Leverage Type System**
   ```python
   from lib.types import TypeParser
   bq_type = TypeParser.parse("ARRAY<STRUCT<name STRING, age INT64>>")
   ```

### For Library Maintainers

1. **Keep Types Pure**
   - No business logic in type definitions
   - Only structure and accept() method

2. **Validate in Builders**
   - Check inputs early
   - Provide helpful error messages

3. **Cache in NodePath**
   - Expensive computations should be cached
   - Clear cache on structural changes

4. **Test Index Management**
   - Critical for insert_before/insert_after
   - The bug fix in NodePath shows this is error-prone

## Testing Strategy

### Unit Tests Needed
- **types_test.py** - Node creation and properties
- **builders_test.py** - Builder validation and construction
- **node_path_test.py** - Tree traversal and manipulation (already exists)
- **visitor_test.py** - Visitor dispatch
- **collection_test.py** - Collection operations
- **serializer_test.py** - SQL generation
- **bsql_test.py** - SQLGlot integration

### Integration Tests
- Parse SQL → Transform → Generate SQL
- Complex query construction
- Scope resolution across CTEs

## Future Enhancements

- [ ] SQL Parser (native, not sqlglot)
- [ ] Query optimization passes
- [ ] Schema validation against catalog
- [ ] Type inference for expressions
- [ ] Query cost estimation
- [ ] AST diffing for migrations
- [ ] Visual AST explorer
- [ ] Performance profiling tools

## Conclusion

This library provides a robust foundation for BigQuery SQL manipulation with two complementary approaches. The native AST system offers type safety and control, while the sqlglot wrapper provides rapid development. The layered architecture ensures clean separation of concerns and enables multiple usage patterns.

The jscodeshift pattern transforms AST manipulation from a complex traversal problem into a declarative transformation pipeline. Your `collection.py` successfully adapts this pattern to Python and BigQuery SQL, providing a powerful and intuitive API for SQL transformations. The combination of jQuery-style chaining, functional programming concepts, and AST awareness makes complex transformations surprisingly readable and maintainable.
