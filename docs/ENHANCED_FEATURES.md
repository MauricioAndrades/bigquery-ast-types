# BigQuery AST Types - Enhanced Features Documentation

This document describes the enhanced features added to support the complete BigQuery lexical specification.

## Overview

The BigQuery AST Types library has been enhanced to support all constructs defined in the BigQuery lexical specification, including:

- Multiple identifier types (unquoted, quoted, enhanced general)
- Path expressions for object navigation
- Table and column names with dash rule support
- Enhanced literals with formatting options
- Query parameters (named and positional)
- Comment preservation
- Complete visitor pattern support

## Enhanced Identifier Types

### UnquotedIdentifier
Standard identifiers that must begin with a letter or underscore.

```python
from lib.types import UnquotedIdentifier

# Valid unquoted identifiers
id1 = UnquotedIdentifier('my_column')
id2 = UnquotedIdentifier('_private_field')
id3 = UnquotedIdentifier('column123')
```

### QuotedIdentifier
Identifiers enclosed by backticks that can contain any characters.

```python
from lib.types import QuotedIdentifier

# Quoted identifiers can contain spaces, symbols, etc.
id1 = QuotedIdentifier('column with spaces')
id2 = QuotedIdentifier('column-with-dashes')
id3 = QuotedIdentifier('123numeric_start')
id4 = QuotedIdentifier('GROUP')  # Reserved keywords
```

### EnhancedGeneralIdentifier
Complex identifiers supporting path expressions with multiple separators.

```python
from lib.types import EnhancedGeneralIdentifier

# Complex path with different separators
path_id = EnhancedGeneralIdentifier(
    name='table.field/subfield:index',
    parts=['table', 'field', 'subfield', 'index'],
    separators=['.', '/', ':']
)
```

## Path Expressions

Path expressions describe navigation through object graphs.

```python
from lib.types import PathExpression, PathPart

# Build a path expression: table.column.field
parts = [
    PathPart('table'),
    PathPart('column', separator='.'),
    PathPart('field', separator='.')
]
path_expr = PathExpression(parts)
```

## Table and Column Names

### TableName
Supports BigQuery's project.dataset.table format with dash rules.

```python
from lib.types import TableName

# Simple table name
table1 = TableName(table='users')

# Dataset qualified
table2 = TableName(dataset='analytics', table='events')

# Fully qualified with project
table3 = TableName(
    project='my-project',
    dataset='my_dataset', 
    table='my_table',
    supports_dashes=True
)
```

### ColumnName and FieldName
Column and field names with dash support.

```python
from lib.types import ColumnName, FieldName

# Column with dashes (BigQuery specific)
col = ColumnName('user-id', supports_dashes=True)

# Struct field
field = FieldName('address_line_1')
```

## Enhanced Literals

### String and Bytes Literals
Support all BigQuery string and bytes literal formats.

```python
from lib.types import StringLiteral, BytesLiteral

# Basic string literal
str1 = StringLiteral('hello world', quote_style='"')

# Raw string literal
str2 = StringLiteral(r'regex\d+', quote_style="'", is_raw=True)

# Triple-quoted string  
str3 = StringLiteral('multi\nline\nstring', quote_style='"""')

# Bytes literal
bytes1 = BytesLiteral(b'binary data', quote_style='"')

# Raw bytes literal
bytes2 = BytesLiteral(b'raw\\bytes', quote_style="'", is_raw=True)
```

### Numeric Literals
High-precision numeric types.

```python
from lib.types import IntegerLiteral, NumericLiteral, BigNumericLiteral

# Integer with hex support
int1 = IntegerLiteral(255)
int2 = IntegerLiteral(255, is_hexadecimal=True)  # 0xFF

# Exact decimal precision
num1 = NumericLiteral('123.456789012345')
num2 = BigNumericLiteral('123456789012345678901234567890.123456789')
```

### Date/Time Literals
All BigQuery temporal literal types.

```python
from lib.types import (
    DateLiteral, TimeLiteral, DatetimeLiteral, 
    TimestampLiteral, IntervalLiteral
)

# Date and time literals
date = DateLiteral('2023-12-25')
time = TimeLiteral('12:30:00.123456')
datetime = DatetimeLiteral('2023-12-25 12:30:00')
timestamp = TimestampLiteral('2023-12-25 12:30:00+08', timezone='+08')

# Interval literal
interval = IntervalLiteral('1-2 3 4:5:6.789', from_part='YEAR', to_part='SECOND')
```

### Complex Literals
Arrays, structs, ranges, and JSON.

```python
from lib.types import ArrayLiteral, StructLiteral, RangeLiteral, JSONLiteral

# Array literal
array = ArrayLiteral([
    IntegerLiteral(1),
    IntegerLiteral(2), 
    IntegerLiteral(3)
], element_type='INT64')

# Struct literal
struct = StructLiteral([
    ('name', StringLiteral('John')),
    ('age', IntegerLiteral(25))
])

# Range literal
range_lit = RangeLiteral(
    range_type='DATE',
    lower_bound=DateLiteral('2023-01-01'),
    upper_bound=DateLiteral('2023-12-31')
)

# JSON literal  
json_lit = JSONLiteral('{"name": "John", "age": 25}')
```

## Query Parameters

### Named Parameters
BigQuery @parameter_name syntax.

```python
from lib.types import NamedParameter

param = NamedParameter('user_id')
# Represents: @user_id
```

### Positional Parameters
BigQuery ? syntax.

```python
from lib.types import PositionalParameter

param = PositionalParameter(1)
# Represents: ? (first positional parameter)
```

## Comments

Preserve comments in the AST with style information.

```python
from lib.types import Comment

# Single-line comments
comment1 = Comment('This is a hash comment', '#')
comment2 = Comment('This is a dash comment', '--')

# Multi-line comment
comment3 = Comment(
    'This is a\nmulti-line comment',
    '/* */',
    is_multiline=True
)
```

## Visitor Pattern

All enhanced node types integrate with the visitor pattern.

```python
from lib.types import ASTVisitor

class MyVisitor(ASTVisitor):
    def visit_unquoted_identifier(self, node):
        print(f"Found identifier: {node.name}")
    
    def visit_table_name(self, node):
        if node.project:
            print(f"Qualified table: {node.project}.{node.dataset}.{node.table}")
        else:
            print(f"Table: {node.table}")
    
    def visit_string_literal(self, node):
        style = "raw " if node.is_raw else ""
        print(f"String literal: {style}{node.quote_style}{node.value}{node.quote_style}")
    
    def visit_named_parameter(self, node):
        print(f"Parameter: @{node.name}")
    
    def visit_comment(self, node):
        print(f"Comment ({node.style}): {node.text}")
    
    # ... implement other required abstract methods

# Use the visitor
visitor = MyVisitor()
node = UnquotedIdentifier('my_column')
node.accept(visitor)  # Calls visit_unquoted_identifier
```

## Parser Integration

The enhanced parser automatically detects and creates appropriate node types.

```python
from parsers.sqlglot import parse

# Parse SQL with enhanced features
sql = '''
SELECT 
    `quoted-column`,
    unquoted_column,
    DATE '2023-01-01' as date_col,
    @param_name as param_col
FROM my-project.dataset.table  
WHERE active = TRUE
'''

ast = parse(sql)
# Returns Select node with enhanced identifier and literal types
```

## Usage Examples

### Building AST Programmatically

```python
from lib.types import *

# Build a complex query AST
select_stmt = Select(
    select_list=[
        SelectColumn(
            expression=QuotedIdentifier('user-id'),
            alias='id'
        ),
        SelectColumn(
            expression=UnquotedIdentifier('name')
        ),
        SelectColumn(
            expression=DateLiteral('2023-01-01'),
            alias='created_date'
        )
    ],
    from_clause=TableRef(
        table=TableName(
            project='my-project',
            dataset='analytics',
            table='users',
            supports_dashes=True
        )
    ),
    where_clause=WhereClause(
        condition=BinaryOp(
            left=UnquotedIdentifier('active'),
            operator='=',
            right=BooleanLiteral(True)
        )
    )
)
```

### Analyzing Parsed SQL

```python
class IdentifierAnalyzer(ASTVisitor):
    def __init__(self):
        self.identifiers = []
        self.tables = []
        self.parameters = []
    
    def visit_unquoted_identifier(self, node):
        self.identifiers.append(('unquoted', node.name))
    
    def visit_quoted_identifier(self, node):
        self.identifiers.append(('quoted', node.name))
    
    def visit_table_name(self, node):
        if node.project:
            self.tables.append(f"{node.project}.{node.dataset}.{node.table}")
        else:
            self.tables.append(node.table)
    
    def visit_named_parameter(self, node):
        self.parameters.append(('named', node.name))
    
    # ... implement other abstract methods
    
analyzer = IdentifierAnalyzer()
ast.accept(analyzer)

print(f"Found {len(analyzer.identifiers)} identifiers")
print(f"Found {len(analyzer.tables)} tables")  
print(f"Found {len(analyzer.parameters)} parameters")
```

## Migration from Basic Types

Existing code using basic identifier and literal types will continue to work. The enhanced types provide additional functionality:

```python
# Old way (still works)
old_id = Identifier('column_name')

# New way (more specific)
new_id = UnquotedIdentifier('column_name')

# Enhanced functionality
quoted_id = QuotedIdentifier('column with spaces')
table = TableName(project='proj', dataset='ds', table='tbl')
raw_string = StringLiteral(r'regex\d+', is_raw=True)
```

The enhanced types are backward compatible while providing the full expressiveness needed for BigQuery SQL analysis and generation.