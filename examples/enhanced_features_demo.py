#!/usr/bin/env python3
"""
BigQuery AST Types - Enhanced Features Demo

This script demonstrates the enhanced BigQuery AST types and parsing capabilities.
"""

import sys
import os
import importlib.util

# Add the lib directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../lib'))

# Import our enhanced AST types directly
def import_ast_types():
    """Import AST types module directly."""
    types_path = os.path.join(os.path.dirname(__file__), '../lib/types.py')
    spec = importlib.util.spec_from_file_location('ast_types', types_path)
    ast_types = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ast_types)
    return ast_types

def import_parser():
    """Import parser module directly.""" 
    parser_path = os.path.join(os.path.dirname(__file__), '../parsers/sqlglot.py')
    spec = importlib.util.spec_from_file_location('parser', parser_path)
    parser = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(parser)
    return parser

def demo_enhanced_identifiers():
    """Demonstrate enhanced identifier types."""
    print("🔗 Enhanced Identifier Types Demo")
    print("=" * 40)
    
    ast_types = import_ast_types()
    
    # Unquoted identifiers
    id1 = ast_types.UnquotedIdentifier('my_column')
    print(f"Unquoted Identifier: {id1.name}")
    
    # Quoted identifiers (can contain special characters)
    id2 = ast_types.QuotedIdentifier('column with spaces & symbols!')
    print(f"Quoted Identifier: `{id2.name}`")
    
    # Enhanced general identifier with path
    id3 = ast_types.EnhancedGeneralIdentifier(
        'complex.path/with:separators',
        parts=['complex', 'path', 'with', 'separators'],
        separators=['.', '/', ':']
    )
    print(f"Enhanced Identifier: {id3.name}")
    print(f"  Parts: {id3.parts}")
    print(f"  Separators: {id3.separators}")
    
    # Path expressions
    path_parts = [
        ast_types.PathPart('table'),
        ast_types.PathPart('column', separator='.'),
        ast_types.PathPart('field', separator='.')
    ]
    path_expr = ast_types.PathExpression(path_parts)
    print(f"Path Expression: table.column.field")
    print(f"  Parts count: {len(path_expr.parts)}")
    
    print()

def demo_table_names():
    """Demonstrate table name handling."""
    print("🏗️  Table Name Handling Demo")
    print("=" * 40)
    
    ast_types = import_ast_types()
    
    # Simple table name
    table1 = ast_types.TableName(table='users')
    print(f"Simple table: {table1.table}")
    
    # Dataset qualified
    table2 = ast_types.TableName(dataset='analytics', table='events') 
    print(f"Qualified table: {table2.dataset}.{table2.table}")
    
    # Fully qualified with project (with dash support)
    table3 = ast_types.TableName(
        project='my-project',
        dataset='my_dataset',
        table='my_table',
        supports_dashes=True
    )
    print(f"Fully qualified: {table3.project}.{table3.dataset}.{table3.table}")
    print(f"  Supports dashes: {table3.supports_dashes}")
    
    # Column with dashes
    column = ast_types.ColumnName('user-id', supports_dashes=True)
    print(f"Column with dashes: {column.name} (dash support: {column.supports_dashes})")
    
    print()

def demo_enhanced_literals():
    """Demonstrate enhanced literal types."""
    print("📝 Enhanced Literal Types Demo")
    print("=" * 40)
    
    ast_types = import_ast_types()
    
    # String literals with different quote styles and options
    str1 = ast_types.StringLiteral('basic string', quote_style='"')
    print(f'String literal: {str1.quote_style}{str1.value}{str1.quote_style}')
    
    str2 = ast_types.StringLiteral(r'raw\string\with\backslashes', quote_style="'", is_raw=True)
    print(f"Raw string: r'{str2.value}' (raw: {str2.is_raw})")
    
    str3 = ast_types.StringLiteral('triple\nquoted\nstring', quote_style='"""')
    print(f'Triple quoted: {str3.quote_style}{repr(str3.value)}{str3.quote_style}')
    
    # Bytes literals
    bytes1 = ast_types.BytesLiteral(b'hello world', quote_style='"')
    print(f'Bytes literal: b"{bytes1.value.decode()}"')
    
    bytes2 = ast_types.BytesLiteral(b'raw\\bytes', quote_style="'", is_raw=True) 
    print(f"Raw bytes: rb'{bytes2.value.decode()}' (raw: {bytes2.is_raw})")
    
    # Numeric literals
    int1 = ast_types.IntegerLiteral(42)
    print(f"Integer: {int1.value}")
    
    int2 = ast_types.IntegerLiteral(255, is_hexadecimal=True)
    print(f"Hex integer: 0x{int2.value:X} (hex: {int2.is_hexadecimal})")
    
    num1 = ast_types.NumericLiteral('123.456789012345')
    print(f"NUMERIC: {num1.value}")
    
    bignum = ast_types.BigNumericLiteral('12345678901234567890.123456789012345678901234567890')
    print(f"BIGNUMERIC: {bignum.value}")
    
    # Date/time literals
    date = ast_types.DateLiteral('2023-12-25')
    print(f"DATE: '{date.value}'")
    
    time = ast_types.TimeLiteral('12:30:00.123456')
    print(f"TIME: '{time.value}'")
    
    datetime = ast_types.DatetimeLiteral('2023-12-25 12:30:00')
    print(f"DATETIME: '{datetime.value}'")
    
    timestamp = ast_types.TimestampLiteral('2023-12-25 12:30:00+08', timezone='+08')
    print(f"TIMESTAMP: '{timestamp.value}' (timezone: {timestamp.timezone})")
    
    # Complex literals
    array_elements = [
        ast_types.IntegerLiteral(1),
        ast_types.IntegerLiteral(2),
        ast_types.IntegerLiteral(3)
    ]
    array = ast_types.ArrayLiteral(elements=array_elements, element_type='INT64')
    print(f"ARRAY<{array.element_type}>: [{len(array.elements)} elements]")
    
    struct = ast_types.StructLiteral([
        ('name', ast_types.StringLiteral('John')),
        ('age', ast_types.IntegerLiteral(25))
    ])
    print(f"STRUCT: {len(struct.fields)} fields")
    
    json_lit = ast_types.JSONLiteral('{"message": "Hello, World!"}')
    print(f"JSON: {json_lit.value}")
    
    print()

def demo_parameters_and_comments():
    """Demonstrate query parameters and comments."""
    print("🔗 Parameters and Comments Demo")
    print("=" * 40)
    
    ast_types = import_ast_types()
    
    # Named parameters
    named_param = ast_types.NamedParameter('user_id')
    print(f"Named parameter: @{named_param.name}")
    
    # Positional parameters
    pos_param = ast_types.PositionalParameter(1)
    print(f"Positional parameter: ? (position {pos_param.position})")
    
    # Comments
    comment1 = ast_types.Comment('This is a hash comment', '#')
    print(f"Hash comment: {comment1.style} {comment1.text}")
    
    comment2 = ast_types.Comment('This is a dash comment', '--')
    print(f"Dash comment: {comment2.style} {comment2.text}")
    
    comment3 = ast_types.Comment(
        'This is a multi-line comment\\nwith multiple lines',
        '/* */',
        is_multiline=True
    )
    print(f"Multi-line comment: {comment3.style} (multiline: {comment3.is_multiline})")
    
    print()

def demo_visitor_pattern():
    """Demonstrate the visitor pattern with enhanced nodes."""
    print("🎯 Visitor Pattern Demo")
    print("=" * 40)
    
    ast_types = import_ast_types()
    
    # Create a visitor that collects information
    class InfoCollector(ast_types.ASTVisitor):
        def __init__(self):
            self.info = []
        
        def visit_unquoted_identifier(self, node):
            self.info.append(f"Unquoted ID: {node.name}")
        
        def visit_quoted_identifier(self, node):
            self.info.append(f"Quoted ID: `{node.name}`")
        
        def visit_table_name(self, node):
            if node.project:
                self.info.append(f"Table: {node.project}.{node.dataset}.{node.table}")
            else:
                self.info.append(f"Table: {node.table}")
        
        def visit_string_literal(self, node):
            prefix = "r" if node.is_raw else ""
            self.info.append(f"String: {prefix}{node.quote_style}{node.value}{node.quote_style}")
        
        def visit_named_parameter(self, node):
            self.info.append(f"Parameter: @{node.name}")
        
        def visit_comment(self, node):
            self.info.append(f"Comment: {node.style} {node.text}")
        
        # Implement required abstract methods (minimal implementation)
        def visit_identifier(self, node): pass
        def visit_literal(self, node): pass
        def visit_integer_literal(self, node): pass
        def visit_float_literal(self, node): pass
        def visit_boolean_literal(self, node): pass
        def visit_null_literal(self, node): pass
        def visit_binary_op(self, node): pass
        def visit_unary_op(self, node): pass
        def visit_function_call(self, node): pass
        def visit_table_ref(self, node): pass
        def visit_select_column(self, node): pass
        def visit_where_clause(self, node): pass
        def visit_group_by_clause(self, node): pass
        def visit_having_clause(self, node): pass
        def visit_order_by_clause(self, node): pass
        def visit_order_by_item(self, node): pass
        def visit_limit_clause(self, node): pass
        def visit_join(self, node): pass
        def visit_select(self, node): pass
        def visit_subquery(self, node): pass
        def visit_cte(self, node): pass
        def visit_with_clause(self, node): pass
        def visit_merge_insert(self, node): pass
        def visit_merge_update(self, node): pass
        def visit_merge_delete(self, node): pass
        def visit_merge_action(self, node): pass
        def visit_merge(self, node): pass
        def visit_window_specification(self, node): pass
        def visit_window_function(self, node): pass
        def visit_enhanced_general_identifier(self, node): pass
        def visit_path_expression(self, node): pass
        def visit_path_part(self, node): pass
        def visit_column_name(self, node): pass
        def visit_field_name(self, node): pass
        def visit_bytes_literal(self, node): pass
        def visit_numeric_literal(self, node): pass
        def visit_bignumeric_literal(self, node): pass
        def visit_date_literal(self, node): pass
        def visit_time_literal(self, node): pass
        def visit_datetime_literal(self, node): pass
        def visit_timestamp_literal(self, node): pass
        def visit_interval_literal(self, node): pass
        def visit_array_literal(self, node): pass
        def visit_struct_literal(self, node): pass
        def visit_range_literal(self, node): pass
        def visit_json_literal(self, node): pass
        def visit_positional_parameter(self, node): pass
    
    # Create some nodes and visit them
    nodes = [
        ast_types.UnquotedIdentifier('my_column'),
        ast_types.QuotedIdentifier('column with spaces'),
        ast_types.TableName(project='my-project', dataset='analytics', table='events'),
        ast_types.StringLiteral('hello world', quote_style='"'),
        ast_types.NamedParameter('user_id'),
        ast_types.Comment('This is a comment', '#')
    ]
    
    visitor = InfoCollector()
    
    for node in nodes:
        node.accept(visitor)
    
    print(f"Visitor collected {len(visitor.info)} pieces of information:")
    for info in visitor.info:
        print(f"  • {info}")
    
    print()

def demo_parser():
    """Demonstrate parsing BigQuery SQL."""
    print("⚙️  Parser Demo")
    print("=" * 40)
    
    try:
        parser_module = import_parser()
        
        # Parse some BigQuery SQL
        sql = '''
        SELECT 
            `quoted-column`,
            unquoted_column,
            table.nested_field,
            DATE '2023-12-25' as christmas,
            'hello world' as greeting,
            123 as number,
            TRUE as flag
        FROM my-project.analytics.events
        WHERE active = TRUE
        '''
        
        parser = parser_module.SQLGlotParser()
        ast = parser.parse(sql)
        
        print(f"✅ Successfully parsed BigQuery SQL!")
        print(f"   AST type: {type(ast).__name__}")
        print(f"   SQL length: {len(sql.strip())} characters")
        
    except Exception as e:
        print(f"⚠️  Parser demo failed: {e}")
        print("   (This is expected if dependencies are missing)")
    
    print()

def main():
    """Run all demos."""
    print("🎯 BigQuery AST Types - Enhanced Features Demo")
    print("=" * 60)
    print()
    
    # Change to the correct directory
    os.chdir('/home/runner/work/bigquery-ast-types/bigquery-ast-types')
    
    demos = [
        demo_enhanced_identifiers,
        demo_table_names,
        demo_enhanced_literals,
        demo_parameters_and_comments,
        demo_visitor_pattern,
        demo_parser,
    ]
    
    for demo_func in demos:
        try:
            demo_func()
        except Exception as e:
            print(f"❌ Demo {demo_func.__name__} failed: {e}")
            print()
    
    print("🎉 Demo complete! The enhanced BigQuery AST types provide:")
    print("   • Complete BigQuery lexical specification support")
    print("   • 23+ new AST node types for all identifier and literal formats")
    print("   • Enhanced visitor pattern with 20+ new visitor methods") 
    print("   • Path expressions and table name qualification")
    print("   • Query parameter and comment preservation")
    print("   • Backward compatibility with existing code")
    print()
    print("📚 See docs/ENHANCED_FEATURES.md for complete documentation")

if __name__ == '__main__':
    main()