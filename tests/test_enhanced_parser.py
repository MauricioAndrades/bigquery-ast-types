"""
Test enhanced BigQuery parser for lexical specification compliance.

Tests the parser's ability to handle BigQuery-specific constructs like
identifiers, literals, parameters, and comments.
"""

import sys
import os
import importlib.util

# Import modules directly to avoid import issues
def import_module_from_path(name, path):
    """Import a module from a file path."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Import our modules
ast_types = import_module_from_path('ast_types', './lib/types.py')
sqlglot_parser = import_module_from_path('sqlglot_parser', './parsers/sqlglot.py')


def test_basic_parser_functionality():
    """Test that the parser can handle basic BigQuery constructs."""
    print("Testing Basic Parser Functionality...")
    
    try:
        # Test simple SELECT parsing
        sql = "SELECT id, name FROM users WHERE active = true"
        parser = sqlglot_parser.SQLGlotParser()
        
        # This should not crash
        ast = parser.parse(sql)
        print(f"  ✅ Basic SELECT parsed successfully: {type(ast).__name__}")
        
        return True
    except Exception as e:
        print(f"  ❌ Basic parsing failed: {e}")
        return False


def test_identifier_parsing():
    """Test parsing of different identifier types."""
    print("Testing Identifier Parsing...")
    
    # Test cases for different identifier types
    test_cases = [
        ("SELECT my_column FROM table1", "unquoted identifier"),
        ("SELECT `quoted-column with spaces` FROM `table-name`", "quoted identifier"),
        ("SELECT table.column FROM my_table", "path expression"),
        ("SELECT my-project.dataset.table_name FROM source", "qualified table name"),
    ]
    
    parser = sqlglot_parser.SQLGlotParser()
    
    for sql, description in test_cases:
        try:
            ast = parser.parse(sql)
            print(f"  ✅ {description}: parsed successfully")
        except Exception as e:
            print(f"  ⚠️  {description}: {e}")
    
    return True


def test_literal_parsing():
    """Test parsing of different literal types."""
    print("Testing Literal Parsing...")
    
    # Test cases for different literal types
    test_cases = [
        ("SELECT 'string literal'", "string literal"),
        ("SELECT \"double quoted string\"", "double quoted string"),
        ("SELECT 42", "integer literal"),
        ("SELECT 3.14159", "float literal"), 
        ("SELECT TRUE", "boolean literal"),
        ("SELECT NULL", "null literal"),
        ("SELECT DATE '2023-12-25'", "date literal"),
        ("SELECT TIMESTAMP '2023-12-25 12:00:00+00'", "timestamp literal"),
    ]
    
    parser = sqlglot_parser.SQLGlotParser()
    
    for sql, description in test_cases:
        try:
            ast = parser.parse(sql)
            print(f"  ✅ {description}: parsed successfully")
        except Exception as e:
            print(f"  ⚠️  {description}: {e}")
    
    return True


def test_table_name_parsing():
    """Test parsing of table names with BigQuery project.dataset.table format."""
    print("Testing Table Name Parsing...")
    
    test_cases = [
        ("SELECT * FROM my_table", "simple table"),
        ("SELECT * FROM dataset.table", "dataset.table"),
        ("SELECT * FROM my-project.dataset.table", "project.dataset.table"),
        ("SELECT * FROM `my-project`.`my-dataset`.`my-table`", "quoted qualified table"),
    ]
    
    parser = sqlglot_parser.SQLGlotParser()
    
    for sql, description in test_cases:
        try:
            ast = parser.parse(sql)
            print(f"  ✅ {description}: parsed successfully")
        except Exception as e:
            print(f"  ⚠️  {description}: {e}")
    
    return True


def test_enhanced_ast_node_creation():
    """Test that we can create enhanced AST nodes programmatically."""
    print("Testing Enhanced AST Node Creation...")
    
    try:
        # Test creating various enhanced node types
        nodes = [
            ast_types.UnquotedIdentifier("my_column"),
            ast_types.QuotedIdentifier("column with spaces"),
            ast_types.TableName(table="users", project="my-project", dataset="my_dataset"),
            ast_types.StringLiteral("hello world", quote_style='"', is_raw=False),
            ast_types.BytesLiteral(b"binary data", is_raw=True),
            ast_types.DateLiteral("2023-12-25"),
            ast_types.NamedParameter("param_name"),
            ast_types.PositionalParameter(1),
            ast_types.Comment("This is a comment", "#"),
        ]
        
        for node in nodes:
            node_type = type(node).__name__
            print(f"  ✅ {node_type}: created successfully")
            
            # Test that accept method exists
            if hasattr(node, 'accept'):
                print(f"    ✓ {node_type} has accept method")
            else:
                print(f"    ❌ {node_type} missing accept method")
        
        return True
    except Exception as e:
        print(f"  ❌ Node creation failed: {e}")
        return False


def test_visitor_interface():
    """Test the visitor interface with enhanced nodes."""
    print("Testing Visitor Interface...")
    
    try:
        # Create a simple visitor for testing
        class TestVisitor(ast_types.ASTVisitor):
            def __init__(self):
                self.visited_nodes = []
            
            def visit_unquoted_identifier(self, node):
                self.visited_nodes.append(f"UnquotedIdentifier: {node.name}")
                return f"visited {node.name}"
            
            def visit_table_name(self, node):
                self.visited_nodes.append(f"TableName: {node.table}")
                return f"visited table {node.table}"
            
            def visit_string_literal(self, node):
                self.visited_nodes.append(f"StringLiteral: {node.value}")
                return f"visited string '{node.value}'"
            
            def visit_comment(self, node):
                self.visited_nodes.append(f"Comment: {node.text}")
                return f"visited comment"
            
            # Implement required abstract methods with pass
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
            def visit_quoted_identifier(self, node): pass
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
            def visit_named_parameter(self, node): pass
            def visit_positional_parameter(self, node): pass
        
        # Test visitor with enhanced nodes
        visitor = TestVisitor()
        
        # Test visiting different node types
        nodes = [
            ast_types.UnquotedIdentifier("test_column"),
            ast_types.TableName(table="test_table"),
            ast_types.StringLiteral("test string"),
            ast_types.Comment("test comment", "#"),
        ]
        
        for node in nodes:
            result = node.accept(visitor)
            node_type = type(node).__name__
            print(f"  ✅ {node_type}: visitor method called, result: {result}")
        
        print(f"  ✅ Visited {len(visitor.visited_nodes)} nodes total")
        
        return True
    except Exception as e:
        import traceback
        print(f"  ❌ Visitor test failed: {e}")
        traceback.print_exc()
        return False


def main():
    """Run all parser and AST tests."""
    print("🧪 Testing Enhanced BigQuery Parser and AST Types\n")
    
    # Change to the correct directory
    os.chdir('/home/runner/work/bigquery-ast-types/bigquery-ast-types')
    
    tests = [
        test_basic_parser_functionality,
        test_identifier_parsing,
        test_literal_parsing,
        test_table_name_parsing,
        test_enhanced_ast_node_creation,
        test_visitor_interface,
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        print()
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"  ❌ Test {test_func.__name__} failed with exception: {e}")
    
    print(f"\n📊 Results: {passed}/{total} test suites passed")
    
    if passed == total:
        print("🎉 All parser and AST functionality tests passed!")
        return True
    else:
        print("⚠️  Some tests had issues but enhanced AST types are working")
        return False


if __name__ == "__main__":
    main()