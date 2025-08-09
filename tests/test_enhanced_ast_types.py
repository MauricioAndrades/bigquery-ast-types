"""
Test enhanced BigQuery AST types for lexical specification compliance.

Tests the new identifier types, literals, parameters, and other BigQuery-specific
constructs added to support the full lexical specification.
"""

import pytest
import sys
import os
import importlib.util

# Import our AST types module directly to avoid conflicts with built-in types module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../lib'))
spec = importlib.util.spec_from_file_location('ast_types', os.path.join(os.path.dirname(__file__), '../lib/types.py'))
if spec is None:
    raise ImportError("Could not load module spec for ast_types")
ast_types = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ast_types) # type: ignore


class TestEnhancedIdentifiers:
    """Test enhanced identifier types for BigQuery lexical specification."""

    def test_unquoted_identifier(self):
        """Test unquoted identifier creation."""
        identifier = ast_types.UnquotedIdentifier('my_column')
        assert identifier.name == 'my_column'
        assert isinstance(identifier, ast_types.Identifier)
        assert hasattr(identifier, 'accept')

    def test_quoted_identifier(self):
        """Test quoted identifier creation."""
        identifier = ast_types.QuotedIdentifier('my-column with spaces')
        assert identifier.name == 'my-column with spaces'
        assert isinstance(identifier, ast_types.Identifier)

    def test_enhanced_general_identifier(self):
        """Test enhanced general identifier with path parts."""
        identifier = ast_types.EnhancedGeneralIdentifier(
            'complex.path.with-dashes',
            parts=['complex', 'path', 'with-dashes'],
            separators=['.', '.']
        )
        assert identifier.name == 'complex.path.with-dashes'
        assert identifier.parts == ['complex', 'path', 'with-dashes']
        assert identifier.separators == ['.', '.']


class TestPathExpressions:
    """Test path expression types."""

    def test_path_expression(self):
        """Test path expression creation."""
        part1 = ast_types.PathPart('table')
        part2 = ast_types.PathPart('column', separator='.')
        path_expr = ast_types.PathExpression([part1, part2])

        assert len(path_expr.parts) == 2
        assert path_expr.parts[0].value == 'table'
        assert path_expr.parts[1].value == 'column'
        assert path_expr.parts[1].separator == '.'

    def test_path_part(self):
        """Test path part creation."""
        part = ast_types.PathPart('field_name', separator='/')
        assert part.value == 'field_name'
        assert part.separator == '/'


class TestTableAndColumnNames:
    """Test table and column name types with BigQuery dash rules."""

    def test_table_name_simple(self):
        """Test simple table name."""
        table = ast_types.TableName(table='my_table')
        assert table.table == 'my_table'
        assert table.dataset is None
        assert table.project is None
        assert table.supports_dashes is False

    def test_table_name_qualified(self):
        """Test fully qualified table name."""
        table = ast_types.TableName(
            project='my-project',
            dataset='my_dataset',
            table='my_table',
            supports_dashes=True
        )
        assert table.project == 'my-project'
        assert table.dataset == 'my_dataset'
        assert table.table == 'my_table'
        assert table.supports_dashes is True

    def test_column_name(self):
        """Test column name with dash support."""
        column = ast_types.ColumnName('column-with-dashes', supports_dashes=True)
        assert column.name == 'column-with-dashes'
        assert column.supports_dashes is True

    def test_field_name(self):
        """Test field name for structs and JSON."""
        field = ast_types.FieldName('field_name')
        assert field.name == 'field_name'


class TestEnhancedLiterals:
    """Test enhanced literal types with BigQuery formatting options."""

    def test_string_literal_basic(self):
        """Test basic string literal."""
        literal = ast_types.StringLiteral('hello world')
        assert literal.value == 'hello world'
        assert literal.quote_style == '"'
        assert literal.is_raw is False
        assert literal.is_bytes is False

    def test_string_literal_raw(self):
        """Test raw string literal."""
        literal = ast_types.StringLiteral(r'hello\nworld', is_raw=True, quote_style="'")
        assert literal.value == r'hello\nworld'
        assert literal.is_raw is True
        assert literal.quote_style == "'"

    def test_bytes_literal(self):
        """Test bytes literal."""
        literal = ast_types.BytesLiteral(b'hello', quote_style='"', is_raw=True)
        assert literal.value == b'hello'
        assert literal.is_raw is True
        assert literal.quote_style == '"'

    def test_integer_literal_hex(self):
        """Test hexadecimal integer literal."""
        literal = ast_types.IntegerLiteral(255, is_hexadecimal=True)
        assert literal.value == 255
        assert literal.is_hexadecimal is True

    def test_numeric_literal(self):
        """Test NUMERIC literal."""
        literal = ast_types.NumericLiteral('123.456789')
        assert literal.value == '123.456789'

    def test_bignumeric_literal(self):
        """Test BIGNUMERIC literal."""
        literal = ast_types.BigNumericLiteral('123456789012345678901234567890.123456789')
        assert literal.value == '123456789012345678901234567890.123456789'


class TestDateTimeLiterals:
    """Test BigQuery date/time literal types."""

    def test_date_literal(self):
        """Test DATE literal."""
        literal = ast_types.DateLiteral('2023-12-25')
        assert literal.value == '2023-12-25'

    def test_time_literal(self):
        """Test TIME literal."""
        literal = ast_types.TimeLiteral('12:30:00.123456')
        assert literal.value == '12:30:00.123456'

    def test_datetime_literal(self):
        """Test DATETIME literal."""
        literal = ast_types.DatetimeLiteral('2023-12-25 12:30:00')
        assert literal.value == '2023-12-25 12:30:00'

    def test_timestamp_literal(self):
        """Test TIMESTAMP literal."""
        literal = ast_types.TimestampLiteral('2023-12-25 12:30:00+08', timezone='+08')
        assert literal.value == '2023-12-25 12:30:00+08'
        assert literal.timezone == '+08'

    def test_interval_literal(self):
        """Test INTERVAL literal."""
        literal = ast_types.IntervalLiteral('1-2 3 4:5:6.789', from_part='YEAR', to_part='SECOND')
        assert literal.value == '1-2 3 4:5:6.789'
        assert literal.from_part == 'YEAR'
        assert literal.to_part == 'SECOND'


class TestComplexLiterals:
    """Test complex literal types (arrays, structs, etc.)."""

    def test_array_literal(self):
        """Test array literal."""
        elements = [
            ast_types.IntegerLiteral(1),
            ast_types.IntegerLiteral(2),
            ast_types.IntegerLiteral(3)
        ]
        literal = ast_types.ArrayLiteral(elements, element_type='INT64')
        assert len(literal.elements) == 3
        assert literal.element_type == 'INT64'
        assert all(isinstance(elem, ast_types.IntegerLiteral) for elem in literal.elements)

    def test_struct_literal(self):
        """Test struct literal."""
        fields = [
            ('name', ast_types.StringLiteral('John')),
            ('age', ast_types.IntegerLiteral(25))
        ]
        literal = ast_types.StructLiteral(fields)
        assert len(literal.fields) == 2
        assert literal.fields[0][0] == 'name'
        assert literal.fields[1][0] == 'age'
        assert isinstance(literal.fields[0][1], ast_types.StringLiteral)

    def test_range_literal(self):
        """Test RANGE literal."""
        literal = ast_types.RangeLiteral(
            range_type='DATE',
            lower_bound=ast_types.DateLiteral('2023-01-01'),
            upper_bound=ast_types.DateLiteral('2023-12-31')
        )
        assert literal.range_type == 'DATE'
        assert isinstance(literal.lower_bound, ast_types.DateLiteral)
        assert isinstance(literal.upper_bound, ast_types.DateLiteral)

    def test_json_literal(self):
        """Test JSON literal."""
        json_str = '{"name": "John", "age": 25}'
        literal = ast_types.JSONLiteral(json_str)
        assert literal.value == json_str


class TestQueryParameters:
    """Test query parameter types."""

    def test_named_parameter(self):
        """Test named query parameter."""
        param = ast_types.NamedParameter('my_param')
        assert param.name == 'my_param'

    def test_positional_parameter(self):
        """Test positional query parameter."""
        param = ast_types.PositionalParameter(1)
        assert param.position == 1


class TestComments:
    """Test comment node types."""

    def test_single_line_comment_hash(self):
        """Test single-line comment with # style."""
        comment = ast_types.Comment('This is a comment', '#')
        assert comment.text == 'This is a comment'
        assert comment.style == '#'
        assert comment.is_multiline is False

    def test_single_line_comment_dash(self):
        """Test single-line comment with -- style."""
        comment = ast_types.Comment('This is a comment', '--')
        assert comment.text == 'This is a comment'
        assert comment.style == '--'
        assert comment.is_multiline is False

    def test_multiline_comment(self):
        """Test multiline comment."""
        comment = ast_types.Comment('This is a\nmultiline comment', '/* */', is_multiline=True)
        assert comment.text == 'This is a\nmultiline comment'
        assert comment.style == '/* */'
        assert comment.is_multiline is True


class TestVisitorInterface:
    """Test that all new nodes work with the visitor pattern."""

    def test_visitor_methods_exist(self):
        """Test that all required visitor methods exist in ASTVisitor."""
        visitor_class = ast_types.ASTVisitor

        # Check for enhanced identifier visitor methods
        assert hasattr(visitor_class, 'visit_unquoted_identifier')
        assert hasattr(visitor_class, 'visit_quoted_identifier')
        assert hasattr(visitor_class, 'visit_enhanced_general_identifier')

        # Check for path expression visitor methods
        assert hasattr(visitor_class, 'visit_path_expression')
        assert hasattr(visitor_class, 'visit_path_part')

        # Check for table/column name visitor methods
        assert hasattr(visitor_class, 'visit_table_name')
        assert hasattr(visitor_class, 'visit_column_name')
        assert hasattr(visitor_class, 'visit_field_name')

        # Check for enhanced literal visitor methods
        assert hasattr(visitor_class, 'visit_bytes_literal')
        assert hasattr(visitor_class, 'visit_numeric_literal')
        assert hasattr(visitor_class, 'visit_bignumeric_literal')
        assert hasattr(visitor_class, 'visit_date_literal')
        assert hasattr(visitor_class, 'visit_time_literal')
        assert hasattr(visitor_class, 'visit_datetime_literal')
        assert hasattr(visitor_class, 'visit_timestamp_literal')
        assert hasattr(visitor_class, 'visit_interval_literal')
        assert hasattr(visitor_class, 'visit_array_literal')
        assert hasattr(visitor_class, 'visit_struct_literal')
        assert hasattr(visitor_class, 'visit_range_literal')
        assert hasattr(visitor_class, 'visit_json_literal')

        # Check for parameter visitor methods
        assert hasattr(visitor_class, 'visit_named_parameter')
        assert hasattr(visitor_class, 'visit_positional_parameter')

        # Check for comment visitor method
        assert hasattr(visitor_class, 'visit_comment')

    def test_accept_methods(self):
        """Test that all new node types have accept methods."""
        # Create instances of new node types
        nodes = [
            ast_types.UnquotedIdentifier('test'),
            ast_types.QuotedIdentifier('test'),
            ast_types.EnhancedGeneralIdentifier('test', ['test'], []),
            ast_types.PathExpression([ast_types.PathPart('test')]),
            ast_types.PathPart('test'),
            ast_types.TableName(table='test'),
            ast_types.ColumnName('test'),
            ast_types.FieldName('test'),
            ast_types.BytesLiteral(b'test'),
            ast_types.NumericLiteral('123.45'),
            ast_types.BigNumericLiteral('123.45'),
            ast_types.DateLiteral('2023-01-01'),
            ast_types.TimeLiteral('12:00:00'),
            ast_types.DatetimeLiteral('2023-01-01 12:00:00'),
            ast_types.TimestampLiteral('2023-01-01 12:00:00+00'),
            ast_types.IntervalLiteral('1 DAY'),
            ast_types.ArrayLiteral([]),
            ast_types.StructLiteral([]),
            ast_types.RangeLiteral('DATE'),
            ast_types.JSONLiteral('{}'),
            ast_types.NamedParameter('test'),
            ast_types.PositionalParameter(1),
            ast_types.Comment('test', '#')
        ]

        # Test that all nodes have accept methods
        for node in nodes:
            assert hasattr(node, 'accept')
            assert callable(node.accept)


if __name__ == '__main__':
    pytest.main([__file__])