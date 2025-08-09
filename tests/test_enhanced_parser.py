"""Test enhanced BigQuery parser for lexical specification compliance.

Tests the parser's ability to handle BigQuery-specific constructs like
identifiers, literals, parameters, and comments.
"""

import importlib.util
import os
import sys
import pytest


def import_module_from_path(name, path):
    """Import a module from a file path."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    ast_types = import_module_from_path("ast_types", os.path.join(os.path.dirname(__file__), "../lib/types.py"))
    sqlglot_parser = import_module_from_path("sqlglot_parser", os.path.join(os.path.dirname(__file__), "../parsers/sqlglot.py"))
except Exception as exc:  # pragma: no cover - skip if dependencies missing
    pytest.skip(f"Required modules not available: {exc}", allow_module_level=True)


def test_basic_parser_functionality():
    sql = "SELECT id, name FROM users WHERE active = true"
    parser = sqlglot_parser.SQLGlotParser()
    ast = parser.parse(sql)
    assert ast is not None


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("SELECT my_column FROM table1", id="unquoted identifier"),
        pytest.param(
            "SELECT `quoted-column with spaces` FROM `table-name`",
            id="quoted identifier",
        ),
        pytest.param("SELECT table.column FROM my_table", id="path expression"),
        pytest.param(
            "SELECT my-project.dataset.table_name FROM source",
            id="qualified table name",
        ),
    ],
)
def test_identifier_parsing(sql):
    parser = sqlglot_parser.SQLGlotParser()
    assert parser.parse(sql) is not None


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("SELECT 'string literal'", id="string literal"),
        pytest.param('SELECT "double quoted string"', id="double quoted string"),
        pytest.param("SELECT 42", id="integer literal"),
        pytest.param("SELECT 3.14159", id="float literal"),
        pytest.param("SELECT TRUE", id="boolean literal"),
        pytest.param("SELECT NULL", id="null literal"),
        pytest.param("SELECT DATE '2023-12-25'", id="date literal"),
        pytest.param(
            "SELECT TIMESTAMP '2023-12-25 12:00:00+00'",
            id="timestamp literal",
        ),
    ],
)
def test_literal_parsing(sql):
    parser = sqlglot_parser.SQLGlotParser()
    assert parser.parse(sql) is not None


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("SELECT * FROM my_table", id="simple table"),
        pytest.param("SELECT * FROM dataset.table", id="dataset.table"),
        pytest.param(
            "SELECT * FROM my-project.dataset.table",
            id="project.dataset.table",
        ),
        pytest.param(
            "SELECT * FROM `my-project`.`my-dataset`.`my-table`",
            id="quoted qualified table",
        ),
    ],
)
def test_table_name_parsing(sql):
    parser = sqlglot_parser.SQLGlotParser()
    assert parser.parse(sql) is not None


def test_enhanced_ast_node_creation():
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
        assert hasattr(node, "accept")


def test_visitor_interface():
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
            return "visited comment"

        # Implement required abstract methods with pass
        def visit_identifier(self, node):
            pass

        def visit_literal(self, node):
            pass

        def visit_integer_literal(self, node):
            pass

        def visit_float_literal(self, node):
            pass

        def visit_boolean_literal(self, node):
            pass

        def visit_null_literal(self, node):
            pass

        def visit_binary_op(self, node):
            pass

        def visit_unary_op(self, node):
            pass

        def visit_function_call(self, node):
            pass

        def visit_table_ref(self, node):
            pass

        def visit_select_column(self, node):
            pass

        def visit_where_clause(self, node):
            pass

        def visit_group_by_clause(self, node):
            pass

        def visit_having_clause(self, node):
            pass

        def visit_order_by_clause(self, node):
            pass

        def visit_order_by_item(self, node):
            pass

        def visit_limit_clause(self, node):
            pass

        def visit_join(self, node):
            pass

        def visit_select(self, node):
            pass

        def visit_subquery(self, node):
            pass

        def visit_cte(self, node):
            pass

        def visit_with_clause(self, node):
            pass

        def visit_merge_insert(self, node):
            pass

        def visit_merge_update(self, node):
            pass

        def visit_merge_delete(self, node):
            pass

        def visit_merge_action(self, node):
            pass

        def visit_merge(self, node):
            pass

        def visit_window_specification(self, node):
            pass

        def visit_window_function(self, node):
            pass

        def visit_quoted_identifier(self, node):
            pass

        def visit_enhanced_general_identifier(self, node):
            pass

        def visit_path_expression(self, node):
            pass

        def visit_path_part(self, node):
            pass

        def visit_column_name(self, node):
            pass

        def visit_field_name(self, node):
            pass

        def visit_bytes_literal(self, node):
            pass

        def visit_numeric_literal(self, node):
            pass

        def visit_bignumeric_literal(self, node):
            pass

        def visit_date_literal(self, node):
            pass

        def visit_time_literal(self, node):
            pass

        def visit_datetime_literal(self, node):
            pass

        def visit_timestamp_literal(self, node):
            pass

        def visit_interval_literal(self, node):
            pass

        def visit_array_literal(self, node):
            pass

        def visit_struct_literal(self, node):
            pass

        def visit_range_literal(self, node):
            pass

        def visit_json_literal(self, node):
            pass

        def visit_named_parameter(self, node):
            pass

        def visit_positional_parameter(self, node):
            pass

<<<<<<< HEAD
        # Missing abstract methods identified in the error
        def visit_case(self, node):
            pass

        def visit_cast(self, node):
            pass

        def visit_create_table(self, node):
            pass

        def visit_insert(self, node):
            pass

        def visit_pivot(self, node):
            pass

        def visit_qualify_clause(self, node):
            pass

        def visit_set_operation(self, node):
            pass

        def visit_star(self, node):
            pass

        def visit_tablesample(self, node):
            pass

        def visit_unnest(self, node):
            pass

        def visit_unpivot(self, node):
            pass

        def visit_update(self, node):
            pass

        def visit_when_clause(self, node):
            pass

        # Missing DDL and scripting visitor methods
        def visit_add_column(self, node):
            pass

        def visit_alter_column(self, node):
            pass

        def visit_alter_table(self, node):
            pass

        def visit_begin_end_block(self, node):
            pass

        def visit_break_statement(self, node):
            pass

        def visit_continue_statement(self, node):
            pass

        def visit_create_function(self, node):
            pass

        def visit_create_view(self, node):
            pass

        def visit_declare_statement(self, node):
            pass

        def visit_drop_column(self, node):
            pass

        def visit_drop_statement(self, node):
            pass

        def visit_elseif_clause(self, node):
            pass

        def visit_exception_handler(self, node):
            pass

        def visit_for_loop(self, node):
            pass

        def visit_if_statement(self, node):
            pass

        def visit_rename_column(self, node):
            pass

        def visit_set_statement(self, node):
            pass

        def visit_set_table_options(self, node):
            pass

        def visit_variable_declaration(self, node):
            pass

        def visit_while_loop(self, node):
            pass

        def visit_call_statement(self, node):
=======
        def visit_case(self, node):
            pass

        def visit_when_clause(self, node):
            pass

        def visit_insert(self, node):
            pass

        def visit_update(self, node):
            pass

        def visit_create_table(self, node):
            pass

        def visit_set_operation(self, node):
>>>>>>> main
            pass

    visitor = TestVisitor()
    nodes = [
        ast_types.UnquotedIdentifier("test_column"),
        ast_types.TableName(table="test_table"),
        ast_types.StringLiteral("test string"),
        ast_types.Comment("test comment", "#"),
    ]

    for node in nodes:
        node.accept(visitor)

    assert len(visitor.visited_nodes) == 4

