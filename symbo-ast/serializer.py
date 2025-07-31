"""
BigQuery SQL Serializer

Converts AST nodes back to properly formatted BigQuery SQL.
Handles indentation, identifier quoting, and BigQuery-specific syntax.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

from typing import Any, List, Optional, Set
from dataclasses import dataclass
from .ast_types import (
    ASTNode, ASTVisitor,
    Identifier, StringLiteral, IntegerLiteral, FloatLiteral,
    BooleanLiteral, NullLiteral, DateLiteral, TimeLiteral,
    DatetimeLiteral, TimestampLiteral, ArrayLiteral, StructLiteral,
    ColumnRef, StarExpression, BinaryOp, UnaryOp, FunctionCall,
    CastExpression, CaseExpression, WhenClause, WindowFunction,
    SelectStatement, SelectItem, FromClause, TableReference,
    JoinExpression, CTE, WithClause, OrderByItem, GroupByClause,
    MergeStatement, MergeWhenClause, MergeInsert, MergeUpdate, MergeDelete,
    SetClause, InsertStatement, UpdateStatement, DeleteStatement
)


@dataclass
class SerializerOptions:
    """Options for SQL serialization."""
    indent: str = "  "
    max_line_length: int = 80
    uppercase_keywords: bool = True
    quote_identifiers: bool = True
    trailing_commas: bool = True
    format_style: str = "expanded"  # 'compact' or 'expanded'


class SQLSerializer(ASTVisitor):
    """
    Serializes AST nodes to BigQuery SQL with proper formatting.
    """

    def __init__(self, options: Optional[SerializerOptions] = None):
        self.options = options or SerializerOptions()
        self.depth = 0
        self.buffer: List[str] = []
        self.reserved_keywords = self._load_reserved_keywords()

    def _load_reserved_keywords(self) -> Set[str]:
        """Load BigQuery reserved keywords."""
        # From BigQueryLexicalIdentifiers.md
        return {
            'ALL', 'AND', 'ANY', 'ARRAY', 'AS', 'ASC', 'ASSERT_ROWS_MODIFIED',
            'AT', 'BETWEEN', 'BY', 'CASE', 'CAST', 'COLLATE', 'CONTAINS', 'CREATE',
            'CROSS', 'CUBE', 'CURRENT', 'DEFAULT', 'DEFINE', 'DESC', 'DISTINCT',
            'ELSE', 'END', 'ENUM', 'ESCAPE', 'EXCEPT', 'EXCLUDE', 'EXISTS',
            'EXTRACT', 'FALSE', 'FETCH', 'FOLLOWING', 'FOR', 'FROM', 'FULL',
            'GROUP', 'GROUPING', 'GROUPS', 'HASH', 'HAVING', 'IF', 'IGNORE',
            'IN', 'INNER', 'INTERSECT', 'INTERVAL', 'INTO', 'IS', 'JOIN',
            'LATERAL', 'LEFT', 'LIKE', 'LIMIT', 'LOOKUP', 'MERGE', 'NATURAL',
            'NEW', 'NO', 'NOT', 'NULL', 'NULLS', 'OF', 'ON', 'OR', 'ORDER',
            'OUTER', 'OVER', 'PARTITION', 'PRECEDING', 'PROTO', 'QUALIFY',
            'RANGE', 'RECURSIVE', 'RESPECT', 'RIGHT', 'ROLLUP', 'ROWS',
            'SELECT', 'SET', 'SOME', 'STRUCT', 'TABLESAMPLE', 'THEN', 'TO',
            'TREAT', 'TRUE', 'UNBOUNDED', 'UNION', 'UNNEST', 'USING', 'WHEN',
            'WHERE', 'WINDOW', 'WITH', 'WITHIN'
        }

    def serialize(self, node: ASTNode) -> str:
        """Serialize an AST node to SQL."""
        self.buffer = []
        self.depth = 0
        node.accept(self)
        return ''.join(self.buffer)

    def _write(self, text: str):
        """Write text to buffer."""
        self.buffer.append(text)

    def _writeln(self, text: str = ""):
        """Write text followed by newline."""
        self._write(text)
        self._write("\n")

    def _write_indent(self):
        """Write current indentation."""
        self._write(self.options.indent * self.depth)

    def _keyword(self, keyword: str) -> str:
        """Format a keyword."""
        if self.options.uppercase_keywords:
            return keyword.upper()
        return keyword.lower()

    def _should_quote_identifier(self, name: str) -> bool:
        """Check if identifier needs quoting."""
        if not self.options.quote_identifiers:
            return False

        # Quote if reserved keyword
        if name.upper() in self.reserved_keywords:
            return True

        # Quote if contains special characters
        if not name.replace('_', '').isalnum():
            return True

        # Quote if starts with number
        if name and name[0].isdigit():
            return True

        return False

    def _quote_identifier(self, name: str) -> str:
        """Quote an identifier if necessary."""
        if self._should_quote_identifier(name):
            # Escape backticks in the name
            escaped = name.replace('`', '\\`')
            return f"`{escaped}`"
        return name

    # Visitor methods for literals

    def visit_identifier(self, node: Identifier) -> Any:
        name = self._quote_identifier(node.name)
        self._write(name)

    def visit_string_literal(self, node: StringLiteral) -> Any:
        # Handle different quote types
        if node.raw:
            prefix = 'r' if node.quote_type.startswith('single') else 'r'
        else:
            prefix = ''

        if node.quote_type == 'single':
            self._write(f"{prefix}'{node.value}'")
        elif node.quote_type == 'double':
            self._write(f'{prefix}"{node.value}"')
        elif node.quote_type == 'triple_single':
            self._write(f"{prefix}'''{node.value}'''")
        else:  # triple_double
            self._write(f'{prefix}"""{node.value}"""')

    def visit_integer_literal(self, node: IntegerLiteral) -> Any:
        if node.hex:
            self._write(f"0x{node.value:X}")
        else:
            self._write(str(node.value))

    def visit_float_literal(self, node: FloatLiteral) -> Any:
        self._write(str(node.value))

    def visit_boolean_literal(self, node: BooleanLiteral) -> Any:
        self._write(self._keyword('TRUE') if node.value else self._keyword('FALSE'))

    def visit_null_literal(self, node: NullLiteral) -> Any:
        self._write(self._keyword('NULL'))

    def visit_date_literal(self, node: DateLiteral) -> Any:
        self._write(f"{self._keyword('DATE')} '{node.value}'")

    def visit_time_literal(self, node: TimeLiteral) -> Any:
        self._write(f"{self._keyword('TIME')} '{node.value}'")

    def visit_datetime_literal(self, node: DatetimeLiteral) -> Any:
        self._write(f"{self._keyword('DATETIME')} '{node.value}'")

    def visit_timestamp_literal(self, node: TimestampLiteral) -> Any:
        self._write(f"{self._keyword('TIMESTAMP')} '{node.value}'")
        if node.timezone:
            self._write(f" {node.timezone}")

    def visit_array_literal(self, node: ArrayLiteral) -> Any:
        if node.element_type:
            self._write(self._keyword('ARRAY'))
            self._write('<')
            node.element_type.accept(self)
            self._write('>')

        self._write('[')
        for i, elem in enumerate(node.elements):
            if i > 0:
                self._write(', ')
            elem.accept(self)
        self._write(']')

    def visit_struct_literal(self, node: StructLiteral) -> Any:
        self._write(self._keyword('STRUCT'))
        self._write('(')

        for i, (name, expr) in enumerate(node.fields):
            if i > 0:
                self._write(', ')
            expr.accept(self)
            if name:
                self._write(f' {self._keyword("AS")} {self._quote_identifier(name)}')

        self._write(')')

    # Visitor methods for expressions

    def visit_column_ref(self, node: ColumnRef) -> Any:
        if node.table:
            node.table.accept(self)
            self._write('.')
        node.column.accept(self)

    def visit_star_expression(self, node: StarExpression) -> Any:
        if node.table:
            node.table.accept(self)
            self._write('.')
        self._write('*')

        if node.except_columns:
            self._write(f' {self._keyword("EXCEPT")} (')
            for i, col in enumerate(node.except_columns):
                if i > 0:
                    self._write(', ')
                col.accept(self)
            self._write(')')

        if node.replace_columns:
            self._write(f' {self._keyword("REPLACE")} (')
            for i, item in enumerate(node.replace_columns):
                if i > 0:
                    self._write(', ')
                item.accept(self)
            self._write(')')

    def visit_binary_op(self, node: BinaryOp) -> Any:
        self._write('(')
        node.left.accept(self)
        self._write(f' {node.operator} ')
        node.right.accept(self)
        self._write(')')

    def visit_unary_op(self, node: UnaryOp) -> Any:
        if node.prefix:
            self._write(node.operator)
            self._write(' ')
            node.operand.accept(self)
        else:
            node.operand.accept(self)
            self._write(' ')
            self._write(node.operator)

    def visit_function_call(self, node: FunctionCall) -> Any:
        node.name.accept(self)
        self._write('(')

        if node.distinct:
            self._write(self._keyword('DISTINCT') + ' ')

        for i, arg in enumerate(node.args):
            if i > 0:
                self._write(', ')
            arg.accept(self)

        if node.ignore_nulls:
            self._write(' ' + self._keyword('IGNORE NULLS'))

        if node.order_by:
            self._write(' ' + self._keyword('ORDER BY') + ' ')
            for i, item in enumerate(node.order_by):
                if i > 0:
                    self._write(', ')
                item.accept(self)

        if node.limit:
            self._write(' ' + self._keyword('LIMIT') + ' ')
            node.limit.accept(self)

        self._write(')')

    def visit_window_function(self, node: WindowFunction) -> Any:
        node.function.accept(self)
        self._write(' ' + self._keyword('OVER') + ' (')

        if node.window.partition_by:
            self._write(self._keyword('PARTITION BY') + ' ')
            for i, expr in enumerate(node.window.partition_by):
                if i > 0:
                    self._write(', ')
                expr.accept(self)

        if node.window.order_by:
            if node.window.partition_by:
                self._write(' ')
            self._write(self._keyword('ORDER BY') + ' ')
            for i, item in enumerate(node.window.order_by):
                if i > 0:
                    self._write(', ')
                item.accept(self)

        self._write(')')

    def visit_cast_expression(self, node: CastExpression) -> Any:
        func = self._keyword('SAFE_CAST' if node.safe else 'CAST')
        self._write(f'{func}(')
        node.expression.accept(self)
        self._write(f' {self._keyword("AS")} ')
        node.target_type.accept(self)

        if node.format:
            self._write(f' {self._keyword("FORMAT")} {node.format}')

        self._write(')')

    def visit_case_expression(self, node: CaseExpression) -> Any:
        self._write(self._keyword('CASE'))

        if node.expr:
            self._write(' ')
            node.expr.accept(self)

        for when in node.when_clauses:
            self._writeln()
            self._write_indent()
            self._write('  ')
            when.accept(self)

        if node.else_clause:
            self._writeln()
            self._write_indent()
            self._write('  ')
            self._write(self._keyword('ELSE') + ' ')
            node.else_clause.accept(self)

        self._writeln()
        self._write_indent()
        self._write(self._keyword('END'))

    def visit_when_clause(self, node: WhenClause) -> Any:
        self._write(self._keyword('WHEN') + ' ')
        node.condition.accept(self)
        self._write(' ' + self._keyword('THEN') + ' ')
        node.result.accept(self)

    # Visitor methods for statements

    def visit_select_statement(self, node: SelectStatement) -> Any:
        # WITH clause
        if node.with_clause:
            node.with_clause.accept(self)
            self._writeln()

        # SELECT clause
        self._write_indent()
        self._write(self._keyword('SELECT'))

        if self.options.format_style == 'expanded':
            self._writeln()
            self.depth += 1

            for i, item in enumerate(node.select_list):
                self._write_indent()
                item.accept(self)
                if i < len(node.select_list) - 1 or self.options.trailing_commas:
                    self._write(',')
                self._writeln()

            self.depth -= 1
        else:
            self._write(' ')
            for i, item in enumerate(node.select_list):
                if i > 0:
                    self._write(', ')
                item.accept(self)

        # FROM clause
        if node.from_clause:
            self._write_indent()
            self._write(self._keyword('FROM'))

            if self.options.format_style == 'expanded':
                self._writeln()
                self.depth += 1
                node.from_clause.accept(self)
                self.depth -= 1
            else:
                self._write(' ')
                node.from_clause.accept(self)

        # WHERE clause
        if node.where:
            self._writeln()
            self._write_indent()
            self._write(self._keyword('WHERE'))

            if self.options.format_style == 'expanded':
                self._writeln()
                self.depth += 1
                self._write_indent()
                node.where.accept(self)
                self.depth -= 1
            else:
                self._write(' ')
                node.where.accept(self)

        # GROUP BY clause
        if node.group_by:
            self._writeln()
            self._write_indent()
            node.group_by.accept(self)

        # HAVING clause
        if node.having:
            self._writeln()
            self._write_indent()
            self._write(self._keyword('HAVING') + ' ')
            node.having.accept(self)

        # QUALIFY clause
        if node.qualify:
            self._writeln()
            self._write_indent()
            self._write(self._keyword('QUALIFY') + ' ')
            node.qualify.accept(self)

        # ORDER BY clause
        if node.order_by:
            self._writeln()
            self._write_indent()
            self._write(self._keyword('ORDER BY') + ' ')
            for i, item in enumerate(node.order_by):
                if i > 0:
                    self._write(', ')
                item.accept(self)

        # LIMIT clause
        if node.limit:
            self._writeln()
            self._write_indent()
            self._write(self._keyword('LIMIT') + ' ')
            node.limit.accept(self)

            if node.offset:
                self._write(' ' + self._keyword('OFFSET') + ' ')
                node.offset.accept(self)

    def visit_select_item(self, node: SelectItem) -> Any:
        node.expression.accept(self)
        if node.alias:
            self._write(' ' + self._keyword('AS') + ' ')
            node.alias.accept(self)

    def visit_with_clause(self, node: WithClause) -> Any:
        self._write(self._keyword('WITH'))

        if node.recursive:
            self._write(' ' + self._keyword('RECURSIVE'))

        if self.options.format_style == 'expanded':
            self._writeln()
            self.depth += 1

            for i, cte in enumerate(node.ctes):
                if i > 0:
                    self._write(',')
                    self._writeln()
                cte.accept(self)

            self.depth -= 1
        else:
            self._write(' ')
            for i, cte in enumerate(node.ctes):
                if i > 0:
                    self._write(', ')
                cte.accept(self)

    def visit_cte(self, node: CTE) -> Any:
        self._write_indent()
        node.name.accept(self)

        if node.columns:
            self._write(' (')
            for i, col in enumerate(node.columns):
                if i > 0:
                    self._write(', ')
                col.accept(self)
            self._write(')')

        self._write(' ' + self._keyword('AS') + ' (')

        if self.options.format_style == 'expanded':
            self._writeln()
            self.depth += 1
            node.query.accept(self)
            self.depth -= 1
            self._writeln()
            self._write_indent()
        else:
            node.query.accept(self)

        self._write(')')

    def visit_table_reference(self, node: TableReference) -> Any:
        self._write_indent()
        node.table.accept(self)

        if node.for_system_time:
            self._write(' ' + self._keyword('FOR SYSTEM_TIME AS OF') + ' ')
            node.for_system_time.accept(self)

        if node.alias:
            self._write(' ' + self._keyword('AS') + ' ')
            node.alias.accept(self)

    def visit_join_expression(self, node: JoinExpression) -> Any:
        node.left.accept(self)
        self._writeln()
        self._write_indent()

        self._write(self._keyword(node.join_type.value) + ' ' + self._keyword('JOIN'))

        if self.options.format_style == 'expanded':
            self._writeln()
            self.depth += 1
            node.right.accept(self)
            self.depth -= 1
        else:
            self._write(' ')
            node.right.accept(self)

        if node.on_condition:
            self._writeln()
            self._write_indent()
            self._write(self._keyword('ON') + ' ')
            node.on_condition.accept(self)
        elif node.using_columns:
            self._writeln()
            self._write_indent()
            self._write(self._keyword('USING') + ' (')
            for i, col in enumerate(node.using_columns):
                if i > 0:
                    self._write(', ')
                col.accept(self)
            self._write(')')

    def visit_order_by_item(self, node: OrderByItem) -> Any:
        node.expression.accept(self)
        self._write(' ' + self._keyword(node.direction.value))

        if node.nulls_order:
            self._write(' ' + self._keyword(node.nulls_order.value))

    def visit_merge_statement(self, node: MergeStatement) -> Any:
        self._write(self._keyword('MERGE') + ' ')
        node.target_table.accept(self)
        self._write(' T')
        self._writeln()

        self._write(self._keyword('USING') + ' (')
        if self.options.format_style == 'expanded':
            self._writeln()
            self.depth += 1
            node.source.accept(self)
            self.depth -= 1
            self._writeln()
        else:
            node.source.accept(self)
        self._write(') S')
        self._writeln()

        self._write(self._keyword('ON') + ' ')
        node.on_condition.accept(self)

        for when in node.when_clauses:
            self._writeln()
            when.accept(self)

    def visit_merge_when_clause(self, node: MergeWhenClause) -> Any:
        self._write(self._keyword('WHEN') + ' ' + self._keyword(node.match_type))

        if node.condition:
            self._write(' ' + self._keyword('AND') + ' ')
            node.condition.accept(self)

        self._write(' ' + self._keyword('THEN'))
        self._writeln()
        self.depth += 1
        self._write_indent()
        node.action.accept(self)
        self.depth -= 1

    def visit_merge_insert(self, node: MergeInsert) -> Any:
        self._write(self._keyword('INSERT'))

        if node.row:
            self._write(' ' + self._keyword('ROW'))
        else:
            if node.columns:
                self._write(' (')
                for i, col in enumerate(node.columns):
                    if i > 0:
                        self._write(', ')
                    col.accept(self)
                self._write(')')

            if node.values:
                self._write(' ' + self._keyword('VALUES') + ' (')
                for i, val in enumerate(node.values):
                    if i > 0:
                        self._write(', ')
                    val.accept(self)
                self._write(')')

    def visit_merge_update(self, node: MergeUpdate) -> Any:
        self._write(self._keyword('UPDATE') + ' ' + self._keyword('SET'))

        for i, clause in enumerate(node.set_clauses):
            if i > 0:
                self._write(',')
            self._writeln()
            self._write_indent()
            self._write('  ')
            clause.accept(self)

    def visit_merge_delete(self, node: MergeDelete) -> Any:
        self._write(self._keyword('DELETE'))

    def visit_set_clause(self, node: SetClause) -> Any:
        node.column.accept(self)
        self._write(' = ')
        node.value.accept(self)

    def visit_table_name(self, node: TableName) -> Any:
        parts = []
        if node.project:
            parts.append(self._quote_identifier(node.project.name))
        if node.dataset:
            parts.append(self._quote_identifier(node.dataset.name))
        if node.table:
            parts.append(self._quote_identifier(node.table.name))

        self._write('.'.join(parts))

    def visit_group_by_clause(self, node: GroupByClause) -> Any:
        self._write(self._keyword('GROUP BY') + ' ')

        for i, expr in enumerate(node.expressions):
            if i > 0:
                self._write(', ')
            expr.accept(self)

        if node.rollup:
            self._write(' ' + self._keyword('ROLLUP') + ' (')
            for i, expr in enumerate(node.rollup):
                if i > 0:
                    self._write(', ')
                expr.accept(self)
            self._write(')')

        if node.cube:
            self._write(' ' + self._keyword('CUBE') + ' (')
            for i, expr in enumerate(node.cube):
                if i > 0:
                    self._write(', ')
                expr.accept(self)
            self._write(')')

        if node.grouping_sets:
            self._write(' ' + self._keyword('GROUPING SETS') + ' (')
            for i, group_set in enumerate(node.grouping_sets):
                if i > 0:
                    self._write(', ')
                self._write('(')
                for j, expr in enumerate(group_set):
                    if j > 0:
                        self._write(', ')
                    expr.accept(self)
                self._write(')')
            self._write(')')

    def visit_from_clause(self, node: FromClause) -> Any:
        for table in node.tables:
            table.accept(self)

    # Generic fallback
    def generic_visit(self, node: ASTNode) -> Any:
        self._write(f"<{node.node_type}>")


# Convenience functions

def to_sql(node: ASTNode, options: Optional[SerializerOptions] = None) -> str:
    """Convert an AST node to SQL string."""
    serializer = SQLSerializer(options)
    return serializer.serialize(node)


def pretty_print(node: ASTNode) -> str:
    """Pretty print an AST node as SQL."""
    options = SerializerOptions(
        format_style='expanded',
        uppercase_keywords=True,
        trailing_commas=True
    )
    return to_sql(node, options)


def compact_print(node: ASTNode) -> str:
    """Compact print an AST node as SQL."""
    options = SerializerOptions(
        format_style='compact',
        uppercase_keywords=True,
        trailing_commas=False
    )
    return to_sql(node, options)