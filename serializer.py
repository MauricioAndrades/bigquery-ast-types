"""
BigQuery SQL Serializer

Serializes AST nodes back to BigQuery SQL.
Simplified version for the basic types.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

from typing import Any, List, Optional, Set
from dataclasses import dataclass
from ast_types import (
    ASTNode,
    ASTVisitor,
    Identifier,
    StringLiteral,
    IntegerLiteral,
    FloatLiteral,
    BooleanLiteral,
    NullLiteral,
    BinaryOp,
    UnaryOp,
    FunctionCall,
    Select,
    SelectColumn,
    TableRef,
    TableName,
    Join,
    WhereClause,
    GroupByClause,
    HavingClause,
    OrderByClause,
    OrderByItem,
    LimitClause,
    CTE,
    WithClause,
    Merge,
    MergeAction,
    MergeInsert,
    MergeUpdate,
    MergeDelete,
    WindowFunction,
    WindowSpecification,
    Subquery,
    JoinType,
    OrderDirection,
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

    def serialize(self, node: ASTNode) -> str:
        """Serialize an AST node to SQL."""
        self.buffer = []
        self.depth = 0
        node.accept(self)
        return "".join(self.buffer)

    def _write(self, text: str):
        """Write text to buffer."""
        self.buffer.append(text)

    def _indent(self):
        """Add indentation."""
        if self.options.format_style == "expanded":
            self._write(self.options.indent * self.depth)

    def _keyword(self, keyword: str):
        """Write a keyword."""
        if self.options.uppercase_keywords:
            self._write(keyword.upper())
        else:
            self._write(keyword.lower())

    # Visitor methods
    def visit_identifier(self, node: Identifier) -> Any:
        if node.table:
            self._write(f"{node.table}.{node.name}")
        else:
            self._write(node.name)

    def visit_literal(self, node) -> Any:
        self._write(str(node.value))

    def visit_string_literal(self, node: StringLiteral) -> Any:
        self._write(f"'{node.value}'")

    def visit_integer_literal(self, node: IntegerLiteral) -> Any:
        self._write(str(node.value))

    def visit_float_literal(self, node: FloatLiteral) -> Any:
        self._write(str(node.value))

    def visit_boolean_literal(self, node: BooleanLiteral) -> Any:
        self._write("TRUE" if node.value else "FALSE")

    def visit_null_literal(self, node: NullLiteral) -> Any:
        self._write("NULL")

    def visit_binary_op(self, node: BinaryOp) -> Any:
        node.left.accept(self)
        self._write(f" {node.operator} ")
        node.right.accept(self)

    def visit_unary_op(self, node: UnaryOp) -> Any:
        self._write(node.operator)
        self._write(" ")
        node.operand.accept(self)

    def visit_function_call(self, node: FunctionCall) -> Any:
        self._write(node.function_name)
        self._write("(")
        for i, arg in enumerate(node.arguments):
            if i > 0:
                self._write(", ")
            arg.accept(self)
        self._write(")")

    def visit_table_name(self, node: TableName) -> Any:
        if node.project:
            node.project.accept(self)
            self._write(".")
        if node.dataset:
            node.dataset.accept(self)
            self._write(".")
        node.table.accept(self)

    def visit_table_ref(self, node: TableRef) -> Any:
        node.table.accept(self)
        if node.alias:
            self._write(f" AS {node.alias}")

    def visit_select_column(self, node: SelectColumn) -> Any:
        node.expression.accept(self)
        if node.alias:
            self._write(f" AS {node.alias}")

    def visit_where_clause(self, node: WhereClause) -> Any:
        self._keyword("WHERE")
        self._write(" ")
        node.condition.accept(self)

    def visit_group_by_clause(self, node: GroupByClause) -> Any:
        self._keyword("GROUP BY")
        self._write(" ")
        for i, expr in enumerate(node.expressions):
            if i > 0:
                self._write(", ")
            expr.accept(self)

    def visit_having_clause(self, node: HavingClause) -> Any:
        self._keyword("HAVING")
        self._write(" ")
        node.condition.accept(self)

    def visit_order_by_item(self, node: OrderByItem) -> Any:
        node.expression.accept(self)
        if node.direction:
            self._write(f" {node.direction.value}")

    def visit_order_by_clause(self, node: OrderByClause) -> Any:
        self._keyword("ORDER BY")
        self._write(" ")
        for i, item in enumerate(node.items):
            if i > 0:
                self._write(", ")
            item.accept(self)

    def visit_limit_clause(self, node: LimitClause) -> Any:
        self._keyword("LIMIT")
        self._write(" ")
        node.limit.accept(self)
        if node.offset:
            self._write(" ")
            self._keyword("OFFSET")
            self._write(" ")
            node.offset.accept(self)

    def visit_join(self, node: Join) -> Any:
        self._write(" ")
        self._keyword(node.join_type.value)
        self._write(" ")
        self._keyword("JOIN")
        self._write(" ")
        node.table.accept(self)
        if node.condition:
            self._write(" ")
            self._keyword("ON")
            self._write(" ")
            node.condition.accept(self)

    def visit_select(self, node: Select) -> Any:
        self._keyword("SELECT")
        if node.distinct:
            self._write(" ")
            self._keyword("DISTINCT")
        self._write(" ")
        
        for i, col in enumerate(node.select_list):
            if i > 0:
                self._write(", ")
            col.accept(self)
        
        if node.from_clause:
            self._write(" ")
            self._keyword("FROM")
            self._write(" ")
            node.from_clause.accept(self)
        
        if node.joins:
            for join in node.joins:
                join.accept(self)
        
        if node.where_clause:
            self._write(" ")
            node.where_clause.accept(self)
        
        if node.group_by_clause:
            self._write(" ")
            node.group_by_clause.accept(self)
        
        if node.having_clause:
            self._write(" ")
            node.having_clause.accept(self)
        
        if node.order_by_clause:
            self._write(" ")
            node.order_by_clause.accept(self)
        
        if node.limit_clause:
            self._write(" ")
            node.limit_clause.accept(self)

    def visit_subquery(self, node: Subquery) -> Any:
        self._write("(")
        node.query.accept(self)
        self._write(")")

    def visit_cte(self, node: CTE) -> Any:
        self._write(node.name)
        if node.columns:
            self._write("(")
            for i, col in enumerate(node.columns):
                if i > 0:
                    self._write(", ")
                self._write(col)
            self._write(")")
        self._write(" ")
        self._keyword("AS")
        self._write(" (")
        node.query.accept(self)
        self._write(")")

    def visit_with_clause(self, node: WithClause) -> Any:
        self._keyword("WITH")
        self._write(" ")
        for i, cte in enumerate(node.ctes):
            if i > 0:
                self._write(", ")
            cte.accept(self)

    def visit_merge_insert(self, node: MergeInsert) -> Any:
        self._keyword("INSERT")
        if node.columns:
            self._write("(")
            for i, col in enumerate(node.columns):
                if i > 0:
                    self._write(", ")
                self._write(col)
            self._write(")")
        if node.values:
            self._write(" ")
            self._keyword("VALUES")
            self._write("(")
            for i, val in enumerate(node.values):
                if i > 0:
                    self._write(", ")
                val.accept(self)
            self._write(")")

    def visit_merge_update(self, node: MergeUpdate) -> Any:
        self._keyword("UPDATE SET")
        self._write(" ")
        items = list(node.assignments.items())
        for i, (col, expr) in enumerate(items):
            if i > 0:
                self._write(", ")
            self._write(f"{col} = ")
            expr.accept(self)

    def visit_merge_delete(self, node: MergeDelete) -> Any:
        self._keyword("DELETE")

    def visit_merge_action(self, node: MergeAction) -> Any:
        self._keyword("WHEN")
        if node.condition:
            self._write(" ")
            node.condition.accept(self)
        self._write(" ")
        self._keyword("THEN")
        self._write(" ")
        if node.action:
            node.action.accept(self)

    def visit_merge(self, node: Merge) -> Any:
        self._keyword("MERGE")
        self._write(" ")
        node.target_table.accept(self)
        self._write(" ")
        self._keyword("USING")
        self._write(" ")
        node.source_table.accept(self)
        self._write(" ")
        self._keyword("ON")
        self._write(" ")
        node.merge_condition.accept(self)
        for action in node.actions:
            self._write(" ")
            action.accept(self)

    def visit_window_specification(self, node: WindowSpecification) -> Any:
        if node.partition_by:
            self._keyword("PARTITION BY")
            self._write(" ")
            for i, expr in enumerate(node.partition_by):
                if i > 0:
                    self._write(", ")
                expr.accept(self)
        if node.order_by:
            if node.partition_by:
                self._write(" ")
            node.order_by.accept(self)

    def visit_window_function(self, node: WindowFunction) -> Any:
        self._write(node.function_name)
        self._write("(")
        for i, arg in enumerate(node.arguments):
            if i > 0:
                self._write(", ")
            arg.accept(self)
        self._write(") OVER (")
        node.window_spec.accept(self)
        self._write(")")


# Convenience functions
def to_sql(node: ASTNode, options: Optional[SerializerOptions] = None) -> str:
    """Serialize an AST node to SQL."""
    serializer = SQLSerializer(options)
    return serializer.serialize(node)


def pretty_print(node: ASTNode) -> str:
    """Pretty print an AST node as formatted SQL."""
    options = SerializerOptions(format_style="expanded")
    return to_sql(node, options)


def compact_print(node: ASTNode) -> str:
    """Print an AST node as compact SQL."""
    options = SerializerOptions(format_style="compact")
    return to_sql(node, options)