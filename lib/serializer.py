"""
BigQuery SQL Serializer

Serializes AST nodes back to BigQuery SQL.
Simplified version for the basic types.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

from typing import Any, List, Optional, Set
from dataclasses import dataclass
from .types import (
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
        """
        Serialize an identifier (column or table).
        Args:
            node (Identifier): The identifier node.
        """
        if node.table:
            self._write(f"{node.table}.{node.name}")
        else:
            self._write(node.name)

    def visit_literal(self, node) -> Any:
        """
        Serialize a literal value.
        Args:
            node (Literal): The literal node.
        """
        self._write(str(node.value))

    def visit_string_literal(self, node: StringLiteral) -> Any:
        """
        Serialize a string literal.
        Args:
            node (StringLiteral): The string literal node.
        """
        self._write(f"'{node.value}'")

    def visit_integer_literal(self, node: IntegerLiteral) -> Any:
        """
        Serialize an integer literal.
        Args:
            node (IntegerLiteral): The integer literal node.
        """
        self._write(str(node.value))

    def visit_float_literal(self, node: FloatLiteral) -> Any:
        """
        Serialize a float literal.
        Args:
            node (FloatLiteral): The float literal node.
        """
        self._write(str(node.value))

    def visit_boolean_literal(self, node: BooleanLiteral) -> Any:
        """
        Serialize a boolean literal.
        Args:
            node (BooleanLiteral): The boolean literal node.
        """
        self._write("TRUE" if node.value else "FALSE")

    def visit_null_literal(self, node: NullLiteral) -> Any:
        """
        Serialize a NULL literal.
        Args:
            node (NullLiteral): The NULL literal node.
        """
        self._write("NULL")

    def visit_binary_op(self, node: BinaryOp) -> Any:
        """
        Serialize a binary operation.
        Args:
            node (BinaryOp): The binary operation node.
        """
        node.left.accept(self)
        self._write(f" {node.operator} ")
        node.right.accept(self)

    def visit_unary_op(self, node: UnaryOp) -> Any:
        """
        Serialize a unary operation.
        Args:
            node (UnaryOp): The unary operation node.
        """
        self._write(node.operator)
        self._write(" ")
        node.operand.accept(self)

    def visit_function_call(self, node: FunctionCall) -> Any:
        """
        Serialize a function call.
        Args:
            node (FunctionCall): The function call node.
        """
        self._write(node.function_name)
        self._write("(")
        for i, arg in enumerate(node.arguments):
            if i > 0:
                self._write(", ")
            arg.accept(self)
        self._write(")")

    def visit_table_name(self, node: TableName) -> Any:
        """
        Serialize a table name (possibly qualified).
        Args:
            node (TableName): The table name node.
        """
        if node.project:
            node.project.accept(self)
            self._write(".")
        if node.dataset:
            node.dataset.accept(self)
            self._write(".")
        node.table.accept(self)

    def visit_table_ref(self, node: TableRef) -> Any:
        """
        Serialize a table reference in FROM clause.
        Args:
            node (TableRef): The table reference node.
        """
        node.table.accept(self)
        if node.alias:
            self._write(f" AS {node.alias}")

    def visit_select_column(self, node: SelectColumn) -> Any:
        """
        Serialize a column in SELECT list.
        Args:
            node (SelectColumn): The select column node.
        """
        node.expression.accept(self)
        if node.alias:
            self._write(f" AS {node.alias}")

    def visit_where_clause(self, node: WhereClause) -> Any:
        """
        Serialize a WHERE clause.
        Args:
            node (WhereClause): The WHERE clause node.
        """
        self._keyword("WHERE")
        self._write(" ")
        node.condition.accept(self)

    def visit_group_by_clause(self, node: GroupByClause) -> Any:
        """
        Serialize a GROUP BY clause.
        Args:
            node (GroupByClause): The GROUP BY clause node.
        """
        self._keyword("GROUP BY")
        self._write(" ")
        for i, expr in enumerate(node.expressions):
            if i > 0:
                self._write(", ")
            expr.accept(self)

    def visit_having_clause(self, node: HavingClause) -> Any:
        """
        Serialize a HAVING clause.
        Args:
            node (HavingClause): The HAVING clause node.
        """
        self._keyword("HAVING")
        self._write(" ")
        node.condition.accept(self)

    def visit_order_by_item(self, node: OrderByItem) -> Any:
        """
        Serialize an ORDER BY item.
        Uses OrderDirection to format ASC/DESC keywords.
        Args:
            node (OrderByItem): The ORDER BY item node, with direction as OrderDirection.
        """
        node.expression.accept(self)
        if node.direction:
            self._write(f" {node.direction.value}")

    def visit_order_by_clause(self, node: OrderByClause) -> Any:
        """
        Serialize an ORDER BY clause.
        Args:
            node (OrderByClause): The ORDER BY clause node.
        """
        self._keyword("ORDER BY")
        self._write(" ")
        for i, item in enumerate(node.items):
            if i > 0:
                self._write(", ")
            item.accept(self)

    def visit_limit_clause(self, node: LimitClause) -> Any:
        """
        Serialize a LIMIT clause.
        Args:
            node (LimitClause): The LIMIT clause node.
        """
        self._keyword("LIMIT")
        self._write(" ")
        node.limit.accept(self)
        if node.offset:
            self._write(" ")
            self._keyword("OFFSET")
            self._write(" ")
            node.offset.accept(self)

    def visit_join(self, node: Join) -> Any:
        """
        Serialize a JOIN clause.
        Uses JoinType to format join type keywords.
        Args:
            node (Join): The JOIN node, with join_type as JoinType.
        """
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
        """
        Serialize a SELECT statement.
        Args:
            node (Select): The SELECT statement node.
        """
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
        """
        Serialize a subquery expression.
        Args:
            node (Subquery): The subquery node.
        """
        self._write("(")
        node.query.accept(self)
        self._write(")")

    def visit_cte(self, node: CTE) -> Any:
        """
        Serialize a Common Table Expression (CTE).
        Args:
            node (CTE): The CTE node.
        """
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
        """
        Serialize a WITH clause containing CTEs.
        Args:
            node (WithClause): The WITH clause node.
        """
        self._keyword("WITH")
        self._write(" ")
        for i, cte in enumerate(node.ctes):
            if i > 0:
                self._write(", ")
            cte.accept(self)

    def visit_merge_insert(self, node: MergeInsert) -> Any:
        """
        Serialize a MERGE INSERT action.
        Args:
            node (MergeInsert): The MERGE INSERT node.
        """
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
        """
        Serialize a MERGE UPDATE action.
        Args:
            node (MergeUpdate): The MERGE UPDATE node.
        """
        self._keyword("UPDATE SET")
        self._write(" ")
        items = list(node.assignments.items())
        for i, (col, expr) in enumerate(items):
            if i > 0:
                self._write(", ")
            self._write(f"{col} = ")
            expr.accept(self)

    def visit_merge_delete(self, node: MergeDelete) -> Any:
        """
        Serialize a MERGE DELETE action.
        Args:
            node (MergeDelete): The MERGE DELETE node.
        """
        self._keyword("DELETE")

    def visit_merge_action(self, node: MergeAction) -> Any:
        """
        Serialize a MERGE WHEN clause.
        Args:
            node (MergeAction): The MERGE WHEN clause node.
        """
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
        """
        Serialize a MERGE statement.
        Args:
            node (Merge): The MERGE statement node.
        """
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
        """
        Serialize a window specification for window functions.
        Args:
            node (WindowSpecification): The window specification node.
        """
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
        """
        Serialize a window function call.
        Args:
            node (WindowFunction): The window function node.
        """
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