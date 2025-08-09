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

    def visit_star(self, node) -> Any:
        """
        Serialize a star expression (*).
        Args:
            node (Star): The star expression node.
        """
        if node.table:
            self._write(f"{node.table}.")
        self._write("*")

        if node.except_columns:
            self._write(" EXCEPT(")
            for i, col in enumerate(node.except_columns):
                if i > 0:
                    self._write(", ")
                self._write(col)
            self._write(")")

        if node.replace_columns:
            self._write(" REPLACE(")
            for i, (col, expr) in enumerate(node.replace_columns.items()):
                if i > 0:
                    self._write(", ")
                expr.accept(self)
                self._write(f" AS {col}")
            self._write(")")

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
            self._write(node.project)
            self._write(".")
        if node.dataset:
            self._write(node.dataset)
            self._write(".")
        self._write(node.table)

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
        from .types import GroupByType

        self._keyword("GROUP BY")
        self._write(" ")

        if node.group_type == GroupByType.ALL:
            self._write("ALL")
        elif node.group_type == GroupByType.ROLLUP:
            self._write("ROLLUP(")
            for i, expr in enumerate(node.expressions):
                if i > 0:
                    self._write(", ")
                expr.accept(self)
            self._write(")")
        elif node.group_type == GroupByType.CUBE:
            self._write("CUBE(")
            for i, expr in enumerate(node.expressions):
                if i > 0:
                    self._write(", ")
                expr.accept(self)
            self._write(")")
        elif node.group_type == GroupByType.GROUPING_SETS:
            self._write("GROUPING SETS(")
            for i, group in enumerate(node.grouping_sets):
                if i > 0:
                    self._write(", ")
                self._write("(")
                for j, expr in enumerate(group):
                    if j > 0:
                        self._write(", ")
                    expr.accept(self)
                self._write(")")
            self._write(")")
        else:
            # Standard GROUP BY
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
        if node.nulls_order:
            self._write(f" NULLS {node.nulls_order.value}")

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
        if node.with_clause:
            node.with_clause.accept(self)
            self._write(" ")
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

    def visit_set_operation(self, node) -> Any:
        """
        Serialize a set operation (UNION/INTERSECT/EXCEPT).
        Args:
            node (SetOperation): The set operation node.
        """
        node.left.accept(self)
        self._write(" ")
        self._keyword(node.operator.value)
        if node.all:
            self._write(" ")
            self._keyword("ALL")
        if node.corresponding:
            self._write(" ")
            self._keyword("CORRESPONDING")
        self._write(" ")
        node.right.accept(self)

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

    # Missing visitor methods - placeholder implementations
    def visit_unquoted_identifier(self, node) -> Any:
        """Visit an unquoted identifier."""
        self._write(node.name)

    def visit_quoted_identifier(self, node) -> Any:
        """Visit a quoted identifier."""
        self._write(f"`{node.name}`")

    def visit_enhanced_general_identifier(self, node) -> Any:
        """Visit an enhanced general identifier."""
        self._write(node.name)

    def visit_path_expression(self, node) -> Any:
        """Visit a path expression."""
        for i, part in enumerate(node.parts):
            if i > 0:
                self._write(".")
            part.accept(self)

    def visit_path_part(self, node) -> Any:
        """Visit a path part."""
        self._write(str(node.name))

    def visit_column_name(self, node) -> Any:
        """Visit a column name."""
        self._write(node.name)

    def visit_field_name(self, node) -> Any:
        """Visit a field name."""
        self._write(node.name)

    def visit_bytes_literal(self, node) -> Any:
        """Visit a bytes literal."""
        self._write(f"b'{node.value}'")

    def visit_numeric_literal(self, node) -> Any:
        """Visit a numeric literal."""
        self._write(f"NUMERIC '{node.value}'")

    def visit_bignumeric_literal(self, node) -> Any:
        """Visit a bignumeric literal."""
        self._write(f"BIGNUMERIC '{node.value}'")

    def visit_date_literal(self, node) -> Any:
        """Visit a date literal."""
        self._write(f"DATE '{node.value}'")

    def visit_time_literal(self, node) -> Any:
        """Visit a time literal."""
        self._write(f"TIME '{node.value}'")

    def visit_datetime_literal(self, node) -> Any:
        """Visit a datetime literal."""
        self._write(f"DATETIME '{node.value}'")

    def visit_timestamp_literal(self, node) -> Any:
        """Visit a timestamp literal."""
        self._write(f"TIMESTAMP '{node.value}'")

    def visit_interval_literal(self, node) -> Any:
        """Visit an interval literal."""
        self._write(f"INTERVAL {node.value} {node.unit}")

    def visit_array_literal(self, node) -> Any:
        """Visit an array literal."""
        self._write("[")
        for i, elem in enumerate(node.elements):
            if i > 0:
                self._write(", ")
            elem.accept(self)
        self._write("]")

    def visit_struct_literal(self, node) -> Any:
        """Visit a struct literal."""
        self._write("STRUCT(")
        for i, (key, value) in enumerate(node.fields):
            if i > 0:
                self._write(", ")
            self._write(f"{key} AS ")
            value.accept(self)
        self._write(")")

    def visit_range_literal(self, node) -> Any:
        """Visit a range literal."""
        self._write(f"RANGE<{node.type}>({node.start}, {node.end})")

    def visit_json_literal(self, node) -> Any:
        """Visit a JSON literal."""
        self._write(f"JSON '{node.value}'")

    def visit_named_parameter(self, node) -> Any:
        """Visit a named parameter."""
        self._write(f"@{node.name}")

    def visit_positional_parameter(self, node) -> Any:
        """Visit a positional parameter."""
        self._write("?")

    def visit_comment(self, node) -> Any:
        """Visit a comment."""
        self._write(f"-- {node.text}")

    def visit_case(self, node) -> Any:
        """Visit a CASE expression."""
        self._write("CASE")
        if hasattr(node, 'expression') and node.expression:
            self._write(" ")
            node.expression.accept(self)
        for when in node.when_clauses:
            when.accept(self)
        if hasattr(node, 'else_clause') and node.else_clause:
            self._write(" ELSE ")
            node.else_clause.accept(self)
        self._write(" END")

    def visit_when_clause(self, node) -> Any:
        """Visit a WHEN clause."""
        self._write(" WHEN ")
        node.condition.accept(self)
        self._write(" THEN ")
        node.result.accept(self)

    def visit_insert(self, node) -> Any:
        """Visit an INSERT statement."""
        self._write("INSERT INTO ")
        node.table.accept(self)
        if node.columns:
            self._write(" (")
            for i, col in enumerate(node.columns):
                if i > 0:
                    self._write(", ")
                col.accept(self)
            self._write(")")
        self._write(" VALUES (")
        for i, val in enumerate(node.values):
            if i > 0:
                self._write(", ")
            val.accept(self)
        self._write(")")

    def visit_update(self, node) -> Any:
        """Visit an UPDATE statement."""
        self._write("UPDATE ")
        node.table.accept(self)
        self._write(" SET ")
        for i, (col, val) in enumerate(node.assignments):
            if i > 0:
                self._write(", ")
            col.accept(self)
            self._write(" = ")
            val.accept(self)
        if node.where:
            self._write(" WHERE ")
            node.where.accept(self)

    def visit_create_table(self, node) -> Any:
        """Visit a CREATE TABLE statement."""
        self._write("CREATE TABLE ")
        if node.if_not_exists:
            self._write("IF NOT EXISTS ")
        node.table.accept(self)
        self._write(" (")
        for i, col in enumerate(node.columns):
            if i > 0:
                self._write(", ")
            col.accept(self)
        self._write(")")

    def visit_hash_comment(self, node) -> Any:
        """Visit a hash comment."""
        self._write(f"# {node.text}")

    def visit_dash_comment(self, node) -> Any:
        """Visit a dash comment."""
        self._write(f"-- {node.text}")

    def visit_block_comment(self, node) -> Any:
        """Visit a block comment."""
        self._write(f"/* {node.text} */")

<<<<<<< HEAD
    # Missing visitor methods that were using generic_visit
    def visit_cast(self, node) -> Any:
        """Visit a CAST expression."""
        func = "SAFE_CAST" if node.safe else "CAST"
        self._write(func)
        self._write("(")
        node.expr.accept(self)
        self._write(" AS ")
        self._write(node.target_type)
        self._write(")")

    def visit_star(self, node) -> Any:
        """Visit a star expression."""
        if node.table:
            self._write(f"{node.table}.*")
        else:
            self._write("*")
        
        if node.except_columns:
            self._write(" EXCEPT(")
            for i, col in enumerate(node.except_columns):
                if i > 0:
                    self._write(", ")
                self._write(col)
            self._write(")")
        
        if node.replace_columns:
            self._write(" REPLACE(")
            items = list(node.replace_columns.items())
            for i, (col, expr) in enumerate(items):
                if i > 0:
                    self._write(", ")
                expr.accept(self)
                self._write(f" AS {col}")
            self._write(")")

    def visit_array_literal(self, node) -> Any:
        """Visit an array literal."""
        self._write("[")
        for i, element in enumerate(node.elements):
            if i > 0:
                self._write(", ")
            element.accept(self)
        self._write("]")

    def visit_struct_literal(self, node) -> Any:
        """Visit a struct literal."""
        self._write("STRUCT(")
        for i, (name, expr) in enumerate(node.fields):
            if i > 0:
                self._write(", ")
            expr.accept(self)
            if name:
                self._write(f" AS {name}")
        self._write(")")

    def visit_range_literal(self, node) -> Any:
        """Visit a range literal."""
        self._write(f"RANGE<{node.range_type}>(")
        if node.lower_bound:
            node.lower_bound.accept(self)
        else:
            self._write("UNBOUNDED")
        self._write(", ")
        if node.upper_bound:
            node.upper_bound.accept(self)
        else:
            self._write("UNBOUNDED")
        self._write(")")

    def visit_json_literal(self, node) -> Any:
        """Visit a JSON literal."""
        self._write(f"JSON '{node.value}'")

    def visit_named_parameter(self, node) -> Any:
        """Visit a named parameter."""
        self._write(f"@{node.name}")

    def visit_positional_parameter(self, node) -> Any:
        """Visit a positional parameter."""
        self._write("?")

    def visit_date_literal(self, node) -> Any:
        """Visit a date literal."""
        self._write(f"DATE '{node.value}'")

    def visit_time_literal(self, node) -> Any:
        """Visit a time literal."""
        self._write(f"TIME '{node.value}'")

    def visit_datetime_literal(self, node) -> Any:
        """Visit a datetime literal."""
        self._write(f"DATETIME '{node.value}'")

    def visit_timestamp_literal(self, node) -> Any:
        """Visit a timestamp literal."""
        self._write(f"TIMESTAMP '{node.value}'")

    def visit_interval_literal(self, node) -> Any:
        """Visit an interval literal."""
        self._write(f"INTERVAL {node.value} {node.unit}")

    def visit_qualify_clause(self, node) -> Any:
        """Visit a QUALIFY clause."""
        self._write("QUALIFY ")
        node.condition.accept(self)

    def visit_unnest(self, node) -> Any:
        """Visit an UNNEST expression."""
        self._write("UNNEST(")
        node.array_expr.accept(self)
        self._write(")")
        if node.with_offset:
            self._write(" WITH OFFSET")
            if node.offset_alias:
                self._write(f" AS {node.offset_alias}")

    def visit_tablesample(self, node) -> Any:
        """Visit a TABLESAMPLE clause."""
        node.table.accept(self)
        self._write(f" TABLESAMPLE {node.method}(")
        if node.percent is not None:
            self._write(f"{node.percent} PERCENT")
        elif node.rows is not None:
            self._write(f"{node.rows} ROWS")
        self._write(")")
        if node.seed is not None:
            self._write(f" REPEATABLE({node.seed})")

    def visit_pivot(self, node) -> Any:
        """Visit a PIVOT clause."""
        node.source.accept(self) if hasattr(node.source, 'accept') else self._write(str(node.source))
        self._write(f" PIVOT({node.aggregate_function}({node.value_column}) FOR {node.pivot_column} IN (")
        for i, value in enumerate(node.pivot_values):
            if i > 0:
                self._write(", ")
            self._write(str(value))
        self._write("))")
        if node.alias:
            self._write(f" AS {node.alias}")

    def visit_unpivot(self, node) -> Any:
        """Visit an UNPIVOT clause."""
        node.source.accept(self) if hasattr(node.source, 'accept') else self._write(str(node.source))
        self._write(f" UNPIVOT{'INCLUDE NULLS' if node.include_nulls else ''}({node.value_column} FOR {node.name_column} IN (")
        for i, col in enumerate(node.columns):
            if i > 0:
                self._write(", ")
            self._write(col)
        self._write("))")
        if node.alias:
            self._write(f" AS {node.alias}")

    # DDL Statement visitors (Task 1)
    def visit_create_view(self, node) -> Any:
        """Visit a CREATE VIEW statement."""
        self._write("CREATE ")
        if node.or_replace:
            self._write("OR REPLACE ")
        if node.materialized:
            self._write("MATERIALIZED ")
        self._write("VIEW ")
        if node.if_not_exists:
            self._write("IF NOT EXISTS ")
        node.view.accept(self)
        
        if node.columns:
            self._write(" (")
            for i, col in enumerate(node.columns):
                if i > 0:
                    self._write(", ")
                self._write(col)
            self._write(")")
        
        if node.partition_by:
            self._write(" PARTITION BY ")
            node.partition_by.accept(self)
        
        if node.cluster_by:
            self._write(" CLUSTER BY ")
            for i, expr in enumerate(node.cluster_by):
                if i > 0:
                    self._write(", ")
                expr.accept(self)
        
        if node.options:
            self._write(" OPTIONS (")
            items = list(node.options.items())
            for i, (key, value) in enumerate(items):
                if i > 0:
                    self._write(", ")
                self._write(f"{key} = {value}")
            self._write(")")
        
        self._write(" AS ")
        node.query.accept(self)

    def visit_create_function(self, node) -> Any:
        """Visit a CREATE FUNCTION statement."""
        self._write("CREATE ")
        if node.or_replace:
            self._write("OR REPLACE ")
        if node.temp:
            self._write("TEMP ")
        self._write("FUNCTION ")
        if node.if_not_exists:
            self._write("IF NOT EXISTS ")
        self._write(node.function_name)
        
        self._write("(")
        for i, (param_name, param_type) in enumerate(node.parameters):
            if i > 0:
                self._write(", ")
            self._write(f"{param_name} {param_type}")
        self._write(")")
        
        if node.return_type:
            self._write(f" RETURNS {node.return_type}")
        
        if node.deterministic:
            self._write(" DETERMINISTIC")
        
        if node.language != "SQL":
            self._write(f" LANGUAGE {node.language}")
        
        if node.options:
            self._write(" OPTIONS (")
            items = list(node.options.items())
            for i, (key, value) in enumerate(items):
                if i > 0:
                    self._write(", ")
                self._write(f"{key} = {value}")
            self._write(")")
        
        if node.body:
            self._write(f" AS {node.body}")
        elif node.query:
            self._write(" AS (")
            node.query.accept(self)
            self._write(")")

    def visit_alter_table(self, node) -> Any:
        """Visit an ALTER TABLE statement."""
        self._write("ALTER TABLE ")
        node.table.accept(self)
        for i, action in enumerate(node.actions):
            if i > 0:
                self._write(", ")
            else:
                self._write(" ")
            action.accept(self)

    def visit_add_column(self, node) -> Any:
        """Visit an ADD COLUMN action."""
        self._write("ADD COLUMN ")
        if node.if_not_exists:
            self._write("IF NOT EXISTS ")
        self._write(f"{node.column_name} {node.column_type}")
        if node.not_null:
            self._write(" NOT NULL")
        if node.default_value:
            self._write(" DEFAULT ")
            node.default_value.accept(self)

    def visit_drop_column(self, node) -> Any:
        """Visit a DROP COLUMN action."""
        self._write("DROP COLUMN ")
        if node.if_exists:
            self._write("IF EXISTS ")
        self._write(node.column_name)

    def visit_rename_column(self, node) -> Any:
        """Visit a RENAME COLUMN action."""
        self._write(f"RENAME COLUMN {node.old_name} TO {node.new_name}")

    def visit_alter_column(self, node) -> Any:
        """Visit an ALTER COLUMN action."""
        self._write(f"ALTER COLUMN {node.column_name} ")
        if node.set_data_type:
            self._write(f"SET DATA TYPE {node.set_data_type}")
        elif node.set_default:
            self._write("SET DEFAULT ")
            node.set_default.accept(self)
        elif node.drop_default:
            self._write("DROP DEFAULT")
        elif node.set_not_null:
            self._write("SET NOT NULL")
        elif node.drop_not_null:
            self._write("DROP NOT NULL")

    def visit_set_table_options(self, node) -> Any:
        """Visit a SET OPTIONS action."""
        self._write("SET OPTIONS (")
        items = list(node.options.items())
        for i, (key, value) in enumerate(items):
            if i > 0:
                self._write(", ")
            self._write(f"{key} = {value}")
        self._write(")")

    def visit_drop_statement(self, node) -> Any:
        """Visit a DROP statement."""
        self._write(f"DROP {node.object_type} ")
        if node.if_exists:
            self._write("IF EXISTS ")
        if hasattr(node.object_name, 'accept'):
            node.object_name.accept(self)
        else:
            self._write(str(node.object_name))
        if node.cascade:
            self._write(" CASCADE")

    # Scripting Statement visitors (Task 3)
    def visit_declare_statement(self, node) -> Any:
        """Visit a DECLARE statement."""
        self._write("DECLARE ")
        for i, var in enumerate(node.variables):
            if i > 0:
                self._write(", ")
            var.accept(self)

    def visit_variable_declaration(self, node) -> Any:
        """Visit a variable declaration."""
        self._write(f"{node.name} {node.data_type}")
        if node.default_value:
            self._write(" DEFAULT ")
            node.default_value.accept(self)

    def visit_set_statement(self, node) -> Any:
        """Visit a SET statement."""
        self._write(f"SET {node.variable_name} = ")
        node.value.accept(self)

    def visit_if_statement(self, node) -> Any:
        """Visit an IF statement."""
        self._write("IF ")
        node.condition.accept(self)
        self._write(" THEN")
        if self.options.format_style == "expanded":
            self._write("\n")
            self.depth += 1
        
        for stmt in node.then_statements:
            if self.options.format_style == "expanded":
                self._indent()
            stmt.accept(self)
            if self.options.format_style == "expanded":
                self._write("\n")
        
        for elseif in node.elseif_clauses:
            if self.options.format_style == "expanded":
                self.depth -= 1
                self._indent()
            else:
                self._write(" ")
            elseif.accept(self)
        
        if node.else_statements:
            if self.options.format_style == "expanded":
                self.depth -= 1
                self._indent()
                self._write("ELSE\n")
                self.depth += 1
            else:
                self._write(" ELSE ")
            
            for stmt in node.else_statements:
                if self.options.format_style == "expanded":
                    self._indent()
                stmt.accept(self)
                if self.options.format_style == "expanded":
                    self._write("\n")
        
        if self.options.format_style == "expanded":
            self.depth -= 1
            self._indent()
        else:
            self._write(" ")
        self._write("END IF")

    def visit_elseif_clause(self, node) -> Any:
        """Visit an ELSEIF clause."""
        self._write("ELSEIF ")
        node.condition.accept(self)
        self._write(" THEN")
        if self.options.format_style == "expanded":
            self._write("\n")
            self.depth += 1
        
        for stmt in node.statements:
            if self.options.format_style == "expanded":
                self._indent()
            stmt.accept(self)
            if self.options.format_style == "expanded":
                self._write("\n")

    def visit_while_loop(self, node) -> Any:
        """Visit a WHILE loop."""
        if node.label:
            self._write(f"{node.label}: ")
        self._write("WHILE ")
        node.condition.accept(self)
        self._write(" DO")
        if self.options.format_style == "expanded":
            self._write("\n")
            self.depth += 1
        
        for stmt in node.statements:
            if self.options.format_style == "expanded":
                self._indent()
            stmt.accept(self)
            if self.options.format_style == "expanded":
                self._write("\n")
        
        if self.options.format_style == "expanded":
            self.depth -= 1
            self._indent()
        else:
            self._write(" ")
        self._write("END WHILE")

    def visit_for_loop(self, node) -> Any:
        """Visit a FOR loop."""
        if node.label:
            self._write(f"{node.label}: ")
        self._write(f"FOR {node.variable} IN (")
        node.query.accept(self)
        self._write(") DO")
        if self.options.format_style == "expanded":
            self._write("\n")
            self.depth += 1
        
        for stmt in node.statements:
            if self.options.format_style == "expanded":
                self._indent()
            stmt.accept(self)
            if self.options.format_style == "expanded":
                self._write("\n")
        
        if self.options.format_style == "expanded":
            self.depth -= 1
            self._indent()
        else:
            self._write(" ")
        self._write("END FOR")

    def visit_begin_end_block(self, node) -> Any:
        """Visit a BEGIN-END block."""
        if node.label:
            self._write(f"{node.label}: ")
        self._write("BEGIN")
        if self.options.format_style == "expanded":
            self._write("\n")
            self.depth += 1
        
        for stmt in node.statements:
            if self.options.format_style == "expanded":
                self._indent()
            stmt.accept(self)
            if self.options.format_style == "expanded":
                self._write("\n")
        
        if node.exception_handler:
            if self.options.format_style == "expanded":
                self.depth -= 1
                self._indent()
            else:
                self._write(" ")
            node.exception_handler.accept(self)
        
        if self.options.format_style == "expanded":
            self.depth -= 1
            self._indent()
        else:
            self._write(" ")
        self._write("END")

    def visit_exception_handler(self, node) -> Any:
        """Visit an exception handler."""
        self._write("EXCEPTION")
        if node.when_conditions:
            self._write(" WHEN ")
            for i, condition in enumerate(node.when_conditions):
                if i > 0:
                    self._write(" OR ")
                self._write(condition)
        self._write(" THEN")
        if self.options.format_style == "expanded":
            self._write("\n")
            self.depth += 1
        
        for stmt in node.statements:
            if self.options.format_style == "expanded":
                self._indent()
            stmt.accept(self)
            if self.options.format_style == "expanded":
                self._write("\n")

    def visit_break_statement(self, node) -> Any:
        """Visit a BREAK statement."""
        self._write("BREAK")
        if node.label:
            self._write(f" {node.label}")

    def visit_continue_statement(self, node) -> Any:
        """Visit a CONTINUE statement."""
        self._write("CONTINUE")
        if node.label:
            self._write(f" {node.label}")

    def visit_call_statement(self, node) -> Any:
        """Visit a CALL statement."""
        self._write(f"CALL {node.procedure_name}(")
        for i, arg in enumerate(node.arguments):
            if i > 0:
                self._write(", ")
            arg.accept(self)
        self._write(")")
=======
    # BigQuery ML and External Table visitor implementations
    def visit_create_model(self, node) -> Any:
        """Visit a CREATE MODEL statement."""
        self._write("CREATE MODEL ")
        self._write(node.model_name)
        
        if node.options:
            self._write(" OPTIONS(")
            option_parts = []
            for key, value_expr in node.options.items():
                option_parts.append(f"{key}=")
                # Store current position to append value
                current_len = len(option_parts[-1])
                value_sql = self._serialize_expression(value_expr)
                option_parts[-1] += value_sql
            self._write(", ".join(option_parts))
            self._write(")")
        
        if node.transform:
            self._write(" TRANSFORM(")
            node.transform.accept(self)
            self._write(")")
        
        if node.as_query:
            self._write(" AS ")
            node.as_query.accept(self)

    def visit_ml_predict(self, node) -> Any:
        """Visit a ML.PREDICT function call."""
        self._write("ML.PREDICT(MODEL ")
        self._write(node.model_name)
        self._write(", ")
        
        # Handle input data (Select or TableRef)
        node.input_data.accept(self)
        
        if node.struct_options:
            self._write(", STRUCT(")
            option_parts = []
            for key, value_expr in node.struct_options.items():
                value_sql = self._serialize_expression(value_expr)
                option_parts.append(f"{value_sql} AS {key}")
            self._write(", ".join(option_parts))
            self._write(")")
        
        self._write(")")

    def visit_ml_evaluate(self, node) -> Any:
        """Visit a ML.EVALUATE function call."""
        self._write("ML.EVALUATE(MODEL ")
        self._write(node.model_name)
        self._write(", ")
        
        # Handle input data (Select or TableRef)
        node.input_data.accept(self)
        
        if node.struct_options:
            self._write(", STRUCT(")
            option_parts = []
            for key, value_expr in node.struct_options.items():
                value_sql = self._serialize_expression(value_expr)
                option_parts.append(f"{value_sql} AS {key}")
            self._write(", ".join(option_parts))
            self._write(")")
        
        self._write(")")

    def visit_ml_explain(self, node) -> Any:
        """Visit a ML.EXPLAIN_PREDICT function call."""
        self._write("ML.EXPLAIN_PREDICT(MODEL ")
        self._write(node.model_name)
        self._write(", ")
        
        # Handle input data (Select or TableRef)
        node.input_data.accept(self)
        
        if node.struct_options:
            self._write(", STRUCT(")
            option_parts = []
            for key, value_expr in node.struct_options.items():
                value_sql = self._serialize_expression(value_expr)
                option_parts.append(f"{value_sql} AS {key}")
            self._write(", ".join(option_parts))
            self._write(")")
        
        self._write(")")

    def visit_create_external_table(self, node) -> Any:
        """Visit a CREATE EXTERNAL TABLE statement."""
        self._write("CREATE OR REPLACE EXTERNAL TABLE ")
        self._write(node.table_name)
        
        if node.schema:
            self._write(" (")
            self._write(", ".join(node.schema))
            self._write(")")
        
        if node.options:
            self._write(" OPTIONS(")
            option_parts = []
            for key, value_expr in node.options.items():
                value_sql = self._serialize_expression(value_expr)
                option_parts.append(f"{key}={value_sql}")
            self._write(", ".join(option_parts))
            self._write(")")

    def visit_export_data(self, node) -> Any:
        """Visit an EXPORT DATA statement."""
        self._write("EXPORT DATA")
        
        if node.options:
            self._write(" OPTIONS(")
            option_parts = []
            for key, value_expr in node.options.items():
                value_sql = self._serialize_expression(value_expr)
                option_parts.append(f"{key}={value_sql}")
            self._write(", ".join(option_parts))
            self._write(")")
        
        self._write(" AS ")
        node.as_query.accept(self)

    def visit_load_data(self, node) -> Any:
        """Visit a LOAD DATA statement."""
        self._write("LOAD DATA INTO ")
        self._write(node.table_name)
        
        if node.source_uris:
            self._write(" FROM FILES(")
            uri_parts = [f"'{uri}'" for uri in node.source_uris]
            self._write(", ".join(uri_parts))
            self._write(")")
        
        if node.options:
            self._write(" WITH OPTIONS(")
            option_parts = []
            for key, value_expr in node.options.items():
                value_sql = self._serialize_expression(value_expr)
                option_parts.append(f"{key}={value_sql}")
            self._write(", ".join(option_parts))
            self._write(")")

    def _serialize_expression(self, expr) -> str:
        """Helper method to serialize an expression to string."""
        if hasattr(expr, 'accept'):
            # Create a temporary serializer for the expression
            temp_serializer = SQLSerializer(self.options)
            expr.accept(temp_serializer)
            return "".join(temp_serializer.buffer).strip()
        else:
            return str(expr)
>>>>>>> main


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