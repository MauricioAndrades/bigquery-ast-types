"""
BigQuery AST Type Definitions

Complete AST node definitions based on BigQuery lexical specification.
Covers all language constructs from the BigQueryLexicalIdentifiers.md.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

# TODO: Add `from __future__ import annotations` to avoid string literal type hints (see issue #2)
# TODO: Add comprehensive docstrings for all node types with usage examples (see issue #4)
from typing import Any, List, Optional, Union, Dict, Tuple
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod


# Base Classes
@dataclass
class ASTNode(ABC):
    """Base class for all AST nodes."""

    location: Optional["SourceLocation"] = None

    @abstractmethod
    def accept(self, visitor: "ASTVisitor") -> Any:
        """Accept a visitor."""
        pass

    @property
    def node_type(self) -> str:
        """Get the node type name."""
        return self.__class__.__name__


@dataclass
class SourceLocation:
    """Source location information."""

    line: int
    column: int
    offset: int
    length: int


# Enums for various options
class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"


class OrderDirection(Enum):
    ASC = "ASC"
    DESC = "DESC"


class NullsOrder(Enum):
    NULLS_FIRST = "NULLS FIRST"
    NULLS_LAST = "NULLS LAST"


class UnionType(Enum):
    UNION_ALL = "UNION ALL"
    UNION_DISTINCT = "UNION DISTINCT"


class IntervalUnit(Enum):
    MICROSECOND = "MICROSECOND"
    MILLISECOND = "MILLISECOND"
    SECOND = "SECOND"
    MINUTE = "MINUTE"
    HOUR = "HOUR"
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    QUARTER = "QUARTER"
    YEAR = "YEAR"


class DataType(Enum):
    INT64 = "INT64"
    FLOAT64 = "FLOAT64"
    NUMERIC = "NUMERIC"
    BIGNUMERIC = "BIGNUMERIC"
    BOOL = "BOOL"
    STRING = "STRING"
    BYTES = "BYTES"
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"
    GEOGRAPHY = "GEOGRAPHY"
    JSON = "JSON"
    INTERVAL = "INTERVAL"
    ARRAY = "ARRAY"
    STRUCT = "STRUCT"
    RANGE = "RANGE"


# Identifiers
@dataclass
class Identifier(ASTNode):
    """Identifier (quoted or unquoted)."""

    name: str
    quoted: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_identifier(self)


@dataclass
class PathExpression(ASTNode):
    """Path expression for navigating object graphs."""

    parts: List["PathPart"]

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_path_expression(self)


@dataclass
class PathPart(ASTNode):
    """Part of a path expression."""

    identifier: Identifier
    separator: Optional[str] = None  # '/', ':', '-', or '.'

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_path_part(self)


@dataclass
class TableName(ASTNode):
    """Table name (possibly qualified)."""

    table: Identifier
    project: Optional[Identifier] = None
    dataset: Optional[Identifier] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_table_name(self)


# Literals
@dataclass
class Literal(ASTNode):
    """Base class for literals."""

    pass


@dataclass
class StringLiteral(Literal):
    """String literal."""

    value: str
    quote_type: str  # 'single', 'double', 'triple_single', 'triple_double'
    raw: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_string_literal(self)


@dataclass
class BytesLiteral(Literal):
    """Bytes literal."""

    value: bytes
    quote_type: str
    raw: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_bytes_literal(self)


@dataclass
class IntegerLiteral(Literal):
    """Integer literal."""

    value: int
    hex: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_integer_literal(self)


@dataclass
class FloatLiteral(Literal):
    """Floating point literal."""

    value: float

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_float_literal(self)


@dataclass
class NumericLiteral(Literal):
    """NUMERIC literal."""

    value: str  # String representation to preserve precision

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_numeric_literal(self)


@dataclass
class BigNumericLiteral(Literal):
    """BIGNUMERIC literal."""

    value: str

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_bignumeric_literal(self)


@dataclass
class BooleanLiteral(Literal):
    """Boolean literal (TRUE/FALSE)."""

    value: bool

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_boolean_literal(self)


@dataclass
class NullLiteral(Literal):
    """NULL literal."""

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_null_literal(self)


@dataclass
class DateLiteral(Literal):
    """DATE literal."""

    value: str  # In canonical format

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_date_literal(self)


@dataclass
class TimeLiteral(Literal):
    """TIME literal."""

    value: str

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_time_literal(self)


@dataclass
class DatetimeLiteral(Literal):
    """DATETIME literal."""

    value: str

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_datetime_literal(self)


@dataclass
class TimestampLiteral(Literal):
    """TIMESTAMP literal."""

    value: str
    timezone: Optional[str] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_timestamp_literal(self)


@dataclass
class IntervalLiteral(Literal):
    """INTERVAL literal."""

    value: Union[int, str]
    unit: IntervalUnit
    end_unit: Optional[IntervalUnit] = None  # For range intervals

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_interval_literal(self)


@dataclass
class ArrayLiteral(Literal):
    """Array literal."""

    elements: List["Expression"]
    element_type: Optional["TypeExpression"] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_array_literal(self)


@dataclass
class StructLiteral(Literal):
    """Struct literal."""

    fields: List[Tuple[Optional[str], "Expression"]]
    type: Optional["StructType"] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_struct_literal(self)


@dataclass
class RangeLiteral(Literal):
    """RANGE literal."""

    element_type: DataType
    lower_bound: Optional["Expression"]
    upper_bound: Optional["Expression"]

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_range_literal(self)


@dataclass
class JSONLiteral(Literal):
    """JSON literal."""

    value: str

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_json_literal(self)


# Expressions
@dataclass
class Expression(ASTNode):
    """Base class for expressions."""

    pass


@dataclass
class ColumnRef(Expression):
    """Column reference."""

    column: Identifier
    table: Optional[Identifier] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_column_ref(self)


@dataclass
class StarExpression(Expression):
    """SELECT * expression."""

    table: Optional[Identifier] = None
    except_columns: List[Identifier] = field(default_factory=list)
    replace_columns: List["SelectItem"] = field(default_factory=list)

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_star_expression(self)


@dataclass
class BinaryOp(Expression):
    """Binary operation."""

    left: Expression
    operator: str
    right: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_binary_op(self)


@dataclass
class UnaryOp(Expression):
    """Unary operation."""

    operator: str
    operand: Expression
    prefix: bool = True  # True for prefix, False for postfix

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_unary_op(self)


@dataclass
class FunctionCall(Expression):
    """Function call."""

    name: Identifier
    args: List[Expression]
    distinct: bool = False
    ignore_nulls: bool = False
    order_by: Optional[List["OrderByItem"]] = None
    limit: Optional[Expression] = None
    having: Optional[Expression] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_function_call(self)


@dataclass
class WindowFunction(Expression):
    """Window function."""

    function: FunctionCall
    window: "WindowSpecification"

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_window_function(self)


@dataclass
class WindowSpecification(ASTNode):
    """Window specification."""

    partition_by: List[Expression] = field(default_factory=list)
    order_by: List["OrderByItem"] = field(default_factory=list)
    frame: Optional["WindowFrame"] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_window_specification(self)


@dataclass
class WindowFrame(ASTNode):
    """Window frame specification."""

    unit: str  # 'ROWS' or 'RANGE'
    start: "WindowBound"
    end: Optional["WindowBound"] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_window_frame(self)


@dataclass
class WindowBound(ASTNode):
    """Window frame bound."""

    type: str  # 'CURRENT ROW', 'UNBOUNDED PRECEDING', 'UNBOUNDED FOLLOWING', 'PRECEDING', 'FOLLOWING'
    offset: Optional[Expression] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_window_bound(self)


@dataclass
class CastExpression(Expression):
    """CAST or SAFE_CAST expression."""

    expression: Expression
    target_type: "TypeExpression"
    safe: bool = False
    format: Optional[str] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_cast_expression(self)


@dataclass
class ExtractExpression(Expression):
    """EXTRACT expression."""

    part: str
    from_expr: Expression
    at_timezone: Optional[Expression] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_extract_expression(self)


@dataclass
class CaseExpression(Expression):
    """CASE expression."""

    expr: Optional[Expression] = None  # For simple CASE
    when_clauses: List["WhenClause"] = field(default_factory=list)
    else_clause: Optional[Expression] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_case_expression(self)


@dataclass
class WhenClause(ASTNode):
    """WHEN clause in CASE."""

    condition: Expression
    result: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_when_clause(self)


@dataclass
class ArrayAccess(Expression):
    """Array access expression."""

    array: Expression
    index: Expression
    safe: bool = False  # SAFE_OFFSET/SAFE_ORDINAL

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_array_access(self)


@dataclass
class FieldAccess(Expression):
    """Struct field access."""

    struct: Expression
    field: Identifier

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_field_access(self)


@dataclass
class InExpression(Expression):
    """IN expression."""

    expr: Expression
    in_list: Union[List[Expression], "SelectStatement", "Unnest"]
    negated: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_in_expression(self)


@dataclass
class BetweenExpression(Expression):
    """BETWEEN expression."""

    expr: Expression
    low: Expression
    high: Expression
    negated: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_between_expression(self)


@dataclass
class LikeExpression(Expression):
    """LIKE expression."""

    expr: Expression
    pattern: Expression
    escape: Optional[Expression] = None
    negated: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_like_expression(self)


@dataclass
class ExistsExpression(Expression):
    """EXISTS expression."""

    subquery: "SelectStatement"
    negated: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_exists_expression(self)


@dataclass
class SubqueryExpression(Expression):
    """Scalar subquery expression."""

    subquery: "SelectStatement"

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_subquery_expression(self)


@dataclass
class NamedParameter(Expression):
    """Named query parameter (@param)."""

    name: Identifier

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_named_parameter(self)


@dataclass
class PositionalParameter(Expression):
    """Positional query parameter (?)."""

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_positional_parameter(self)


# Type Expressions
@dataclass
class TypeExpression(ASTNode):
    """Base class for type expressions."""

    pass


@dataclass
class SimpleType(TypeExpression):
    """Simple type like INT64, STRING, etc."""

    name: DataType

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_simple_type(self)


@dataclass
class ArrayType(TypeExpression):
    """ARRAY<T> type."""

    element_type: TypeExpression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_array_type(self)


@dataclass
class StructType(TypeExpression):
    """STRUCT type."""

    fields: List[Tuple[Optional[str], TypeExpression]]

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_struct_type(self)


@dataclass
class RangeType(TypeExpression):
    """RANGE<T> type."""

    element_type: TypeExpression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_range_type(self)


# Statements
@dataclass
class Statement(ASTNode):
    """Base class for statements."""

    pass


@dataclass
class SelectStatement(Statement):
    """SELECT statement."""

    with_clause: Optional["WithClause"] = None
    select_list: List["SelectItem"] = field(default_factory=list)
    from_clause: Optional["FromClause"] = None
    where: Optional[Expression] = None
    group_by: Optional["GroupByClause"] = None
    having: Optional[Expression] = None
    qualify: Optional[Expression] = None
    window_clause: Optional[List["NamedWindow"]] = None
    order_by: Optional[List["OrderByItem"]] = None
    limit: Optional[Expression] = None
    offset: Optional[Expression] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_select_statement(self)


@dataclass
class WithClause(ASTNode):
    """WITH clause."""

    ctes: List["CTE"]
    recursive: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_with_clause(self)


@dataclass
class CTE(ASTNode):
    """Common Table Expression."""

    name: Identifier
    columns: Optional[List[Identifier]] = None
    query: Statement

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_cte(self)


@dataclass
class SelectItem(ASTNode):
    """Item in SELECT list."""

    expression: Expression
    alias: Optional[Identifier] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_select_item(self)


@dataclass
class FromClause(ASTNode):
    """FROM clause."""

    tables: List["TableExpression"]

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_from_clause(self)


@dataclass
class TableExpression(ASTNode):
    """Base class for table expressions."""

    alias: Optional[Identifier] = None


@dataclass
class TableReference(TableExpression):
    """Simple table reference."""

    table: TableName
    hints: Optional[List["TableHint"]] = None
    for_system_time: Optional[Expression] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_table_reference(self)


@dataclass
class JoinExpression(TableExpression):
    """JOIN expression."""

    left: TableExpression
    join_type: JoinType
    right: TableExpression
    on_condition: Optional[Expression] = None
    using_columns: Optional[List[Identifier]] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_join_expression(self)


@dataclass
class Unnest(TableExpression):
    """UNNEST expression."""

    expression: Expression
    with_offset: bool = False
    offset_alias: Optional[Identifier] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_unnest(self)


@dataclass
class TableFunction(TableExpression):
    """Table function call."""

    function: FunctionCall

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_table_function(self)


@dataclass
class SubqueryTable(TableExpression):
    """Subquery as table."""

    subquery: SelectStatement

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_subquery_table(self)


@dataclass
class GroupByClause(ASTNode):
    """GROUP BY clause."""

    expressions: List[Expression]
    rollup: Optional[List[Expression]] = None
    cube: Optional[List[Expression]] = None
    grouping_sets: Optional[List[List[Expression]]] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_group_by_clause(self)


@dataclass
class OrderByItem(ASTNode):
    """ORDER BY item."""

    expression: Expression
    direction: OrderDirection = OrderDirection.ASC
    nulls_order: Optional[NullsOrder] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_order_by_item(self)


@dataclass
class NamedWindow(ASTNode):
    """Named window definition."""

    name: Identifier
    window: WindowSpecification

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_named_window(self)


# DML Statements
@dataclass
class InsertStatement(Statement):
    """INSERT statement."""

    table: TableName
    columns: Optional[List[Identifier]] = None
    values: Optional[List[List[Expression]]] = None
    select: Optional[SelectStatement] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_insert_statement(self)


@dataclass
class UpdateStatement(Statement):
    """UPDATE statement."""

    table: TableName
    set_clauses: List["SetClause"]
    from_clause: Optional[FromClause] = None
    where: Optional[Expression] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_update_statement(self)


@dataclass
class SetClause(ASTNode):
    """SET clause in UPDATE."""

    column: Identifier
    value: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_set_clause(self)


@dataclass
class DeleteStatement(Statement):
    """DELETE statement."""

    table: TableName
    where: Optional[Expression] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_delete_statement(self)


@dataclass
class MergeStatement(Statement):
    """MERGE statement."""

    target_table: TableName
    source: TableExpression
    on_condition: Expression
    when_clauses: List["MergeWhenClause"]

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_merge_statement(self)


@dataclass
class MergeWhenClause(ASTNode):
    """WHEN clause in MERGE."""

    match_type: str  # 'MATCHED', 'NOT MATCHED', 'NOT MATCHED BY SOURCE', 'NOT MATCHED BY TARGET'
    condition: Optional[Expression] = None
    action: Union["MergeInsert", "MergeUpdate", "MergeDelete"]

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_merge_when_clause(self)


@dataclass
class MergeInsert(ASTNode):
    """INSERT action in MERGE."""

    columns: Optional[List[Identifier]] = None
    values: Optional[List[Expression]] = None
    row: bool = False  # INSERT ROW

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_merge_insert(self)


@dataclass
class MergeUpdate(ASTNode):
    """UPDATE action in MERGE."""

    set_clauses: List[SetClause]

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_merge_update(self)


@dataclass
class MergeDelete(ASTNode):
    """DELETE action in MERGE."""

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_merge_delete(self)


# DDL Statements
@dataclass
class CreateTableStatement(Statement):
    """CREATE TABLE statement."""

    table: TableName
    columns: List["ColumnDefinition"]
    options: Optional[Dict[str, Expression]] = None
    partition_by: Optional[Expression] = None
    cluster_by: Optional[List[Identifier]] = None
    as_select: Optional[SelectStatement] = None
    if_not_exists: bool = False
    or_replace: bool = False
    temporary: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_create_table_statement(self)


@dataclass
class ColumnDefinition(ASTNode):
    """Column definition in CREATE TABLE."""

    name: Identifier
    data_type: TypeExpression
    not_null: bool = False
    default: Optional[Expression] = None
    options: Optional[Dict[str, Expression]] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_column_definition(self)


# Set Operations
@dataclass
class SetOperation(Statement):
    """Set operation (UNION, INTERSECT, EXCEPT)."""

    left: SelectStatement
    operator: UnionType
    right: SelectStatement

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_set_operation(self)


# Special Features
@dataclass
class TableHint(ASTNode):
    """Table hint."""

    name: str
    value: Optional[str] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_table_hint(self)


@dataclass
class Script(ASTNode):
    """SQL script containing multiple statements."""

    statements: List[Statement]

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_script(self)


# Visitor Interface
class ASTVisitor(ABC):
    """Visitor interface for AST nodes."""

    def visit_identifier(self, node: Identifier) -> Any:
        pass

    def visit_path_expression(self, node: PathExpression) -> Any:
        pass

    def visit_table_name(self, node: TableName) -> Any:
        pass


    def generic_visit(self, node: ASTNode) -> Any:
        """Default visit method."""
        pass

    def is_scan(self, node: ASTNode) -> bool:
        return "Scan" in node.__class__.__name__

    def is_expression(self, node: ASTNode) -> bool:
        return "Expr" in node.__class__.__name__ or "Expression" in node.__class__.__name__


    def is_statement(self, node: ASTNode) -> bool:
        return "Statement" in node.__class__.__name__ or "Stmt" in node.__class__.__name__
