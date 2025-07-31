"""
BigQuery AST Type Definitions

Complete AST node definitions based on BigQuery lexical specification.
Covers all language constructs from the BigQueryLexicalIdentifiers.md.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""
from typing import Any, List, Optional, Union, Dict, Tuple
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod


# Base Classes
class ASTNode(ABC):
    """Base class for all AST nodes."""
    def __init__(self, location: Optional['SourceLocation'] = None):
        self.location = location
    
    @abstractmethod
    def accept(self, visitor: 'ASTVisitor') -> Any:
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
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_identifier(self)


@dataclass
class PathExpression(ASTNode):
    """Path expression for navigating object graphs."""
    parts: List['PathPart']
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_path_expression(self)


@dataclass 
class PathPart(ASTNode):
    """Part of a path expression."""
    identifier: Identifier
    separator: Optional[str] = None  # '/', ':', '-', or '.'
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_path_part(self)


@dataclass
class TableName(ASTNode):
    """Table name (possibly qualified)."""
    table: Identifier
    project: Optional[Identifier] = None
    dataset: Optional[Identifier] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
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
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_string_literal(self)


@dataclass
class BytesLiteral(Literal):
    """Bytes literal."""
    value: bytes
    quote_type: str
    raw: bool = False
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_bytes_literal(self)


@dataclass
class IntegerLiteral(Literal):
    """Integer literal."""
    value: int
    hex: bool = False
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_integer_literal(self)


@dataclass
class FloatLiteral(Literal):
    """Floating point literal."""
    value: float
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_float_literal(self)


@dataclass
class NumericLiteral(Literal):
    """NUMERIC literal."""
    value: str  # String representation to preserve precision
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_numeric_literal(self)


@dataclass
class BigNumericLiteral(Literal):
    """BIGNUMERIC literal."""
    value: str
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_bignumeric_literal(self)


@dataclass
class BooleanLiteral(Literal):
    """Boolean literal (TRUE/FALSE)."""
    value: bool
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_boolean_literal(self)


@dataclass
class NullLiteral(Literal):
    """NULL literal."""
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_null_literal(self)


@dataclass
class DateLiteral(Literal):
    """DATE literal."""
    value: str  # In canonical format
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_date_literal(self)


@dataclass
class TimeLiteral(Literal):
    """TIME literal."""
    value: str
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_time_literal(self)


@dataclass
class DatetimeLiteral(Literal):
    """DATETIME literal."""
    value: str
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_datetime_literal(self)


@dataclass
class TimestampLiteral(Literal):
    """TIMESTAMP literal."""
    value: str
    timezone: Optional[str] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_timestamp_literal(self)


@dataclass
class IntervalLiteral(Literal):
    """INTERVAL literal."""
    value: Union[int, str]
    unit: IntervalUnit
    end_unit: Optional[IntervalUnit] = None  # For range intervals
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_interval_literal(self)


@dataclass
class ArrayLiteral(Literal):
    """Array literal."""
    elements: List['Expression']
    element_type: Optional['TypeExpression'] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_array_literal(self)


@dataclass
class StructLiteral(Literal):
    """Struct literal."""
    fields: List[Tuple[Optional[str], 'Expression']]
    type: Optional['StructType'] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_struct_literal(self)


@dataclass
class RangeLiteral(Literal):
    """RANGE literal."""
    element_type: DataType
    lower_bound: Optional['Expression']
    upper_bound: Optional['Expression']
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_range_literal(self)


@dataclass
class JSONLiteral(Literal):
    """JSON literal."""
    value: str
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
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
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_column_ref(self)


@dataclass
class StarExpression(Expression):
    """SELECT * expression."""
    table: Optional[Identifier] = None
    except_columns: List[Identifier] = field(default_factory=list)
    replace_columns: List['SelectItem'] = field(default_factory=list)
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_star_expression(self)


@dataclass
class BinaryOp(Expression):
    """Binary operation."""
    left: Expression
    operator: str
    right: Expression
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_binary_op(self)


@dataclass
class UnaryOp(Expression):
    """Unary operation."""
    operator: str
    operand: Expression
    prefix: bool = True  # True for prefix, False for postfix
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_unary_op(self)


@dataclass
class FunctionCall(Expression):
    """Function call."""
    name: Identifier
    args: List[Expression]
    distinct: bool = False
    ignore_nulls: bool = False
    order_by: Optional[List['OrderByItem']] = None
    limit: Optional[Expression] = None
    having: Optional[Expression] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_function_call(self)


@dataclass
class WindowFunction(Expression):
    """Window function."""
    function: FunctionCall
    window: 'WindowSpecification'
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_window_function(self)


@dataclass
class WindowSpecification(ASTNode):
    """Window specification."""
    partition_by: List[Expression] = field(default_factory=list)
    order_by: List['OrderByItem'] = field(default_factory=list)
    frame: Optional['WindowFrame'] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_window_specification(self)


@dataclass
class WindowFrame(ASTNode):
    """Window frame specification."""
    unit: str  # 'ROWS' or 'RANGE'
    start: 'WindowBound'
    end: Optional['WindowBound'] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_window_frame(self)


@dataclass
class WindowBound(ASTNode):
    """Window frame bound."""
    type: str  # 'CURRENT ROW', 'UNBOUNDED PRECEDING', 'UNBOUNDED FOLLOWING', 'PRECEDING', 'FOLLOWING'
    offset: Optional[Expression] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_window_bound(self)


@dataclass
class CastExpression(Expression):
    """CAST or SAFE_CAST expression."""
    expression: Expression
    target_type: 'TypeExpression'
    safe: bool = False
    format: Optional[str] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_cast_expression(self)


@dataclass
class ExtractExpression(Expression):
    """EXTRACT expression."""
    part: str
    from_expr: Expression
    at_timezone: Optional[Expression] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_extract_expression(self)


@dataclass
class CaseExpression(Expression):
    """CASE expression."""
    expr: Optional[Expression] = None  # For simple CASE
    when_clauses: List['WhenClause'] = field(default_factory=list)
    else_clause: Optional[Expression] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_case_expression(self)


@dataclass
class WhenClause(ASTNode):
    """WHEN clause in CASE."""
    condition: Expression
    result: Expression
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_when_clause(self)


@dataclass
class ArrayAccess(Expression):
    """Array access expression."""
    array: Expression
    index: Expression
    safe: bool = False  # SAFE_OFFSET/SAFE_ORDINAL
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_array_access(self)


@dataclass
class FieldAccess(Expression):
    """Struct field access."""
    struct: Expression
    field: Identifier
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_field_access(self)


@dataclass
class InExpression(Expression):
    """IN expression."""
    expr: Expression
    in_list: Union[List[Expression], 'SelectStatement', 'Unnest']
    negated: bool = False
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_in_expression(self)


@dataclass
class BetweenExpression(Expression):
    """BETWEEN expression."""
    expr: Expression
    low: Expression
    high: Expression
    negated: bool = False
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_between_expression(self)


@dataclass
class LikeExpression(Expression):
    """LIKE expression."""
    expr: Expression
    pattern: Expression
    escape: Optional[Expression] = None
    negated: bool = False
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_like_expression(self)


@dataclass
class ExistsExpression(Expression):
    """EXISTS expression."""
    subquery: 'SelectStatement'
    negated: bool = False
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_exists_expression(self)


@dataclass
class SubqueryExpression(Expression):
    """Scalar subquery expression."""
    subquery: 'SelectStatement'
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_subquery_expression(self)


@dataclass
class NamedParameter(Expression):
    """Named query parameter (@param)."""
    name: Identifier
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_named_parameter(self)


@dataclass
class PositionalParameter(Expression):
    """Positional query parameter (?)."""
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
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
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_simple_type(self)


@dataclass
class ArrayType(TypeExpression):
    """ARRAY<T> type."""
    element_type: TypeExpression
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_array_type(self)


@dataclass
class StructType(TypeExpression):
    """STRUCT type."""
    fields: List[Tuple[Optional[str], TypeExpression]]
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_struct_type(self)


@dataclass
class RangeType(TypeExpression):
    """RANGE<T> type."""
    element_type: TypeExpression
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_range_type(self)


# Statements
@dataclass
class Statement(ASTNode):
    """Base class for statements."""
    pass


@dataclass
class SelectStatement(Statement):
    """SELECT statement."""
    with_clause: Optional['WithClause'] = None
    select_list: List['SelectItem'] = field(default_factory=list)
    from_clause: Optional['FromClause'] = None
    where: Optional[Expression] = None
    group_by: Optional['GroupByClause'] = None
    having: Optional[Expression] = None
    qualify: Optional[Expression] = None
    window_clause: Optional[List['NamedWindow']] = None
    order_by: Optional[List['OrderByItem']] = None
    limit: Optional[Expression] = None
    offset: Optional[Expression] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_select_statement(self)


@dataclass
class WithClause(ASTNode):
    """WITH clause."""
    ctes: List['CTE']
    recursive: bool = False
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_with_clause(self)


@dataclass
class CTE(ASTNode):
    """Common Table Expression."""
    name: Identifier
    query: Statement
    columns: Optional[List[Identifier]] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_cte(self)


@dataclass
class SelectItem(ASTNode):
    """Item in SELECT list."""
    expression: Expression
    alias: Optional[Identifier] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_select_item(self)


@dataclass
class FromClause(ASTNode):
    """FROM clause."""
    tables: List['TableExpression']
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_from_clause(self)


class TableExpression(ASTNode):
    """Base class for table expressions."""
    def __init__(self, alias: Optional[Identifier] = None, location: Optional['SourceLocation'] = None):
        super().__init__(location)
        self.alias = alias
    
    
@dataclass
class TableReference(TableExpression):
    """Simple table reference."""
    table: TableName
    hints: Optional[List['TableHint']] = None
    for_system_time: Optional[Expression] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_table_reference(self)


@dataclass
class JoinExpression(TableExpression):
    """JOIN expression."""
    left: TableExpression
    join_type: JoinType
    right: TableExpression
    on_condition: Optional[Expression] = None
    using_columns: Optional[List[Identifier]] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_join_expression(self)


@dataclass
class Unnest(TableExpression):
    """UNNEST expression."""
    expression: Expression
    with_offset: bool = False
    offset_alias: Optional[Identifier] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_unnest(self)


@dataclass
class TableFunction(TableExpression):
    """Table function call."""
    function: FunctionCall
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_table_function(self)


@dataclass
class SubqueryTable(TableExpression):
    """Subquery as table."""
    subquery: SelectStatement
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_subquery_table(self)


@dataclass
class GroupByClause(ASTNode):
    """GROUP BY clause."""
    expressions: List[Expression]
    rollup: Optional[List[Expression]] = None
    cube: Optional[List[Expression]] = None
    grouping_sets: Optional[List[List[Expression]]] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_group_by_clause(self)


@dataclass
class OrderByItem(ASTNode):
    """ORDER BY item."""
    expression: Expression
    direction: OrderDirection = OrderDirection.ASC
    nulls_order: Optional[NullsOrder] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_order_by_item(self)


@dataclass
class NamedWindow(ASTNode):
    """Named window definition."""
    name: Identifier
    window: WindowSpecification
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_named_window(self)


# DML Statements
@dataclass
class InsertStatement(Statement):
    """INSERT statement."""
    table: TableName
    columns: Optional[List[Identifier]] = None
    values: Optional[List[List[Expression]]] = None
    select: Optional[SelectStatement] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_insert_statement(self)


@dataclass
class UpdateStatement(Statement):
    """UPDATE statement."""
    table: TableName
    set_clauses: List['SetClause']
    from_clause: Optional[FromClause] = None
    where: Optional[Expression] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_update_statement(self)


@dataclass
class SetClause(ASTNode):
    """SET clause in UPDATE."""
    column: Identifier
    value: Expression
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_set_clause(self)


@dataclass
class DeleteStatement(Statement):
    """DELETE statement."""
    table: TableName
    where: Optional[Expression] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_delete_statement(self)


@dataclass
class MergeStatement(Statement):
    """MERGE statement."""
    target_table: TableName
    source: TableExpression
    on_condition: Expression
    when_clauses: List['MergeWhenClause']
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_merge_statement(self)


@dataclass
class MergeWhenClause(ASTNode):
    """WHEN clause in MERGE."""
    match_type: str  # 'MATCHED', 'NOT MATCHED', 'NOT MATCHED BY SOURCE', 'NOT MATCHED BY TARGET'
    action: Union['MergeInsert', 'MergeUpdate', 'MergeDelete']
    condition: Optional[Expression] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_merge_when_clause(self)


@dataclass
class MergeInsert(ASTNode):
    """INSERT action in MERGE."""
    columns: Optional[List[Identifier]] = None
    values: Optional[List[Expression]] = None
    row: bool = False  # INSERT ROW
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_merge_insert(self)


@dataclass
class MergeUpdate(ASTNode):
    """UPDATE action in MERGE."""
    set_clauses: List[SetClause]
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_merge_update(self)


@dataclass
class MergeDelete(ASTNode):
    """DELETE action in MERGE."""
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_merge_delete(self)


# DDL Statements
@dataclass
class CreateTableStatement(Statement):
    """CREATE TABLE statement."""
    table: TableName
    columns: List['ColumnDefinition']
    options: Optional[Dict[str, Expression]] = None
    partition_by: Optional[Expression] = None
    cluster_by: Optional[List[Identifier]] = None
    as_select: Optional[SelectStatement] = None
    if_not_exists: bool = False
    or_replace: bool = False
    temporary: bool = False
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_create_table_statement(self)


@dataclass
class ColumnDefinition(ASTNode):
    """Column definition in CREATE TABLE."""
    name: Identifier
    data_type: TypeExpression
    not_null: bool = False
    default: Optional[Expression] = None
    options: Optional[Dict[str, Expression]] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_column_definition(self)


# Set Operations
@dataclass
class SetOperation(Statement):
    """Set operation (UNION, INTERSECT, EXCEPT)."""
    left: SelectStatement
    operator: UnionType
    right: SelectStatement
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_set_operation(self)


# Special Features
@dataclass
class TableHint(ASTNode):
    """Table hint."""
    name: str
    value: Optional[str] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_table_hint(self)


@dataclass
class Script(ASTNode):
    """SQL script containing multiple statements."""
    statements: List[Statement]
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_script(self)


# Visitor Interface
class ASTVisitor(ABC):
    """Visitor interface for AST nodes."""
    
    def visit_identifier(self, node: Identifier) -> Any:
        pass
    
    def visit_unquoted_identifier(self, node: 'UnquotedIdentifier') -> Any:
        pass
    
    def visit_quoted_identifier(self, node: 'QuotedIdentifier') -> Any:
        pass
    
    def visit_path_expression(self, node: PathExpression) -> Any:
        pass
    
    def visit_path_part(self, node: PathPart) -> Any:
        pass
    
    def visit_table_name(self, node: TableName) -> Any:
        pass
    
    def visit_column_name(self, node: 'ColumnName') -> Any:
        pass
    
    def visit_field_name(self, node: 'FieldName') -> Any:
        pass
    
    # Literal visitors
    def visit_string_literal(self, node: StringLiteral) -> Any:
        pass
    
    def visit_bytes_literal(self, node: BytesLiteral) -> Any:
        pass
    
    def visit_integer_literal(self, node: IntegerLiteral) -> Any:
        pass
    
    def visit_float_literal(self, node: FloatLiteral) -> Any:
        pass
    
    def visit_numeric_literal(self, node: NumericLiteral) -> Any:
        pass
    
    def visit_bignumeric_literal(self, node: BigNumericLiteral) -> Any:
        pass
    
    def visit_boolean_literal(self, node: BooleanLiteral) -> Any:
        pass
    
    def visit_null_literal(self, node: NullLiteral) -> Any:
        pass
    
    def visit_date_literal(self, node: DateLiteral) -> Any:
        pass
    
    def visit_time_literal(self, node: TimeLiteral) -> Any:
        pass
    
    def visit_datetime_literal(self, node: DatetimeLiteral) -> Any:
        pass
    
    def visit_timestamp_literal(self, node: TimestampLiteral) -> Any:
        pass
    
    def visit_interval_literal(self, node: IntervalLiteral) -> Any:
        pass
    
    def visit_array_literal(self, node: ArrayLiteral) -> Any:
        pass
    
    def visit_struct_literal(self, node: StructLiteral) -> Any:
        pass
    
    def visit_range_literal(self, node: RangeLiteral) -> Any:
        pass
    
    def visit_json_literal(self, node: JSONLiteral) -> Any:
        pass
    
    # Expression visitors
    def visit_column_ref(self, node: ColumnRef) -> Any:
        pass
    
    def visit_star_expression(self, node: StarExpression) -> Any:
        pass
    
    def visit_binary_op(self, node: BinaryOp) -> Any:
        pass
    
    def visit_unary_op(self, node: UnaryOp) -> Any:
        pass
    
    def visit_function_call(self, node: FunctionCall) -> Any:
        pass
    
    def visit_window_function(self, node: WindowFunction) -> Any:
        pass
    
    def visit_window_specification(self, node: WindowSpecification) -> Any:
        pass
    
    def visit_window_frame(self, node: WindowFrame) -> Any:
        pass
    
    def visit_window_bound(self, node: WindowBound) -> Any:
        pass
    
    def visit_cast_expression(self, node: CastExpression) -> Any:
        pass
    
    def visit_extract_expression(self, node: ExtractExpression) -> Any:
        pass
    
    def visit_case_expression(self, node: CaseExpression) -> Any:
        pass
    
    def visit_when_clause(self, node: WhenClause) -> Any:
        pass
    
    def visit_array_access(self, node: ArrayAccess) -> Any:
        pass
    
    def visit_field_access(self, node: FieldAccess) -> Any:
        pass
    
    def visit_in_expression(self, node: InExpression) -> Any:
        pass
    
    def visit_between_expression(self, node: BetweenExpression) -> Any:
        pass
    
    def visit_like_expression(self, node: LikeExpression) -> Any:
        pass
    
    def visit_exists_expression(self, node: ExistsExpression) -> Any:
        pass
    
    def visit_subquery_expression(self, node: SubqueryExpression) -> Any:
        pass
    
    def visit_named_parameter(self, node: NamedParameter) -> Any:
        pass
    
    def visit_positional_parameter(self, node: PositionalParameter) -> Any:
        pass
    
    # Type visitors
    def visit_simple_type(self, node: SimpleType) -> Any:
        pass
    
    def visit_array_type(self, node: ArrayType) -> Any:
        pass
    
    def visit_struct_type(self, node: StructType) -> Any:
        pass
    
    def visit_range_type(self, node: RangeType) -> Any:
        pass
    
    # Statement visitors
    def visit_select_statement(self, node: SelectStatement) -> Any:
        pass
    
    def visit_with_clause(self, node: WithClause) -> Any:
        pass
    
    def visit_cte(self, node: CTE) -> Any:
        pass
    
    def visit_select_item(self, node: SelectItem) -> Any:
        pass
    
    def visit_from_clause(self, node: FromClause) -> Any:
        pass
    
    def visit_table_reference(self, node: TableReference) -> Any:
        pass
    
    def visit_join_expression(self, node: JoinExpression) -> Any:
        pass
    
    def visit_unnest(self, node: Unnest) -> Any:
        pass
    
    def visit_table_function(self, node: TableFunction) -> Any:
        pass
    
    def visit_subquery_table(self, node: SubqueryTable) -> Any:
        pass
    
    def visit_group_by_clause(self, node: GroupByClause) -> Any:
        pass
    
    def visit_order_by_item(self, node: OrderByItem) -> Any:
        pass
    
    def visit_named_window(self, node: NamedWindow) -> Any:
        pass
    
    # DML statement visitors
    def visit_insert_statement(self, node: InsertStatement) -> Any:
        pass
    
    def visit_update_statement(self, node: UpdateStatement) -> Any:
        pass
    
    def visit_set_clause(self, node: SetClause) -> Any:
        pass
    
    def visit_delete_statement(self, node: DeleteStatement) -> Any:
        pass
    
    def visit_merge_statement(self, node: MergeStatement) -> Any:
        pass
    
    def visit_merge_when_clause(self, node: MergeWhenClause) -> Any:
        pass
    
    def visit_merge_insert(self, node: MergeInsert) -> Any:
        pass
    
    def visit_merge_update(self, node: MergeUpdate) -> Any:
        pass
    
    def visit_merge_delete(self, node: MergeDelete) -> Any:
        pass
    
    # DDL statement visitors
    def visit_create_table_statement(self, node: CreateTableStatement) -> Any:
        pass
    
    def visit_column_definition(self, node: ColumnDefinition) -> Any:
        pass
    
    # Set operation visitors
    def visit_set_operation(self, node: SetOperation) -> Any:
        pass
    
    # Special feature visitors
    def visit_table_hint(self, node: TableHint) -> Any:
        pass
    
    def visit_script(self, node: Script) -> Any:
        pass
    
    def visit_comment(self, node: 'Comment') -> Any:
        pass
    
    # Additional visitor methods for extended BigQuery support
    def visit_create_view_statement(self, node: 'CreateViewStatement') -> Any:
        pass

    def visit_create_function_statement(self, node: 'CreateFunctionStatement') -> Any:
        pass

    def visit_function_parameter(self, node: 'FunctionParameter') -> Any:
        pass

    def visit_create_procedure_statement(self, node: 'CreateProcedureStatement') -> Any:
        pass

    def visit_procedure_parameter(self, node: 'ProcedureParameter') -> Any:
        pass

    def visit_drop_statement(self, node: 'DropStatement') -> Any:
        pass

    def visit_truncate_statement(self, node: 'TruncateStatement') -> Any:
        pass

    def visit_pivot_expression(self, node: 'PivotExpression') -> Any:
        pass

    def visit_unpivot_expression(self, node: 'UnpivotExpression') -> Any:
        pass

    def visit_array_subquery_expression(self, node: 'ArraySubqueryExpression') -> Any:
        pass

    def visit_struct_expression(self, node: 'StructExpression') -> Any:
        pass
    
    # Additional visitor methods for extended BigQuery support
    def visit_for_system_time_as_of_expression(self, node: 'ForSystemTimeAsOfExpression') -> Any:
        pass
    
    def visit_qualify_clause(self, node: 'QualifyClause') -> Any:
        pass
    
    def visit_create_schema_statement(self, node: 'CreateSchemaStatement') -> Any:
        pass
    
    def visit_alter_table_statement(self, node: 'AlterTableStatement') -> Any:
        pass
    
    def visit_add_column_action(self, node: 'AddColumnAction') -> Any:
        pass
    
    def visit_drop_column_action(self, node: 'DropColumnAction') -> Any:
        pass
    
    def visit_rename_column_action(self, node: 'RenameColumnAction') -> Any:
        pass
    
    def visit_set_options_action(self, node: 'SetOptionsAction') -> Any:
        pass
    
    def visit_export_data_statement(self, node: 'ExportDataStatement') -> Any:
        pass
    
    def visit_call_statement(self, node: 'CallStatement') -> Any:
        pass
    
    def visit_execute_immediate_statement(self, node: 'ExecuteImmediateStatement') -> Any:
        pass
    
    def visit_begin_statement(self, node: 'BeginStatement') -> Any:
        pass
    
    def visit_commit_statement(self, node: 'CommitStatement') -> Any:
        pass
    
    def visit_rollback_statement(self, node: 'RollbackStatement') -> Any:
        pass
    
    def visit_assert_statement(self, node: 'AssertStatement') -> Any:
        pass
    
    def visit_clone_data_statement(self, node: 'CloneDataStatement') -> Any:
        pass
    
    def visit_analytic_function_call(self, node: 'AnalyticFunctionCall') -> Any:
        pass
    
    def visit_system_variable_expression(self, node: 'SystemVariableExpression') -> Any:
        pass
    
    def visit_session_variable_expression(self, node: 'SessionVariableExpression') -> Any:
        pass
    
    # Additional BigQuery-specific visitors
    def visit_table_sample_expression(self, node: 'TableSampleExpression') -> Any:
        pass
    
    def visit_create_model_statement(self, node: 'CreateModelStatement') -> Any:
        pass
    
    def visit_ml_predict_expression(self, node: 'MLPredictExpression') -> Any:
        pass
    
    def visit_safe_navigation_expression(self, node: 'SafeNavigationExpression') -> Any:
        pass
    
    def visit_generate_array_expression(self, node: 'GenerateArrayExpression') -> Any:
        pass
    
    def visit_generate_date_array_expression(self, node: 'GenerateDateArrayExpression') -> Any:
        pass
    
    def visit_with_partition_columns_clause(self, node: 'WithPartitionColumnsClause') -> Any:
        pass
    
    def visit_define_table_statement(self, node: 'DefineTableStatement') -> Any:
        pass
    
    def visit_load_data_statement(self, node: 'LoadDataStatement') -> Any:
        pass
    
    def visit_geography_literal(self, node: 'GeographyLiteral') -> Any:
        pass
    
    def visit_new_expression(self, node: 'NewExpression') -> Any:
        pass
    
    def visit_descriptor_expression(self, node: 'DescriptorExpression') -> Any:
        pass
    
    def visit_flatten_expression(self, node: 'FlattenExpression') -> Any:
        pass
    
    def visit_with_connection_clause(self, node: 'WithConnectionClause') -> Any:
        pass
    
    def visit_replace_fields_expression(self, node: 'ReplaceFieldsExpression') -> Any:
        pass
    
    def visit_add_to_fields_expression(self, node: 'AddToFieldsExpression') -> Any:
        pass
    
    def visit_drop_fields_expression(self, node: 'DropFieldsExpression') -> Any:
        pass
    
    def generic_visit(self, node: ASTNode) -> Any:
        """Default visit method."""
        pass


# Additional AST Node types to complete BigQuery support

@dataclass
class UnquotedIdentifier(ASTNode):
    """Unquoted identifier (starts with letter/underscore, letters/numbers/underscores)."""
    name: str

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_unquoted_identifier(self)

@dataclass
class QuotedIdentifier(ASTNode):
    """Quoted identifier (enclosed by backticks, may contain any chars)."""
    name: str  # Unescaped value

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_quoted_identifier(self)

@dataclass
class ColumnName(ASTNode):
    """Column name (quoted or unquoted, may support dash in some contexts)."""
    identifier: Identifier

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_column_name(self)

@dataclass
class FieldName(ASTNode):
    """Field name inside struct/JSON (inherits column rules)."""
    identifier: Identifier

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_field_name(self)

@dataclass
class Comment(ASTNode):
    """SQL comment: single-line (#, --) or multi-line (/* ... */)."""
    value: str
    multiline: bool = False
    style: str = "#"  # "#", "--", or "/*"

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_comment(self)

# Additional DDL Statements for complete BigQuery support

@dataclass
class CreateViewStatement(Statement):
    """CREATE VIEW statement."""
    view: TableName
    columns: Optional[List[Identifier]] = None
    as_select: Optional[SelectStatement] = None
    or_replace: bool = False
    if_not_exists: bool = False
    temporary: bool = False
    materialized: bool = False
    options: Optional[Dict[str, Expression]] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_create_view_statement(self)

@dataclass
class CreateFunctionStatement(Statement):
    """CREATE FUNCTION statement."""
    function_name: TableName
    parameters: List['FunctionParameter'] = field(default_factory=list)
    returns: Optional[TypeExpression] = None
    language: Optional[str] = None
    body: Optional[str] = None
    options: Optional[Dict[str, Expression]] = None
    or_replace: bool = False
    if_not_exists: bool = False
    temporary: bool = False
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_create_function_statement(self)

@dataclass
class FunctionParameter(ASTNode):
    """Function parameter definition."""
    name: Optional[Identifier] = None
    data_type: Optional[TypeExpression] = None
    default: Optional[Expression] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_function_parameter(self)

@dataclass
class CreateProcedureStatement(Statement):
    """CREATE PROCEDURE statement."""
    procedure_name: TableName
    parameters: List['ProcedureParameter'] = field(default_factory=list)
    options: Optional[Dict[str, Expression]] = None
    or_replace: bool = False
    body: Optional[str] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_create_procedure_statement(self)

@dataclass
class ProcedureParameter(ASTNode):
    """Procedure parameter definition."""
    name: Identifier
    data_type: TypeExpression
    mode: str = "IN"  # IN, OUT, INOUT
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_procedure_parameter(self)

@dataclass
class DropStatement(Statement):
    """DROP statement for tables, views, functions, etc."""
    object_type: str  # TABLE, VIEW, FUNCTION, PROCEDURE, etc.
    object_name: TableName
    if_exists: bool = False
    cascade: bool = False
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_drop_statement(self)

@dataclass
class TruncateStatement(Statement):
    """TRUNCATE TABLE statement."""
    table: TableName
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_truncate_statement(self)

# Additional expression types

@dataclass
class PivotExpression(Expression):
    """PIVOT expression."""
    aggregate_list: List[FunctionCall]
    for_clause: Expression
    in_clause: List[Expression]
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_pivot_expression(self)

@dataclass
class UnpivotExpression(Expression):
    """UNPIVOT expression."""
    value_column: Identifier
    name_column: Identifier
    in_clause: List[Expression]
    include_nulls: bool = False
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_unpivot_expression(self)

@dataclass
class ArraySubqueryExpression(Expression):
    """ARRAY(subquery) expression."""
    subquery: SelectStatement
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_array_subquery_expression(self)

@dataclass
class StructExpression(Expression):
    """STRUCT constructor expression."""
    fields: List[Tuple[Optional[str], Expression]]
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_struct_expression(self)


# Additional BigQuery-specific constructs
@dataclass
class ForSystemTimeAsOfExpression(ASTNode):
    """FOR SYSTEM_TIME AS OF expression."""
    timestamp_expression: Expression
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_for_system_time_as_of_expression(self)


@dataclass
class QualifyClause(ASTNode):
    """QUALIFY clause for window functions."""
    condition: Expression
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_qualify_clause(self)


@dataclass
class CreateSchemaStatement(Statement):
    """CREATE SCHEMA statement."""
    schema_name: Identifier
    if_not_exists: bool = False
    options: Optional[Dict[str, Expression]] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_create_schema_statement(self)


@dataclass
class AlterTableStatement(Statement):
    """ALTER TABLE statement."""
    table: TableName
    actions: List['AlterAction']
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_alter_table_statement(self)


@dataclass
class AlterAction(ASTNode):
    """Base class for ALTER TABLE actions."""
    pass


@dataclass
class AddColumnAction(AlterAction):
    """ADD COLUMN action."""
    column: ColumnDefinition
    if_not_exists: bool = False
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_add_column_action(self)


@dataclass
class DropColumnAction(AlterAction):
    """DROP COLUMN action."""
    column_name: Identifier
    if_exists: bool = False
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_drop_column_action(self)


@dataclass
class RenameColumnAction(AlterAction):
    """RENAME COLUMN action."""
    old_name: Identifier
    new_name: Identifier
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_rename_column_action(self)


@dataclass
class SetOptionsAction(AlterAction):
    """SET OPTIONS action."""
    options: Dict[str, Expression]
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_set_options_action(self)


@dataclass
class ExportDataStatement(Statement):
    """EXPORT DATA statement."""
    query: SelectStatement
    destination_uris: List[StringLiteral]
    options: Optional[Dict[str, Expression]] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_export_data_statement(self)


@dataclass
class CallStatement(Statement):
    """CALL procedure statement."""
    procedure_name: TableName
    arguments: List[Expression] = field(default_factory=list)
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_call_statement(self)


@dataclass
class ExecuteImmediateStatement(Statement):
    """EXECUTE IMMEDIATE statement."""
    sql_expression: Expression
    into_clause: Optional[List[Identifier]] = None
    using_clause: Optional[List[Expression]] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_execute_immediate_statement(self)


@dataclass
class BeginStatement(Statement):
    """BEGIN statement for transaction/block."""
    transaction: bool = False
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_begin_statement(self)


@dataclass
class CommitStatement(Statement):
    """COMMIT statement."""
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_commit_statement(self)


@dataclass
class RollbackStatement(Statement):
    """ROLLBACK statement."""
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_rollback_statement(self)


@dataclass
class AssertStatement(Statement):
    """ASSERT statement."""
    condition: Expression
    message: Optional[StringLiteral] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_assert_statement(self)


@dataclass
class CloneDataStatement(Statement):
    """CREATE TABLE ... CLONE statement."""
    target_table: TableName
    source_table: TableName
    snapshot_time: Optional[Expression] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_clone_data_statement(self)


@dataclass
class AnalyticFunctionCall(Expression):
    """Analytic function call (window function)."""
    function: FunctionCall
    over_clause: WindowSpecification
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_analytic_function_call(self)


@dataclass
class SystemVariableExpression(Expression):
    """System variable like @@project_id."""
    variable_name: str
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_system_variable_expression(self)


@dataclass
class SessionVariableExpression(Expression):
    """Session variable like @@session.time_zone."""
    variable_name: str
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_session_variable_expression(self)


# BigQuery-specific table sampling and ML constructs
@dataclass
class TableSampleExpression(ASTNode):
    """TABLESAMPLE clause."""
    method: str  # BERNOULLI or SYSTEM
    percent: Expression
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_table_sample_expression(self)


@dataclass
class CreateModelStatement(Statement):
    """CREATE MODEL statement for BigQuery ML."""
    model_name: TableName
    options: Dict[str, Expression]
    as_select: Optional[SelectStatement] = None
    or_replace: bool = False
    if_not_exists: bool = False
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_create_model_statement(self)


@dataclass
class MLPredictExpression(Expression):
    """ML.PREDICT function call."""
    model: TableName
    table_or_query: Union[TableName, SelectStatement]
    options: Optional[Dict[str, Expression]] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_ml_predict_expression(self)


@dataclass
class SafeNavigationExpression(Expression):
    """Safe navigation with ?. operator."""
    base_expression: Expression
    field_or_index: Union[Identifier, Expression]
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_safe_navigation_expression(self)


@dataclass
class GenerateArrayExpression(Expression):
    """GENERATE_ARRAY function."""
    start: Expression
    end: Expression
    step: Optional[Expression] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_generate_array_expression(self)


@dataclass
class GenerateDateArrayExpression(Expression):
    """GENERATE_DATE_ARRAY function."""
    start_date: Expression
    end_date: Expression
    interval_expr: Optional[IntervalLiteral] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_generate_date_array_expression(self)


@dataclass
class WithPartitionColumnsClause(ASTNode):
    """WITH PARTITION COLUMNS clause."""
    columns: List[ColumnDefinition]
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_with_partition_columns_clause(self)


@dataclass
class DefineTableStatement(Statement):
    """DEFINE TABLE statement for external tables."""
    table_name: TableName
    columns: List[ColumnDefinition]
    options: Dict[str, Expression]
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_define_table_statement(self)


@dataclass  
class LoadDataStatement(Statement):
    """LOAD DATA statement."""
    table_name: TableName
    from_files: List[StringLiteral]
    options: Optional[Dict[str, Expression]] = None
    with_partition_columns: Optional[WithPartitionColumnsClause] = None
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_load_data_statement(self)


# Geography and JSON functions
@dataclass
class GeographyLiteral(Literal):
    """GEOGRAPHY literal."""
    value: str  # WKT or GeoJSON string
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_geography_literal(self)


@dataclass
class NewExpression(Expression):
    """NEW constructor expression."""
    type_name: TypeExpression
    arguments: List[Expression] = field(default_factory=list)
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_new_expression(self)


# More advanced BigQuery constructs
@dataclass
class DescriptorExpression(Expression):
    """DESCRIPTOR expression for flexible column references."""
    columns: List[Identifier]
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_descriptor_expression(self)


@dataclass
class FlattenExpression(Expression):
    """FLATTEN expression (legacy)."""
    table_expression: Expression
    flatten_column: Identifier
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_flatten_expression(self)


@dataclass
class WithConnectionClause(ASTNode):
    """WITH CONNECTION clause."""
    connection_name: Identifier
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_with_connection_clause(self)


# Data transformation expressions
@dataclass
class ReplaceFieldsExpression(Expression):
    """REPLACE expression for struct field replacement."""
    struct_expression: Expression
    field_replacements: List[Tuple[Identifier, Expression]]
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_replace_fields_expression(self)


@dataclass
class AddToFieldsExpression(Expression):
    """Expression to add fields to struct."""
    struct_expression: Expression
    new_fields: List[Tuple[Identifier, Expression]]
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_add_to_fields_expression(self)


@dataclass
class DropFieldsExpression(Expression):
    """Expression to drop fields from struct."""
    struct_expression: Expression
    fields_to_drop: List[Identifier]
    
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_drop_fields_expression(self)


