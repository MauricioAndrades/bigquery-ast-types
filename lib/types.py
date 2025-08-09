"""
BigQuery AST Type Definitions

Complete AST node definitions based on BigQuery lexical specification.

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
    def accept(self, visitor: "ASTVisitor") -> Any:
        """Accept a visitor."""
        pass

    @property
    def node_type(self) -> str:
        """Get the node type name."""
        return self.__class__.__name__

class ASTVisitor(ABC):
    """Base visitor interface for AST traversal."""

    @abstractmethod
    def visit_identifier(self, node: "Identifier") -> Any:
        pass

    @abstractmethod
    def visit_literal(self, node: "Literal") -> Any:
        pass

    @abstractmethod
    def visit_string_literal(self, node: "StringLiteral") -> Any:
        pass

    @abstractmethod
    def visit_integer_literal(self, node: "IntegerLiteral") -> Any:
        pass

    @abstractmethod
    def visit_float_literal(self, node: "FloatLiteral") -> Any:
        pass

    @abstractmethod
    def visit_boolean_literal(self, node: "BooleanLiteral") -> Any:
        pass

    @abstractmethod
    def visit_null_literal(self, node: "NullLiteral") -> Any:
        pass

    @abstractmethod
    def visit_binary_op(self, node: "BinaryOp") -> Any:
        pass

    @abstractmethod
    def visit_unary_op(self, node: "UnaryOp") -> Any:
        pass

    @abstractmethod
    def visit_function_call(self, node: "FunctionCall") -> Any:
        pass

    @abstractmethod
    def visit_table_name(self, node: "TableName") -> Any:
        pass

    @abstractmethod
    def visit_table_ref(self, node: "TableRef") -> Any:
        pass

    @abstractmethod
    def visit_select_column(self, node: "SelectColumn") -> Any:
        pass

    @abstractmethod
    def visit_where_clause(self, node: "WhereClause") -> Any:
        pass

    @abstractmethod
    def visit_group_by_clause(self, node: "GroupByClause") -> Any:
        pass

    @abstractmethod
    def visit_having_clause(self, node: "HavingClause") -> Any:
        pass

    @abstractmethod
    def visit_order_by_item(self, node: "OrderByItem") -> Any:
        pass

    @abstractmethod
    def visit_order_by_clause(self, node: "OrderByClause") -> Any:
        pass

    @abstractmethod
    def visit_limit_clause(self, node: "LimitClause") -> Any:
        pass

    @abstractmethod
    def visit_join(self, node: "Join") -> Any:
        pass

    @abstractmethod
    def visit_select(self, node: "Select") -> Any:
        pass

    @abstractmethod
    def visit_subquery(self, node: "Subquery") -> Any:
        pass

    @abstractmethod
    def visit_cte(self, node: "CTE") -> Any:
        pass

    @abstractmethod
    def visit_with_clause(self, node: "WithClause") -> Any:
        pass

    @abstractmethod
    def visit_merge_insert(self, node: "MergeInsert") -> Any:
        pass

    @abstractmethod
    def visit_merge_update(self, node: "MergeUpdate") -> Any:
        pass

    @abstractmethod
    def visit_merge_delete(self, node: "MergeDelete") -> Any:
        pass

    @abstractmethod
    def visit_merge_action(self, node: "MergeAction") -> Any:
        pass

    @abstractmethod
    def visit_merge(self, node: "Merge") -> Any:
        pass

    @abstractmethod
    def visit_window_specification(self, node: "WindowSpecification") -> Any:
        pass

    @abstractmethod
    def visit_window_function(self, node: "WindowFunction") -> Any:
        pass

    # Enhanced identifier visitors
    @abstractmethod
    def visit_unquoted_identifier(self, node: "UnquotedIdentifier") -> Any:
        pass

    @abstractmethod
    def visit_quoted_identifier(self, node: "QuotedIdentifier") -> Any:
        pass

    @abstractmethod
    def visit_enhanced_general_identifier(self, node: "EnhancedGeneralIdentifier") -> Any:
        pass

    # Path expression visitors
    @abstractmethod
    def visit_path_expression(self, node: "PathExpression") -> Any:
        pass

    @abstractmethod
    def visit_path_part(self, node: "PathPart") -> Any:
        pass

    # Table and column name visitors
    @abstractmethod
    def visit_column_name(self, node: "ColumnName") -> Any:
        pass

    @abstractmethod
    def visit_field_name(self, node: "FieldName") -> Any:
        pass

    # Enhanced literal visitors
    @abstractmethod
    def visit_bytes_literal(self, node: "BytesLiteral") -> Any:
        pass

    @abstractmethod
    def visit_numeric_literal(self, node: "NumericLiteral") -> Any:
        pass

    @abstractmethod
    def visit_bignumeric_literal(self, node: "BigNumericLiteral") -> Any:
        pass

    @abstractmethod
    def visit_date_literal(self, node: "DateLiteral") -> Any:
        pass

    @abstractmethod
    def visit_time_literal(self, node: "TimeLiteral") -> Any:
        pass

    @abstractmethod
    def visit_datetime_literal(self, node: "DatetimeLiteral") -> Any:
        pass

    @abstractmethod
    def visit_timestamp_literal(self, node: "TimestampLiteral") -> Any:
        pass

    @abstractmethod
    def visit_interval_literal(self, node: "IntervalLiteral") -> Any:
        pass

    @abstractmethod
    def visit_array_literal(self, node: "ArrayLiteral") -> Any:
        pass

    @abstractmethod
    def visit_struct_literal(self, node: "StructLiteral") -> Any:
        pass

    @abstractmethod
    def visit_range_literal(self, node: "RangeLiteral") -> Any:
        pass

    @abstractmethod
    def visit_json_literal(self, node: "JSONLiteral") -> Any:
        pass

    # Parameter visitors
    @abstractmethod
    def visit_named_parameter(self, node: "NamedParameter") -> Any:
        pass

    @abstractmethod
    def visit_positional_parameter(self, node: "PositionalParameter") -> Any:
        pass

    # Comment visitor
    @abstractmethod
    def visit_comment(self, node: "Comment") -> Any:
        pass

    # Specific comment style visitors
    def visit_hash_comment(self, node: "HashComment") -> Any:
        return self.visit_comment(node)

    def visit_dash_comment(self, node: "DashComment") -> Any:
        return self.visit_comment(node)

    def visit_block_comment(self, node: "BlockComment") -> Any:
        return self.visit_comment(node)

    def generic_visit(self, node: ASTNode) -> Any:
        """Default visitor method for unhandled node types."""
        pass

# Enums for various options
class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"

class ComparisonOp(Enum):
    EQ = "="
    NE = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    LIKE = "LIKE"
    IN = "IN"

class LogicalOp(Enum):
    AND = "AND"
    OR = "OR"
    NOT = "NOT"

class ArithmeticOp(Enum):
    ADD = "+"
    SUB = "-"
    MUL = "*"
    DIV = "/"
    MOD = "%"

class OrderDirection(Enum):
    ASC = "ASC"
    DESC = "DESC"

@dataclass
class SourceLocation:
    """Source location information."""
    line: int
    column: int
    offset: int
    length: int

# Expression Base Classes
class Expression(ASTNode):
    """Base class for all expressions."""
    pass

class Statement(ASTNode):
    """Base class for all statements."""
    pass

# Identifier Types - Enhanced for BigQuery lexical specification
@dataclass
class Identifier(Expression):
    """Column or table identifier."""
    name: str
    table: Optional[str] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_identifier(self)

    def __str__(self):
        if self.table:
            return f"{self.table}.{self.name}"
        return self.name

@dataclass
class UnquotedIdentifier(Identifier):
    """Unquoted identifier - must begin with letter or underscore."""
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_unquoted_identifier(self)

@dataclass
class QuotedIdentifier(Identifier):
    """Quoted identifier - enclosed by backticks, can contain any characters."""
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_quoted_identifier(self)

@dataclass
class EnhancedGeneralIdentifier(Identifier):
    """Enhanced general identifier supporting complex path expressions."""
    parts: List[Union[str, int]] = field(default_factory=list)
    separators: List[str] = field(default_factory=list)  # ., /, :, -

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_enhanced_general_identifier(self)

# Path Expression Types
@dataclass
class PathExpression(Expression):
    """Path expression for navigating object graphs."""
    parts: List['PathPart']

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_path_expression(self)

@dataclass
class PathPart(Expression):
    """Part of a path expression."""
    value: Union[str, int, Identifier]
    separator: Optional[str] = None  # ., /, :, -

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_path_part(self)

# Table and Column Name Types
@dataclass
class TableName(Expression):
    """Table name with optional project and dataset qualification."""
    table: str
    dataset: Optional[str] = None
    project: Optional[str] = None
    supports_dashes: bool = False  # For dash rules in BigQuery

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_table_name(self)

@dataclass
class ColumnName(Expression):
    """Column name with dash support."""
    name: str
    supports_dashes: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_column_name(self)

@dataclass
class FieldName(Expression):
    """Field name for struct and JSON objects."""
    name: str

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_field_name(self)

    def __str__(self):
        return self.name

# Literal Types
@dataclass
class Literal(Expression):
    """Base class for literal values."""
    value: Any

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_literal(self)

@dataclass
class StringLiteral(Literal):
    """String literal with BigQuery formatting options."""
    quote_style: str = '"'  # ", ', """, '''
    is_raw: bool = False  # r"..." or R"..."
    is_bytes: bool = False  # b"..." or B"..."

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_string_literal(self)

@dataclass
class BytesLiteral(Literal):
    """Bytes literal with formatting options."""
    quote_style: str = '"'  # ", ', """, '''
    is_raw: bool = False  # br"..." or RB"..."

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_bytes_literal(self)

@dataclass
class IntegerLiteral(Literal):
    """Integer literal."""
    is_hexadecimal: bool = False  # 0x prefix

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_integer_literal(self)

@dataclass
class NumericLiteral(Literal):
    """NUMERIC literal - exact decimal."""
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_numeric_literal(self)

@dataclass
class BigNumericLiteral(Literal):
    """BIGNUMERIC literal - high precision decimal."""
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_bignumeric_literal(self)

@dataclass
class BooleanLiteral(Literal):
    """Boolean literal."""
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_boolean_literal(self)

@dataclass
class FloatLiteral(Literal):
    """Float literal."""
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_float_literal(self)

@dataclass
class NullLiteral(Literal):
    """NULL literal."""
    value: None = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_null_literal(self)

# BigQuery Date/Time Literals
@dataclass
class DateLiteral(Literal):
    """DATE literal."""
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_date_literal(self)

@dataclass
class TimeLiteral(Literal):
    """TIME literal."""
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_time_literal(self)

@dataclass
class DatetimeLiteral(Literal):
    """DATETIME literal."""
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_datetime_literal(self)

@dataclass
class TimestampLiteral(Literal):
    """TIMESTAMP literal."""
    timezone: Optional[str] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_timestamp_literal(self)

@dataclass
class IntervalLiteral(Literal):
    """INTERVAL literal."""
    from_part: Optional[str] = None
    to_part: Optional[str] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_interval_literal(self)

@dataclass
class ArrayLiteral(Literal):
    """Array literal."""
    elements: List[Expression] = field(default_factory=list)
    element_type: Optional[str] = None  # ARRAY<type>

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_array_literal(self)

@dataclass
class StructLiteral(Literal):
    """Struct literal."""
    fields: List[Tuple[Optional[str], Expression]] = field(default_factory=list)  # [(field_name, value), ...]
    struct_type: Optional[str] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_struct_literal(self)

@dataclass
class RangeLiteral(Literal):
    """RANGE literal."""
    range_type: str = "DATE"  # DATE, DATETIME, TIMESTAMP
    lower_bound: Optional[Expression] = None
    upper_bound: Optional[Expression] = None
    lower_unbounded: bool = False
    upper_unbounded: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_range_literal(self)

@dataclass
class JSONLiteral(Literal):
    """JSON literal."""
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_json_literal(self)

# Query Parameter Types
@dataclass
class NamedParameter(Expression):
    """Named query parameter (@param)."""
    name: str

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_named_parameter(self)

@dataclass
class PositionalParameter(Expression):
    """Positional query parameter (?)."""
    position: int

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_positional_parameter(self)

# Comment Types
@dataclass
class Comment(ASTNode):
    """Comment node to preserve comments in AST."""
    text: str
    style: str
    is_multiline: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        method = {
            "#": "visit_hash_comment",
            "--": "visit_dash_comment",
            "/* */": "visit_block_comment",
        }.get(self.style, "visit_comment")
        return getattr(visitor, method)(self)


@dataclass
class HashComment(Comment):
    """Single-line comment using #."""

    def __init__(self, text: str):
        super().__init__(text=text, style="#", is_multiline=False)


@dataclass
class DashComment(Comment):
    """Single-line comment using --."""

    def __init__(self, text: str):
        super().__init__(text=text, style="--", is_multiline=False)


@dataclass
class BlockComment(Comment):
    """Block comment enclosed in /* */."""

    def __init__(self, text: str, is_multiline: bool = True):
        super().__init__(text=text, style="/* */", is_multiline=is_multiline)

# Expression Nodes
@dataclass
class BinaryOp(Expression):
    """Binary operation."""
    operator: str
    left: Expression
    right: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_binary_op(self)


@dataclass
class UnaryOp(Expression):
    """Unary operation."""
    operator: str
    operand: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_unary_op(self)


@dataclass
class FunctionCall(Expression):
    """Function call expression."""
    function_name: str
    arguments: List[Expression] = field(default_factory=list)

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_function_call(self)


# Statement and Clause Types
@dataclass
class TableRef(ASTNode):
    """Table reference in FROM clause."""
    table: TableName
    alias: Optional[str] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_table_ref(self)


@dataclass
class SelectColumn(ASTNode):
    """Column selected in a SELECT list."""
    expression: Expression
    alias: Optional[str] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_select_column(self)


@dataclass
class WhereClause(ASTNode):
    """WHERE clause."""
    condition: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_where_clause(self)


@dataclass
class GroupByClause(ASTNode):
    """GROUP BY clause."""
    expressions: List[Expression] = field(default_factory=list)

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_group_by_clause(self)


@dataclass
class HavingClause(ASTNode):
    """HAVING clause."""
    condition: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_having_clause(self)


@dataclass
class OrderByItem(ASTNode):
    """Single ORDER BY item."""
    expression: Expression
    direction: OrderDirection = OrderDirection.ASC

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_order_by_item(self)


@dataclass
class OrderByClause(ASTNode):
    """ORDER BY clause."""
    items: List[OrderByItem] = field(default_factory=list)

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_order_by_clause(self)


@dataclass
class LimitClause(ASTNode):
    """LIMIT clause."""
    limit: Expression
    offset: Optional[Expression] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_limit_clause(self)


@dataclass
class Join(ASTNode):
    """JOIN clause."""
    join_type: JoinType
    table: TableRef
    condition: Optional[Expression] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_join(self)


@dataclass
class Select(Statement):
    """SELECT statement."""
    select_list: List[SelectColumn]
    from_clause: Optional[TableRef] = None
    distinct: bool = False
    joins: List[Join] = field(default_factory=list)
    where_clause: Optional[WhereClause] = None
    group_by_clause: Optional[GroupByClause] = None
    having_clause: Optional[HavingClause] = None
    order_by_clause: Optional[OrderByClause] = None
    limit_clause: Optional[LimitClause] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_select(self)


@dataclass
class Subquery(Expression):
    """Subquery expression."""
    query: Select

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_subquery(self)


@dataclass
class CTE(ASTNode):
    """Common table expression."""
    name: str
    query: Select
    columns: Optional[List[str]] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_cte(self)


@dataclass
class WithClause(ASTNode):
    """WITH clause containing CTEs."""
    ctes: List[CTE] = field(default_factory=list)

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_with_clause(self)


@dataclass
class MergeInsert(ASTNode):
    """INSERT action in MERGE statement."""
    columns: List[str] = field(default_factory=list)
    values: List[Expression] = field(default_factory=list)

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_merge_insert(self)


@dataclass
class MergeUpdate(ASTNode):
    """UPDATE action in MERGE statement."""
    assignments: Dict[str, Expression] = field(default_factory=dict)

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_merge_update(self)


@dataclass
class MergeDelete(ASTNode):
    """DELETE action in MERGE statement."""

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_merge_delete(self)


@dataclass
class MergeAction(ASTNode):
    """WHEN ... THEN action in MERGE."""
    action: Union[MergeInsert, MergeUpdate, MergeDelete]
    condition: Optional[Expression] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_merge_action(self)


@dataclass
class Merge(Statement):
    """MERGE statement."""
    target_table: TableRef
    source_table: TableRef
    merge_condition: Expression
    actions: List[MergeAction] = field(default_factory=list)

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_merge(self)


@dataclass
class WindowSpecification(ASTNode):
    """Window specification for window functions."""
    partition_by: List[Expression] = field(default_factory=list)
    order_by: Optional[OrderByClause] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_window_specification(self)


@dataclass
class WindowFunction(Expression):
    """Window function call."""
    function_name: str
    arguments: List[Expression] = field(default_factory=list)
    window_spec: WindowSpecification = field(default_factory=WindowSpecification)

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_window_function(self)
