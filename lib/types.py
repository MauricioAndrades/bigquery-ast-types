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
    parts: List[Union[str, int]]
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

    def __str__(self) -> str:
        elements_str = ", ".join(str(e) for e in self.elements)
        return f"[{elements_str}]"

@dataclass
class StructLiteral(Literal):
    """Struct literal."""
    fields: List[Tuple[Optional[str], Expression]] = field(default_factory=list)  # [(field_name, value), ...]
    struct_type: Optional[str] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_struct_literal(self)

    def __str__(self) -> str:
        fields_str = ", ".join(f"{expr} AS {name}" for name, expr in self.fields)
        return f"STRUCT({fields_str})"

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
    style: str  #


# ---------------------------------------------------------------------------
# Additional core node types used by the builder utilities.


@dataclass
class BinaryOp(Expression):
    """Binary operation."""
    operator: str
    left: Expression
    right: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        if hasattr(visitor, "visit_binary_op"):
            return visitor.visit_binary_op(self)
        return visitor.generic_visit(self)

    def __str__(self) -> str:
        return f"({self.left} {self.operator} {self.right})"


@dataclass
class UnaryOp(Expression):
    """Unary operation."""
    operator: str
    operand: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        if hasattr(visitor, "visit_unary_op"):
            return visitor.visit_unary_op(self)
        return visitor.generic_visit(self)

    def __str__(self) -> str:
        return f"{self.operator} {self.operand}"


@dataclass
class FunctionCall(Expression):
    """Function call."""
    name: str
    args: List[Expression]

    def accept(self, visitor: "ASTVisitor") -> Any:
        if hasattr(visitor, "visit_function_call"):
            return visitor.visit_function_call(self)
        return visitor.generic_visit(self)

    def __str__(self) -> str:
        args_str = ", ".join(str(a) for a in self.args)
        return f"{self.name}({args_str})"


@dataclass
class Cast(Expression):
    """CAST expression."""
    expr: Expression
    target_type: str
    safe: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.generic_visit(self)

    def __str__(self) -> str:
        func = "SAFE_CAST" if self.safe else "CAST"
        return f"{func}({self.expr} AS {self.target_type})"


@dataclass
class WhenClause(ASTNode):
    """WHEN clause in CASE."""
    condition: Expression
    result: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.generic_visit(self)

    def __str__(self) -> str:
        return f"WHEN {self.condition} THEN {self.result}"


@dataclass
class Case(Expression):
    """CASE expression."""
    when_clauses: List[WhenClause]
    else_clause: Optional[Expression] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.generic_visit(self)

    def __str__(self) -> str:
        clauses = "\n  ".join(str(w) for w in self.when_clauses)
        result = f"CASE\n  {clauses}"
        if self.else_clause:
            result += f"\n  ELSE {self.else_clause}"
        result += "\n  END"
        return result


@dataclass
class OrderByClause(ASTNode):
    """ORDER BY clause element."""
    expr: Expression
    direction: str = "ASC"  # 'ASC' or 'DESC'

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.generic_visit(self)

    def __str__(self) -> str:
        return f"{self.expr} {self.direction}"


@dataclass
class WindowFunction(Expression):
    """Window function."""
    name: str
    args: List[Expression]
    partition_by: List[Expression] = field(default_factory=list)
    order_by: List[OrderByClause] = field(default_factory=list)

    def accept(self, visitor: "ASTVisitor") -> Any:
        if hasattr(visitor, "visit_window_function"):
            return visitor.visit_window_function(self)
        return visitor.generic_visit(self)

    def __str__(self) -> str:
        args_str = ", ".join(str(a) for a in self.args)
        parts = []
        if self.partition_by:
            partition_str = ", ".join(str(e) for e in self.partition_by)
            parts.append(f"PARTITION BY {partition_str}")
        if self.order_by:
            order_str = ", ".join(str(o) for o in self.order_by)
            parts.append(f"ORDER BY {order_str}")
        over = " ".join(parts)
        return f"{self.name}({args_str}) OVER ({over})"


@dataclass
class Star(Expression):
    """SELECT * expression."""
    except_columns: List[str] = field(default_factory=list)

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.generic_visit(self)

    def __str__(self) -> str:
        if self.except_columns:
            cols = ", ".join(self.except_columns)
            return f"* EXCEPT ({cols})"
        return "*"
