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
@dataclass  
class ASTNode(ABC):
    """Base class for all AST nodes."""
    
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



# --- Identifier Nodes ---

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
class Identifier(ASTNode):
    """General identifier: either quoted or unquoted, with quoting info."""
    name: str
    quoted: bool = False


    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_identifier(self)

# --- Path and Table/Field/Column Names ---

@dataclass
class PathPart(ASTNode):
    """Path part: identifier or number, with separator."""
    value: Union[Identifier, int]
    separator: Optional[str] = None  # '/', ':', '-', or '.'


    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_path_part(self)

@dataclass
class PathExpression(ASTNode):
    """Path expression: sequence of path parts."""
    parts: List[PathPart]

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_path_expression(self)

@dataclass
class TableName(ASTNode):
    """Table name: project, dataset, table (each can be quoted/unquoted, with dash support for project/table)."""
    project: Optional[Identifier] = None
    dataset: Optional[Identifier] = None
    table: Identifier = None  # Required

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_table_name(self)

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

# --- Literals ---

@dataclass
class Literal(ASTNode):
    """Base class for literals."""
    pass

@dataclass
class StringLiteral(Literal):
    value: str
    quote_type: str  # single, double, triple_single, triple_double
    raw: bool = False  # r"..." or not

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_string_literal(self)

@dataclass
class BytesLiteral(Literal):
    value: bytes
    quote_type: str
    raw: bool = False  # br"..." or rb"..."

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_bytes_literal(self)

@dataclass
class IntegerLiteral(Literal):
    value: int
    hex: bool = False  # 0x... or plain decimal

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_integer_literal(self)

@dataclass
class FloatLiteral(Literal):
    value: float

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_float_literal(self)

@dataclass
class NumericLiteral(Literal):
    value: str  # Keep as string for precision

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_numeric_literal(self)

@dataclass
class BigNumericLiteral(Literal):
    value: str

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_bignumeric_literal(self)

@dataclass
class DateLiteral(Literal):
    value: str  # Canonical date format

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_date_literal(self)

@dataclass
class TimeLiteral(Literal):
    value: str  # Canonical time format

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_time_literal(self)

@dataclass
class DatetimeLiteral(Literal):
    value: str  # Canonical datetime format

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_datetime_literal(self)

@dataclass
class TimestampLiteral(Literal):
    value: str  # Canonical timestamp format
    timezone: Optional[str] = None  # May include zone info

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_timestamp_literal(self)

@dataclass
class ArrayLiteral(Literal):
    elements: List['Expression']
    element_type: Optional['TypeExpression'] = None

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_array_literal(self)

@dataclass
class StructLiteral(Literal):
    fields: List[Tuple[Optional[str], 'Expression']]
    type: Optional['StructType'] = None

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_struct_literal(self)

@dataclass
class RangeLiteral(Literal):
    element_type: DataType
    lower_bound: Optional['Expression']
    upper_bound: Optional['Expression']

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_range_literal(self)

@dataclass
class IntervalLiteral(Literal):
    value: Union[int, str]  # int for simple, str for range (e.g. '10:20:30.52')
    unit: IntervalUnit
    end_unit: Optional[IntervalUnit] = None  # For range intervals

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_interval_literal(self)

@dataclass
class JSONLiteral(Literal):
    value: str

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_json_literal(self)

# --- Query Parameters ---

@dataclass
class Expression(ASTNode):
    """Base class for expressions."""
    pass

@dataclass
class NamedParameter(Expression):
    """@parameter_name (quoted or unquoted, can be reserved word)."""
    name: Identifier

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_named_parameter(self)

@dataclass
class PositionalParameter(Expression):
    """? parameter."""
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_positional_parameter(self)

# --- Comments ---

@dataclass
class Comment(ASTNode):
    """SQL comment: single-line (#, --) or multi-line (/* ... */)."""
    value: str
    multiline: bool = False
    style: str = "#"  # "#", "--", or "/*"

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_comment(self)
