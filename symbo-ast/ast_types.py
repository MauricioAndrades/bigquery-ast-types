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
    """Unquoted identifier following GoogleSQL rules.
    
    Must begin with a letter (a-z, A-Z) or underscore (_).
    Subsequent characters can be letters, numbers, or underscores.
    Case-insensitive except for user-defined function names.
    Cannot be a reserved keyword unless quoted.
    """
    name: str


    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_unquoted_identifier(self)

@dataclass
class QuotedIdentifier(ASTNode):
    """Quoted identifier enclosed by backticks (`).
    
    Can contain any characters including spaces and symbols.
    Cannot be empty.
    Uses the same escape sequences as string literals.
    Required when identifier matches a reserved keyword.
    """
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
    """Part of a path expression supporting GoogleSQL path syntax.
    
    Can contain:
    - Identifier (quoted or unquoted)  
    - Numeric value for array/struct access
    
    Separators supported:
    - '.' for standard path navigation
    - '/' for hierarchical paths
    - ':' for namespaced paths  
    - '-' for dashed paths (context-dependent)
    """
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
    """Table name: project, dataset, table (each can be quoted/unquoted).
    
    Dash support rules:
    - Project identifiers: may contain dashes when unquoted (e.g., my-project) 
    - Dataset identifiers: do NOT support dashes when unquoted
    - Table identifiers: may contain dashes when unquoted in FROM/TABLE clauses (e.g., my-table)
    
    Only the first identifier in a table path (project) or the table name itself
    can have dashes when unquoted. Dashes are not supported in dataset names.
    """
    project: Optional[Identifier] = None
    dataset: Optional[Identifier] = None
    table: Identifier = None  # Required

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_table_name(self)

@dataclass
class ColumnName(ASTNode):
    """Column name (quoted or unquoted).
    
    Dash support: Unquoted column identifiers support dashes when referenced 
    in FROM or TABLE clauses (e.g., column-name).
    """
    identifier: Identifier

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_column_name(self)

@dataclass
class FieldName(ASTNode):
    """Field name inside struct/JSON.
    
    Field names follow the same rules as column names, including dash support
    when used in FROM or TABLE clause contexts.
    """
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
    """String literal supporting all GoogleSQL string formats.
    
    Quote types supported:
    - 'single': enclosed by single quotes (')
    - 'double': enclosed by double quotes (")  
    - 'triple_single': enclosed by triple single quotes (''')
    - 'triple_double': enclosed by triple double quotes (\"\"\")
    
    Raw strings (prefix r or R) treat backslashes literally.
    String literal concatenation is supported with whitespace/comments between parts.
    """
    value: str
    quote_type: str  # single, double, triple_single, triple_double
    raw: bool = False  # r"..." or not

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_string_literal(self)

@dataclass
class BytesLiteral(Literal):
    """Bytes literal with GoogleSQL prefix and quote support.
    
    Must have bytes prefix (b or B).
    Supports same quote types as string literals.
    Can be combined with raw prefix (br, rb, Br, bR, etc.).
    Raw bytes literals treat backslashes literally.
    """
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
    """Named query parameter (@parameter_name).
    
    Denoted by @ character followed by identifier.
    Identifier can be unquoted or quoted.
    Can start with reserved keyword when quoted.
    Cannot be used alongside positional parameters.
    """
    name: Identifier

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_named_parameter(self)

@dataclass
class PositionalParameter(Expression):
    """Positional query parameter (?).
    
    Denoted using the ? character.
    Evaluated by the order in which they are passed.
    Cannot be used alongside named parameters.
    """
    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_positional_parameter(self)

# --- Comments ---

@dataclass
class Comment(ASTNode):
    """SQL comment supporting GoogleSQL comment styles.
    
    Single-line comments:
    - # comment (hash style)
    - -- comment (double dash style)
    
    Multi-line comments:
    - /* comment */ (C-style, no nesting supported)
    
    Can appear inline or on separate lines.
    """
    value: str
    multiline: bool = False
    style: str = "#"  # "#", "--", or "/*"

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_comment(self)


# Visitor Interface
class ASTVisitor(ABC):
    """Visitor interface for AST nodes with explicit methods for all node types."""
    
    # Identifier nodes
    def visit_unquoted_identifier(self, node: 'UnquotedIdentifier') -> Any:
        """Visit an unquoted identifier node."""
        pass
    
    def visit_quoted_identifier(self, node: 'QuotedIdentifier') -> Any:
        """Visit a quoted identifier node."""
        pass
    
    def visit_identifier(self, node: 'Identifier') -> Any:
        """Visit a general identifier node."""
        pass
    
    # Path and name nodes
    def visit_path_part(self, node: 'PathPart') -> Any:
        """Visit a path part node."""
        pass
    
    def visit_path_expression(self, node: 'PathExpression') -> Any:
        """Visit a path expression node."""
        pass
    
    def visit_table_name(self, node: 'TableName') -> Any:
        """Visit a table name node."""
        pass
    
    def visit_column_name(self, node: 'ColumnName') -> Any:
        """Visit a column name node."""
        pass
    
    def visit_field_name(self, node: 'FieldName') -> Any:
        """Visit a field name node."""
        pass
    
    # Literal nodes
    def visit_string_literal(self, node: 'StringLiteral') -> Any:
        """Visit a string literal node."""
        pass
    
    def visit_bytes_literal(self, node: 'BytesLiteral') -> Any:
        """Visit a bytes literal node."""
        pass
    
    def visit_integer_literal(self, node: 'IntegerLiteral') -> Any:
        """Visit an integer literal node."""
        pass
    
    def visit_boolean_literal(self, node: 'BooleanLiteral') -> Any:
        """Visit a boolean literal node."""
        pass
    
    def visit_null_literal(self, node: 'NullLiteral') -> Any:
        """Visit a null literal node."""
        pass
    
    def visit_float_literal(self, node: 'FloatLiteral') -> Any:
        """Visit a float literal node."""
        pass
    
    def visit_numeric_literal(self, node: 'NumericLiteral') -> Any:
        """Visit a numeric literal node."""
        pass
    
    def visit_bignumeric_literal(self, node: 'BigNumericLiteral') -> Any:
        """Visit a bignumeric literal node."""
        pass
    
    def visit_date_literal(self, node: 'DateLiteral') -> Any:
        """Visit a date literal node."""
        pass
    
    def visit_time_literal(self, node: 'TimeLiteral') -> Any:
        """Visit a time literal node."""
        pass
    
    def visit_datetime_literal(self, node: 'DatetimeLiteral') -> Any:
        """Visit a datetime literal node."""
        pass
    
    def visit_timestamp_literal(self, node: 'TimestampLiteral') -> Any:
        """Visit a timestamp literal node."""
        pass
    
    def visit_array_literal(self, node: 'ArrayLiteral') -> Any:
        """Visit an array literal node."""
        pass
    
    def visit_struct_literal(self, node: 'StructLiteral') -> Any:
        """Visit a struct literal node."""
        pass
    
    def visit_range_literal(self, node: 'RangeLiteral') -> Any:
        """Visit a range literal node."""
        pass
    
    def visit_interval_literal(self, node: 'IntervalLiteral') -> Any:
        """Visit an interval literal node."""
        pass
    
    def visit_json_literal(self, node: 'JSONLiteral') -> Any:
        """Visit a JSON literal node."""
        pass
    
    # Expression nodes
    def visit_named_parameter(self, node: 'NamedParameter') -> Any:
        """Visit a named parameter node."""
        pass
    
    def visit_positional_parameter(self, node: 'PositionalParameter') -> Any:
        """Visit a positional parameter node."""
        pass
    
    # Comment nodes
    def visit_comment(self, node: 'Comment') -> Any:
        """Visit a comment node."""
        pass
    
    def generic_visit(self, node: ASTNode) -> Any:
        """Default visit method for nodes without specific handlers."""
        pass
