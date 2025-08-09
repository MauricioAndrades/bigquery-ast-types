"""
BigQuery AST Type Definitions

Complete AST node definitions based on BigQuery lexical specification.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""
import re
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
    def visit_set_operation(self, node: "SetOperation") -> Any:
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

    # Additional statement visitors from main branch
    @abstractmethod
    def visit_case(self, node: "Case") -> Any:
        pass

    @abstractmethod
    def visit_when_clause(self, node: "WhenClause") -> Any:
        pass

    @abstractmethod
    def visit_insert(self, node: "Insert") -> Any:
        pass

    @abstractmethod
    def visit_update(self, node: "Update") -> Any:
        pass

    @abstractmethod
    def visit_create_table(self, node: "CreateTable") -> Any:
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

class NullsOrder(Enum):
    """NULL value ordering in ORDER BY."""
    FIRST = "FIRST"
    LAST = "LAST"

class GroupByType(Enum):
    """Type of GROUP BY operation."""
    STANDARD = "STANDARD"
    ROLLUP = "ROLLUP"
    CUBE = "CUBE"
    GROUPING_SETS = "GROUPING_SETS"
    ALL = "ALL"

class FrameType(Enum):
    """Window frame type."""
    ROWS = "ROWS"
    RANGE = "RANGE"

class FrameBound(Enum):
    """Window frame bound."""
    UNBOUNDED_PRECEDING = "UNBOUNDED PRECEDING"
    UNBOUNDED_FOLLOWING = "UNBOUNDED FOLLOWING"
    CURRENT_ROW = "CURRENT ROW"

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
    type: Optional[str] = None  # Type of the literal

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_literal(self)

@dataclass
class StringLiteral(Literal):
    """String literal with BigQuery formatting options."""
    quote_style: str = '"'  # ", ', """, '''
    is_raw: bool = False  # r"..." or R"..."
    is_bytes: bool = False  # b"..." or B"..."

    def __post_init__(self):
        self.type = "string"

    def __str__(self) -> str:
        return f"'{self.value}'"

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

    def __post_init__(self):
        self.type = "number"

    def __str__(self) -> str:
        if self.is_hexadecimal:
            return f"0x{self.value:x}"
        return str(self.value)

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

    def __post_init__(self):
        self.type = "boolean"

    def __str__(self) -> str:
        return "TRUE" if self.value else "FALSE"

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_boolean_literal(self)

@dataclass
class FloatLiteral(Literal):
    """Float literal."""

    def __post_init__(self):
        self.type = "number"

    def __str__(self) -> str:
        return str(self.value)

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_float_literal(self)

@dataclass
class NullLiteral(Literal):
    """NULL literal."""
    value: None = None

    def __post_init__(self):
        self.type = "null"

    def __str__(self) -> str:
        return "NULL"

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_null_literal(self)

# BigQuery Date/Time Literals
@dataclass
class DateLiteral(Literal):
    """DATE literal."""

    def __post_init__(self):
        self.type = "date"

    def __str__(self) -> str:
        return f"DATE '{self.value}'"

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

    def __post_init__(self):
        self.type = "timestamp"

    def __str__(self) -> str:
        return f"TIMESTAMP '{self.value}'"

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

    def __post_init__(self):
        # If value is passed as a list, use it as elements
        if isinstance(self.value, list) and not self.elements:
            self.elements = self.value
        elif not hasattr(self, 'value') or self.value is None:
            self.value = self.elements

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

    def __post_init__(self):
        # If value is passed as a list of tuples, use it as fields
        if isinstance(self.value, list) and not self.fields:
            self.fields = self.value
        elif not hasattr(self, 'value') or self.value is None:
            self.value = self.fields

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

    def __post_init__(self):
        # Set value to a tuple of bounds if not provided
        if not hasattr(self, 'value') or self.value is None:
            self.value = (self.lower_bound, self.upper_bound)

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

# ---------------------------------------------------------------------------
# Additional core node types used by the builder utilities.

# Expression Nodes
@dataclass
class BinaryOp(Expression):
    """Binary operation."""
    left: Expression
    operator: str
    right: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_binary_op(self)

    def __str__(self) -> str:
        return f"({self.left} {self.operator} {self.right})"

@dataclass
class UnaryOp(Expression):
    """Unary operation."""
    operator: str
    operand: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_unary_op(self)

    def __str__(self) -> str:
        return f"{self.operator} {self.operand}"

@dataclass
class FunctionCall(Expression):
    """Function call expression."""
    function_name: str
    arguments: List[Expression] = field(default_factory=list)

    @property
    def name(self) -> str:
        """Alias for function_name for compatibility."""
        return self.function_name

    @property
    def args(self) -> List[Expression]:
        """Alias for arguments for compatibility."""
        return self.arguments

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_function_call(self)

    def __str__(self) -> str:
        args_str = ", ".join(str(a) for a in self.arguments)
        return f"{self.function_name}({args_str})"

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
class Star(Expression):
    """Star expression (*) for SELECT ALL."""
    table: Optional[str] = None  # For table.* syntax
    except_columns: List[str] = field(default_factory=list)  # For * EXCEPT(col1, col2)
    replace_columns: Dict[str, Expression] = field(default_factory=dict)  # For * REPLACE(expr AS col)
    
    def __init__(self, table: Optional[str] = None, 
                 except_columns: Optional[List[str]] = None,
                 replace_columns: Optional[Dict[str, Expression]] = None,
                 location: Optional['SourceLocation'] = None):
        super().__init__(location)
        self.table = table
        self.except_columns = except_columns or []
        self.replace_columns = replace_columns or {}
    
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.generic_visit(self)  # Will be updated to visit_star
    
    def __str__(self) -> str:
        result = f"{self.table}.*" if self.table else "*"
        if self.except_columns:
            cols = ", ".join(self.except_columns)
            result += f" EXCEPT({cols})"
        if self.replace_columns:
            replacements = ", ".join(f"{expr} AS {col}" for col, expr in self.replace_columns.items())
            result += f" REPLACE({replacements})"
        return result

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
    """GROUP BY clause with advanced grouping support."""
    expressions: List[Expression] = field(default_factory=list)
    group_type: GroupByType = GroupByType.STANDARD
    grouping_sets: Optional[List[List[Expression]]] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_group_by_clause(self)

@dataclass
class HavingClause(ASTNode):
    """HAVING clause."""
    condition: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_having_clause(self)

@dataclass
class QualifyClause(ASTNode):
    """QUALIFY clause for window function filtering."""
    condition: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.generic_visit(self)

@dataclass
class OrderByItem(ASTNode):
    """Single ORDER BY item with NULLS ordering."""
    expression: Expression
    direction: OrderDirection = OrderDirection.ASC
    nulls_order: Optional[NullsOrder] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_order_by_item(self)

    def __str__(self) -> str:
        result = f"{self.expression} {self.direction.value}"
        if self.nulls_order:
            result += f" NULLS {self.nulls_order.value}"
        return result

@dataclass
class OrderByClause(ASTNode):
    """ORDER BY clause."""
    items: List[OrderByItem] = field(default_factory=list)

    # Support for alternate construction patterns
    def __init__(self, items: List[OrderByItem] = None, expr: Expression = None, direction: str = "ASC"):
        """Initialize OrderByClause supporting two patterns:
        1. items=[OrderByItem(...), ...]
        2. expr=..., direction=... (creates single item clause)
        """
        if items is not None:
            self.items = items
        elif expr is not None:
            # Create single item clause from expr and direction
            dir_enum = OrderDirection.DESC if direction.upper() == "DESC" else OrderDirection.ASC
            self.items = [OrderByItem(expression=expr, direction=dir_enum)]
        else:
            self.items = []

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
    qualify_clause: Optional[QualifyClause] = None
    order_by_clause: Optional[OrderByClause] = None
    limit_clause: Optional[LimitClause] = None
    with_clause: Optional["WithClause"] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_select(self)

@dataclass
class Subquery(Expression):
    """Subquery expression."""
    query: Select
    alias: Optional[str] = None

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
    recursive: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_with_clause(self)


class SetOperator(Enum):
    """Set operation types for combining SELECT statements."""
    UNION = "UNION"
    INTERSECT = "INTERSECT"
    EXCEPT = "EXCEPT"


@dataclass
class SetOperation(Statement):
    """Set operation combining SELECT statements."""
    left: Statement
    right: Statement
    operator: SetOperator
    all: bool = False
    corresponding: bool = False  # BigQuery CORRESPONDING support

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_set_operation(self)

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

from enum import Enum

class MergeAction(Enum):
    """Action types for MERGE WHEN clauses."""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"

@dataclass
class MergeWhenClause(ASTNode):
    """Single WHEN clause in MERGE."""
    match_type: str  # "MATCHED", "NOT_MATCHED", "NOT_MATCHED_BY_SOURCE"
    action: MergeAction
    condition: Optional[Expression] = None
    # For UPDATE action
    update_assignments: Optional[Dict[str, Expression]] = None
    # For INSERT action
    insert_columns: Optional[List[str]] = None
    insert_values: Optional[List[Expression]] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.generic_visit(self)


@dataclass
class Merge(Statement):
    """MERGE statement with enhanced support."""
    target: TableRef
    source: Union[TableRef, Select]
    on_condition: Expression
    when_clauses: List[MergeWhenClause] = field(default_factory=list)

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_merge(self)

@dataclass
class WindowFrame(ASTNode):
    """Window frame specification."""
    frame_type: FrameType
    start_bound: Union[FrameBound, Tuple[int, str]]  # (n, "PRECEDING"/"FOLLOWING")
    end_bound: Optional[Union[FrameBound, Tuple[int, str]]] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.generic_visit(self)

@dataclass
class WindowSpecification(ASTNode):
    """Window specification for window functions."""
    partition_by: List[Expression] = field(default_factory=list)
    order_by: Optional[OrderByClause] = None
    frame: Optional[WindowFrame] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_window_specification(self)

@dataclass
class WindowFunction(Expression):
    """Window function call."""
    function_name: str
    arguments: List[Expression] = field(default_factory=list)
    window_spec: WindowSpecification = field(default_factory=WindowSpecification)

    @property
    def name(self) -> str:
        """Alias for function_name for compatibility."""
        return self.function_name

    @property
    def args(self) -> List[Expression]:
        """Alias for arguments for compatibility."""
        return self.arguments

    @property
    def partition_by(self) -> List[Expression]:
        """Access partition_by from window_spec."""
        return self.window_spec.partition_by

    @partition_by.setter
    def partition_by(self, value: List[Expression]):
        """Set partition_by on window_spec."""
        self.window_spec.partition_by = value

    @property
    def order_by(self) -> Optional[OrderByClause]:
        """Access order_by from window_spec."""
        return self.window_spec.order_by

    @order_by.setter
    def order_by(self, value):
        """Set order_by on window_spec. Accepts OrderByClause or list of OrderByClause-like items."""
        if isinstance(value, list):
            # If it's a list, merge all items into one OrderByClause
            items = []
            for item in value:
                if isinstance(item, OrderByClause):
                    items.extend(item.items)
                elif isinstance(item, OrderByItem):
                    items.append(item)
            self.window_spec.order_by = OrderByClause(items=items) if items else None
        else:
            self.window_spec.order_by = value

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_window_function(self)

    def __str__(self) -> str:
        args_str = ", ".join(str(a) for a in self.arguments)
        parts = []
        if self.window_spec.partition_by:
            partition_str = ", ".join(str(e) for e in self.window_spec.partition_by)
            parts.append(f"PARTITION BY {partition_str}")
        if self.window_spec.order_by:
            order_str = ", ".join(str(o) for o in self.window_spec.order_by.items)
            parts.append(f"ORDER BY {order_str}")
        over = " ".join(parts)
        return f"{self.function_name}({args_str}) OVER ({over})"

# Additional Statement Types
@dataclass
class WhenClause(ASTNode):
    """WHEN clause in CASE expression."""
    condition: Expression
    result: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_when_clause(self)

    def __str__(self) -> str:
        return f"WHEN {self.condition} THEN {self.result}"

@dataclass
class Case(Expression):
    """CASE expression."""
    whens: List[WhenClause]
    else_result: Optional[Expression] = None

    @property
    def when_clauses(self) -> List[WhenClause]:
        """Alias for whens for compatibility."""
        return self.whens

    @property
    def else_clause(self) -> Optional[Expression]:
        """Alias for else_result for compatibility."""
        return self.else_result

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_case(self)

    def __str__(self) -> str:
        clauses = "\n  ".join(str(w) for w in self.whens)
        result = f"CASE\n  {clauses}"
        if self.else_result:
            result += f"\n  ELSE {self.else_result}"
        result += "\n  END"
        return result

@dataclass
class Insert(Statement):
    """INSERT statement supporting VALUES or SELECT."""
    table: TableRef
    columns: List[Identifier] = field(default_factory=list)
    values: List[List[Expression]] = field(default_factory=list)
    query: Optional[Select] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_insert(self)

@dataclass
class Update(Statement):
    """UPDATE statement."""
    table: TableRef
    assignments: Dict[str, Expression]
    where: Optional[WhereClause] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_update(self)

@dataclass
class Unnest(Expression):
    """UNNEST table function."""
    array_expr: Expression
    with_offset: bool = False
    offset_alias: Optional[str] = None
    
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.generic_visit(self)

@dataclass
class TableSample(ASTNode):
    """TABLESAMPLE clause."""
    table: TableRef
    method: str = "BERNOULLI"  # "SYSTEM", "BERNOULLI", or "RESERVOIR"
    percent: Optional[float] = None
    rows: Optional[int] = None
    seed: Optional[int] = None
    
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.generic_visit(self)

@dataclass
class Pivot(ASTNode):
    """PIVOT operation."""
    source: Union[TableRef, Select]
    aggregate_function: str
    value_column: str
    pivot_column: str
    pivot_values: List[Any]
    alias: Optional[str] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.generic_visit(self)

@dataclass
class Unpivot(ASTNode):
    """UNPIVOT operation."""
    source: Union[TableRef, Select]
    value_column: str
    name_column: str
    columns: List[str]
    include_nulls: bool = False
    alias: Optional[str] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.generic_visit(self)

@dataclass
class CreateTable(Statement):
    """CREATE TABLE statement."""
    table: TableName

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_create_table(self)

@dataclass

class BQDataType(Enum):
    """BigQuery base data types."""
    # Scalar types
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
    INTERVAL = "INTERVAL"
    JSON = "JSON"

    # Complex types
    ARRAY = "ARRAY"
    STRUCT = "STRUCT"
    RANGE = "RANGE"


# Type aliases mapping
TYPE_ALIASES = {
    "INT": BQDataType.INT64,
    "SMALLINT": BQDataType.INT64,
    "INTEGER": BQDataType.INT64,
    "BIGINT": BQDataType.INT64,
    "TINYINT": BQDataType.INT64,
    "BYTEINT": BQDataType.INT64,
    "BOOLEAN": BQDataType.BOOL,
    "DECIMAL": BQDataType.NUMERIC,
    "BIGDECIMAL": BQDataType.BIGNUMERIC,
}


@dataclass
class TypeParameter:
    """Parameter for parameterized types."""
    name: str
    value: Union[int, str]


@dataclass
class BigQueryType:
    """Complete BigQuery type with parameters and nested types."""
    base_type: BQDataType
    parameters: List[TypeParameter] = field(default_factory=list)
    element_type: Optional['BigQueryType'] = None  # For ARRAY
    fields: List[Tuple[Optional[str], 'BigQueryType']] = field(default_factory=list)  # For STRUCT
    range_type: Optional['BigQueryType'] = None  # For RANGE

    def __str__(self) -> str:
        """String representation of the type."""
        if self.base_type == BQDataType.ARRAY:
            return f"ARRAY<{self.element_type}>"
        elif self.base_type == BQDataType.STRUCT:
            field_strs = []
            for name, type_ in self.fields:
                if name:
                    field_strs.append(f"{name} {type_}")
                else:
                    field_strs.append(str(type_))
            return f"STRUCT<{', '.join(field_strs)}>"
        elif self.base_type == BQDataType.RANGE:
            return f"RANGE<{self.range_type}>"
        elif self.parameters:
            param_strs = [str(p.value) for p in self.parameters]
            return f"{self.base_type.value}({', '.join(param_strs)})"
        else:
            return self.base_type.value


class TypeParser:
    """Parser for BigQuery type strings."""

    @staticmethod
    def parse(type_str: str) -> BigQueryType:
        """Parse a BigQuery type string into a BigQueryType object."""
        type_str = type_str.strip()

        # Handle aliases
        type_upper = type_str.upper()
        for alias, base_type in TYPE_ALIASES.items():
            if type_upper.startswith(alias):
                type_str = type_upper.replace(alias, base_type.value, 1)
                break

        # Match complex types
        if type_str.upper().startswith("ARRAY<"):
            return TypeParser._parse_array(type_str)
        elif type_str.upper().startswith("STRUCT<"):
            return TypeParser._parse_struct(type_str)
        elif type_str.upper().startswith("RANGE<"):
            return TypeParser._parse_range(type_str)

        # Match parameterized types
        match = re.match(r'^(\w+)\s*\((.*)\)$', type_str)
        if match:
            base_name = match.group(1).upper()
            params_str = match.group(2)
            return TypeParser._parse_parameterized(base_name, params_str)

        # Simple type
        try:
            base_type = BQDataType[type_str.upper()]
            return BigQueryType(base_type=base_type)
        except KeyError:
            raise ValueError(f"Unknown BigQuery type: {type_str}")

    @staticmethod
    def _parse_array(type_str: str) -> BigQueryType:
        """Parse ARRAY<element_type>."""
        content = TypeParser._extract_angle_bracket_content(type_str)
        element_type = TypeParser.parse(content)
        return BigQueryType(base_type=BQDataType.ARRAY, element_type=element_type)

    @staticmethod
    def _parse_struct(type_str: str) -> BigQueryType:
        """Parse STRUCT<field1 type1, field2 type2, ...>."""
        content = TypeParser._extract_angle_bracket_content(type_str)
        fields = []

        # Simple parser for struct fields
        # This is a simplified version - a full parser would handle nested types better
        field_strs = TypeParser._split_fields(content)

        for field_str in field_strs:
            field_str = field_str.strip()
            # Check if it's a named field
            parts = field_str.split(None, 1)
            if len(parts) == 2 and not parts[0].upper() in [t.value for t in BQDataType]:
                # Named field
                name = parts[0]
                type_str = parts[1]
                field_type = TypeParser.parse(type_str)
                fields.append((name, field_type))
            else:
                # Unnamed field
                field_type = TypeParser.parse(field_str)
                fields.append((None, field_type))

        return BigQueryType(base_type=BQDataType.STRUCT, fields=fields)

    @staticmethod
    def _parse_range(type_str: str) -> BigQueryType:
        """Parse RANGE<element_type>."""
        content = TypeParser._extract_angle_bracket_content(type_str)
        range_type = TypeParser.parse(content)
        return BigQueryType(base_type=BQDataType.RANGE, range_type=range_type)

    @staticmethod
    def _parse_parameterized(base_name: str, params_str: str) -> BigQueryType:
        """Parse parameterized types like NUMERIC(10, 2)."""
        try:
            base_type = BQDataType[base_name]
        except KeyError:
            raise ValueError(f"Unknown BigQuery type: {base_name}")

        params = []
        param_values = [p.strip() for p in params_str.split(',')]

        if base_type in {BQDataType.STRING, BQDataType.BYTES}:
            if param_values:
                params.append(TypeParameter(name="length", value=int(param_values[0])))
        elif base_type in {BQDataType.NUMERIC, BQDataType.BIGNUMERIC}:
            if len(param_values) >= 1:
                params.append(TypeParameter(name="precision", value=int(param_values[0])))
            if len(param_values) >= 2:
                params.append(TypeParameter(name="scale", value=int(param_values[1])))

        return BigQueryType(base_type=base_type, parameters=params)

    @staticmethod
    def _extract_angle_bracket_content(type_str: str) -> str:
        """Extract content between < and >."""
        start = type_str.index('<')
        end = type_str.rindex('>')
        return type_str[start + 1:end]

    @staticmethod
    def _split_fields(content: str) -> List[str]:
        """Split struct fields by comma, respecting nested types."""
        fields = []
        current = []
        depth = 0

        for char in content:
            if char == '<':
                depth += 1
            elif char == '>':
                depth -= 1
            elif char == ',' and depth == 0:
                fields.append(''.join(current))
                current = []
                continue
            current.append(char)

        if current:
            fields.append(''.join(current))

        return fields


class TypeValidator:
    """Validator for BigQuery types and values."""

    @staticmethod
    def validate_type(type_: BigQueryType) -> bool:
        """Validate that a type is well-formed."""
        if type_.base_type == BQDataType.ARRAY:
            return type_.element_type is not None
        elif type_.base_type == BQDataType.STRUCT:
            return len(type_.fields) > 0
        elif type_.base_type == BQDataType.RANGE:
            return (type_.range_type is not None and
                   type_.range_type.base_type in {BQDataType.DATE, BQDataType.DATETIME, BQDataType.TIMESTAMP})
        return True

    @staticmethod
    def validate_value(value: Any, type_: BigQueryType) -> bool:
        """Validate that a value matches a type."""
        # This would implement full validation logic
        # For now, just a placeholder
        return True


class TypeCaster:
    """Type casting utilities for BigQuery."""

    @staticmethod
    def can_cast(from_type: BigQueryType, to_type: BigQueryType) -> bool:
        """Check if a cast is valid."""
        # Implement BigQuery casting rules
        # This is a simplified version

        # Same type - always valid
        if from_type.base_type == to_type.base_type:
            return True

        # Numeric casts
        numeric_types = {BQDataType.INT64, BQDataType.FLOAT64, BQDataType.NUMERIC, BQDataType.BIGNUMERIC}
        if from_type.base_type in numeric_types and to_type.base_type in numeric_types:
            return True

        # String can be cast to most types
        if from_type.base_type == BQDataType.STRING:
            return to_type.base_type != BQDataType.STRUCT

        # Most types can be cast to string
        if to_type.base_type == BQDataType.STRING:
            return True

        # Date/time casts
        datetime_types = {BQDataType.DATE, BQDataType.DATETIME, BQDataType.TIMESTAMP}
        if from_type.base_type in datetime_types and to_type.base_type in datetime_types:
            return True

        return False

    @staticmethod
    def find_common_supertype(types: List["BQDataType"]) -> Optional["BQDataType"]:
        """
        Find a common supertype for a list of BQDataType enums.
        For simplicity, this implementation returns the first type if all are the same,
        otherwise returns a type according to a simple hierarchy (NUMERIC > FLOAT64 > INT64 > STRING).
        """
        if not types:
            return None
        # If all types are the same, return that type
        if all(t == types[0] for t in types):
            return types[0]
        # Simple hierarchy for demonstration
        hierarchy = [BQDataType.NUMERIC, BQDataType.FLOAT64, BQDataType.INT64, BQDataType.STRING]
        for htype in hierarchy:
            if htype in types:
                return htype
        # Fallback: return STRING if mixed types
        return BQDataType.STRING

    @staticmethod
    def requires_safe_cast(from_type: BigQueryType, to_type: BigQueryType) -> bool:
        """Check if a cast might fail and should use SAFE_CAST."""
        # String to numeric/date/time might fail
        if from_type.base_type == BQDataType.STRING:
            risky_types = {BQDataType.INT64, BQDataType.FLOAT64, BQDataType.NUMERIC,
                          BQDataType.BIGNUMERIC, BQDataType.DATE, BQDataType.DATETIME,
                          BQDataType.TIMESTAMP, BQDataType.TIME}
            return to_type.base_type in risky_types

        # Numeric truncation
        if (from_type.base_type in {BQDataType.FLOAT64, BQDataType.NUMERIC, BQDataType.BIGNUMERIC} and
            to_type.base_type == BQDataType.INT64):
            return True

        return False
