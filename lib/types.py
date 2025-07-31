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
    def visit_cte(self, node: CTE) -> Any:
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

class ArithmeticOp(Enum):
    ADD = "+"
    SUB = "-"
    MUL = "*"
    DIV = "/"
    MOD = "%"

class ComparisonOp(Enum):
    EQ = "="
    NE = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    LIKE = "LIKE"
    IN = "IN"

class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"

class LogicalOp(Enum):
    AND = "AND"
    OR = "OR"
    NOT = "NOT"

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

@dataclass
class CTE(ASTNode):
    """Common Table Expression."""
    name: str
    query: Select
    columns: Optional[List[str]] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_cte(self)

class Expression(ASTNode):
    """Base class for all expressions."""
    pass

@dataclass
class GroupByClause(ASTNode):
    """GROUP BY clause."""
    expressions: List[Expression]

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_group_by_clause(self)

@dataclass
class HavingClause(ASTNode):
    """HAVING clause."""
    condition: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_having_clause(self)

@dataclass
class Join(ASTNode):
    """JOIN clause."""
    join_type: JoinType
    table: TableRef
    condition: Optional[Expression] = None
    using: Optional[List[str]] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_join(self)

@dataclass
class LimitClause(ASTNode):
    """LIMIT clause."""
    limit: Expression
    offset: Optional[Expression] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_limit_clause(self)

@dataclass
class MergeAction(ASTNode):
    """MERGE WHEN clause."""
    condition: Optional[Expression] = None
    action: Union[MergeInsert, MergeUpdate, MergeDelete] = None

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_merge_action(self)

@dataclass
class MergeDelete(ASTNode):
    """MERGE DELETE action."""

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_merge_delete(self)

@dataclass
class MergeInsert(ASTNode):
    """MERGE INSERT action."""
    columns: Optional[List[str]] = None
    values: Optional[List[Expression]] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_merge_insert(self)

@dataclass
class MergeUpdate(ASTNode):
    """MERGE UPDATE action."""
    assignments: Dict[str, Expression]

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_merge_update(self)

@dataclass
class OrderByItem(ASTNode):
    """Single ORDER BY item."""
    expression: Expression
    direction: Optional[OrderDirection] = None
    nulls_first: Optional[bool] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_order_by_item(self)

@dataclass
class OrderByClause(ASTNode):
    """ORDER BY clause."""
    items: List[OrderByItem]

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_order_by_clause(self)

@dataclass
class SelectColumn(ASTNode):
    """Column in SELECT list."""
    expression: Expression
    alias: Optional[str] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_select_column(self)

class Statement(ASTNode):
    """Base class for all statements."""
    pass

@dataclass
class TableName(ASTNode):
    """Table name (possibly qualified)."""
    table: 'Identifier'
    project: Optional['Identifier'] = None
    dataset: Optional['Identifier'] = None

    def accept(self, visitor: 'ASTVisitor') -> Any:
        return visitor.visit_table_name(self)

@dataclass
class TableRef(ASTNode):
    """Table reference in FROM clause."""
    table: Union[TableName, 'Subquery']
    alias: Optional[str] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_table_ref(self)

@dataclass
class WhereClause(ASTNode):
    """WHERE clause."""
    condition: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_where_clause(self)

@dataclass
class WindowSpecification(ASTNode):
    """Window specification for window functions."""
    partition_by: Optional[List[Expression]] = None
    order_by: Optional[OrderByClause] = None
    frame_clause: Optional[str] = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_window_specification(self)

@dataclass
class WithClause(ASTNode):
    """WITH clause containing CTEs."""
    ctes: List[CTE]

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_with_clause(self)

@dataclass
class BinaryOp(Expression):
    """Binary operation."""
    left: Expression
    operator: str
    right: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_binary_op(self)

@dataclass
class FunctionCall(Expression):
    """Function call."""
    function_name: str
    arguments: List[Expression]
    distinct: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_function_call(self)

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
class Literal(Expression):
    """Base class for literal values."""
    value: Any

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_literal(self)

@dataclass
class Merge(Statement):
    """MERGE statement."""
    target_table: TableRef
    source_table: TableRef
    merge_condition: Expression
    actions: List[MergeAction]

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_merge(self)

@dataclass
class Select(Statement):
    """SELECT statement."""
    select_list: List[SelectColumn]
    from_clause: Optional[TableRef] = None
    joins: Optional[List[Join]] = None
    where_clause: Optional[WhereClause] = None
    group_by_clause: Optional[GroupByClause] = None
    having_clause: Optional[HavingClause] = None
    order_by_clause: Optional[OrderByClause] = None
    limit_clause: Optional[LimitClause] = None
    distinct: bool = False

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_select(self)

@dataclass
class Subquery(Expression):
    """Subquery expression."""
    query: Select

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_subquery(self)

@dataclass
class UnaryOp(Expression):
    """Unary operation."""
    operator: str
    operand: Expression

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_unary_op(self)

@dataclass
class WindowFunction(Expression):
    """Window function call."""
    function_name: str
    arguments: List[Expression]
    window_spec: WindowSpecification

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_window_function(self)

@dataclass
class BooleanLiteral(Literal):
    """Boolean literal."""
    value: bool

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_boolean_literal(self)

@dataclass
class FloatLiteral(Literal):
    """Float literal."""
    value: float

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_float_literal(self)

@dataclass
class IntegerLiteral(Literal):
    """Integer literal."""
    value: int

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_integer_literal(self)

@dataclass
class NullLiteral(Literal):
    """NULL literal."""
    value: None = None

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_null_literal(self)

@dataclass
class StringLiteral(Literal):
    """String literal."""
    value: str

    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_string_literal(self)

