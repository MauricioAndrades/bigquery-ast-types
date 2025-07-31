"""
BigQuery SQL AST Builder Functions

Provides a fluent API for constructing BigQuery SQL AST nodes,
inspired by ast-types builders pattern.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

# TODO: Add `from __future__ import annotations` for cleaner type hints (see issue #2)
from typing import Any, List, Optional, Union, Dict
from dataclasses import dataclass, field


# Base AST Node Types
@dataclass
class ASTNode:
    """Base class for all AST nodes."""

    _type: str = field(init=False)

    def __post_init__(self):
        self._type = self.__class__.__name__


# Expression Nodes
@dataclass
class Identifier(ASTNode):
    """Column or table identifier."""

    name: str
    table: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()

    def __str__(self):
        if self.table:
            return f"{self.table}.{self.name}"
        return self.name


@dataclass
class Literal(ASTNode):
    """Literal value."""

    value: Any
    datatype: str  # 'string', 'number', 'boolean', 'null'

    def __str__(self):
        if self.datatype == "null":
            return "NULL"
        elif self.datatype == "boolean":
            return "TRUE" if self.value else "FALSE"
        elif self.datatype == "string":
            return f"'{self.value}'"
        return str(self.value)


@dataclass
class BinaryOp(ASTNode):
    """Binary operation."""

    operator: str
    left: "Expression"
    right: "Expression"

    def __str__(self):
        return f"({self.left} {self.operator} {self.right})"


@dataclass
class UnaryOp(ASTNode):
    """Unary operation."""

    operator: str
    operand: "Expression"

    def __str__(self):
        return f"{self.operator} {self.operand}"


@dataclass
class FunctionCall(ASTNode):
    """Function call."""

    name: str
    args: List["Expression"]

    def __str__(self):
        args_str = ", ".join(str(arg) for arg in self.args)
        return f"{self.name}({args_str})"


@dataclass
class Cast(ASTNode):
    """CAST expression."""

    expr: "Expression"
    target_type: str
    safe: bool = False

    def __str__(self):
        func = "SAFE_CAST" if self.safe else "CAST"
        return f"{func}({self.expr} AS {self.target_type})"


@dataclass
class Case(ASTNode):
    """CASE expression."""

    when_clauses: List["WhenClause"]
    else_clause: Optional["Expression"] = None

    def __str__(self):
        clauses = "\n  ".join(str(w) for w in self.when_clauses)
        result = f"CASE\n  {clauses}"
        if self.else_clause:
            result += f"\n  ELSE {self.else_clause}"
        result += "\n  END"
        return result


@dataclass
class WhenClause(ASTNode):
    """WHEN clause in CASE."""

    condition: "Expression"
    result: "Expression"

    def __str__(self):
        return f"WHEN {self.condition} THEN {self.result}"


@dataclass
class WindowFunction(ASTNode):
    """Window function."""

    name: str
    args: List["Expression"]
    partition_by: List["Expression"] = field(default_factory=list)
    order_by: List["OrderByClause"] = field(default_factory=list)

    def __str__(self):
        args_str = ", ".join(str(arg) for arg in self.args)
        result = f"{self.name}({args_str}) OVER ("
        parts = []
        if self.partition_by:
            partition_str = ", ".join(str(e) for e in self.partition_by)
            parts.append(f"PARTITION BY {partition_str}")
        if self.order_by:
            order_str = ", ".join(str(o) for o in self.order_by)
            parts.append(f"ORDER BY {order_str}")
        result += " ".join(parts) + ")"
        return result


@dataclass
class OrderByClause(ASTNode):
    """ORDER BY clause element."""

    expr: "Expression"
    direction: str = "ASC"  # 'ASC' or 'DESC'

    def __str__(self):
        return f"{self.expr} {self.direction}"


@dataclass
class Array(ASTNode):
    """Array literal."""

    elements: List["Expression"]

    def __str__(self):
        elements_str = ", ".join(str(e) for e in self.elements)
        return f"[{elements_str}]"


@dataclass
class Struct(ASTNode):
    """STRUCT literal."""

    fields: List[tuple[str, "Expression"]]

    def __str__(self):
        fields_str = ", ".join(f"{expr} AS {name}" for name, expr in self.fields)
        return f"STRUCT({fields_str})"


@dataclass
class Star(ASTNode):
    """SELECT * expression."""

    except_columns: List[str] = field(default_factory=list)

    def __str__(self):
        if self.except_columns:
            except_str = ", ".join(self.except_columns)
            return f"* EXCEPT ({except_str})"
        return "*"


# Statement Nodes
@dataclass
class SelectColumn(ASTNode):
    """Single column in SELECT."""

    expr: "Expression"
    alias: Optional[str] = None

    def __str__(self):
        if self.alias and str(self.expr) != self.alias:
            return f"{self.expr} AS {self.alias}"
        return str(self.expr)


@dataclass
class TableRef(ASTNode):
    """Table reference."""

    name: str
    alias: Optional[str] = None

    def __str__(self):
        if self.alias:
            return f"{self.name} AS {self.alias}"
        return self.name


@dataclass
class Join(ASTNode):
    """JOIN clause."""

    join_type: str  # 'INNER', 'LEFT', 'RIGHT', 'FULL OUTER'
    table: TableRef
    condition: "Expression"

    def __str__(self):
        return f"{self.join_type} JOIN {self.table} ON {self.condition}"


@dataclass
class Select(ASTNode):
    """SELECT statement."""

    columns: List[SelectColumn]
    from_clause: List[Union[TableRef, "Select"]]
    joins: List[Join] = field(default_factory=list)
    where: Optional["Expression"] = None
    group_by: List["Expression"] = field(default_factory=list)
    order_by: List[OrderByClause] = field(default_factory=list)
    limit: Optional[int] = None

    def __str__(self):
        # Complex formatting logic would go here
        return "SELECT ..."


@dataclass
class CTE(ASTNode):
    """Common Table Expression."""

    name: str
    query: Select

    def __str__(self):
        return f"{self.name} AS (\n{self.query}\n)"


@dataclass
class WithClause(ASTNode):
    """WITH clause containing CTEs."""

    ctes: List[CTE]

    def __str__(self):
        cte_strs = ",\n".join(str(cte) for cte in self.ctes)
        return f"WITH\n{cte_strs}"


@dataclass
class Merge(ASTNode):
    """MERGE statement."""

    target_table: str
    source: Union[Select, str]
    on_condition: "Expression"
    when_matched: List["MergeAction"] = field(default_factory=list)
    when_not_matched: List["MergeAction"] = field(default_factory=list)
    when_not_matched_by_source: List["MergeAction"] = field(default_factory=list)


@dataclass
class MergeAction(ASTNode):
    """Action in MERGE statement."""

    action_type: str  # 'INSERT', 'UPDATE', 'DELETE'
    condition: Optional["Expression"] = None
    values: Optional[Dict[str, "Expression"]] = None


# Type alias for any expression
Expression = Union[
    Identifier,
    Literal,
    BinaryOp,
    UnaryOp,
    FunctionCall,
    Cast,
    Case,
    WindowFunction,
    Array,
    Struct,
    Star,
]


# Builder Functions - The fluent API
class Builders:
    """Builder functions for creating AST nodes."""

    # Identifiers
    @staticmethod
    def col(name: str, table: Optional[str] = None) -> Identifier:
        """Create a column reference."""
        return Identifier(name, table)

    @staticmethod
    def id(name: str) -> Identifier:
        """Alias for col()."""
        return Identifier(name)

    # Literals
    @staticmethod
    def lit(value: Any) -> Literal:
        """Create a literal value."""
        # TODO: Add validation for BigQuery-specific literal constraints (max string length, numeric precision)
        # TODO: Support additional literal types like BYTES, DATE, TIMESTAMP (see issue #3)
        if value is None:
            return Literal(None, "null")
        elif isinstance(value, bool):
            return Literal(value, "boolean")
        elif isinstance(value, str):
            return Literal(value, "string")
        elif isinstance(value, (int, float)):
            return Literal(value, "number")
        else:
            raise ValueError(f"Unsupported literal type: {type(value)}")

    @staticmethod
    def null() -> Literal:
        """Create a NULL literal."""
        return Literal(None, "null")

    @staticmethod
    def true() -> Literal:
        """Create a TRUE literal."""
        return Literal(True, "boolean")

    @staticmethod
    def false() -> Literal:
        """Create a FALSE literal."""
        return Literal(False, "boolean")

    @staticmethod
    def date(value: str) -> Literal:
        """Create a DATE literal."""
        # Validate date format YYYY-MM-DD without regex
        if not isinstance(value, str) or len(value) != 10:
            raise ValidationError(f"Invalid date format: {value}, expected YYYY-MM-DD")

        parts = value.split("-")
        if len(parts) != 3:
            raise ValidationError(f"Invalid date format: {value}, expected YYYY-MM-DD")

        year, month, day = parts
        if not (
            len(year) == 4
            and year.isdigit()
            and len(month) == 2
            and month.isdigit()
            and len(day) == 2
            and day.isdigit()
        ):
            raise ValidationError(f"Invalid date format: {value}, expected YYYY-MM-DD")

        # Basic range validation
        y, m, d = int(year), int(month), int(day)
        if not (1 <= m <= 12 and 1 <= d <= 31):
            raise ValidationError(f"Invalid date values: {value}")

        return Literal(value, "date")

    @staticmethod
    def timestamp(value: str) -> Literal:
        """Create a TIMESTAMP literal."""
        # Validate timestamp format without regex
        if not isinstance(value, str) or len(value) < 19:
            raise ValidationError(f"Invalid timestamp format: {value}")

        # Check date part (YYYY-MM-DD)
        date_part = value[:10]
        if len(date_part) != 10 or date_part[4] != "-" or date_part[7] != "-":
            raise ValidationError(f"Invalid timestamp format: {value}")

        # Check separator (T or space)
        if len(value) > 10 and value[10] not in ["T", " "]:
            raise ValidationError(f"Invalid timestamp format: {value}")

        # Check time part (HH:MM:SS)
        time_part = value[11:19]
        if len(time_part) != 8 or time_part[2] != ":" or time_part[5] != ":":
            raise ValidationError(f"Invalid timestamp format: {value}")

        # Validate digits
        try:
            year = int(value[0:4])
            month = int(value[5:7])
            day = int(value[8:10])
            hour = int(value[11:13])
            minute = int(value[14:16])
            second = int(value[17:19])

            if not (
                1 <= month <= 12
                and 1 <= day <= 31
                and 0 <= hour <= 23
                and 0 <= minute <= 59
                and 0 <= second <= 59
            ):
                raise ValidationError(f"Invalid timestamp values: {value}")
        except ValueError:
            raise ValidationError(f"Invalid timestamp format: {value}")

        return Literal(value, "timestamp")

    # Binary Operations
    @staticmethod
    def eq(left: Expression, right: Expression) -> BinaryOp:
        """Equality comparison."""
        return BinaryOp("=", left, right)

    @staticmethod
    def neq(left: Expression, right: Expression) -> BinaryOp:
        """Inequality comparison."""
        return BinaryOp("!=", left, right)

    @staticmethod
    def lt(left: Expression, right: Expression) -> BinaryOp:
        """Less than comparison."""
        return BinaryOp("<", left, right)

    @staticmethod
    def lte(left: Expression, right: Expression) -> BinaryOp:
        """Less than or equal comparison."""
        return BinaryOp("<=", left, right)

    @staticmethod
    def gt(left: Expression, right: Expression) -> BinaryOp:
        """Greater than comparison."""
        return BinaryOp(">", left, right)

    @staticmethod
    def gte(left: Expression, right: Expression) -> BinaryOp:
        """Greater than or equal comparison."""
        return BinaryOp(">=", left, right)

    @staticmethod
    def and_(*conditions: Expression) -> Expression:
        """AND multiple conditions."""
        if not conditions:
            return Builders.true()
        if len(conditions) == 1:
            return conditions[0]
        result = conditions[0]
        for cond in conditions[1:]:
            result = BinaryOp("AND", result, cond)
        return result

    @staticmethod
    def or_(*conditions: Expression) -> Expression:
        """OR multiple conditions."""
        if not conditions:
            return Builders.false()
        if len(conditions) == 1:
            return conditions[0]
        result = conditions[0]
        for cond in conditions[1:]:
            result = BinaryOp("OR", result, cond)
        return result

    # Unary Operations
    @staticmethod
    def not_(expr: Expression) -> UnaryOp:
        """NOT operation."""
        return UnaryOp("NOT", expr)

    @staticmethod
    def is_null(expr: Expression) -> UnaryOp:
        """IS NULL check."""
        return UnaryOp("IS NULL", expr)

    @staticmethod
    def is_not_null(expr: Expression) -> UnaryOp:
        """IS NOT NULL check."""
        return UnaryOp("IS NOT NULL", expr)

    @staticmethod
    def is_(expr: Expression, value: Expression) -> BinaryOp:
        """IS comparison (for TRUE/FALSE/NULL)."""
        return BinaryOp("IS", expr, value)

    @staticmethod
    def is_not(expr: Expression, value: Expression) -> BinaryOp:
        """IS NOT comparison."""
        return BinaryOp("IS NOT", expr, value)

    # Functions
    @staticmethod
    def func(name: str, *args: Expression) -> FunctionCall:
        """Generic function call."""
        return FunctionCall(name, list(args))

    @staticmethod
    def coalesce(*args: Expression) -> FunctionCall:
        """COALESCE function."""
        return FunctionCall("COALESCE", list(args))

    @staticmethod
    def nullif(expr: Expression, value: Expression) -> FunctionCall:
        """NULLIF function."""
        return FunctionCall("NULLIF", [expr, value])

    @staticmethod
    def trim(expr: Expression) -> FunctionCall:
        """TRIM function."""
        return FunctionCall("TRIM", [expr])

    @staticmethod
    def concat(*args: Expression) -> FunctionCall:
        """CONCAT function."""
        return FunctionCall("CONCAT", list(args))

    @staticmethod
    def current_timestamp() -> FunctionCall:
        """CURRENT_TIMESTAMP function."""
        return FunctionCall("CURRENT_TIMESTAMP", [])

    @staticmethod
    def timestamp_func(
        expr: Expression, timezone: Optional[Expression] = None
    ) -> FunctionCall:
        """TIMESTAMP function (converts expression to timestamp)."""
        args = [expr]
        if timezone:
            args.append(timezone)
        return FunctionCall("TIMESTAMP", args)

    # Casting
    @staticmethod
    def cast(expr: Expression, target_type: str) -> Cast:
        """CAST expression."""
        return Cast(expr, target_type, safe=False)

    @staticmethod
    def safe_cast(expr: Expression, target_type: str) -> Cast:
        """SAFE_CAST expression."""
        return Cast(expr, target_type, safe=True)

    # CASE expressions
    @staticmethod
    def case(*when_clauses, else_: Optional[Expression] = None) -> Case:
        """CASE expression."""
        return Case(list(when_clauses), else_)

    @staticmethod
    def when(condition: Expression, result: Expression) -> WhenClause:
        """WHEN clause for CASE."""
        return WhenClause(condition, result)

    # Window functions
    @staticmethod
    def row_number() -> WindowFunction:
        """ROW_NUMBER window function."""
        return WindowFunction("ROW_NUMBER", [])

    @staticmethod
    def rank() -> WindowFunction:
        """RANK window function."""
        return WindowFunction("RANK", [])

    # Arrays and Structs
    @staticmethod
    def array(*elements: Expression) -> Array:
        """Array literal."""
        return Array(list(elements))

    @staticmethod
    def struct(**fields: Expression) -> Struct:
        """STRUCT literal."""
        return Struct([(name, expr) for name, expr in fields.items()])

    # SELECT components
    @staticmethod
    def star(except_columns: Optional[List[str]] = None) -> Star:
        """SELECT * or * EXCEPT(...)."""
        return Star(except_columns or [])

    @staticmethod
    def select_col(expr: Expression, alias: Optional[str] = None) -> SelectColumn:
        """Column in SELECT clause."""
        return SelectColumn(expr, alias)

    @staticmethod
    def table(name: str, alias: Optional[str] = None) -> TableRef:
        """Table reference."""
        return TableRef(name, alias)

    # Complex helpers
    @staticmethod
    def null_safe_eq(left: Expression, right: Expression) -> Expression:
        """Null-safe equality: (A = B OR (A IS NULL AND B IS NULL))."""
        return Builders.or_(
            Builders.eq(left, right),
            Builders.and_(Builders.is_null(left), Builders.is_null(right)),
        )


# Export builder instance
b = Builders()
