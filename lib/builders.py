"""
BigQuery SQL AST Builder Functions

Provides a fluent API for constructing BigQuery SQL AST nodes,
inspired by ast-types builders pattern.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

# TODO: Add `from __future__ import annotations` for cleaner type hints (see issue #2)
from typing import Any, List, Optional, Union, Dict
from dataclasses import dataclass, field  # pyright: ignore [unused-import]

# Import types from types module instead of redefining them
from .types import (
    ASTNode,
    Expression,
    Identifier,
    Literal,
    BinaryOp,
    UnaryOp,
    FunctionCall,
    Cast,
    Case,
    WindowFunction,
    ArrayLiteral as Array,
    StructLiteral as Struct,
    Star,
    SelectColumn,
    TableRef,
    GroupByClause,
    HavingClause,
    OrderByClause,
    OrderByItem,
    OrderDirection,
    NullsOrder,
    LimitClause,
    IntervalLiteral,
    JSONLiteral,
    NamedParameter,
    PositionalParameter,
    WhenClause,
    StringLiteral,
    IntegerLiteral,
    FloatLiteral,
    BooleanLiteral,
    NullLiteral,
    DateLiteral,
    TimestampLiteral,
    SetOperation,
    SetOperator,
    Select
)

class ValidationError(Exception):
    """Custom exception for builder validation errors in BigQuery AST."""
    pass


# Builder Functions - The fluent API
class Builders:
    """Builder functions for creating AST nodes."""

    # Identifiers
    @staticmethod
    def col(name: str, table: Optional[str] = None) -> Identifier:
        """Create a column reference."""
        if name is None:
            raise ValidationError("Column name cannot be None")
        if not name or not str(name).strip():
            raise ValidationError("Column name cannot be empty")
        return Identifier(name, table)

    @staticmethod
    def id(name: str) -> Identifier:
        """Alias for col()."""
        return Identifier(name)

    # Literals
    @staticmethod
    def lit(value: Any) -> Literal:
        """Create a literal value."""
        # TODO: Support additional literal types like BYTES, DATE, TIMESTAMP (see issue #3)
        if value is None:
            return NullLiteral()
        if isinstance(value, bool):
            return BooleanLiteral(value)
        elif isinstance(value, str):
            # Validate string size (1MB limit)
            if len(value) > 1048576:  # 1MB = 1048576 bytes
                raise ValidationError("String literal exceeds 1MB limit")
            return StringLiteral(value)
        elif isinstance(value, int):
            # Validate INT64 range
            if value < -9223372036854775808 or value > 9223372036854775807:
                raise ValidationError(f"Integer {value} out of INT64 range")
            return IntegerLiteral(value)
        elif isinstance(value, float):
            return FloatLiteral(value)
        else:
            raise TypeError(f"Unsupported literal type: {type(value)}")

    @staticmethod
    def null() -> Literal:
        """Create a NULL literal."""
        return NullLiteral()

    @staticmethod
    def true() -> Literal:
        """Create a TRUE literal."""
        return BooleanLiteral(True)

    @staticmethod
    def false() -> Literal:
        """Create a FALSE literal."""
        return BooleanLiteral(False)

    @staticmethod
    def date(value: str) -> Literal:
        """Create a DATE literal."""
        # Validate date format YYYY-MM-DD without regex
        if not isinstance(value, str) or len(value) != 10:
            raise ValidationError(f"Invalid date format: {value}, expected YYYY-MM-DD")

        parts = value.split('-')
        if len(parts) != 3:
            raise ValidationError(f"Invalid date format: {value}, expected YYYY-MM-DD")

        year, month, day = parts
        if not (len(year) == 4 and year.isdigit() and
                len(month) == 2 and month.isdigit() and
                len(day) == 2 and day.isdigit()):
            raise ValidationError(f"Invalid date format: {value}, expected YYYY-MM-DD")

        # Basic range validation
        y, m, d = int(year), int(month), int(day)
        if not (1 <= m <= 12 and 1 <= d <= 31):
            raise ValidationError(f"Invalid date values: {value}")

        return DateLiteral(value)

    @staticmethod
    def timestamp(value: str) -> Literal:
        """Create a TIMESTAMP literal."""
        # Validate timestamp format without regex
        if not isinstance(value, str) or len(value) < 19:
            raise ValidationError(f"Invalid timestamp format: {value}")

        # Check date part (YYYY-MM-DD)
        date_part = value[:10]
        if len(date_part) != 10 or date_part[4] != '-' or date_part[7] != '-':
            raise ValidationError(f"Invalid timestamp format: {value}")

        # Check separator (T or space)
        if len(value) > 10 and value[10] not in ['T', ' ']:
            raise ValidationError(f"Invalid timestamp format: {value}")

        # Check time part (HH:MM:SS)
        time_part = value[11:19]
        if len(time_part) != 8 or time_part[2] != ':' or time_part[5] != ':':
            raise ValidationError(f"Invalid timestamp format: {value}")

        # Validate digits
        try:
            year = int(value[0:4])
            month = int(value[5:7])
            day = int(value[8:10])
            hour = int(value[11:13])
            minute = int(value[14:16])
            second = int(value[17:19])

            if not (1 <= month <= 12 and 1 <= day <= 31 and
                    0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
                raise ValidationError(f"Invalid timestamp values: {value}")
        except ValueError:
            raise ValidationError(f"Invalid timestamp format: {value}")

        return TimestampLiteral(value)

    # Binary Operations
    @staticmethod
    def eq(left: Expression, right: Expression) -> BinaryOp:
        """Equality comparison."""
        if not isinstance(left, Expression) or not isinstance(right, Expression):
            raise TypeError("Both operands must be Expression instances")
        return BinaryOp(left, '=', right)

    @staticmethod
    def neq(left: Expression, right: Expression) -> BinaryOp:
        """Inequality comparison."""
        if not isinstance(left, Expression) or not isinstance(right, Expression):
            raise TypeError("Both operands must be Expression instances")
        return BinaryOp(left, '!=', right)

    @staticmethod
    def lt(left: Expression, right: Expression) -> BinaryOp:
        """Less than comparison."""
        if not isinstance(left, Expression) or not isinstance(right, Expression):
            raise TypeError("Both operands must be Expression instances")
        return BinaryOp(left, '<', right)

    @staticmethod
    def lte(left: Expression, right: Expression) -> BinaryOp:
        """Less than or equal comparison."""
        if not isinstance(left, Expression) or not isinstance(right, Expression):
            raise TypeError("Both operands must be Expression instances")
        return BinaryOp(left, '<=', right)

    @staticmethod
    def gt(left: Expression, right: Expression) -> BinaryOp:
        """Greater than comparison."""
        if not isinstance(left, Expression) or not isinstance(right, Expression):
            raise TypeError("Both operands must be Expression instances")
        return BinaryOp(left, '>', right)

    @staticmethod
    def gte(left: Expression, right: Expression) -> BinaryOp:
        """Greater than or equal comparison."""
        if not isinstance(left, Expression) or not isinstance(right, Expression):
            raise TypeError("Both operands must be Expression instances")
        return BinaryOp(left, '>=', right)

    @staticmethod
    def and_(*conditions: Expression) -> Expression:
        """AND multiple conditions."""
        if not conditions:
            return Builders.true()
        if len(conditions) == 1:
            return conditions[0]
        result = conditions[0]
        for cond in conditions[1:]:
            result = BinaryOp(result, 'AND', cond)
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
            result = BinaryOp(result, 'OR', cond)
        return result

    # Unary Operations
    @staticmethod
    def not_(expr: Expression) -> UnaryOp:
        """NOT operation."""
        return UnaryOp('NOT', expr)

    @staticmethod
    def is_null(expr: Expression) -> UnaryOp:
        """IS NULL check."""
        return UnaryOp('IS NULL', expr)

    @staticmethod
    def is_not_null(expr: Expression) -> UnaryOp:
        """IS NOT NULL check."""
        return UnaryOp('IS NOT NULL', expr)

    @staticmethod
    def is_(expr: Expression, value: Expression) -> BinaryOp:
        """IS comparison (for TRUE/FALSE/NULL)."""
        return BinaryOp(expr, 'IS', value)

    @staticmethod
    def is_not(expr: Expression, value: Expression) -> BinaryOp:
        """IS NOT comparison."""
        return BinaryOp(expr, 'IS NOT', value)

    # Functions
    @staticmethod
    def func(name: str, *args: Expression) -> FunctionCall:
        """Generic function call."""
        if not name or not str(name).strip():
            raise ValidationError("Function name cannot be empty")
        # Validate args are proper expressions
        for arg in args:
            if arg is not None and not isinstance(arg, Expression):
                raise TypeError(f"All function arguments must be Expression instances, got {type(arg)}")
        return FunctionCall(function_name=name, arguments=list(args))

    @staticmethod
    def coalesce(*args: Expression) -> FunctionCall:
        """COALESCE function."""
        return FunctionCall(function_name='COALESCE', arguments=list(args))

    @staticmethod
    def nullif(expr: Expression, value: Expression) -> FunctionCall:
        """NULLIF function."""
        return FunctionCall(function_name='NULLIF', arguments=[expr, value])

    @staticmethod
    def trim(expr: Expression) -> FunctionCall:
        """TRIM function."""
        return FunctionCall(function_name='TRIM', arguments=[expr])

    @staticmethod
    def concat(*args: Expression) -> FunctionCall:
        """CONCAT function."""
        return FunctionCall(function_name='CONCAT', arguments=list(args))

    @staticmethod
    def current_timestamp() -> FunctionCall:
        """CURRENT_TIMESTAMP function."""
        return FunctionCall(function_name='CURRENT_TIMESTAMP', arguments=[])

    @staticmethod
    def timestamp_func(expr: Expression, timezone: Optional[Expression] = None) -> FunctionCall:
        """TIMESTAMP function (converts expression to timestamp)."""
        args = [expr]
        if timezone:
            args.append(timezone)
        return FunctionCall(function_name='TIMESTAMP', arguments=args)

    # Casting
    @staticmethod
    def cast(expr: Expression, target_type: str) -> Cast:
        """CAST expression."""
        # Validate target type
        valid_types = {
            'STRING', 'INT64', 'FLOAT64', 'NUMERIC', 'BIGNUMERIC', 
            'BOOL', 'DATE', 'DATETIME', 'TIME', 'TIMESTAMP',
            'BYTES', 'ARRAY', 'STRUCT', 'JSON'
        }
        if target_type.upper() not in valid_types:
            raise ValidationError(f"Invalid data type: {target_type}")
        return Cast(expr, target_type, safe=False)

    @staticmethod
    def safe_cast(expr: Expression, target_type: str) -> Cast:
        """SAFE_CAST expression."""
        # Validate target type
        valid_types = {
            'STRING', 'INT64', 'FLOAT64', 'NUMERIC', 'BIGNUMERIC', 
            'BOOL', 'DATE', 'DATETIME', 'TIME', 'TIMESTAMP',
            'BYTES', 'ARRAY', 'STRUCT', 'JSON'
        }
        if target_type.upper() not in valid_types:
            raise ValidationError(f"Invalid data type: {target_type}")
        return Cast(expr, target_type, safe=True)

    # CASE expressions
    @staticmethod
    def case(*when_clauses, else_: Optional[Expression] = None) -> Case:
        """CASE expression."""
        return Case(whens=list(when_clauses), else_result=else_)

    @staticmethod
    def when(condition: Expression, result: Expression) -> WhenClause:
        """WHEN clause for CASE."""
        return WhenClause(condition, result)

    # Window functions
    @staticmethod
    def row_number() -> WindowFunction:
        """ROW_NUMBER window function."""
        return WindowFunction(function_name='ROW_NUMBER', arguments=[])

    @staticmethod
    def rank() -> WindowFunction:
        """RANK window function."""
        return WindowFunction(function_name='RANK', arguments=[])

    # Arrays and Structs
    @staticmethod
    def array(*elements: Expression) -> Array:
        """Array literal."""
        return Array(elements=list(elements))

    @staticmethod
    def struct(**fields: Expression) -> Struct:
        """STRUCT literal."""
        return Struct(fields=[(name, expr) for name, expr in fields.items()])

    # SELECT components
    @staticmethod
    def star(except_columns: Optional[List[str]] = None) -> Star:
        """SELECT * or * EXCEPT(...)."""
        return Star(except_columns=except_columns or [])

    @staticmethod
    def select_col(expr: Expression, alias: Optional[str] = None) -> SelectColumn:
        """Column in SELECT clause."""
        return SelectColumn(expression=expr, alias=alias)

    @staticmethod
    def table(name: str, alias: Optional[str] = None) -> TableRef:
        """Table reference."""
        from .types import TableName
        table_node = TableName(table=name)
        return TableRef(table=table_node, alias=alias)

    # Clauses
    @staticmethod
    def group_by(*expressions: Union[Expression, int, str]) -> GroupByClause:
        """Create GROUP BY clause.

        Args:
            expressions: Column expressions, positions (1,2,3), or 'ALL'

        Examples:
            b.group_by(b.col("department"), b.col("team"))
            b.group_by(1, 2)  # By position
            b.group_by("ALL")  # GROUP BY ALL
        """
        if len(expressions) == 1 and expressions[0] == "ALL":
            return GroupByClause(all=True)

        parsed_exprs: List[Expression] = []
        for expr in expressions:
            if isinstance(expr, int):
                parsed_exprs.append(IntegerLiteral(expr))
            elif isinstance(expr, str):
                parsed_exprs.append(Identifier(expr))
            elif isinstance(expr, Expression):
                parsed_exprs.append(expr)
            else:
                raise ValidationError(f"Invalid GROUP BY expression: {expr}")

        return GroupByClause(expressions=parsed_exprs)

    @staticmethod
    def group_by_rollup(*expressions: Expression) -> GroupByClause:
        """GROUP BY with ROLLUP."""
        return GroupByClause(rollup=list(expressions))

    @staticmethod
    def group_by_cube(*expressions: Expression) -> GroupByClause:
        """GROUP BY with CUBE."""
        return GroupByClause(cube=list(expressions))

    @staticmethod
    def grouping_sets(*sets: List[Expression]) -> GroupByClause:
        """GROUP BY with GROUPING SETS."""
        return GroupByClause(grouping_sets=list(sets))

    @staticmethod
    def having(condition: Expression) -> HavingClause:
        """Create HAVING clause for filtering grouped results."""
        if not isinstance(condition, Expression):
            raise ValidationError("HAVING condition must be an Expression")
        return HavingClause(condition=condition)

    @staticmethod
    def order_by(*items: Union[Expression, tuple]) -> OrderByClause:
        """Create ORDER BY clause with flexible syntax."""
        order_items: List[OrderByItem] = []
        for item in items:
            if isinstance(item, Expression):
                order_items.append(OrderByItem(expression=item))
            elif isinstance(item, tuple):
                expr = item[0]
                direction = OrderDirection.ASC
                nulls_order: Optional[NullsOrder] = None
                if len(item) >= 2:
                    direction = OrderDirection[item[1].upper()]
                if len(item) >= 3:
                    nulls_part = item[2].upper().replace("NULLS ", "")
                    nulls_order = NullsOrder[nulls_part]
                order_items.append(OrderByItem(expression=expr, direction=direction, nulls_order=nulls_order))
            else:
                raise ValidationError(f"Invalid ORDER BY item: {item}")

        return OrderByClause(items=order_items)

    @staticmethod
    def limit(limit: Union[int, Expression], offset: Optional[Union[int, Expression]] = None) -> LimitClause:
        """Create LIMIT clause with optional OFFSET."""
        limit_expr = IntegerLiteral(limit) if isinstance(limit, int) else limit
        offset_expr = None
        if offset is not None:
            offset_expr = IntegerLiteral(offset) if isinstance(offset, int) else offset
        return LimitClause(limit=limit_expr, offset=offset_expr)

    @staticmethod
    def union(left: Select, right: Select, all: bool = False, corresponding: bool = False) -> SetOperation:
        """Create UNION operation."""
        return SetOperation(left=left, right=right, operator=SetOperator.UNION, all=all, corresponding=corresponding)

    @staticmethod
    def intersect(left: Select, right: Select, all: bool = False, corresponding: bool = False) -> SetOperation:
        """Create INTERSECT operation."""
        return SetOperation(left=left, right=right, operator=SetOperator.INTERSECT, all=all, corresponding=corresponding)

    @staticmethod
    def except_(left: Select, right: Select, all: bool = False, corresponding: bool = False) -> SetOperation:
        """Create EXCEPT operation."""
        return SetOperation(left=left, right=right, operator=SetOperator.EXCEPT, all=all, corresponding=corresponding)

    @staticmethod
    def interval(value: int, unit: str) -> IntervalLiteral:
        """Create INTERVAL literal with single datetime part."""
        valid_units = {"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"}
        if unit.upper() not in valid_units:
            raise ValidationError(f"Invalid interval unit: {unit}")
        return IntervalLiteral(value=f"{value} {unit.upper()}")

    @staticmethod
    def interval_range(value: str, start_unit: str, end_unit: str) -> IntervalLiteral:
        """Create INTERVAL with datetime range."""
        return IntervalLiteral(value=value, from_part=start_unit.upper(), to_part=end_unit.upper())

    @staticmethod
    def json(value: Union[dict, list, str]) -> JSONLiteral:
        """Create JSON literal from Python objects or string."""
        if isinstance(value, (dict, list)):
            import json as _json
            json_str = _json.dumps(value, indent=2)
        else:
            json_str = value
        return JSONLiteral(value=json_str)

    @staticmethod
    def param(name: str) -> NamedParameter:
        """Create named query parameter."""
        if not name or not isinstance(name, str):
            raise ValidationError("Parameter name must be a non-empty string")
        if not (name[0].isalpha() or name[0] == '_'):
            raise ValidationError(
                f"Parameter name must start with letter or underscore: {name}"
            )
        return NamedParameter(name=name)

    @staticmethod
    def param_positional(position: int = None) -> PositionalParameter:
        """Create positional query parameter."""
        return PositionalParameter(position=position or 0)

    # Complex helpers
    @staticmethod
    def null_safe_eq(left: Expression, right: Expression) -> Expression:
        """Null-safe equality: (A = B OR (A IS NULL AND B IS NULL))."""
        return Builders.or_(
            Builders.eq(left, right),
            Builders.and_(
                Builders.is_null(left),
                Builders.is_null(right)
            )
        )


# Export builder instance
b = Builders()
