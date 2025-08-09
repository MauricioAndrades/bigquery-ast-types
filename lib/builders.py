"""
BigQuery SQL AST Builder Functions

Provides a fluent API for constructing BigQuery SQL AST nodes,
inspired by ast-types builders pattern.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

# TODO: Add `from __future__ import annotations` for cleaner type hints (see issue #2)
from typing import Any, List, Optional, Union, Dict, Tuple
from dataclasses import dataclass, field

# Import types from types module instead of redefining them
from .types import (
    ASTNode,
    Expression,
    Statement,  # Add Statement to imports
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
    TableName,  # Add TableName to main imports
    GroupByClause,
    HavingClause,
    OrderByClause,
    OrderByItem,
    OrderDirection,
    LimitClause,
    IntervalLiteral,
    JSONLiteral,
    NamedParameter,
    OrderDirection,
    NullsOrder,
    WhenClause,
    StringLiteral,
    IntegerLiteral,
    FloatLiteral,
    BooleanLiteral,
    NullLiteral,
    DateLiteral,
    TimestampLiteral,
    IntervalLiteral,
    JSONLiteral,
    SetOperation,
    SetOperator,
    Select,
    GroupByClause,
    GroupByType,
    HavingClause,
    LimitClause,
    NamedParameter,
    PositionalParameter,
    # Phase 2 imports
    WithClause,
    CTE,
    QualifyClause,
    WindowFrame,
    FrameType,
    FrameBound,
    Unnest,
    TableSample,
    Pivot,
    Unpivot,
    Merge,
    MergeWhenClause,
    MergeAction,
    # DDL types (Task 1)
    CreateView,
    CreateFunction,
    AlterTable,
    AddColumn,
    DropColumn,
    RenameColumn,
    AlterColumn,
    SetTableOptions,
    DropStatement,
    # Scripting types (Task 3)
    DeclareStatement,
    VariableDeclaration,
    SetStatement,
    IfStatement,
    ElseIfClause,
    WhileLoop,
    ForLoop,
    BeginEndBlock,
    ExceptionHandler,
    BreakStatement,
    ContinueStatement,
    CallStatement,
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
    def star(table: Optional[str] = None,
             except_: Optional[List[str]] = None,
             replace: Optional[Dict[str, Expression]] = None) -> Star:
        """
        Create star expression with BigQuery extensions.

        Examples:
            b.star()  # SELECT *
            b.star("t1")  # SELECT t1.*
            b.star(except_=["password", "ssn"])  # SELECT * EXCEPT(password, ssn)
            b.star(replace={"name": b.upper(b.col("name"))})  # SELECT * REPLACE(UPPER(name) AS name)
        """
        return Star(
            table=table,
            except_columns=except_ or [],
            replace_columns=replace or {}
        )

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
    def group_by(*expressions: Union[Expression, int, str], rollup: bool = False, cube: bool = False) -> GroupByClause:
        """
        GROUP BY builder with ROLLUP/CUBE support.
        
        Args:
            expressions: Column expressions, positions (1,2,3), or 'ALL'
            rollup: Whether to use GROUP BY ROLLUP
            cube: Whether to use GROUP BY CUBE

        Examples:
            b.group_by(b.col("department"), b.col("team"))
            b.group_by(1, 2)  # By position
            b.group_by("ALL")  # GROUP BY ALL
            b.group_by(b.col("category"), rollup=True)
        """
        if rollup and cube:
            raise ValidationError("GROUP BY cannot have both rollup and cube")
            
        # Handle special case for "ALL"
        if len(expressions) == 1 and expressions[0] == "ALL":
            if rollup or cube:
                raise ValidationError("GROUP BY ALL cannot be used with ROLLUP or CUBE")
            return GroupByClause(group_type=GroupByType.ALL)

        # Parse expressions
        parsed_exprs = []
        for expr in expressions:
            if isinstance(expr, int):
                if expr < 1:
                    raise ValidationError(f"GROUP BY position must be >= 1, got {expr}")
                parsed_exprs.append(IntegerLiteral(expr))
            elif isinstance(expr, str):
                parsed_exprs.append(Identifier(expr))
            elif isinstance(expr, Expression):
                parsed_exprs.append(expr)
            else:
                raise ValidationError(f"Invalid GROUP BY expression type: {type(expr)}")
        
        # Determine group_type based on flags
        if rollup:
            group_type = GroupByType.ROLLUP
        elif cube:
            group_type = GroupByType.CUBE
        else:
            group_type = GroupByType.STANDARD
            
        return GroupByClause(expressions=parsed_exprs, group_type=group_type)

    @staticmethod
    def having(condition: Expression) -> HavingClause:
        """HAVING clause builder."""
        if not isinstance(condition, Expression):
            raise TypeError("condition must be an Expression")
        return HavingClause(condition)

    @staticmethod
    @staticmethod
    def order_by(expr: Expression, desc: bool = False, nulls_first: Optional[bool] = None) -> OrderByItem:
        """ORDER BY builder with NULLS FIRST/LAST."""
        if not isinstance(expr, Expression):
            raise TypeError("expr must be an Expression")
        direction = OrderDirection.DESC if desc else OrderDirection.ASC
        nulls_order = None
        if nulls_first is not None:
            nulls_order = NullsOrder.FIRST if nulls_first else NullsOrder.LAST
        return OrderByItem(expression=expr, direction=direction, nulls_order=nulls_order)

    @staticmethod
    def limit(limit: int, offset: Optional[int] = None) -> LimitClause:
        """LIMIT/OFFSET builder."""
        limit_expr = Builders.lit(limit)
        offset_expr = Builders.lit(offset) if offset is not None else None
        return LimitClause(limit=limit_expr, offset=offset_expr)

    @staticmethod
    def interval(value: int, unit: str) -> IntervalLiteral:
        """INTERVAL literal builder."""
        return IntervalLiteral(value=f"{value} {unit}")

    @staticmethod
    def json(value: Union[dict, list, str]) -> JSONLiteral:
        """
        Create JSON literal from Python objects or string.

        Examples:
            b.json({"name": "Alice", "age": 30})
            b.json([1, 2, 3])
            b.json('{"raw": "json"}')
        """
        if isinstance(value, (dict, list)):
            import json
            try:
                json_str = json.dumps(value, separators=(', ', ': '))  # Include spaces for readability
            except (TypeError, ValueError) as e:
                raise ValidationError(f"Invalid JSON value: {e}")
        elif isinstance(value, str):
            # Validate it's valid JSON
            import json
            try:
                json.loads(value)  # Validate
                json_str = value
            except json.JSONDecodeError as e:
                raise ValidationError(f"Invalid JSON string: {e}")
        else:
            raise ValidationError(f"JSON value must be dict, list, or str, got {type(value)}")
        
        return JSONLiteral(value=json_str)

    @staticmethod
    def param(name: str) -> NamedParameter:
        """Named parameter builder."""
        if not name:
            raise ValidationError("Parameter name cannot be empty")
        return NamedParameter(name)

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

    # Set operations
    @staticmethod
    def union(left: Select, right: Select, all: bool = False,
              corresponding: bool = False) -> SetOperation:
        """
        Create UNION operation.

        Examples:
            b.union(select1, select2)  # UNION DISTINCT
            b.union(select1, select2, all=True)  # UNION ALL
            b.union(select1, select2, corresponding=True)  # CORRESPONDING
        """
        if not isinstance(left, Select) or not isinstance(right, Select):
            raise ValidationError("UNION operands must be Select statements")

        return SetOperation(
            left=left,
            right=right,
            operator=SetOperator.UNION,
            all=all,
            corresponding=corresponding
        )

    @staticmethod
    def intersect(left: Select, right: Select, all: bool = False,
                  corresponding: bool = False) -> SetOperation:
        """
        Create INTERSECT operation.

        Examples:
            b.intersect(select1, select2)  # INTERSECT DISTINCT
            b.intersect(select1, select2, all=True)  # INTERSECT ALL
        """
        if not isinstance(left, Select) or not isinstance(right, Select):
            raise ValidationError("INTERSECT operands must be Select statements")

        return SetOperation(
            left=left,
            right=right,
            operator=SetOperator.INTERSECT,
            all=all,
            corresponding=corresponding
        )

    @staticmethod
    def except_(left: Select, right: Select, all: bool = False,
                corresponding: bool = False) -> SetOperation:
        """
        Create EXCEPT operation (except_ to avoid Python keyword).

        Examples:
            b.except_(select1, select2)  # EXCEPT DISTINCT
            b.except_(select1, select2, all=True)  # EXCEPT ALL
        """
        if not isinstance(left, Select) or not isinstance(right, Select):
            raise ValidationError("EXCEPT operands must be Select statements")

        return SetOperation(
            left=left,
            right=right,
            operator=SetOperator.EXCEPT,
            all=all,
            corresponding=corresponding
        )

    # GROUP BY builders
    @staticmethod
    def group_by_rollup(*expressions: Expression) -> GroupByClause:
        """
        GROUP BY with ROLLUP for hierarchical aggregations.

        Example:
            b.group_by_rollup(b.col("year"), b.col("month"), b.col("day"))
            # Generates: GROUP BY ROLLUP(year, month, day)
            # Creates groupings: (year,month,day), (year,month), (year), ()
        """
        if not expressions:
            raise ValidationError("ROLLUP requires at least one expression")

        for expr in expressions:
            if not isinstance(expr, Expression):
                raise ValidationError(f"ROLLUP expressions must be Expression instances, got {type(expr)}")

        return GroupByClause(
            expressions=list(expressions),
            group_type=GroupByType.ROLLUP
        )

    @staticmethod
    def group_by_cube(*expressions: Expression) -> GroupByClause:
        """
        GROUP BY with CUBE for all combinations.

        Example:
            b.group_by_cube(b.col("product"), b.col("region"))
            # Generates: GROUP BY CUBE(product, region)
            # Creates all possible grouping combinations
        """
        if not expressions:
            raise ValidationError("CUBE requires at least one expression")

        for expr in expressions:
            if not isinstance(expr, Expression):
                raise ValidationError(f"CUBE expressions must be Expression instances, got {type(expr)}")

        return GroupByClause(
            expressions=list(expressions),
            group_type=GroupByType.CUBE
        )

    @staticmethod
    def grouping_sets(*sets: List[Expression]) -> GroupByClause:
        """
        GROUP BY with GROUPING SETS for specific grouping combinations.

        Example:
            b.grouping_sets(
                [b.col("a"), b.col("b")],
                [b.col("c")],
                []  # Grand total
            )
            # Generates: GROUP BY GROUPING SETS((a,b), (c), ())
        """
        if not sets:
            raise ValidationError("GROUPING SETS requires at least one set")

        validated_sets = []
        for s in sets:
            if not isinstance(s, list):
                raise ValidationError(f"Each grouping set must be a list, got {type(s)}")
            for expr in s:
                if not isinstance(expr, Expression):
                    raise ValidationError(f"Grouping set expressions must be Expression instances")
            validated_sets.append(s)

        return GroupByClause(
            group_type=GroupByType.GROUPING_SETS,
            grouping_sets=validated_sets
        )

    # HAVING builder
    @staticmethod
    def having(condition: Expression) -> HavingClause:
        """
        Create HAVING clause for filtering grouped results.

        Examples:
            b.having(b.gt(b.func("COUNT", b.star()), b.lit(5)))
            b.having(b.and_(
                b.gte(b.func("SUM", b.col("amount")), b.lit(1000)),
                b.lt(b.func("AVG", b.col("amount")), b.lit(500))
            ))
        """
        if not isinstance(condition, Expression):
            raise ValidationError(f"HAVING condition must be an Expression, got {type(condition)}")
        return HavingClause(condition=condition)

    # ORDER BY clause builder
    @staticmethod
    def order_by_clause(*items: Union[Expression, Tuple[Expression, str],
                 Tuple[Expression, str, str]]) -> OrderByClause:
        """
        Create ORDER BY clause with flexible syntax.

        Examples:
            b.order_by(b.col("name"))  # Default ASC
            b.order_by((b.col("age"), "DESC"))
            b.order_by((b.col("salary"), "DESC", "NULLS LAST"))
            b.order_by(
                b.col("dept"),
                (b.col("salary"), "DESC", "NULLS FIRST")
            )
        """
        if not items:
            raise ValidationError("ORDER BY requires at least one item")

        order_items = []

        for item in items:
            if isinstance(item, Expression):
                order_items.append(OrderByItem(expression=item))
            elif isinstance(item, tuple):
                if len(item) < 1 or len(item) > 3:
                    raise ValidationError(f"ORDER BY tuple must have 1-3 elements, got {len(item)}")

                expr = item[0]
                if not isinstance(expr, Expression):
                    raise ValidationError(f"First element must be Expression, got {type(expr)}")

                direction = OrderDirection.ASC
                nulls_order = None

                if len(item) >= 2:
                    try:
                        direction = OrderDirection[item[1].upper()]
                    except (KeyError, AttributeError):
                        raise ValidationError(f"Invalid direction: {item[1]}")

                if len(item) >= 3:
                    nulls_part = item[2].upper().replace("NULLS ", "")
                    try:
                        nulls_order = NullsOrder[nulls_part]
                    except KeyError:
                        raise ValidationError(f"Invalid NULLS ordering: {item[2]}")

                order_items.append(OrderByItem(
                    expression=expr,
                    direction=direction,
                    nulls_order=nulls_order
                ))
            else:
                raise ValidationError(f"Invalid ORDER BY item type: {type(item)}")

        return OrderByClause(items=order_items)

    # LIMIT/OFFSET builder
    @staticmethod
    def limit(limit: Union[int, Expression],
             offset: Optional[Union[int, Expression]] = None) -> LimitClause:
        """
        Create LIMIT clause with optional OFFSET.

        Examples:
            b.limit(10)
            b.limit(10, offset=5)
            b.limit(b.param("limit"), offset=b.param("offset"))
        """
        if isinstance(limit, int):
            if limit < 0:
                raise ValidationError(f"LIMIT must be non-negative, got {limit}")
            limit_expr = IntegerLiteral(limit)
        elif isinstance(limit, Expression):
            limit_expr = limit
        else:
            raise ValidationError(f"LIMIT must be int or Expression, got {type(limit)}")

        offset_expr = None
        if offset is not None:
            if isinstance(offset, int):
                if offset < 0:
                    raise ValidationError(f"OFFSET must be non-negative, got {offset}")
                offset_expr = IntegerLiteral(offset)
            elif isinstance(offset, Expression):
                offset_expr = offset
            else:
                raise ValidationError(f"OFFSET must be int or Expression, got {type(offset)}")

        return LimitClause(limit=limit_expr, offset=offset_expr)

    # BigQuery-specific literals
    @staticmethod
    def interval(value: Union[int, str], unit: str,
                to_unit: Optional[str] = None) -> IntervalLiteral:
        """
        Create INTERVAL literal.

        Examples:
            b.interval(5, "DAY")  # INTERVAL 5 DAY
            b.interval(-30, "MINUTE")  # INTERVAL -30 MINUTE
            b.interval("10:20:30", "HOUR", "SECOND")  # INTERVAL '10:20:30' HOUR TO SECOND
        """
        valid_units = {"YEAR", "QUARTER", "MONTH", "WEEK", "DAY",
                       "HOUR", "MINUTE", "SECOND", "MILLISECOND", "MICROSECOND"}

        unit_upper = unit.upper()
        if unit_upper not in valid_units:
            raise ValidationError(f"Invalid interval unit: {unit}")

        if to_unit:
            to_unit_upper = to_unit.upper()
            if to_unit_upper not in valid_units:
                raise ValidationError(f"Invalid interval to_unit: {to_unit}")

            # Validate unit hierarchy for range intervals
            unit_order = ["YEAR", "QUARTER", "MONTH", "WEEK", "DAY",
                         "HOUR", "MINUTE", "SECOND", "MILLISECOND", "MICROSECOND"]
            if unit_order.index(unit_upper) >= unit_order.index(to_unit_upper):
                raise ValidationError(f"Invalid interval range: {unit} TO {to_unit}")

            return IntervalLiteral(value=str(value), from_part=unit_upper, to_part=to_unit_upper)
        else:
            if isinstance(value, int):
                return IntervalLiteral(value=f"{value} {unit_upper}")
            else:
                return IntervalLiteral(value=f"'{value}' {unit_upper}")

    @staticmethod
    def param(name: str) -> NamedParameter:
        """
        Create named query parameter.

        Examples:
            b.param("user_id")  # @user_id
            b.param("start_date")  # @start_date
        """
        if not name or not isinstance(name, str):
            raise ValidationError("Parameter name must be a non-empty string")

        # Validate parameter name follows BigQuery rules
        if not (name[0].isalpha() or name[0] == '_'):
            raise ValidationError(
                f"Parameter name must start with letter or underscore: {name}"
            )

        # Check rest of name
        for char in name[1:]:
            if not (char.isalnum() or char == '_'):
                raise ValidationError(
                    f"Parameter name can only contain letters, numbers, and underscores: {name}"
                )

        return NamedParameter(name=name)

    @staticmethod
    def positional_param(position: Optional[int] = None) -> PositionalParameter:
        """
        Create positional query parameter.

        Examples:
            b.positional_param()  # ? (auto-positioned)
            b.positional_param(0)  # First parameter
            b.positional_param(1)  # Second parameter
        """
        if position is not None and position < 0:
            raise ValidationError(f"Parameter position must be non-negative, got {position}")

        return PositionalParameter(position=position or 0)

    # Phase 2: WITH clause and CTEs
    @staticmethod
    def with_(*ctes: Tuple[str, 'Select'], recursive: bool = False) -> 'WithClause':
        """
        Create WITH clause for CTEs.

        Examples:
            b.with_(
                ("recent_orders", b.select(...).where(...)),
                ("top_customers", b.select(...).where(...))
            )
            b.with_(  # Recursive CTE
                ("fibonacci", b.select(...).union_all(...)),
                recursive=True
            )
        """
        from .types import WithClause, CTE
        
        if not ctes:
            raise ValidationError("WITH clause requires at least one CTE")
        
        cte_list = []
        for name, query in ctes:
            if not isinstance(name, str) or not name:
                raise ValidationError("CTE name must be a non-empty string")
            if not isinstance(query, Select):
                raise ValidationError(f"CTE query must be a Select statement, got {type(query)}")
            cte_list.append(CTE(name=name, query=query))
        
        return WithClause(ctes=cte_list, recursive=recursive)

    @staticmethod
    def with_recursive(*ctes: Tuple[str, 'Select']) -> 'WithClause':
        """
        Create recursive WITH clause.

        Example:
            b.with_recursive(
                ("ancestors", b.select(...).union_all(...))
            )
        """
        return Builders.with_(*ctes, recursive=True)

    # Phase 2: QUALIFY clause
    @staticmethod
    def qualify(condition: Expression) -> 'QualifyClause':
        """
        Create QUALIFY clause for window function filtering.

        Examples:
            b.qualify(b.eq(b.row_number().over(...), b.lit(1)))
            b.qualify(b.lte(b.rank().over(...), b.lit(10)))
        """
        from .types import QualifyClause
        
        if not isinstance(condition, Expression):
            raise ValidationError(f"QUALIFY condition must be an Expression, got {type(condition)}")
        
        return QualifyClause(condition=condition)

    # Phase 2: UNNEST table function
    @staticmethod
    def unnest(array_expr: Expression, ordinality: bool = False,
              alias: Optional[str] = None) -> 'Unnest':
        """
        Create UNNEST table function.

        Examples:
            b.unnest(b.col("array_column"))
            b.unnest(b.col("array_column"), ordinality=True, alias="item")
            b.unnest(b.array(b.lit(1), b.lit(2), b.lit(3)))
        """
        from .types import Unnest
        
        if not isinstance(array_expr, Expression):
            raise ValidationError(f"UNNEST requires an Expression, got {type(array_expr)}")
        
        return Unnest(
            array_expr=array_expr,
            with_offset=ordinality,
            offset_alias=alias
        )

    # Phase 2: TABLESAMPLE
    @staticmethod
    def tablesample(table: Union[str, 'TableRef'],
                    method: str = "BERNOULLI",
                    percent: Optional[float] = None,
                    rows: Optional[int] = None) -> 'TableSample':
        """
        Create TABLESAMPLE clause.

        Examples:
            b.tablesample("orders", percent=10.0)  # Sample 10%
            b.tablesample("orders", rows=1000)  # Sample 1000 rows
            b.tablesample("orders", method="SYSTEM", percent=5.0)
        """
        from .types import TableSample, TableName
        
        if percent is None and rows is None:
            raise ValidationError("TABLESAMPLE requires either percent or rows")
        if percent is not None and rows is not None:
            raise ValidationError("TABLESAMPLE cannot have both percent and rows")
        
        if percent is not None and (percent <= 0 or percent > 100):
            raise ValidationError(f"TABLESAMPLE percent must be between 0 and 100, got {percent}")
        if rows is not None and rows <= 0:
            raise ValidationError(f"TABLESAMPLE rows must be positive, got {rows}")
        
        method_upper = method.upper()
        if method_upper not in ["BERNOULLI", "SYSTEM", "RESERVOIR"]:
            raise ValidationError(f"Invalid TABLESAMPLE method: {method}")
        
        if isinstance(table, str):
            table_ref = TableRef(table=TableName(table=table))
        elif isinstance(table, TableRef):
            table_ref = table
        else:
            raise ValidationError(f"TABLESAMPLE table must be str or TableRef, got {type(table)}")
        
        return TableSample(
            table=table_ref,
            method=method_upper,
            percent=percent,
            rows=rows
        )

    # Phase 2: Window frame specifications
    @staticmethod
    def window_frame(frame_type: str = "ROWS",
                    start: Union[str, int] = "UNBOUNDED PRECEDING",
                    end: Optional[Union[str, int]] = "CURRENT ROW") -> 'WindowFrame':
        """
        Create window frame specification.

        Examples:
            b.window_frame("ROWS", "UNBOUNDED PRECEDING", "CURRENT ROW")
            b.window_frame("ROWS", 2, 0)  # 2 PRECEDING to CURRENT ROW
            b.window_frame("RANGE", "CURRENT ROW", "UNBOUNDED FOLLOWING")
        """
        from .types import WindowFrame, FrameType, FrameBound
        
        # Validate frame type
        try:
            frame_type_enum = FrameType[frame_type.upper()]
        except KeyError:
            raise ValidationError(f"Invalid frame type: {frame_type}. Must be ROWS or RANGE")
        
        # Parse start bound
        if isinstance(start, str):
            start_upper = start.upper()
            if start_upper == "UNBOUNDED PRECEDING":
                start_bound = FrameBound.UNBOUNDED_PRECEDING
            elif start_upper == "CURRENT ROW":
                start_bound = FrameBound.CURRENT_ROW
            elif start_upper == "UNBOUNDED FOLLOWING":
                start_bound = FrameBound.UNBOUNDED_FOLLOWING
            else:
                raise ValidationError(f"Invalid start bound: {start}")
        elif isinstance(start, int):
            if start < 0:
                start_bound = (abs(start), "FOLLOWING")
            else:
                start_bound = (start, "PRECEDING")
        else:
            raise ValidationError(f"Start bound must be str or int, got {type(start)}")
        
        # Parse end bound
        end_bound = None
        if end is not None:
            if isinstance(end, str):
                end_upper = end.upper()
                if end_upper == "UNBOUNDED PRECEDING":
                    end_bound = FrameBound.UNBOUNDED_PRECEDING
                elif end_upper == "CURRENT ROW":
                    end_bound = FrameBound.CURRENT_ROW
                elif end_upper == "UNBOUNDED FOLLOWING":
                    end_bound = FrameBound.UNBOUNDED_FOLLOWING
                else:
                    raise ValidationError(f"Invalid end bound: {end}")
            elif isinstance(end, int):
                if end < 0:
                    end_bound = (abs(end), "FOLLOWING")
                else:
                    end_bound = (end, "PRECEDING")
            else:
                raise ValidationError(f"End bound must be str or int, got {type(end)}")
        
        return WindowFrame(
            frame_type=frame_type_enum,
            start_bound=start_bound,
            end_bound=end_bound
        )

    # Phase 2: PIVOT operation
    @staticmethod
    def pivot(table: Union[str, 'Select'],
             aggregate_function: str,
             value_column: str,
             pivot_column: str,
             pivot_values: List[Any],
             alias: Optional[str] = None) -> 'Pivot':
        """
        Create PIVOT operation.

        Examples:
            b.pivot(
                "sales",
                "SUM", "amount", "quarter", ["Q1", "Q2", "Q3", "Q4"]
            )
            b.pivot(
                b.select(...),
                "AVG", "score", "subject", ["Math", "Science", "English"]
            )
        """
        from .types import Pivot, Select, TableName, TableRef
        
        if isinstance(table, str):
            source = TableRef(table=TableName(table=table))
        elif isinstance(table, (Select, TableRef)):
            source = table
        else:
            raise ValidationError(f"PIVOT table must be str, Select, or TableRef, got {type(table)}")
        
        if not aggregate_function or not isinstance(aggregate_function, str):
            raise ValidationError("PIVOT aggregate_function must be a non-empty string")
        
        if not value_column or not isinstance(value_column, str):
            raise ValidationError("PIVOT value_column must be a non-empty string")
        
        if not pivot_column or not isinstance(pivot_column, str):
            raise ValidationError("PIVOT pivot_column must be a non-empty string")
        
        if not pivot_values or not isinstance(pivot_values, list):
            raise ValidationError("PIVOT pivot_values must be a non-empty list")
        
        return Pivot(
            source=source,
            aggregate_function=aggregate_function,
            value_column=value_column,
            pivot_column=pivot_column,
            pivot_values=pivot_values,
            alias=alias
        )

    # Phase 2: UNPIVOT operation
    @staticmethod
    def unpivot(table: Union[str, 'Select'],
               value_column: str,
               name_column: str,
               columns: List[str],
               include_nulls: bool = False,
               alias: Optional[str] = None) -> 'Unpivot':
        """
        Create UNPIVOT operation.

        Examples:
            b.unpivot(
                "quarterly_sales",
                "sales_amount", "quarter", ["Q1", "Q2", "Q3", "Q4"]
            )
            b.unpivot(
                b.select(...),
                "score", "subject", ["math_score", "science_score", "english_score"],
                include_nulls=True
            )
        """
        from .types import Unpivot, Select, TableName, TableRef
        
        if isinstance(table, str):
            source = TableRef(table=TableName(table=table))
        elif isinstance(table, (Select, TableRef)):
            source = table
        else:
            raise ValidationError(f"UNPIVOT table must be str, Select, or TableRef, got {type(table)}")
        
        if not value_column or not isinstance(value_column, str):
            raise ValidationError("UNPIVOT value_column must be a non-empty string")
        
        if not name_column or not isinstance(name_column, str):
            raise ValidationError("UNPIVOT name_column must be a non-empty string")
        
        if not columns or not isinstance(columns, list):
            raise ValidationError("UNPIVOT columns must be a non-empty list")
        
        for col in columns:
            if not isinstance(col, str):
                raise ValidationError(f"UNPIVOT columns must contain strings, got {type(col)}")
        
        return Unpivot(
            source=source,
            value_column=value_column,
            name_column=name_column,
            columns=columns,
            include_nulls=include_nulls,
            alias=alias
        )

    # Phase 2: MERGE statement builder
    @staticmethod
    def merge(target: Union[str, 'TableRef'],
             source: Union[str, 'TableRef', 'Select'],
             on: Expression) -> 'MergeBuilder':
        """
        Create MERGE statement with fluent API.

        Examples:
            b.merge("target_table", "source_table", b.eq(b.col("t.id"), b.col("s.id")))
                .when_matched_update({"name": b.col("s.name")})
                .when_not_matched_insert(["id", "name"], [b.col("s.id"), b.col("s.name")])
                .when_not_matched_by_source_delete()
        """
        return MergeBuilder(target, source, on)

    # DDL Builder Functions (Task 1)
    @staticmethod
    def create_view(view_name: Union[str, TableName], query: Select, 
                   or_replace: bool = False, materialized: bool = False,
                   if_not_exists: bool = False, columns: Optional[List[str]] = None,
                   partition_by: Optional[Expression] = None,
                   cluster_by: Optional[List[Expression]] = None,
                   options: Optional[Dict[str, Any]] = None) -> CreateView:
        """
        Create a CREATE VIEW statement.
        
        Examples:
            b.create_view("my_view", b.select(b.col("*")).from_(b.table("users")))
            b.create_view("my_view", query, or_replace=True, materialized=True)
        """
        if isinstance(view_name, str):
            view = TableName(table=view_name)
        else:
            view = view_name
            
        if not isinstance(query, Select):
            raise ValidationError("query must be a Select statement")
            
        return CreateView(
            view=view,
            query=query,
            or_replace=or_replace,
            materialized=materialized,
            if_not_exists=if_not_exists,
            columns=columns,
            partition_by=partition_by,
            cluster_by=cluster_by,
            options=options
        )

    @staticmethod
    def create_function(function_name: str, 
                       parameters: Optional[List[Tuple[str, str]]] = None,
                       return_type: Optional[str] = None,
                       language: str = "SQL",
                       body: Optional[str] = None,
                       query: Optional[Select] = None,
                       or_replace: bool = False,
                       if_not_exists: bool = False,
                       temp: bool = False,
                       deterministic: bool = False,
                       options: Optional[Dict[str, Any]] = None) -> CreateFunction:
        """
        Create a CREATE FUNCTION statement.
        
        Examples:
            b.create_function("my_udf", [("x", "INT64")], "INT64", body="x * 2")
            b.create_function("my_func", query=b.select(b.col("count")).from_(b.table("table1")))
        """
        if not function_name:
            raise ValidationError("Function name cannot be empty")
            
        if body and query:
            raise ValidationError("Function cannot have both body and query")
            
        return CreateFunction(
            function_name=function_name,
            parameters=parameters or [],
            return_type=return_type,
            language=language,
            body=body,
            query=query,
            or_replace=or_replace,
            if_not_exists=if_not_exists,
            temp=temp,
            deterministic=deterministic,
            options=options
        )

    @staticmethod
    def alter_table(table_name: Union[str, TableName], *actions) -> AlterTable:
        """
        Create an ALTER TABLE statement.
        
        Examples:
            b.alter_table("my_table", b.add_column("new_col", "STRING"))
            b.alter_table("my_table", b.drop_column("old_col"), b.rename_column("a", "b"))
        """
        if isinstance(table_name, str):
            table = TableName(table=table_name)
        else:
            table = table_name
            
        return AlterTable(table=table, actions=list(actions))

    @staticmethod
    def add_column(column_name: str, column_type: str, 
                  if_not_exists: bool = False,
                  default_value: Optional[Expression] = None,
                  not_null: bool = False) -> AddColumn:
        """
        Create an ADD COLUMN action.
        
        Examples:
            b.add_column("new_col", "STRING")
            b.add_column("id", "INT64", not_null=True, default_value=b.lit(0))
        """
        if not column_name:
            raise ValidationError("Column name cannot be empty")
        if not column_type:
            raise ValidationError("Column type cannot be empty")
            
        return AddColumn(
            column_name=column_name,
            column_type=column_type,
            if_not_exists=if_not_exists,
            default_value=default_value,
            not_null=not_null
        )

    @staticmethod
    def drop_column(column_name: str, if_exists: bool = False) -> DropColumn:
        """
        Create a DROP COLUMN action.
        
        Examples:
            b.drop_column("old_col")
            b.drop_column("col", if_exists=True)
        """
        if not column_name:
            raise ValidationError("Column name cannot be empty")
            
        return DropColumn(column_name=column_name, if_exists=if_exists)

    @staticmethod
    def rename_column(old_name: str, new_name: str) -> RenameColumn:
        """
        Create a RENAME COLUMN action.
        
        Examples:
            b.rename_column("old_name", "new_name")
        """
        if not old_name:
            raise ValidationError("Old column name cannot be empty")
        if not new_name:
            raise ValidationError("New column name cannot be empty")
            
        return RenameColumn(old_name=old_name, new_name=new_name)

    @staticmethod
    def alter_column(column_name: str,
                    set_data_type: Optional[str] = None,
                    set_default: Optional[Expression] = None,
                    drop_default: bool = False,
                    set_not_null: bool = False,
                    drop_not_null: bool = False) -> AlterColumn:
        """
        Create an ALTER COLUMN action.
        
        Examples:
            b.alter_column("col", set_data_type="STRING")
            b.alter_column("col", set_default=b.lit("default"))
            b.alter_column("col", drop_default=True)
        """
        if not column_name:
            raise ValidationError("Column name cannot be empty")
            
        # Ensure only one action is specified
        actions = [set_data_type, set_default, drop_default, set_not_null, drop_not_null]
        specified_actions = [action for action in actions if action]
        if len(specified_actions) != 1:
            raise ValidationError("Exactly one ALTER COLUMN action must be specified")
            
        return AlterColumn(
            column_name=column_name,
            set_data_type=set_data_type,
            set_default=set_default,
            drop_default=drop_default,
            set_not_null=set_not_null,
            drop_not_null=drop_not_null
        )

    @staticmethod
    def set_table_options(options: Dict[str, Any]) -> SetTableOptions:
        """
        Create a SET OPTIONS action.
        
        Examples:
            b.set_table_options({"description": "My table"})
        """
        if not options:
            raise ValidationError("Options dictionary cannot be empty")
            
        return SetTableOptions(options=options)

    @staticmethod
    def drop(object_type: str, object_name: Union[str, TableName],
             if_exists: bool = False, cascade: bool = False) -> DropStatement:
        """
        Create a DROP statement.
        
        Examples:
            b.drop("TABLE", "my_table")
            b.drop("VIEW", "my_view", if_exists=True)
            b.drop("FUNCTION", "my_function", cascade=True)
        """
        if not object_type:
            raise ValidationError("Object type cannot be empty")
        if not object_name:
            raise ValidationError("Object name cannot be empty")
            
        return DropStatement(
            object_type=object_type.upper(),
            object_name=object_name,
            if_exists=if_exists,
            cascade=cascade
        )

    # BigQuery Scripting Builders (Task 3)
    @staticmethod
    def declare(*variables: Tuple[str, str, Optional[Expression]]) -> DeclareStatement:
        """
        Create a DECLARE statement.
        
        Examples:
            b.declare(("var1", "INT64", b.lit(0)), ("var2", "STRING", None))
            b.declare(("counter", "INT64", b.lit(1)))
        """
        var_declarations = []
        for var_info in variables:
            if len(var_info) == 2:
                name, data_type = var_info
                default_value = None
            elif len(var_info) == 3:
                name, data_type, default_value = var_info
            else:
                raise ValidationError("Variable declaration must be (name, type) or (name, type, default)")
            
            var_declarations.append(VariableDeclaration(
                name=name,
                data_type=data_type,
                default_value=default_value
            ))
        
        return DeclareStatement(variables=var_declarations)

    @staticmethod
    def set_var(variable_name: str, value: Expression) -> SetStatement:
        """
        Create a SET statement for variable assignment.
        
        Examples:
            b.set_var("counter", b.lit(5))
            b.set_var("result", b.add(b.col("a"), b.col("b")))
        """
        if not variable_name:
            raise ValidationError("Variable name cannot be empty")
        if not isinstance(value, Expression):
            raise ValidationError("Value must be an Expression")
            
        return SetStatement(variable_name=variable_name, value=value)

    @staticmethod
    def if_(condition: Expression, *then_statements: "Statement") -> IfStatement:
        """
        Create an IF statement.
        
        Examples:
            b.if_(b.gt(b.col("x"), b.lit(0)), b.set_var("result", b.lit("positive")))
        """
        if not isinstance(condition, Expression):
            raise ValidationError("Condition must be an Expression")
            
        return IfStatement(
            condition=condition,
            then_statements=list(then_statements)
        )

    @staticmethod
    def elseif(condition: Expression, *statements: "Statement") -> ElseIfClause:
        """
        Create an ELSEIF clause.
        
        Examples:
            elseif_clause = b.elseif(b.eq(b.col("x"), b.lit(0)), b.set_var("result", b.lit("zero")))
        """
        if not isinstance(condition, Expression):
            raise ValidationError("Condition must be an Expression")
            
        return ElseIfClause(
            condition=condition,
            statements=list(statements)
        )

    @staticmethod
    def while_(condition: Expression, *statements: "Statement", label: Optional[str] = None) -> WhileLoop:
        """
        Create a WHILE loop.
        
        Examples:
            b.while_(b.gt(b.col("counter"), b.lit(0)), 
                    b.set_var("counter", b.sub(b.col("counter"), b.lit(1))))
        """
        if not isinstance(condition, Expression):
            raise ValidationError("Condition must be an Expression")
            
        return WhileLoop(
            condition=condition,
            statements=list(statements),
            label=label
        )

    @staticmethod
    def for_(variable: str, query: Select, *statements: "Statement", label: Optional[str] = None) -> ForLoop:
        """
        Create a FOR loop.
        
        Examples:
            b.for_("row", b.select(b.col("*")).from_(b.table("users")),
                   b.set_var("count", b.add(b.col("count"), b.lit(1))))
        """
        if not variable:
            raise ValidationError("Variable name cannot be empty")
        if not isinstance(query, Select):
            raise ValidationError("Query must be a Select statement")
            
        return ForLoop(
            variable=variable,
            query=query,
            statements=list(statements),
            label=label
        )

    @staticmethod
    def begin_end(*statements: "Statement", 
                  exception_handler: Optional[ExceptionHandler] = None,
                  label: Optional[str] = None) -> BeginEndBlock:
        """
        Create a BEGIN-END block.
        
        Examples:
            b.begin_end(
                b.set_var("x", b.lit(1)),
                b.set_var("y", b.lit(2))
            )
        """
        return BeginEndBlock(
            statements=list(statements),
            exception_handler=exception_handler,
            label=label
        )

    @staticmethod
    def exception_handler(when_conditions: Optional[List[str]] = None,
                         *statements: "Statement") -> ExceptionHandler:
        """
        Create an exception handler.
        
        Examples:
            b.exception_handler(["INVALID_ARGUMENT"], b.set_var("error", b.lit("Invalid input")))
        """
        return ExceptionHandler(
            when_conditions=when_conditions or [],
            statements=list(statements)
        )

    @staticmethod
    def break_(label: Optional[str] = None) -> BreakStatement:
        """
        Create a BREAK statement.
        
        Examples:
            b.break_()
            b.break_("loop_label")
        """
        return BreakStatement(label=label)

    @staticmethod
    def continue_(label: Optional[str] = None) -> ContinueStatement:
        """
        Create a CONTINUE statement.
        
        Examples:
            b.continue_()
            b.continue_("loop_label")
        """
        return ContinueStatement(label=label)

    @staticmethod
    def call(procedure_name: str, *arguments: Expression) -> CallStatement:
        """
        Create a CALL statement.
        
        Examples:
            b.call("my_procedure", b.lit("arg1"), b.lit(42))
        """
        if not procedure_name:
            raise ValidationError("Procedure name cannot be empty")
            
        return CallStatement(
            procedure_name=procedure_name,
            arguments=list(arguments)
        )


# Phase 2: MergeBuilder class for fluent API
class MergeBuilder:
    """Builder for MERGE statements with method chaining."""
    
    def __init__(self, target: Union[str, 'TableRef'],
                 source: Union[str, 'TableRef', 'Select'],
                 on: Expression):
        from .types import Merge, TableRef, TableName, Select
        
        # Convert target to TableRef if string
        if isinstance(target, str):
            target_ref = TableRef(table=TableName(table=target))
        elif isinstance(target, TableRef):
            target_ref = target
        else:
            raise ValidationError(f"MERGE target must be str or TableRef, got {type(target)}")
        
        # Convert source to appropriate type
        if isinstance(source, str):
            source_ref = TableRef(table=TableName(table=source))
        elif isinstance(source, (TableRef, Select)):
            source_ref = source
        else:
            raise ValidationError(f"MERGE source must be str, TableRef, or Select, got {type(source)}")
        
        if not isinstance(on, Expression):
            raise ValidationError(f"MERGE ON condition must be an Expression, got {type(on)}")
        
        self.merge = Merge(
            target=target_ref,
            source=source_ref,
            on_condition=on,
            when_clauses=[]
        )
    
    def when_matched_update(self, updates: Dict[str, Expression],
                           condition: Optional[Expression] = None) -> 'MergeBuilder':
        """Add WHEN MATCHED THEN UPDATE clause."""
        from .types import MergeWhenClause, MergeAction
        
        if not updates:
            raise ValidationError("WHEN MATCHED UPDATE requires at least one update")
        
        for col, expr in updates.items():
            if not isinstance(col, str):
                raise ValidationError(f"Update column must be string, got {type(col)}")
            if not isinstance(expr, Expression):
                raise ValidationError(f"Update expression must be Expression, got {type(expr)}")
        
        clause = MergeWhenClause(
            match_type="MATCHED",
            condition=condition,
            action=MergeAction.UPDATE,
            update_assignments=updates
        )
        self.merge.when_clauses.append(clause)
        return self
    
    def when_matched_delete(self, condition: Optional[Expression] = None) -> 'MergeBuilder':
        """Add WHEN MATCHED THEN DELETE clause."""
        from .types import MergeWhenClause, MergeAction
        
        clause = MergeWhenClause(
            match_type="MATCHED",
            condition=condition,
            action=MergeAction.DELETE
        )
        self.merge.when_clauses.append(clause)
        return self
    
    def when_not_matched_insert(self, columns: Optional[List[str]] = None,
                               values: Optional[List[Expression]] = None,
                               condition: Optional[Expression] = None) -> 'MergeBuilder':
        """Add WHEN NOT MATCHED THEN INSERT clause."""
        from .types import MergeWhenClause, MergeAction
        
        if columns and values:
            if len(columns) != len(values):
                raise ValidationError(f"INSERT columns ({len(columns)}) and values ({len(values)}) must match")
            
            for col in columns:
                if not isinstance(col, str):
                    raise ValidationError(f"INSERT column must be string, got {type(col)}")
            
            for val in values:
                if not isinstance(val, Expression):
                    raise ValidationError(f"INSERT value must be Expression, got {type(val)}")
        
        clause = MergeWhenClause(
            match_type="NOT MATCHED",
            condition=condition,
            action=MergeAction.INSERT,
            insert_columns=columns,
            insert_values=values
        )
        self.merge.when_clauses.append(clause)
        return self
    
    def when_not_matched_by_source_update(self, updates: Dict[str, Expression],
                                         condition: Optional[Expression] = None) -> 'MergeBuilder':
        """Add WHEN NOT MATCHED BY SOURCE THEN UPDATE clause."""
        from .types import MergeWhenClause, MergeAction
        
        if not updates:
            raise ValidationError("WHEN NOT MATCHED BY SOURCE UPDATE requires at least one update")
        
        for col, expr in updates.items():
            if not isinstance(col, str):
                raise ValidationError(f"Update column must be string, got {type(col)}")
            if not isinstance(expr, Expression):
                raise ValidationError(f"Update expression must be Expression, got {type(expr)}")
        
        clause = MergeWhenClause(
            match_type="NOT MATCHED BY SOURCE",
            condition=condition,
            action=MergeAction.UPDATE,
            update_assignments=updates
        )
        self.merge.when_clauses.append(clause)
        return self
    
    def when_not_matched_by_source_delete(self, condition: Optional[Expression] = None) -> 'MergeBuilder':
        """Add WHEN NOT MATCHED BY SOURCE THEN DELETE clause."""
        from .types import MergeWhenClause, MergeAction
        
        clause = MergeWhenClause(
            match_type="NOT MATCHED BY SOURCE",
            condition=condition,
            action=MergeAction.DELETE
        )
        self.merge.when_clauses.append(clause)
        return self
    
    def build(self) -> 'Merge':
        """Build and return the final Merge statement."""
        if not self.merge.when_clauses:
            raise ValidationError("MERGE statement requires at least one WHEN clause")
        return self.merge


# Export builder instance
b = Builders()
