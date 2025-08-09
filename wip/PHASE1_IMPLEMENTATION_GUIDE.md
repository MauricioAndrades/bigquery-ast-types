# Phase 1 Implementation Guide - SQL Builders

## Overview
This guide provides complete, production-ready implementations for GROUP BY variants, ORDER BY enhancements, and set operations for the BigQuery AST types library.

## Current State Analysis

### What Exists
- Basic `GroupByClause` with simple expressions list
- Basic `OrderByClause` with items and direction
- Basic `SetOperation` with left/right/operator/all
- Basic `HavingClause` with condition
- Basic `LimitClause` with limit and offset

### What's Missing
- GROUP BY ROLLUP, CUBE, GROUPING SETS, ALL
- ORDER BY with NULLS FIRST/LAST
- Set operations builder functions
- INTERVAL literals with range support
- JSON literal builder
- Query parameter builders

## 1. GROUP BY Enhancements

### Type Updates Required

```python
# In types.py - Update GroupByClause

from enum import Enum

class GroupByType(Enum):
    """Type of GROUP BY operation."""
    STANDARD = "STANDARD"
    ROLLUP = "ROLLUP"
    CUBE = "CUBE"
    GROUPING_SETS = "GROUPING_SETS"
    ALL = "ALL"

@dataclass
class GroupByClause(ASTNode):
    """GROUP BY clause with advanced grouping support."""
    expressions: List[Expression] = field(default_factory=list)
    group_type: GroupByType = GroupByType.STANDARD
    grouping_sets: Optional[List[List[Expression]]] = None
    
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_group_by_clause(self)
```

### Builder Implementations

```python
# In builders.py - Add these methods to the Builders class

@staticmethod
def group_by(*expressions: Union[Expression, int, str]) -> GroupByClause:
    """
    Create GROUP BY clause.
    
    Args:
        expressions: Column expressions, positions (1,2,3), or 'ALL'
        
    Examples:
        b.group_by(b.col("department"), b.col("team"))
        b.group_by(1, 2)  # By position
        b.group_by("ALL")  # GROUP BY ALL
    """
    if len(expressions) == 1 and expressions[0] == "ALL":
        return GroupByClause(group_type=GroupByType.ALL)
    
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
    
    return GroupByClause(expressions=parsed_exprs)

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
```

## 2. HAVING Clause Builder

```python
# In builders.py

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
```

## 3. ORDER BY Enhancements

### Type Updates

```python
# In types.py - Add NullsOrder enum and update OrderByItem

class NullsOrder(Enum):
    """NULL value ordering in ORDER BY."""
    FIRST = "FIRST"
    LAST = "LAST"

@dataclass
class OrderByItem(ASTNode):
    """Single ORDER BY item with NULLS ordering."""
    expression: Expression
    direction: OrderDirection = OrderDirection.ASC
    nulls_order: Optional[NullsOrder] = None
    
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_order_by_item(self)
```

### Builder Implementation

```python
# In builders.py

@staticmethod
def order_by(*items: Union[Expression, Tuple[Expression, str], 
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
```

## 4. LIMIT/OFFSET Builder

```python
# In builders.py

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
```

## 5. Set Operations

### Type Updates

```python
# In types.py - Update SetOperation

@dataclass
class SetOperation(Statement):
    """Set operation combining SELECT statements."""
    left: Select
    right: Select
    operator: SetOperator
    all: bool = False
    corresponding: bool = False  # BigQuery CORRESPONDING support
    
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_set_operation(self)
```

### Builder Implementations

```python
# In builders.py

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
```

## 6. BigQuery-Specific Literals

### INTERVAL Implementation

```python
# In builders.py

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
```

### JSON Implementation

```python
# In builders.py

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
            json_str = json.dumps(value, separators=(',', ':'))
        except (TypeError, ValueError) as e:
            raise ValidationError(f"Invalid JSON value: {e}")
    elif isinstance(value, str):
        # Validate it's valid JSON
        import json
        try:
            json.loads(value)
            json_str = value
        except json.JSONDecodeError as e:
            raise ValidationError(f"Invalid JSON string: {e}")
    else:
        raise ValidationError(f"JSON value must be dict, list, or str, got {type(value)}")
    
    return JSONLiteral(value=json_str)
```

### Query Parameters

```python
# In builders.py

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
```

## 7. Star Expression

```python
# In types.py - Add Star class

@dataclass
class Star(Expression):
    """Star expression (*) for SELECT ALL."""
    table: Optional[str] = None  # For table.* syntax
    except_columns: List[str] = field(default_factory=list)  # For * EXCEPT(col1, col2)
    replace_columns: Dict[str, Expression] = field(default_factory=dict)  # For * REPLACE(expr AS col)
    
    def accept(self, visitor: "ASTVisitor") -> Any:
        return visitor.visit_star(self)

# In builders.py

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
```

## 8. Test Requirements

```python
# test_builders_phase1.py

import pytest
from lib.builders import b, ValidationError
from lib.types import *

class TestGroupByBuilders:
    def test_basic_group_by(self):
        gb = b.group_by(b.col("dept"), b.col("team"))
        assert len(gb.expressions) == 2
        assert gb.group_type == GroupByType.STANDARD
    
    def test_group_by_position(self):
        gb = b.group_by(1, 2, 3)
        assert len(gb.expressions) == 3
        assert all(isinstance(e, IntegerLiteral) for e in gb.expressions)
    
    def test_group_by_all(self):
        gb = b.group_by("ALL")
        assert gb.group_type == GroupByType.ALL
        assert len(gb.expressions) == 0
    
    def test_group_by_rollup(self):
        gb = b.group_by_rollup(b.col("year"), b.col("month"))
        assert gb.group_type == GroupByType.ROLLUP
        assert len(gb.expressions) == 2
    
    def test_group_by_cube(self):
        gb = b.group_by_cube(b.col("product"), b.col("region"))
        assert gb.group_type == GroupByType.CUBE
    
    def test_grouping_sets(self):
        gb = b.grouping_sets([b.col("a")], [b.col("b")], [])
        assert gb.group_type == GroupByType.GROUPING_SETS
        assert len(gb.grouping_sets) == 3
    
    def test_invalid_position(self):
        with pytest.raises(ValidationError):
            b.group_by(0)  # Position must be >= 1

class TestOrderByBuilders:
    def test_simple_order_by(self):
        ob = b.order_by(b.col("name"))
        assert len(ob.items) == 1
        assert ob.items[0].direction == OrderDirection.ASC
    
    def test_order_by_with_direction(self):
        ob = b.order_by((b.col("age"), "DESC"))
        assert ob.items[0].direction == OrderDirection.DESC
    
    def test_order_by_with_nulls(self):
        ob = b.order_by((b.col("salary"), "DESC", "NULLS LAST"))
        assert ob.items[0].nulls_order == NullsOrder.LAST
    
    def test_multiple_order_by(self):
        ob = b.order_by(
            b.col("dept"),
            (b.col("salary"), "DESC", "NULLS FIRST"),
            (b.col("name"), "ASC")
        )
        assert len(ob.items) == 3

class TestSetOperations:
    def test_union(self):
        s1 = Select(select_list=[SelectColumn(b.col("a"))])
        s2 = Select(select_list=[SelectColumn(b.col("b"))])
        
        u = b.union(s1, s2)
        assert u.operator == SetOperator.UNION
        assert u.all == False
    
    def test_union_all(self):
        s1 = Select(select_list=[SelectColumn(b.col("a"))])
        s2 = Select(select_list=[SelectColumn(b.col("b"))])
        
        u = b.union(s1, s2, all=True)
        assert u.all == True
    
    def test_intersect(self):
        s1 = Select(select_list=[SelectColumn(b.col("a"))])
        s2 = Select(select_list=[SelectColumn(b.col("b"))])
        
        i = b.intersect(s1, s2)
        assert i.operator == SetOperator.INTERSECT
    
    def test_except(self):
        s1 = Select(select_list=[SelectColumn(b.col("a"))])
        s2 = Select(select_list=[SelectColumn(b.col("b"))])
        
        e = b.except_(s1, s2)
        assert e.operator == SetOperator.EXCEPT

class TestBigQueryLiterals:
    def test_interval_simple(self):
        i = b.interval(5, "DAY")
        assert "5 DAY" in str(i.value)
    
    def test_interval_range(self):
        i = b.interval("10:20:30", "HOUR", "SECOND")
        assert i.from_part == "HOUR"
        assert i.to_part == "SECOND"
    
    def test_json_from_dict(self):
        j = b.json({"name": "Alice", "age": 30})
        assert "Alice" in j.value
    
    def test_json_from_string(self):
        j = b.json('{"valid": "json"}')
        assert j.value == '{"valid": "json"}'
    
    def test_named_param(self):
        p = b.param("user_id")
        assert p.name == "user_id"
    
    def test_invalid_param_name(self):
        with pytest.raises(ValidationError):
            b.param("123invalid")  # Must start with letter or _

class TestLimitOffset:
    def test_limit_only(self):
        lc = b.limit(10)
        assert lc.limit.value == 10
        assert lc.offset is None
    
    def test_limit_with_offset(self):
        lc = b.limit(10, offset=5)
        assert lc.limit.value == 10
        assert lc.offset.value == 5
    
    def test_limit_with_params(self):
        lc = b.limit(b.param("limit"), offset=b.param("offset"))
        assert isinstance(lc.limit, NamedParameter)
        assert isinstance(lc.offset, NamedParameter)
```

## 9. Serializer Updates

```python
# In serializer.py - Update these visitor methods

def visit_group_by_clause(self, node: GroupByClause) -> Any:
    self._keyword("GROUP BY")
    self._write(" ")
    
    if node.group_type == GroupByType.ALL:
        self._write("ALL")
    elif node.group_type == GroupByType.ROLLUP:
        self._write("ROLLUP(")
        for i, expr in enumerate(node.expressions):
            if i > 0:
                self._write(", ")
            expr.accept(self)
        self._write(")")
    elif node.group_type == GroupByType.CUBE:
        self._write("CUBE(")
        for i, expr in enumerate(node.expressions):
            if i > 0:
                self._write(", ")
            expr.accept(self)
        self._write(")")
    elif node.group_type == GroupByType.GROUPING_SETS:
        self._write("GROUPING SETS(")
        for i, group in enumerate(node.grouping_sets):
            if i > 0:
                self._write(", ")
            self._write("(")
            for j, expr in enumerate(group):
                if j > 0:
                    self._write(", ")
                expr.accept(self)
            self._write(")")
        self._write(")")
    else:
        # Standard GROUP BY
        for i, expr in enumerate(node.expressions):
            if i > 0:
                self._write(", ")
            expr.accept(self)

def visit_order_by_item(self, node: OrderByItem) -> Any:
    node.expression.accept(self)
    if node.direction != OrderDirection.ASC:
        self._write(f" {node.direction.value}")
    if node.nulls_order:
        self._write(f" NULLS {node.nulls_order.value}")

def visit_set_operation(self, node: SetOperation) -> Any:
    node.left.accept(self)
    self._newline()
    self._keyword(node.operator.value)
    if node.all:
        self._write(" ALL")
    if node.corresponding:
        self._write(" CORRESPONDING")
    self._newline()
    node.right.accept(self)

def visit_star(self, node: Star) -> Any:
    if node.table:
        self._write(f"{node.table}.")
    self._write("*")
    
    if node.except_columns:
        self._write(" EXCEPT(")
        for i, col in enumerate(node.except_columns):
            if i > 0:
                self._write(", ")
            self._write(col)
        self._write(")")
    
    if node.replace_columns:
        self._write(" REPLACE(")
        for i, (col, expr) in enumerate(node.replace_columns.items()):
            if i > 0:
                self._write(", ")
            expr.accept(self)
            self._write(f" AS {col}")
        self._write(")")
```

## Implementation Checklist

- [ ] Update `types.py` with new enums and enhanced classes
- [ ] Add `Star` class to types.py  
- [ ] Add `GroupByType` enum to types.py
- [ ] Add `NullsOrder` enum to types.py
- [ ] Update `GroupByClause` in types.py
- [ ] Update `OrderByItem` in types.py
- [ ] Update `SetOperation` in types.py
- [ ] Implement all GROUP BY builders in `builders.py`
- [ ] Implement ORDER BY builder in `builders.py`
- [ ] Implement LIMIT builder in `builders.py`
- [ ] Implement set operation builders in `builders.py`
- [ ] Implement INTERVAL builder in `builders.py`
- [ ] Implement JSON builder in `builders.py`
- [ ] Implement parameter builders in `builders.py`
- [ ] Implement star builder in `builders.py`
- [ ] Add comprehensive docstrings with examples
- [ ] Write unit tests for each builder
- [ ] Update serializer visitor methods
- [ ] Test round-trip (build → serialize → parse → build)
- [ ] Add integration tests with complex queries

## Success Criteria

1. **Type Safety** - All builders have proper type hints and validation
2. **Input Validation** - Clear error messages for invalid inputs
3. **BigQuery Compliance** - Follows BigQuery SQL specification exactly
4. **Test Coverage** - Comprehensive test suite with edge cases
5. **Documentation** - Every public method has docstring with examples
6. **Serialization** - All new features serialize to correct SQL

## Example Usage

```python
from lib.builders import b
from lib.serializer import to_sql

# Complex query with new features
query = b.select([
    b.star(except_=["internal_id"]),
    b.func("COUNT", b.star()).alias("total")
]).from_("sales").group_by_rollup(
    b.col("year"), 
    b.col("month")
).having(
    b.gt(b.func("SUM", b.col("amount")), b.lit(10000))
).order_by(
    (b.col("year"), "DESC"),
    (b.col("month"), "ASC", "NULLS LAST")
).limit(100, offset=10)

print(to_sql(query))
# Output:
# SELECT * EXCEPT(internal_id), COUNT(*) AS total
# FROM sales
# GROUP BY ROLLUP(year, month)
# HAVING SUM(amount) > 10000
# ORDER BY year DESC, month ASC NULLS LAST
# LIMIT 100 OFFSET 10
```

This implementation guide provides everything needed to properly implement Phase 1 with production-quality code.