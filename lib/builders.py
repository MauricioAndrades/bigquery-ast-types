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
from .types import *


class ValidationError(Exception):
    """Raised when builder validation fails."""
    pass


class Builders:
    """Fluent builder API for BigQuery AST nodes."""
    
    def col(self, name: str, table: Optional[str] = None) -> Identifier:
        """Create a column identifier."""
        return Identifier(name=name, table=table)
    
    def lit(self, value: Any) -> Literal:
        """Create a literal value."""
        if isinstance(value, str):
            return StringLiteral(value=value)
        elif isinstance(value, int):
            return IntegerLiteral(value=value)
        elif isinstance(value, float):
            return FloatLiteral(value=value)
        elif isinstance(value, bool):
            return BooleanLiteral(value=value)
        elif value is None:
            return NullLiteral()
        else:
            return Literal(value=value)
    
    def select(self, *columns) -> Select:
        """Create a SELECT statement."""
        select_columns = []
        for col in columns:
            if isinstance(col, str):
                select_columns.append(SelectColumn(expression=self.col(col)))
            elif isinstance(col, SelectColumn):
                select_columns.append(col)
            else:
                select_columns.append(SelectColumn(expression=col))
        return Select(select_list=select_columns)
    
    def from_table(self, table_name: str, alias: Optional[str] = None) -> TableRef:
        """Create a table reference."""
        return TableRef(table=self.col(table_name), alias=alias)
    
    def where(self, condition: Expression) -> WhereClause:
        """Create a WHERE clause."""
        return WhereClause(condition=condition)
    
    def join(self, table: TableRef, condition: Expression, join_type: JoinType = JoinType.INNER) -> Join:
        """Create a JOIN clause."""
        return Join(
            join_type=join_type,
            table=table,
            condition=condition
        )
    
    def eq(self, left: Expression, right: Expression) -> BinaryOp:
        """Create an equality comparison."""
        return BinaryOp(left=left, operator="=", right=right)
    
    def and_(self, left: Expression, right: Expression) -> BinaryOp:
        """Create an AND expression."""
        return BinaryOp(left=left, operator="AND", right=right)
    
    def or_(self, left: Expression, right: Expression) -> BinaryOp:
        """Create an OR expression."""
        return BinaryOp(left=left, operator="OR", right=right)
    
    def func(self, name: str, *args) -> FunctionCall:
        """Create a function call."""
        return FunctionCall(function_name=name, arguments=list(args))


# Global builder instance
b = Builders()