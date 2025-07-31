"""
SQLGlot Parser for BigQuery AST Types

Wraps SQLGlot to parse BigQuery SQL into our AST representation.
"""

from typing import Any, Optional
import sqlglot
from sqlglot import exp
from sqlglot.dialects import BigQuery

from ..lib.types import *
from ..lib.builders import b


class SQLGlotParser:
    """Parse BigQuery SQL using SQLGlot and transform to our AST."""
    
    def __init__(self, dialect: str = "bigquery"):
        self.dialect = dialect
    
    def parse(self, sql: str) -> Statement:
        """Parse SQL string into our AST representation."""
        try:
            # Parse with SQLGlot
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            
            # Transform to our AST
            return self._transform(parsed)
        except Exception as e:
            raise ValueError(f"Failed to parse SQL: {e}")
    
    def _transform(self, node: exp.Expression) -> Any:
        """Transform SQLGlot AST to our AST."""
        if isinstance(node, exp.Select):
            return self._transform_select(node)
        elif isinstance(node, exp.Merge):
            return self._transform_merge(node)
        else:
            # For now, return a placeholder
            return b.select(b.col("*"))
    
    def _transform_select(self, select: exp.Select) -> Select:
        """Transform SQLGlot Select to our Select."""
        # Extract select columns
        select_list = []
        for projection in select.expressions:
            if isinstance(projection, exp.Star):
                select_list.append(SelectColumn(expression=b.col("*")))
            elif isinstance(projection, exp.Alias):
                expr = self._transform_expression(projection.this)
                select_list.append(SelectColumn(expression=expr, alias=projection.alias))
            else:
                expr = self._transform_expression(projection)
                select_list.append(SelectColumn(expression=expr))
        
        # Create Select node
        result = Select(select_list=select_list)
        
        # Add FROM clause if present
        if select.args.get("from"):
            from_expr = select.args["from"].this
            if isinstance(from_expr, exp.Table):
                table_name = from_expr.name
                alias = from_expr.alias if hasattr(from_expr, 'alias') else None
                result.from_clause = TableRef(
                    table=TableName(table=Identifier(name=table_name)),
                    alias=alias
                )
        
        # Add WHERE clause if present
        if select.args.get("where"):
            condition = self._transform_expression(select.args["where"].this)
            result.where_clause = WhereClause(condition=condition)
        
        return result
    
    def _transform_merge(self, merge: exp.Merge) -> Merge:
        """Transform SQLGlot Merge to our Merge."""
        # Placeholder implementation
        target = TableRef(table=TableName(table=Identifier(name="target")))
        source = TableRef(table=TableName(table=Identifier(name="source")))
        condition = b.eq(b.col("id"), b.col("id"))
        
        return Merge(
            target_table=target,
            source_table=source,
            merge_condition=condition,
            actions=[]
        )
    
    def _transform_expression(self, expr: exp.Expression) -> Expression:
        """Transform SQLGlot expression to our Expression."""
        if isinstance(expr, exp.Column):
            table = expr.table if hasattr(expr, 'table') else None
            return Identifier(name=expr.name, table=table)
        elif isinstance(expr, exp.Literal):
            return self._transform_literal(expr)
        elif isinstance(expr, exp.Binary):
            return BinaryOp(
                left=self._transform_expression(expr.left),
                operator=expr.key.upper(),
                right=self._transform_expression(expr.right)
            )
        elif isinstance(expr, exp.Func):
            args = [self._transform_expression(arg) for arg in expr.args.values() if arg]
            return FunctionCall(function_name=expr.name, arguments=args)
        else:
            # Default: return column placeholder
            return b.col(str(expr))
    
    def _transform_literal(self, lit: exp.Literal) -> Literal:
        """Transform SQLGlot literal to our Literal."""
        if lit.is_string:
            return StringLiteral(value=lit.this)
        elif lit.is_int:
            return IntegerLiteral(value=int(lit.this))
        elif lit.is_number:
            return FloatLiteral(value=float(lit.this))
        else:
            return b.lit(lit.this)


# Convenience function
def parse(sql: str, dialect: str = "bigquery") -> Statement:
    """Parse SQL string into AST using SQLGlot."""
    parser = SQLGlotParser(dialect)
    return parser.parse(sql)