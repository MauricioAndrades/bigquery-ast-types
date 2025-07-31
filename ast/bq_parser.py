"""
BigQuery SQL Parser

Parses BigQuery SQL into our AST representation.
Uses bigquery-sql-parser for the heavy lifting.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

from typing import List, Optional, Union, Any
from bigquery_sql_parser import Query
from bigquery_sql_parser.script import Script

from .bq_builders import (
    b, ASTNode, Expression, Select, SelectColumn, TableRef, 
    Join, CTE, WithClause, Merge, MergeAction, OrderByClause,
    Identifier, Literal, BinaryOp, UnaryOp, FunctionCall,
    Cast, Case, WhenClause, WindowFunction, Array, Struct, Star
)


class BQParser:
    """Parser for BigQuery SQL using bigquery-sql-parser."""
    
    def parse(self, sql: str) -> ASTNode:
        """Parse SQL string into AST."""
        try:
            # Try as script first (might have multiple statements)
            script = Script(sql)
            if len(script.statements) == 1:
                return self._parse_statement(script.statements[0])
            else:
                # Return a script node containing multiple statements
                return self._parse_script(script)
        except:
            # Try as single query
            query = Query(sql)
            return self._parse_query(query)
    
    def _parse_script(self, script: Script) -> List[ASTNode]:
        """Parse a script with multiple statements."""
        return [self._parse_statement(stmt) for stmt in script.statements]
    
    def _parse_statement(self, stmt: Any) -> ASTNode:
        """Parse a single statement."""
        # This would need to inspect the statement type
        # For now, assume it's a query
        return self._parse_query(stmt)
    
    def _parse_query(self, query: Query) -> Select:
        """Parse a SELECT query."""
        # Extract columns
        columns = []
        for col in query.columns:
            expr = self._parse_column_value(col.value)
            columns.append(SelectColumn(expr, col.name if col.name != col.value else None))
        
        # Extract FROM clause
        from_clause = []
        for table_id in query.full_table_ids:
            from_clause.append(TableRef(table_id))
        
        # Create SELECT node
        return Select(
            columns=columns,
            from_clause=from_clause,
            joins=[],  # Would parse JOINs
            where=None,  # Would parse WHERE
            group_by=[],  # Would parse GROUP BY
            order_by=[],  # Would parse ORDER BY
            limit=None  # Would parse LIMIT
        )
    
    def _parse_column_value(self, value: str) -> Expression:
        """Parse a column value expression."""
        # TODO: Replace heuristic parsing with proper lexer/parser (see issue #7)
        # TODO: Handle all BigQuery expression types (ARRAY, STRUCT, etc.)
        # Simple heuristic parsing
        value = value.strip()
        
        # Check for literals
        if value.upper() == 'NULL':
            return b.null()
        elif value.upper() == 'TRUE':
            return b.true()
        elif value.upper() == 'FALSE':
            return b.false()
        elif value.startswith("'") and value.endswith("'"):
            return b.lit(value[1:-1])
        elif value.replace('.', '').replace('-', '').isdigit():
            if '.' in value:
                return b.lit(float(value))
            else:
                return b.lit(int(value))
        
        # Check for function calls
        if '(' in value and value.endswith(')'):
            return self._parse_function_call(value)
        
        # Check for qualified identifiers
        if '.' in value:
            parts = value.split('.', 1)
            return b.col(parts[1], parts[0])
        
        # Default to identifier
        return b.col(value)
    
    def _parse_function_call(self, value: str) -> FunctionCall:
        """Parse a function call."""
        # Find function name and args
        paren_idx = value.index('(')
        func_name = value[:paren_idx].strip()
        args_str = value[paren_idx+1:-1].strip()
        
        # Parse arguments (simplified)
        args = []
        if args_str:
            # Very simple comma splitting (doesn't handle nested functions)
            for arg in args_str.split(','):
                args.append(self._parse_column_value(arg.strip()))
        
        return b.func(func_name, *args)


class BQTransformer:
    """Transform parsed AST nodes."""
    
    def transform(self, node: ASTNode) -> ASTNode:
        """Transform an AST node."""
        method_name = f"transform_{node._type}"
        method = getattr(self, method_name, self.generic_transform)
        return method(node)
    
    def generic_transform(self, node: ASTNode) -> ASTNode:
        """Default transformation - return node as-is."""
        return node
    
    def transform_Select(self, node: Select) -> Select:
        """Transform SELECT statement."""
        # Transform all child nodes
        return Select(
            columns=[self.transform(col) for col in node.columns],
            from_clause=[self.transform(table) for table in node.from_clause],
            joins=[self.transform(join) for join in node.joins],
            where=self.transform(node.where) if node.where else None,
            group_by=[self.transform(expr) for expr in node.group_by],
            order_by=[self.transform(order) for order in node.order_by],
            limit=node.limit
        )
    
    def transform_SelectColumn(self, node: SelectColumn) -> SelectColumn:
        """Transform SELECT column."""
        return SelectColumn(
            expr=self.transform(node.expr),
            alias=node.alias
        )
    
    def transform_BinaryOp(self, node: BinaryOp) -> BinaryOp:
        """Transform binary operation."""
        return BinaryOp(
            operator=node.operator,
            left=self.transform(node.left),
            right=self.transform(node.right)
        )
    
    # Add more transform methods as needed...


# Convenience functions
def parse(sql: str) -> ASTNode:
    """Parse BigQuery SQL into AST."""
    parser = BQParser()
    return parser.parse(sql)


def transform(node: ASTNode, transformer: BQTransformer) -> ASTNode:
    """Transform an AST using the given transformer."""
    return transformer.transform(node)