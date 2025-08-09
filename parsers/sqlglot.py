"""
SQLGlot Parser for BigQuery AST Types

Wraps SQLGlot to parse BigQuery SQL into our AST representation.
"""

from typing import Any, Optional, Dict, List

import sqlglot
try:
    from sqlglot import exp
except ImportError:
    from sqlglot import expressions as exp
from sqlglot.dialects import BigQuery

from dataclasses import dataclass

# Import AST node types and builders
from lib.builders import SelectColumn
from lib.types import (
    Select, WhereClause, Identifier, TableName, TableRef, EnhancedGeneralIdentifier,
    Join, JoinType, BinaryOp, Expression, FunctionCall, PathExpression, PathPart,
    Subquery, Case, WhenClause, Insert, Update, CreateTable,
    QuotedIdentifier, UnquotedIdentifier, ColumnName,
    StringLiteral, BytesLiteral, IntegerLiteral, NumericLiteral, BigNumericLiteral,
    FloatLiteral, BooleanLiteral, NullLiteral, DateLiteral, TimeLiteral, DatetimeLiteral,
    TimestampLiteral, IntervalLiteral, JSONLiteral, ArrayLiteral,
    NamedParameter, PositionalParameter
)

# Handle imports for both package and direct usage
try:
    from lib.types import *
    from lib.builders import b
except ImportError:
    # Direct import when running as script
    import sys
    import os
    import importlib.util

    # Import types module
    types_path = os.path.join(os.path.dirname(__file__), '../lib/types.py')
    spec = importlib.util.spec_from_file_location('types', types_path)
    if spec:
        types_module = importlib.util.module_from_spec(spec)
        if spec is not None and spec.loader is not None:
            spec.loader.exec_module(types_module)

    # Import all types
    globals().update({name: getattr(types_module, name) for name in dir(types_module)
                     if not name.startswith('_')})

    # Remove local WhereClause definition to avoid shadowing
    # Mock builders for now
    class MockBuilders:
        def col(self, name): return UnquotedIdentifier(name)
        def lit(self, value): return StringLiteral(str(value))
        def select(self, *args): return Select(select_list=list(args))
        def eq(self, left, right): return BinaryOp(left=left, operator="=", right=right)

    b = MockBuilders()


class SQLGlotParser:
    """Parse BigQuery SQL using SQLGlot and transform to our AST."""

    def __init__(self, dialect: str = "bigquery"):
        self.dialect = dialect

    def parse(self, sql: str) -> "Statement":
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
        elif isinstance(node, exp.Insert):
            return self._transform_insert(node)
        elif isinstance(node, exp.Update):
            return self._transform_update(node)
        elif isinstance(node, exp.Create):
            return self._transform_create(node)
        elif isinstance(node, exp.Merge):
            return self._transform_merge(node)
        else:
            raise NotImplementedError(f"Unsupported expression type: {type(node).__name__}")

    def _transform_select(self, select: exp.Select) -> Select:
        """Transform SQLGlot Select to our Select."""
        # Extract select columns
        select_list = []
        for projection in select.expressions:
            if isinstance(projection, exp.Star):
                select_list.append(SelectColumn(expr=b.col("*")))
            elif isinstance(projection, exp.Alias):
                expr = self._transform_expression(projection.this)
                select_list.append(SelectColumn(expr=expr, alias=projection.alias))
            else:
                expr = self._transform_expression(projection)
                select_list.append(SelectColumn(expr=expr))

        # Create Select node
        result = Select(select_list=select_list)

        # Add FROM clause if present
        if select.args.get("from"):
            from_expr = select.args["from"].this
            if isinstance(from_expr, exp.Table):
                table_ref = self._transform_table_reference(from_expr)
                result.from_clause = table_ref

        # Handle JOINs
        for join_expr in select.args.get("joins") or []:
            result.joins.append(self._transform_join(join_expr))

        # Add WHERE clause if present
        if select.args.get("where"):
            condition = self._transform_expression(select.args["where"].this)
            result.where_clause = WhereClause(condition=condition)

        return result

    def _transform_join(self, join: exp.Join) -> Join:
        """Transform SQLGlot Join to our Join node."""
        table = self._transform_table_reference(join.this)
        condition = None
        if join.args.get("on"):
            condition = self._transform_expression(join.args["on"])
        kind = join.args.get("kind") or "INNER"
        return Join(join_type=JoinType[kind.upper()], table=table, condition=condition)

    def _transform_merge(self, merge: exp.Merge) -> "Merge":
        """Transform SQLGlot Merge to our Merge."""
        # Placeholder implementation
        target = TableRef(table=TableName(table="target"))
        source = TableRef(table=TableName(table="source"))
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
            # Handle identifier types
            return self._transform_identifier(expr)
        elif isinstance(expr, exp.Literal):
            return self._transform_literal(expr)
        elif isinstance(expr, exp.Binary):
            return BinaryOp(
                left=self._transform_expression(expr.left),
                operator=expr.key.upper(),
                right=self._transform_expression(expr.right)
            )
        elif isinstance(expr, exp.Case):
            return self._transform_case(expr)
        elif isinstance(expr, exp.Subquery):
            return self._transform_subquery(expr)
        elif isinstance(expr, exp.Func):
            args = [self._transform_expression(arg) for arg in expr.args.values() if arg]
            return FunctionCall(function_name=expr.name, arguments=args)
        elif isinstance(expr, exp.Dot):
            # Handle path expressions like table.column or field access
            return self._transform_path_expression(expr)
        elif isinstance(expr, exp.Placeholder):
            # Handle query parameters
            return self._transform_parameter(expr)
        else:
            # Default: return column placeholder
            return Identifier(name=str(expr))

    def _transform_subquery(self, sub: exp.Subquery) -> Subquery:
        """Transform SQLGlot subquery to our Subquery."""
        select = self._transform_select(sub.this)
        return Subquery(select=select, alias=sub.alias)

    def _transform_case(self, case: exp.Case) -> Case:
        """Transform SQLGlot CASE expression."""
        whens = []
        for w in case.args.get("ifs") or []:
            condition = self._transform_expression(w.this)
            result = self._transform_expression(w.args.get("true"))
            whens.append(WhenClause(condition=condition, result=result))
        default = None
        if case.args.get("default"):
            default = self._transform_expression(case.args["default"])
        return Case(whens=whens, else_result=default)

    def _transform_insert(self, insert: exp.Insert) -> Insert:
        """Transform SQLGlot Insert to our Insert statement."""
        schema = insert.this  # exp.Schema
        table = self._transform_table_reference(schema.this)
        columns = []
        for col in schema.expressions or []:
            columns.append(Identifier(name=col.name))
        expr = insert.args.get("expression")
        values = []
        query = None
        if isinstance(expr, exp.Values):
            for row in expr.expressions:
                values.append([self._transform_expression(e) for e in row.expressions])
        elif isinstance(expr, exp.Select):
            query = self._transform_select(expr)
        return Insert(table=table, columns=columns, values=values, query=query)

    def _transform_update(self, update: exp.Update) -> Update:
        """Transform SQLGlot Update to our Update statement."""
        table = self._transform_table_reference(update.this)
        assignments: Dict[str, Expression] = {}
        for assignment in update.args.get("expressions") or []:
            col = assignment.this.name
            assignments[col] = self._transform_expression(assignment.expression)
        where = None
        if update.args.get("where"):
            where = WhereClause(condition=self._transform_expression(update.args["where"].this))
        return Update(table=table, assignments=assignments, where=where)

    def _transform_create(self, create: exp.Create) -> CreateTable:
        """Transform SQLGlot Create to our CreateTable statement."""
        schema = create.this  # exp.Schema
        table_ref = self._transform_table_reference(schema.this)
        return CreateTable(table=table_ref.table)

    def _transform_identifier(self, expr: exp.Column) -> Expression:
        """Transform column/identifier with proper BigQuery identifier type detection."""
        name = expr.name
        table = expr.table if hasattr(expr, 'table') else None

        # Detect identifier type based on format
        if name.startswith('`') and name.endswith('`'):
            # Quoted identifier
            return QuotedIdentifier(name=name[1:-1])  # Remove backticks
        elif name.replace('_', '').replace('-', '').isalnum() and (name[0].isalpha() or name[0] == '_'):
            # Unquoted identifier (starts with letter or underscore)
            if '-' in name:
                # Supports dashes (BigQuery table names)
                return ColumnName(name=name, supports_dashes=True)
            else:
                return UnquotedIdentifier(name=name)
        else:
            # Enhanced general identifier for complex cases
            parts = name.split('.')
            separators = ['.'] * (len(parts) - 1)
            return EnhancedGeneralIdentifier(name=name, parts=parts, separators=separators)

    def _transform_path_expression(self, expr: exp.Dot) -> PathExpression:
        """Transform dot notation into path expressions."""
        parts = []
        current = expr

        # Traverse the dot chain to build path parts
        while hasattr(current, 'this') and hasattr(current, 'expression'):
            # Add the right-most part
            right_name = current.expression.name if hasattr(current.expression, 'name') else str(current.expression)
            parts.insert(0, PathPart(value=right_name, separator='.'))

            # Move to the left part
            current = current.this
            if not isinstance(current, exp.Dot):
                # Add the leftmost part
                left_name = current.name if hasattr(current, 'name') else str(current)
                parts.insert(0, PathPart(value=left_name))
                break

        return PathExpression(parts=parts)

    def _transform_parameter(self, expr: exp.Placeholder) -> Expression:
        """Transform query parameters."""
        if hasattr(expr, 'name') and expr.name:
            # Named parameter (@param)
            return NamedParameter(name=expr.name)
        else:
            # Positional parameter (?)
            return PositionalParameter(position=0)  # Position would need to be tracked

    def _transform_literal(self, lit: exp.Literal) -> Literal:
        """Transform SQLGlot literal to our Literal with BigQuery-specific handling."""
        literal_str = str(lit)

        # Handle string and bytes literals with prefixes and quotes
        if lit.is_string:
            value = lit.this
            quote_style = '"'  # Default
            is_raw = False
            is_bytes = False

            # Detect quote style and prefixes from original literal
            if literal_str.startswith(('r"', 'R"', "r'", "R'")):
                is_raw = True
                quote_style = literal_str[2]
            elif literal_str.startswith(('b"', 'B"', "b'", "B'")):
                is_bytes = True
                quote_style = literal_str[2]
            elif literal_str.startswith(('br"', 'BR"', 'rb"', 'RB"', "br'", "BR'", "rb'", "RB'")):
                is_raw = True
                is_bytes = True
                quote_style = literal_str[-1]
            elif literal_str.startswith(('"""', "'''")):
                quote_style = literal_str[:3]
            elif literal_str.startswith(('"', "'")):
                quote_style = literal_str[0]

            if is_bytes:
                return BytesLiteral(
                    value=value.encode() if isinstance(value, str) else value,
                    quote_style=quote_style,
                    is_raw=is_raw
                )
            else:
                return StringLiteral(
                    value=value,
                    quote_style=quote_style,
                    is_raw=is_raw,
                    is_bytes=False
                )

        # Handle numeric literals
        elif lit.is_int:
            value = int(lit.this)
            is_hex = literal_str.lower().startswith('0x')
            return IntegerLiteral(value=value, is_hexadecimal=is_hex)

        elif lit.is_number:
            # Check for NUMERIC/BIGNUMERIC literals
            if literal_str.upper().startswith('NUMERIC'):
                return NumericLiteral(value=lit.this)
            elif literal_str.upper().startswith('BIGNUMERIC'):
                return BigNumericLiteral(value=lit.this)
            else:
                return FloatLiteral(value=float(lit.this))

        # Handle special literals
        elif literal_str.upper() in ('TRUE', 'FALSE'):
            return BooleanLiteral(value=literal_str.upper() == 'TRUE')

        elif literal_str.upper() == 'NULL':
            return NullLiteral()

        # Handle date/time literals
        elif literal_str.upper().startswith('DATE'):
            # Extract the date string from DATE 'YYYY-MM-DD'
            date_value = lit.this if hasattr(lit, 'this') else literal_str
            return DateLiteral(value=date_value)

        elif literal_str.upper().startswith('TIME'):
            time_value = lit.this if hasattr(lit, 'this') else literal_str
            return TimeLiteral(value=time_value)

        elif literal_str.upper().startswith('DATETIME'):
            datetime_value = lit.this if hasattr(lit, 'this') else literal_str
            return DatetimeLiteral(value=datetime_value)

        elif literal_str.upper().startswith('TIMESTAMP'):
            timestamp_value = lit.this if hasattr(lit, 'this') else literal_str
            return TimestampLiteral(value=timestamp_value)

        elif literal_str.upper().startswith('INTERVAL'):
            interval_value = lit.this if hasattr(lit, 'this') else literal_str
            return IntervalLiteral(value=interval_value)

        elif literal_str.upper().startswith('JSON'):
            json_value = lit.this if hasattr(lit, 'this') else literal_str
            return JSONLiteral(value=json_value)

        # Handle array literals
        elif isinstance(lit, exp.Array):
            elements = [self._transform_expression(elem) for elem in lit.expressions]
            return ArrayLiteral(elements=elements)

        else:
            # Default fallback
            return StringLiteral(value=str(lit.this) if hasattr(lit, 'this') else str(lit))

    def _transform_table_reference(self, table_expr: exp.Table) -> TableRef:
        """Transform table reference with BigQuery project.dataset.table support."""
        table_name_str = table_expr.name
        alias = table_expr.alias if hasattr(table_expr, 'alias') else None

        # Parse BigQuery table name format: [project].[dataset].table
        parts = table_name_str.split('.')
        supports_dashes = '-' in table_name_str

        if len(parts) == 3:
            # project.dataset.table
            project, dataset, table = parts
            table_name = TableName(
                project=project,
                dataset=dataset,
                table=table,
                supports_dashes=supports_dashes
            )
        elif len(parts) == 2:
            # dataset.table
            dataset, table = parts
            table_name = TableName(
                dataset=dataset,
                table=table,
                supports_dashes=supports_dashes
            )
        else:
            # Just table name
            table_name = TableName(
                table=table_name_str,
                supports_dashes=supports_dashes
            )

        return TableRef(table=table_name, alias=alias)


# Convenience function
def parse(sql: str, dialect: str = "bigquery") -> "Statement":
    """Parse SQL string into AST using SQLGlot."""
    parser = SQLGlotParser(dialect)
    return parser.parse(sql)