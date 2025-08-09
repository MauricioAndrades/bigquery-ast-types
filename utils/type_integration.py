#!/usr/bin/env python3
"""
BigQuery Type Integration with AST

Integrates BigQuery data types with the AST transformation system,
providing type inference, validation, and transformation capabilities.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

from typing import Optional, List, Dict, Union, Any, Callable
from dataclasses import dataclass, field
from sqlglot import exp, parse_one
from sqlglot.expressions import DataType as SQLGlotDataType

from ..lib.types import (
    BQDataType, BigQueryType, TypeParser, TypeValidator,
    TypeCaster, TYPE_ALIASES
)


class TypeMapper:
    """Maps between sqlglot types and BigQuery types."""

    # Mapping from sqlglot type kinds to BigQuery types
    SQLGLOT_TO_BQ = {
        "BIGINT": BQDataType.INT64,
        "INT": BQDataType.INT64,
        "INT64": BQDataType.INT64,
        "SMALLINT": BQDataType.INT64,
        "TINYINT": BQDataType.INT64,
        "FLOAT": BQDataType.FLOAT64,
        "DOUBLE": BQDataType.FLOAT64,
        "FLOAT64": BQDataType.FLOAT64,
        "DECIMAL": BQDataType.NUMERIC,
        "NUMERIC": BQDataType.NUMERIC,
        "BIGNUMERIC": BQDataType.BIGNUMERIC,
        "BOOLEAN": BQDataType.BOOL,
        "BOOL": BQDataType.BOOL,
        "STRING": BQDataType.STRING,
        "TEXT": BQDataType.STRING,
        "VARCHAR": BQDataType.STRING,
        "CHAR": BQDataType.STRING,
        "BINARY": BQDataType.BYTES,
        "VARBINARY": BQDataType.BYTES,
        "BYTES": BQDataType.BYTES,
        "DATE": BQDataType.DATE,
        "TIME": BQDataType.TIME,
        "DATETIME": BQDataType.DATETIME,
        "TIMESTAMP": BQDataType.TIMESTAMP,
        "TIMESTAMPTZ": BQDataType.TIMESTAMP,
        "GEOGRAPHY": BQDataType.GEOGRAPHY,
        "JSON": BQDataType.JSON,
        "JSONB": BQDataType.JSON,
        "ARRAY": BQDataType.ARRAY,
        "STRUCT": BQDataType.STRUCT,
        "OBJECT": BQDataType.STRUCT,
    }

    @staticmethod
    def from_sqlglot(sqlglot_type: SQLGlotDataType) -> BigQueryType:
        """Convert sqlglot DataType to BigQueryType."""
        type_name = sqlglot_type.this.name if hasattr(sqlglot_type.this, 'name') else str(sqlglot_type.this)

        # Get base type
        base_type = TypeMapper.SQLGLOT_TO_BQ.get(type_name.upper())
        if not base_type:
            # Try to parse as BigQuery type directly
            try:
                return TypeParser.parse(type_name)
            except:
                raise ValueError(f"Unknown type: {type_name}")

        bq_type = BigQueryType(base_type=base_type)

        # Handle nested types
        if base_type == BQDataType.ARRAY and sqlglot_type.expressions:
            # Array element type
            element_sqlglot = sqlglot_type.expressions[0]
            bq_type.element_type = TypeMapper.from_sqlglot(element_sqlglot)

        elif base_type == BQDataType.STRUCT and sqlglot_type.expressions:
            # Struct fields
            for field_expr in sqlglot_type.expressions:
                if isinstance(field_expr, exp.ColumnDef):
                    field_name = field_expr.this.name if field_expr.this else None
                    field_type = TypeMapper.from_sqlglot(field_expr.args['kind'])
                    bq_type.fields.append((field_name, field_type))

        # Handle parameters (e.g., STRING(100), NUMERIC(10,2))
        if hasattr(sqlglot_type, 'expressions') and sqlglot_type.expressions:
            if base_type in {BQDataType.STRING, BQDataType.BYTES}:
                # Single length parameter
                if len(sqlglot_type.expressions) == 1:
                    length = sqlglot_type.expressions[0]
                    if isinstance(length, exp.Literal):
                        bq_type.parameters.append(
                            TypeParameter(name="length", value=int(length.this))
                        )

            elif base_type in {BQDataType.NUMERIC, BQDataType.BIGNUMERIC}:
                # Precision and optional scale
                if len(sqlglot_type.expressions) >= 1:
                    precision = sqlglot_type.expressions[0]
                    if isinstance(precision, exp.Literal):
                        bq_type.parameters.append(
                            TypeParameter(name="precision", value=int(precision.this))
                        )

                if len(sqlglot_type.expressions) >= 2:
                    scale = sqlglot_type.expressions[1]
                    if isinstance(scale, exp.Literal):
                        bq_type.parameters.append(
                            TypeParameter(name="scale", value=int(scale.this))
                        )

        return bq_type

    @staticmethod
    def to_sqlglot(bq_type: BigQueryType) -> exp.DataType:
        """Convert BigQueryType to sqlglot DataType expression."""
        # Base type
        type_name = bq_type.base_type.value

        if bq_type.base_type == BQDataType.ARRAY:
            # ARRAY<element_type>
            if bq_type.element_type:
                element_expr = TypeMapper.to_sqlglot(bq_type.element_type)
                return exp.DataType(
                    this=exp.DataType.Type.ARRAY,
                    expressions=[element_expr]
                )

        elif bq_type.base_type == BQDataType.STRUCT:
            # STRUCT<field1 type1, field2 type2>
            field_exprs = []
            for field_name, field_type in bq_type.fields:
                field_type_expr = TypeMapper.to_sqlglot(field_type)
                if field_name:
                    field_def = exp.ColumnDef(
                        this=exp.Identifier(this=field_name),
                        kind=field_type_expr
                    )
                    field_exprs.append(field_def)
                else:
                    field_exprs.append(field_type_expr)

            return exp.DataType(
                this=exp.DataType.Type.STRUCT,
                expressions=field_exprs
            )

        elif bq_type.parameters:
            # Parameterized type like STRING(100), NUMERIC(10,2)
            param_exprs = [
                exp.Literal(this=str(p.value), is_string=False)
                for p in bq_type.parameters
            ]
            return exp.DataType(
                this=exp.Identifier(this=type_name),
                expressions=param_exprs
            )

        else:
            # Simple type
            return exp.DataType(this=exp.Identifier(this=type_name))


@dataclass
class TypedColumn:
    """Column with type information."""
    name: str
    type: BigQueryType
    nullable: bool = True

    def to_column_def(self) -> exp.ColumnDef:
        """Convert to sqlglot ColumnDef."""
        return exp.ColumnDef(
            this=exp.Identifier(this=self.name),
            kind=TypeMapper.to_sqlglot(self.type),
            constraints=[
                exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())
            ] if not self.nullable else []
        )


@dataclass
class TableSchema:
    """Schema definition for a table."""
    name: str
    columns: List[TypedColumn]

    def get_column(self, name: str) -> Optional[TypedColumn]:
        """Get column by name."""
        for col in self.columns:
            if col.name.lower() == name.lower():
                return col
        return None

    def to_create_table(self) -> exp.Create:
        """Convert to CREATE TABLE statement."""
        return exp.Create(
            this=exp.Table(this=exp.Identifier(this=self.name)),
            kind="TABLE",
            expression=exp.Schema(
                expressions=[col.to_column_def() for col in self.columns]
            )
        )


class TypeInferrer:
    """Infers types in SQL expressions."""

    def __init__(self, schemas: Dict[str, TableSchema]):
        """Initialize with table schemas."""
        self.schemas = schemas

    def infer_expression_type(self, expr: exp.Expression) -> Optional[BigQueryType]:
        """Infer the type of an expression."""
        if isinstance(expr, exp.Literal):
            return self._infer_literal_type(expr)

        elif isinstance(expr, exp.Column):
            return self._infer_column_type(expr)

        elif isinstance(expr, exp.Cast):
            # Cast explicitly defines the type
            target_type = expr.args.get('to')
            if target_type:
                return TypeMapper.from_sqlglot(target_type)

        elif isinstance(expr, exp.Func):
            return self._infer_function_type(expr)

        elif isinstance(expr, (exp.Add, exp.Sub, exp.Mul, exp.Div)):
            # Arithmetic operations
            left_type = self.infer_expression_type(expr.left)
            right_type = self.infer_expression_type(expr.right)
            return self._infer_arithmetic_type(left_type, right_type)

        elif isinstance(expr, (exp.EQ, exp.NEQ, exp.LT, exp.GT, exp.LTE, exp.GTE)):
            # Comparison operations always return BOOL
            return BigQueryType(base_type=BQDataType.BOOL)

        elif isinstance(expr, (exp.And, exp.Or, exp.Not)):
            # Logical operations return BOOL
            return BigQueryType(base_type=BQDataType.BOOL)

        elif isinstance(expr, exp.Case):
            # CASE expression type is the common type of all results
            result_types = []
            for when_clause in expr.args.get('ifs', []):
                result_expr = when_clause.args.get('true')
                if result_expr:
                    result_type = self.infer_expression_type(result_expr)
                    if result_type:
                        result_types.append(result_type.base_type)

            else_expr = expr.args.get('default')
            if else_expr:
                else_type = self.infer_expression_type(else_expr)
                if else_type:
                    result_types.append(else_type.base_type)

            if result_types:
                common_type = TypeCaster.find_common_supertype(result_types)
                if common_type:
                    return BigQueryType(base_type=common_type)

        return None

    def _infer_literal_type(self, literal: exp.Literal) -> Optional[BigQueryType]:
        """Infer type of a literal."""
        value = literal.this

        if literal.is_string:
            return BigQueryType(base_type=BQDataType.STRING)
        elif value.upper() in ('TRUE', 'FALSE'):
            return BigQueryType(base_type=BQDataType.BOOL)
        elif value.upper() == 'NULL':
            return None  # NULL has no specific type
        elif '.' in value:
            # Floating point
            return BigQueryType(base_type=BQDataType.FLOAT64)
        else:
            # Integer
            return BigQueryType(base_type=BQDataType.INT64)

    def _infer_column_type(self, column: exp.Column) -> Optional[BigQueryType]:
        """Infer type of a column reference."""
        table_name = column.table
        column_name = column.name

        if table_name and table_name in self.schemas:
            schema = self.schemas[table_name]
            col = schema.get_column(column_name)
            if col:
                return col.type
        else:
            # Search all schemas
            for schema in self.schemas.values():
                col = schema.get_column(column_name)
                if col:
                    return col.type

        return None

    def _infer_function_type(self, func: exp.Func) -> Optional[BigQueryType]:
        """Infer type of a function call."""
        func_name = func.name.upper()

        # Aggregate functions
        if func_name in ('COUNT', 'COUNT_DISTINCT'):
            return BigQueryType(base_type=BQDataType.INT64)
        elif func_name in ('SUM', 'AVG', 'MIN', 'MAX'):
            # Type depends on input
            if func.args.get('this'):
                input_type = self.infer_expression_type(func.args['this'])
                if input_type and input_type.base_type in {BQDataType.INT64, BQDataType.FLOAT64,
                                                           BQDataType.NUMERIC, BQDataType.BIGNUMERIC}:
                    return input_type
            return BigQueryType(base_type=BQDataType.FLOAT64)

        # String functions
        elif func_name in ('CONCAT', 'SUBSTR', 'TRIM', 'UPPER', 'LOWER', 'REPLACE'):
            return BigQueryType(base_type=BQDataType.STRING)
        elif func_name in ('LENGTH', 'STRPOS'):
            return BigQueryType(base_type=BQDataType.INT64)

        # Date/time functions
        elif func_name == 'CURRENT_DATE':
            return BigQueryType(base_type=BQDataType.DATE)
        elif func_name == 'CURRENT_TIME':
            return BigQueryType(base_type=BQDataType.TIME)
        elif func_name == 'CURRENT_DATETIME':
            return BigQueryType(base_type=BQDataType.DATETIME)
        elif func_name == 'CURRENT_TIMESTAMP':
            return BigQueryType(base_type=BQDataType.TIMESTAMP)

        # Type conversion functions
        elif func_name in ('DATE', 'DATETIME', 'TIME', 'TIMESTAMP'):
            return BigQueryType(base_type=BQDataType[func_name])

        # Array functions
        elif func_name == 'ARRAY_LENGTH':
            return BigQueryType(base_type=BQDataType.INT64)

        # Geography functions
        elif func_name.startswith('ST_'):
            if func_name in ('ST_X', 'ST_Y', 'ST_LENGTH', 'ST_AREA'):
                return BigQueryType(base_type=BQDataType.FLOAT64)
            elif func_name in ('ST_NUMPOINTS', 'ST_DIMENSION'):
                return BigQueryType(base_type=BQDataType.INT64)
            elif func_name in ('ST_EQUALS', 'ST_CONTAINS', 'ST_WITHIN'):
                return BigQueryType(base_type=BQDataType.BOOL)
            else:
                return BigQueryType(base_type=BQDataType.GEOGRAPHY)

        return None

    def _infer_arithmetic_type(self, left: Optional[BigQueryType],
                              right: Optional[BigQueryType]) -> Optional[BigQueryType]:
        """Infer type of arithmetic operation."""
        if not left or not right:
            return None

        # Find common supertype
        common = TypeCaster.find_common_supertype([left.base_type, right.base_type])
        if common:
            return BigQueryType(base_type=common)

        return None


class TypeEnforcer:
    """Enforces type constraints in SQL transformations."""

    def __init__(self, strict: bool = False):
        """Initialize enforcer.

        Args:
            strict: If True, raise errors on type mismatches. If False, insert casts.
        """
        self.strict = strict

    def enforce_column_type(self, expr: exp.Expression,
                           expected_type: BigQueryType,
                           actual_type: Optional[BigQueryType]) -> exp.Expression:
        """Enforce that expression matches expected type."""
        if not actual_type:
            # Unknown type, can't enforce
            return expr

        if actual_type.base_type == expected_type.base_type:
            # Types match (ignoring parameters for now)
            return expr

        if TypeCaster.can_implicit_cast(actual_type.base_type, expected_type.base_type):
            # Implicit cast allowed, no change needed
            return expr

        if self.strict:
            raise TypeError(
                f"Type mismatch: expected {expected_type}, got {actual_type}"
            )
        else:
            # Insert explicit cast
            return exp.Cast(
                this=expr,
                to=TypeMapper.to_sqlglot(expected_type)
            )

    def validate_comparison(self, left_type: Optional[BigQueryType],
                           right_type: Optional[BigQueryType]) -> bool:
        """Validate that two types can be compared."""
        if not left_type or not right_type:
            # Unknown types, assume valid
            return True

        # Same type is always comparable
        if left_type.base_type == right_type.base_type:
            return True

        # Numeric types can be compared
        numeric_types = {BQDataType.INT64, BQDataType.FLOAT64,
                        BQDataType.NUMERIC, BQDataType.BIGNUMERIC}
        if (left_type.base_type in numeric_types and
            right_type.base_type in numeric_types):
            return True

        # Date/time types can be compared within their group
        date_types = {BQDataType.DATE, BQDataType.DATETIME, BQDataType.TIMESTAMP}
        if (left_type.base_type in date_types and
            right_type.base_type in date_types):
            return True

        return False


# Example schemas for testing
def create_example_schemas() -> Dict[str, TableSchema]:
    """Create example table schemas."""
    return {
        "orders": TableSchema(
            name="orders",
            columns=[
                TypedColumn("order_id", BigQueryType(base_type=BQDataType.INT64), nullable=False),
                TypedColumn("user_id", BigQueryType(base_type=BQDataType.INT64)),
                TypedColumn("order_date", BigQueryType(base_type=BQDataType.DATE)),
                TypedColumn("total_amount", BigQueryType(base_type=BQDataType.NUMERIC,
                                                       parameters=[TypeParameter("precision", 10),
                                                                  TypeParameter("scale", 2)])),
                TypedColumn("status", BigQueryType(base_type=BQDataType.STRING,
                                                 parameters=[TypeParameter("length", 50)])),
                TypedColumn("metadata", BigQueryType(base_type=BQDataType.JSON)),
            ]
        ),
        "users": TableSchema(
            name="users",
            columns=[
                TypedColumn("user_id", BigQueryType(base_type=BQDataType.INT64), nullable=False),
                TypedColumn("email", BigQueryType(base_type=BQDataType.STRING)),
                TypedColumn("created_at", BigQueryType(base_type=BQDataType.TIMESTAMP)),
                TypedColumn("is_active", BigQueryType(base_type=BQDataType.BOOL)),
            ]
        )
    }


if __name__ == "__main__":
    # Test type integration
    print("=== BigQuery Type Integration Test ===\n")

    # Create schemas
    schemas = create_example_schemas()
    inferrer = TypeInferrer(schemas)

    # Test SQL with type inference
    sql = """
    SELECT
        o.order_id,
        o.total_amount * 1.1 as total_with_tax,
        CASE
            WHEN o.total_amount > 100 THEN 'High'
            WHEN o.total_amount > 50 THEN 'Medium'
            ELSE 'Low'
        END as order_size,
        COUNT(*) as order_count
    FROM orders o
    WHERE o.order_date = CURRENT_DATE()
    """

    ast = parse_one(sql)

    print("Analyzing SQL:")
    print(sql)
    print("\nType Inference Results:")
    print("-" * 50)

    # Analyze SELECT expressions
    for select_expr in ast.find_all(exp.Alias):
        expr = select_expr.this
        alias = select_expr.alias

        inferred_type = inferrer.infer_expression_type(expr)
        print(f"{alias}: {inferred_type if inferred_type else 'Unknown'}")

    print("\n✅ Type integration complete! 🐕")