#!/usr/bin/env python3
"""
Test BigQuery AST Builders

Comprehensive tests for builder validation and functionality.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pytest
from lib import b, ValidationError, Identifier, Literal, BinaryOp
from lib.types import OrderByClause


class TestIdentifierBuilder:
    """Test identifier creation and validation."""

    def test_simple_identifier(self):
        col = b.col("order_id")
        assert isinstance(col, Identifier)
        assert col.name == "order_id"
        assert col.table is None
        assert str(col) == "order_id"

    def test_qualified_identifier(self):
        col = b.col("order_id", "orders")
        assert col.name == "order_id"
        assert col.table == "orders"
        assert str(col) == "orders.order_id"

    def test_empty_identifier_fails(self):
        with pytest.raises(ValidationError, match="Column name cannot be empty"):
            b.col("")

    def test_none_identifier_fails(self):
        with pytest.raises(ValidationError):
            b.col(None) # pyright: ignore[reportArgumentType]


class TestLiteralBuilder:
    """Test literal creation and validation."""

    def test_null_literal(self):
        null = b.null()
        assert isinstance(null, Literal)
        assert null.value is None
        assert null.type == "null"
        assert str(null) == "NULL"

    def test_boolean_literals(self):
        true = b.true()
        false = b.false()

        assert str(true) == "TRUE"
        assert str(false) == "FALSE"

        # Via lit()
        assert str(b.lit(True)) == "TRUE"
        assert str(b.lit(False)) == "FALSE"

    def test_string_literal(self):
        s = b.lit("hello world")
        assert s.type == "string"
        assert str(s) == "'hello world'"

    def test_string_literal_max_size(self):
        # Test 1MB limit
        large_string = "x" * 1048577  # Just over 1MB
        with pytest.raises(ValidationError, match="String literal exceeds 1MB limit"):
            b.lit(large_string)

    def test_integer_literal(self):
        i = b.lit(42)
        assert i.type == "number"
        assert str(i) == "42"

    def test_integer_range_validation(self):
        # Valid INT64
        b.lit(9223372036854775807)  # Max INT64
        b.lit(-9223372036854775808)  # Min INT64

        # Out of range
        with pytest.raises(ValidationError, match="out of INT64 range"):
            b.lit(9223372036854775808)

    def test_float_literal(self):
        f = b.lit(3.14)
        assert f.type == "number"
        assert str(f) == "3.14"

    def test_date_literal(self):
        d = b.date("2024-01-15")
        assert d.type == "date"
        assert str(d) == "DATE '2024-01-15'"

    def test_invalid_date_format(self):
        with pytest.raises(ValidationError, match="Invalid date format"):
            b.date("2024/01/15")  # Wrong separator

        with pytest.raises(ValidationError, match="Invalid date format"):
            b.date("01-15-2024")  # Wrong order

    def test_timestamp_literal(self):
        ts = b.timestamp("2024-01-15 10:30:45")
        assert ts.type == "timestamp"
        assert str(ts) == "TIMESTAMP '2024-01-15 10:30:45'"

        # Also accept ISO format
        ts2 = b.timestamp("2024-01-15T10:30:45")
        assert str(ts2) == "TIMESTAMP '2024-01-15T10:30:45'"

    def test_invalid_timestamp_format(self):
        with pytest.raises(ValidationError, match="Invalid timestamp format"):
            b.timestamp("2024-01-15")  # Missing time

    def test_unsupported_literal_type(self):
        with pytest.raises(TypeError, match="Unsupported literal type"):
            b.lit([1, 2, 3])  # Lists not supported


class TestBinaryOperations:
    """Test binary operation builders."""

    def test_equality_operations(self):
        left = b.col("a")
        right = b.lit(1)

        eq = b.eq(left, right)
        assert isinstance(eq, BinaryOp)
        assert eq.operator == "="
        assert str(eq) == "(a = 1)"

        neq = b.neq(left, right)
        assert neq.operator == "!="
        assert str(neq) == "(a != 1)"

    def test_comparison_operations(self):
        a = b.col("a")
        ten = b.lit(10)

        assert str(b.lt(a, ten)) == "(a < 10)"
        assert str(b.lte(a, ten)) == "(a <= 10)"
        assert str(b.gt(a, ten)) == "(a > 10)"
        assert str(b.gte(a, ten)) == "(a >= 10)"

    def test_invalid_operands(self):
        with pytest.raises(
            TypeError, match="Both operands must be Expression instances"
        ):
            b.eq("not_an_expression", b.lit(1)) # pyright: ignore[reportArgumentType]

    def test_logical_operations(self):
        a = b.eq(b.col("a"), b.lit(1))
        b_cond = b.eq(b.col("b"), b.lit(2))
        c = b.eq(b.col("c"), b.lit(3))

        # AND
        and_expr = b.and_(a, b_cond)
        assert "(a = 1) AND (b = 2)" in str(and_expr)

        # Multiple AND
        multi_and = b.and_(a, b_cond, c)
        assert "AND" in str(multi_and)

        # OR
        or_expr = b.or_(a, b_cond)
        assert "(a = 1) OR (b = 2)" in str(or_expr)

    def test_empty_logical_operations(self):
        # Empty AND returns TRUE
        assert str(b.and_()) == "TRUE"

        # Empty OR returns FALSE
        assert str(b.or_()) == "FALSE"

    def test_is_operations(self):
        col = b.col("email")

        # IS NULL / IS NOT NULL
        assert "IS NULL" in str(b.is_null(col))
        assert "IS NOT NULL" in str(b.is_not_null(col))

        # IS / IS NOT
        assert str(b.is_(col, b.null())) == "(email IS NULL)"
        assert str(b.is_not(col, b.true())) == "(email IS NOT TRUE)"


class TestFunctions:
    """Test function builders."""

    def test_simple_function(self):
        fn = b.func("UPPER", b.col("name"))
        assert fn.name == "UPPER"
        assert len(fn.args) == 1
        assert str(fn) == "UPPER(name)"

    def test_empty_function_name_fails(self):
        with pytest.raises(ValidationError, match="Function name cannot be empty"):
            b.func("", b.col("a"))

    def test_invalid_function_args(self):
        with pytest.raises(
            TypeError, match="All function arguments must be Expression instances"
        ):
            b.func("UPPER", "not_an_expression") # pyright: ignore[reportArgumentType]

    def test_builtin_functions(self):
        # COALESCE
        coal = b.coalesce(b.col("a"), b.col("b"), b.lit(0))
        assert coal.name == "COALESCE"
        assert len(coal.args) == 3

        # NULLIF
        nullif = b.nullif(b.col("name"), b.lit(""))
        assert nullif.name == "NULLIF"
        assert len(nullif.args) == 2

        # TRIM
        trim = b.trim(b.col("name"))
        assert trim.name == "TRIM"

        # CONCAT
        concat = b.concat(b.lit("Hello "), b.col("name"))
        assert concat.name == "CONCAT"

        # CURRENT_TIMESTAMP
        ts = b.current_timestamp()
        assert ts.name == "CURRENT_TIMESTAMP"
        assert len(ts.args) == 0


class TestCasting:
    """Test CAST operations."""

    def test_cast(self):
        cast = b.cast(b.col("quantity"), "INT64")
        assert not cast.safe
        assert cast.target_type == "INT64"
        assert str(cast) == "CAST(quantity AS INT64)"

    def test_safe_cast(self):
        safe = b.safe_cast(b.col("price"), "NUMERIC")
        assert safe.safe
        assert str(safe) == "SAFE_CAST(price AS NUMERIC)"

    def test_invalid_cast_type(self):
        with pytest.raises(ValidationError, match="Invalid data type"):
            b.cast(b.col("a"), "INVALID_TYPE")


class TestCaseExpressions:
    """Test CASE expression builders."""

    def test_simple_case(self):
        case = b.case(
            b.when(b.gt(b.col("total"), b.lit(100)), b.lit("HIGH")),
            b.when(b.gt(b.col("total"), b.lit(50)), b.lit("MEDIUM")),
            else_=b.lit("LOW"),
        )

        assert len(case.when_clauses) == 2
        assert case.else_clause is not None

        case_str = str(case)
        assert "CASE" in case_str
        assert "WHEN" in case_str
        assert "THEN" in case_str
        assert "ELSE" in case_str
        assert "END" in case_str


class TestComplexBuilders:
    """Test complex builder patterns."""

    def test_null_safe_equality(self):
        nse = b.null_safe_eq(b.col("a"), b.col("b"))
        nse_str = str(nse)

        # Should contain both regular equality and NULL checks
        assert "(a = b)" in nse_str
        assert "IS NULL" in nse_str
        assert "AND" in nse_str
        assert "OR" in nse_str

    def test_array_builder(self):
        arr = b.array(b.lit(1), b.lit(2), b.lit(3))
        assert len(arr.elements) == 3
        assert str(arr) == "[1, 2, 3]"

    def test_struct_builder(self):
        struct = b.struct(name=b.lit("John"), age=b.lit(30))
        assert len(struct.fields) == 2
        assert "STRUCT" in str(struct)
        assert "AS name" in str(struct)
        assert "AS age" in str(struct)

    def test_window_function(self):
        row_num = b.row_number()
        assert row_num.name == "ROW_NUMBER"
        assert len(row_num.args) == 0

        row_num.partition_by = [b.col("customer_id")]
        row_num.order_by = [OrderByClause(expr=b.col("order_date"), direction="DESC")]

        win_str = str(row_num)
        assert "ROW_NUMBER()" in win_str
        assert "ROW_NUMBER()" in win_str
        assert "OVER" in win_str
        assert "PARTITION BY" in win_str
        assert "ORDER BY" in win_str


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
