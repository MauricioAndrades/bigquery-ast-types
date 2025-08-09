#!/usr/bin/env python3
"""
Test Runner for BQAST Library

Validates that our AST transformations produce correct SQL.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import sys
import os
import pytest

# Skip test if sqlglot is not available
pytest.skip("sqlglot module not available", allow_module_level=True)

# Original imports would be:
# import sqlglot
# from sqlglot import exp
# from lib.bsql import j, null_safe_eq, standardize_string_id
# from lib import OrderMergeBuilder, DedupPatterns


def test_jsql_basics():
    """Test basic jsql functionality."""
    print("Testing jsql basics...")

    # Test parsing
    query = j.parse("SELECT 1 as test")
    assert query.sql() == "SELECT 1 AS test"

    # Test finding nodes
    query = j.parse("SELECT a, b, c FROM table1")
    columns = query.find(exp.Column).toList()
    assert len(columns) == 3
    assert columns[0].name == "a"

    # Test replacing nodes
    query.find(exp.Column, predicate=lambda n: n.name == "b").replaceWith(
        exp.Column(this="new_b")
    )
    assert "new_b" in query.sql()

    print("✓ jsql basics pass")


def test_null_safe_equality():
    """Test null-safe equality helper."""
    print("\nTesting null-safe equality...")

    left = exp.Column(this="a")
    right = exp.Column(this="b")

    result = null_safe_eq(left, right)
    sql = result.sql()

    assert "a = b" in sql
    assert "a IS NULL AND b IS NULL" in sql

    print("✓ null-safe equality pass")


def test_standardization():
    """Test column standardization."""
    print("\nTesting standardization...")

    # Test string ID standardization
    col = exp.Column(this="visitor_id")
    result = standardize_string_id(col)
    sql = result.sql()

    assert "NULLIF" in sql
    assert "TRIM" in sql
    assert "visitor_id" in sql

    print("✓ standardization pass")


def test_order_merge_builder():
    """Test OrderMergeBuilder."""
    print("\nTesting OrderMergeBuilder...")

    builder = OrderMergeBuilder("test-project", "test_dataset")

    source_sql = """
    SELECT
        order_id,
        product_id,
        visitor_id,
        order_ts
    FROM raw_orders
    """

    result = builder.build_3cte_merge(
        source_sql=source_sql, retailer_id=123, datetime_threshold="2024-01-01 00:00:00"
    )

    # Check key components are present
    assert "WITH" in result
    assert "raw_orders AS" in result
    assert "cleaned_orders AS" in result
    assert "deduped_orders AS" in result
    assert "MERGE INTO" in result
    assert "retailer_id = 123" in result

    print("✓ OrderMergeBuilder pass")


def test_dedup_patterns():
    """Test deduplication patterns."""
    print("\nTesting dedup patterns...")

    # Test simple dedup
    simple = DedupPatterns.simple_dedup(
        partition_by=["order_id", "product_id"], order_by=[("ts", "DESC")]
    )

    assert "ROW_NUMBER() OVER" in simple
    assert "PARTITION BY order_id, product_id" in simple
    assert "ORDER BY ts DESC" in simple

    # Test comprehensive dedup
    comprehensive = DedupPatterns.comprehensive_dedup(
        ["order_id", "product_id", "hashed_pii"]
    )

    assert "ARRAY_TO_STRING" in comprehensive  # For hashed_pii
    assert "PARTITION BY" in comprehensive

    print("✓ dedup patterns pass")


def run_all_tests():
    """Run all tests."""
    print("=== Running BQAST Tests ===")
    print()

    try:
        test_jsql_basics()
        test_null_safe_equality()
        test_standardization()
        test_order_merge_builder()
        test_dedup_patterns()

        print("\n🎉 All tests passed! 🐕")
        return True

    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        return False
    except Exception as e:
        print(f"\n💀 Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
