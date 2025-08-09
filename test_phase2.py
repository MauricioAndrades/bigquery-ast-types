#!/usr/bin/env python3
"""
Test suite for Phase 2 BigQuery AST builders.

Tests WITH/CTEs, QUALIFY, UNNEST, TABLESAMPLE, MERGE, 
Window frames, PIVOT/UNPIVOT operations.

Author: Implementation Test Suite
Date: 2025-08-09
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib'))

# Import the modules directly
import importlib.util

# Load types module with a different name to avoid conflict
spec = importlib.util.spec_from_file_location("ast_types", "./lib/types.py")
ast_types = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ast_types)

# Load builders module - need to inject types module first
import sys
sys.modules['lib.types'] = ast_types  # Make types available for builders import

spec = importlib.util.spec_from_file_location("builders", "./lib/builders.py")
builders = importlib.util.module_from_spec(spec)
# Patch the module to use our ast_types
builders.types = ast_types
spec.loader.exec_module(builders)

# Load serializer module
spec = importlib.util.spec_from_file_location("serializer", "./lib/serializer.py")
serializer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(serializer)

# Import what we need
from ast_types import *
b = builders.b
ValidationError = builders.ValidationError
MergeBuilder = builders.MergeBuilder
to_sql = serializer.to_sql

def test_with_clause():
    """Test WITH clause and CTEs."""
    print("Testing WITH clause and CTEs...")
    
    # Simple CTE
    select1 = Select(select_list=[SelectColumn(expression=b.col("id"))])
    select2 = Select(select_list=[SelectColumn(expression=b.col("name"))])
    
    with_clause = b.with_(
        ("recent_orders", select1),
        ("top_customers", select2)
    )
    assert len(with_clause.ctes) == 2
    assert with_clause.ctes[0].name == "recent_orders"
    assert not with_clause.recursive
    print(f"  Simple WITH: Created {len(with_clause.ctes)} CTEs")
    
    # Recursive CTE
    recursive_with = b.with_recursive(
        ("ancestors", select1)
    )
    assert recursive_with.recursive
    print(f"  Recursive WITH: recursive={recursive_with.recursive}")
    
    # Test validation
    try:
        b.with_()  # No CTEs
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works: {e}")
    
    print("  ✓ All WITH clause tests passed!")

def test_qualify():
    """Test QUALIFY clause."""
    print("\nTesting QUALIFY clause...")
    
    # Simple QUALIFY
    qualify = b.qualify(b.eq(b.func("ROW_NUMBER"), b.lit(1)))
    assert isinstance(qualify.condition, BinaryOp)
    print(f"  Simple QUALIFY: condition type={type(qualify.condition).__name__}")
    
    # Complex QUALIFY with window function
    qualify = b.qualify(
        b.lte(
            b.func("RANK"),
            b.lit(10)
        )
    )
    assert isinstance(qualify.condition, BinaryOp)
    print(f"  Complex QUALIFY: Created with window function condition")
    
    # Test validation
    try:
        b.qualify("not an expression")
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works: {e}")
    
    print("  ✓ All QUALIFY tests passed!")

def test_unnest():
    """Test UNNEST table function."""
    print("\nTesting UNNEST table function...")
    
    # Simple UNNEST
    unnest = b.unnest(b.col("array_column"))
    assert isinstance(unnest.array_expression, Identifier)
    assert not unnest.ordinality
    print(f"  Simple UNNEST: array_expression type={type(unnest.array_expression).__name__}")
    
    # UNNEST with ordinality
    unnest = b.unnest(b.col("array_column"), ordinality=True, alias="item")
    assert unnest.ordinality
    assert unnest.alias == "item"
    print(f"  UNNEST with ordinality: alias={unnest.alias}")
    
    # UNNEST with array literal
    unnest = b.unnest(b.array(b.lit(1), b.lit(2), b.lit(3)))
    assert isinstance(unnest.array_expression, Array)
    print(f"  UNNEST with array literal: Created successfully")
    
    # Test validation
    try:
        b.unnest("not an expression")
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works: {e}")
    
    print("  ✓ All UNNEST tests passed!")

def test_tablesample():
    """Test TABLESAMPLE clause."""
    print("\nTesting TABLESAMPLE clause...")
    
    # TABLESAMPLE with percent
    sample = b.tablesample("orders", percent=10.0)
    assert sample.percent == 10.0
    assert sample.rows is None
    assert sample.method == "BERNOULLI"
    print(f"  TABLESAMPLE percent: {sample.percent}% using {sample.method}")
    
    # TABLESAMPLE with rows
    sample = b.tablesample("orders", rows=1000)
    assert sample.rows == 1000
    assert sample.percent is None
    print(f"  TABLESAMPLE rows: {sample.rows} rows")
    
    # TABLESAMPLE with SYSTEM method
    sample = b.tablesample("orders", method="SYSTEM", percent=5.0)
    assert sample.method == "SYSTEM"
    print(f"  TABLESAMPLE method: {sample.method}")
    
    # Test validation - no percent or rows
    try:
        b.tablesample("orders")
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works (no args): {e}")
    
    # Test validation - both percent and rows
    try:
        b.tablesample("orders", percent=10.0, rows=100)
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works (both args): {e}")
    
    # Test validation - invalid percent
    try:
        b.tablesample("orders", percent=150.0)
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works (invalid percent): {e}")
    
    print("  ✓ All TABLESAMPLE tests passed!")

def test_window_frame():
    """Test window frame specifications."""
    print("\nTesting window frame specifications...")
    
    # Default frame (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    frame = b.window_frame()
    assert frame.frame_type == FrameType.ROWS
    assert frame.start_bound == FrameBound.UNBOUNDED_PRECEDING
    assert frame.end_bound == FrameBound.CURRENT_ROW
    print(f"  Default frame: {frame.frame_type.value} frame")
    
    # Numeric bounds
    frame = b.window_frame("ROWS", 2, 0)
    assert frame.frame_type == FrameType.ROWS
    assert frame.start_bound == (2, "PRECEDING")
    assert frame.end_bound == (0, "PRECEDING")
    print(f"  Numeric bounds: 2 PRECEDING to CURRENT ROW")
    
    # RANGE frame
    frame = b.window_frame("RANGE", "CURRENT ROW", "UNBOUNDED FOLLOWING")
    assert frame.frame_type == FrameType.RANGE
    assert frame.start_bound == FrameBound.CURRENT_ROW
    assert frame.end_bound == FrameBound.UNBOUNDED_FOLLOWING
    print(f"  RANGE frame: CURRENT ROW to UNBOUNDED FOLLOWING")
    
    # Test validation - invalid frame type
    try:
        b.window_frame("INVALID")
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works: {e}")
    
    print("  ✓ All window frame tests passed!")

def test_pivot():
    """Test PIVOT operation."""
    print("\nTesting PIVOT operation...")
    
    # Simple PIVOT
    pivot = b.pivot(
        "sales",
        "SUM", "amount", "quarter", ["Q1", "Q2", "Q3", "Q4"]
    )
    assert isinstance(pivot.source, TableRef)
    assert pivot.aggregate_function == "SUM"
    assert pivot.value_column == "amount"
    assert pivot.pivot_column == "quarter"
    assert len(pivot.pivot_values) == 4
    print(f"  Simple PIVOT: {pivot.aggregate_function}({pivot.value_column}) BY {pivot.pivot_column}")
    
    # PIVOT with SELECT source
    select_source = Select(select_list=[SelectColumn(expression=b.col("*"))])
    pivot = b.pivot(
        select_source,
        "AVG", "score", "subject", ["Math", "Science", "English"],
        alias="pivoted"
    )
    assert isinstance(pivot.source, Select)
    assert pivot.alias == "pivoted"
    print(f"  PIVOT with SELECT: alias={pivot.alias}")
    
    # Test validation
    try:
        b.pivot("sales", "", "amount", "quarter", ["Q1"])
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works: {e}")
    
    print("  ✓ All PIVOT tests passed!")

def test_unpivot():
    """Test UNPIVOT operation."""
    print("\nTesting UNPIVOT operation...")
    
    # Simple UNPIVOT
    unpivot = b.unpivot(
        "quarterly_sales",
        "sales_amount", "quarter", ["Q1", "Q2", "Q3", "Q4"]
    )
    assert isinstance(unpivot.source, TableRef)
    assert unpivot.value_column == "sales_amount"
    assert unpivot.name_column == "quarter"
    assert len(unpivot.columns) == 4
    assert not unpivot.include_nulls
    print(f"  Simple UNPIVOT: {unpivot.value_column} FOR {unpivot.name_column}")
    
    # UNPIVOT with include_nulls
    unpivot = b.unpivot(
        "scores",
        "score", "subject", ["math_score", "science_score", "english_score"],
        include_nulls=True,
        alias="unpivoted"
    )
    assert unpivot.include_nulls
    assert unpivot.alias == "unpivoted"
    print(f"  UNPIVOT with nulls: include_nulls={unpivot.include_nulls}")
    
    # Test validation
    try:
        b.unpivot("sales", "", "quarter", ["Q1"])
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works: {e}")
    
    print("  ✓ All UNPIVOT tests passed!")

def test_merge():
    """Test MERGE statement builder."""
    print("\nTesting MERGE statement...")
    
    # Simple MERGE with all clause types
    merge = (b.merge("target", "source", b.eq(b.col("t.id"), b.col("s.id")))
            .when_matched_update({"name": b.col("s.name"), "updated": b.current_timestamp()})
            .when_not_matched_insert(["id", "name"], [b.col("s.id"), b.col("s.name")])
            .when_not_matched_by_source_delete()
            .build())
    
    assert isinstance(merge, Merge)
    assert len(merge.when_clauses) == 3
    print(f"  MERGE created with {len(merge.when_clauses)} WHEN clauses")
    
    # MERGE with conditions
    merge = (b.merge("target", "source", b.eq(b.col("t.id"), b.col("s.id")))
            .when_matched_update(
                {"status": b.lit("updated")},
                condition=b.gt(b.col("s.amount"), b.lit(100))
            )
            .when_matched_delete(
                condition=b.eq(b.col("s.status"), b.lit("deleted"))
            )
            .build())
    
    assert merge.when_clauses[0].condition is not None
    assert merge.when_clauses[1].condition is not None
    print(f"  MERGE with conditions: {len(merge.when_clauses)} conditional clauses")
    
    # MERGE with Select as source
    select_source = Select(select_list=[SelectColumn(expression=b.col("*"))])
    merge = (b.merge("target", select_source, b.eq(b.col("t.id"), b.col("s.id")))
            .when_not_matched_insert()  # Insert all columns
            .build())
    
    assert isinstance(merge.source, Select)
    print(f"  MERGE with SELECT source: Created successfully")
    
    # Test validation - no WHEN clauses
    try:
        b.merge("target", "source", b.eq(b.col("t.id"), b.col("s.id"))).build()
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works (no WHEN): {e}")
    
    # Test validation - mismatched columns/values
    try:
        (b.merge("target", "source", b.eq(b.col("t.id"), b.col("s.id")))
         .when_not_matched_insert(["id", "name"], [b.col("s.id")])  # Mismatch
         .build())
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works (mismatch): {e}")
    
    print("  ✓ All MERGE tests passed!")

def test_complex_query():
    """Test a complex query using multiple Phase 2 features."""
    print("\nTesting complex query with Phase 2 features...")
    
    # Build a complex query using CTEs, QUALIFY, and window functions
    cte_query = Select(
        select_list=[
            SelectColumn(expression=b.col("order_id")),
            SelectColumn(expression=b.col("customer_id")),
            SelectColumn(expression=b.func("SUM", b.col("amount")), alias="total")
        ],
        from_clause=TableRef(table=TableName(table="orders")),
        group_by_clause=b.group_by(b.col("order_id"), b.col("customer_id"))
    )
    
    main_query = Select(
        select_list=[
            SelectColumn(expression=b.col("*")),
            SelectColumn(
                expression=b.func("ROW_NUMBER"),
                alias="rn"
            )
        ],
        from_clause=TableRef(table=TableName(table="recent_orders")),
        qualify_clause=b.qualify(b.lte(b.col("rn"), b.lit(10)))
    )
    
    # Add WITH clause to main query
    main_query.with_clause = b.with_(
        ("recent_orders", cte_query)
    )
    
    print(f"  Complex query structure:")
    print(f"    - WITH clause: {len(main_query.with_clause.ctes)} CTEs")
    print(f"    - Main SELECT: {len(main_query.select_list)} columns")
    print(f"    - QUALIFY clause: Present")
    
    # Test MERGE with complex source
    merge = (b.merge("target_table", main_query, b.eq(b.col("t.id"), b.col("s.customer_id")))
            .when_matched_update({"last_order": b.col("s.order_id")})
            .when_not_matched_insert(
                ["customer_id", "last_order"],
                [b.col("s.customer_id"), b.col("s.order_id")]
            )
            .build())
    
    assert isinstance(merge.source, Select)
    assert merge.source.with_clause is not None
    print(f"  MERGE with CTE source: Created successfully")
    
    print("  ✓ Complex query test passed!")

def main():
    """Run all Phase 2 tests."""
    print("=" * 60)
    print("PHASE 2 IMPLEMENTATION TESTS")
    print("=" * 60)
    
    try:
        test_with_clause()
        test_qualify()
        test_unnest()
        test_tablesample()
        test_window_frame()
        test_pivot()
        test_unpivot()
        test_merge()
        test_complex_query()
        
        print("\n" + "=" * 60)
        print("✅ ALL PHASE 2 TESTS PASSED!")
        print("=" * 60)
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()