#!/usr/bin/env python3
"""
Simplified test runner for Phase 2 features.
Tests core builder functionality without complex imports.
"""

import sys
import os

# Add lib to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib'))

# Now we can import normally
from lib.builders import b, ValidationError, MergeBuilder
from lib.types import *

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
    print(f"  ✓ Simple WITH: {len(with_clause.ctes)} CTEs")
    
    # Recursive CTE
    recursive_with = b.with_recursive(
        ("ancestors", select1)
    )
    assert recursive_with.recursive
    print(f"  ✓ Recursive WITH: recursive={recursive_with.recursive}")
    
    print("  ✓ All WITH clause tests passed!")

def test_qualify():
    """Test QUALIFY clause."""
    print("\nTesting QUALIFY clause...")
    
    # Simple QUALIFY
    qualify = b.qualify(b.eq(b.func("ROW_NUMBER"), b.lit(1)))
    assert isinstance(qualify.condition, BinaryOp)
    print(f"  ✓ QUALIFY created with condition")
    
    print("  ✓ All QUALIFY tests passed!")

def test_unnest():
    """Test UNNEST table function."""
    print("\nTesting UNNEST table function...")
    
    # Simple UNNEST
    unnest = b.unnest(b.col("array_column"))
    assert isinstance(unnest.array_expr, Identifier)
    assert not unnest.with_offset
    print(f"  ✓ Simple UNNEST created")
    
    # UNNEST with ordinality
    unnest = b.unnest(b.col("array_column"), ordinality=True, alias="item")
    assert unnest.with_offset
    assert unnest.offset_alias == "item"
    print(f"  ✓ UNNEST with ordinality and alias")
    
    print("  ✓ All UNNEST tests passed!")

def test_tablesample():
    """Test TABLESAMPLE clause."""
    print("\nTesting TABLESAMPLE clause...")
    
    # TABLESAMPLE with percent
    sample = b.tablesample("orders", percent=10.0)
    assert sample.percent == 10.0
    assert sample.rows is None
    print(f"  ✓ TABLESAMPLE with {sample.percent}%")
    
    # TABLESAMPLE with rows
    sample = b.tablesample("orders", rows=1000)
    assert sample.rows == 1000
    assert sample.percent is None
    print(f"  ✓ TABLESAMPLE with {sample.rows} rows")
    
    print("  ✓ All TABLESAMPLE tests passed!")

def test_window_frame():
    """Test window frame specifications."""
    print("\nTesting window frame specifications...")
    
    # Default frame
    frame = b.window_frame()
    assert frame.frame_type == FrameType.ROWS
    assert frame.start_bound == FrameBound.UNBOUNDED_PRECEDING
    assert frame.end_bound == FrameBound.CURRENT_ROW
    print(f"  ✓ Default window frame created")
    
    # Numeric bounds
    frame = b.window_frame("ROWS", 2, 0)
    assert frame.frame_type == FrameType.ROWS
    assert frame.start_bound == (2, "PRECEDING")
    assert frame.end_bound == (0, "PRECEDING")
    print(f"  ✓ Window frame with numeric bounds")
    
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
    assert len(pivot.pivot_values) == 4
    print(f"  ✓ PIVOT created with {len(pivot.pivot_values)} values")
    
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
    assert len(unpivot.columns) == 4
    print(f"  ✓ UNPIVOT created with {len(unpivot.columns)} columns")
    
    print("  ✓ All UNPIVOT tests passed!")

def test_merge():
    """Test MERGE statement builder."""
    print("\nTesting MERGE statement...")
    
    # Simple MERGE
    merge = (b.merge("target", "source", b.eq(b.col("t.id"), b.col("s.id")))
            .when_matched_update({"name": b.col("s.name")})
            .when_not_matched_insert(["id", "name"], [b.col("s.id"), b.col("s.name")])
            .build())
    
    assert isinstance(merge, Merge)
    assert len(merge.when_clauses) == 2
    print(f"  ✓ MERGE created with {len(merge.when_clauses)} WHEN clauses")
    
    print("  ✓ All MERGE tests passed!")

def main():
    """Run all Phase 2 tests."""
    print("=" * 60)
    print("PHASE 2 BUILDER TESTS")
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
        
        print("\n" + "=" * 60)
        print("✅ ALL PHASE 2 BUILDER TESTS PASSED!")
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