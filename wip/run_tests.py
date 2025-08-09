#!/usr/bin/env python3
"""
Simple test runner for Phase 1 implementation.
"""

import sys
import os

# Add lib to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib'))

# Now we can import normally
from lib.builders import b, ValidationError
from lib.types import *
from lib.serializer import to_sql

def test_group_by():
    """Test GROUP BY builders."""
    print("Testing GROUP BY builders...")
    
    # Basic GROUP BY
    gb = b.group_by(b.col("dept"), b.col("team"))
    assert len(gb.expressions) == 2
    assert gb.group_type == GroupByType.STANDARD
    sql = to_sql(gb)
    print(f"  Basic GROUP BY: {sql}")
    
    # GROUP BY position
    gb = b.group_by(1, 2, 3)
    assert len(gb.expressions) == 3
    assert all(isinstance(e, IntegerLiteral) for e in gb.expressions)
    sql = to_sql(gb)
    print(f"  GROUP BY position: {sql}")
    
    # GROUP BY ALL
    gb = b.group_by("ALL")
    assert gb.group_type == GroupByType.ALL
    sql = to_sql(gb)
    print(f"  GROUP BY ALL: {sql}")
    
    # ROLLUP
    gb = b.group_by_rollup(b.col("year"), b.col("month"))
    assert gb.group_type == GroupByType.ROLLUP
    sql = to_sql(gb)
    print(f"  GROUP BY ROLLUP: {sql}")
    
    # CUBE
    gb = b.group_by_cube(b.col("product"), b.col("region"))
    assert gb.group_type == GroupByType.CUBE
    sql = to_sql(gb)
    print(f"  GROUP BY CUBE: {sql}")
    
    # GROUPING SETS
    gb = b.grouping_sets([b.col("a")], [b.col("b")], [])
    assert gb.group_type == GroupByType.GROUPING_SETS
    sql = to_sql(gb)
    print(f"  GROUPING SETS: {sql}")
    
    print("  ✓ All GROUP BY tests passed!")

def test_order_by():
    """Test ORDER BY builders."""
    print("\nTesting ORDER BY builders...")
    
    # Simple ORDER BY
    ob = b.order_by(b.col("name"))
    assert len(ob.items) == 1
    assert ob.items[0].direction == OrderDirection.ASC
    sql = to_sql(ob)
    print(f"  Simple ORDER BY: {sql}")
    
    # With direction
    ob = b.order_by((b.col("age"), "DESC"))
    assert ob.items[0].direction == OrderDirection.DESC
    sql = to_sql(ob)
    print(f"  ORDER BY DESC: {sql}")
    
    # With NULLS ordering
    ob = b.order_by((b.col("salary"), "DESC", "NULLS LAST"))
    assert ob.items[0].nulls_order == NullsOrder.LAST
    sql = to_sql(ob)
    print(f"  ORDER BY with NULLS: {sql}")
    
    print("  ✓ All ORDER BY tests passed!")

def test_star():
    """Test star expression builder."""
    print("\nTesting star expression...")
    
    # Simple star
    star = b.star()
    sql = to_sql(star)
    print(f"  Simple star: {sql}")
    
    # Table star
    star = b.star("t1")
    sql = to_sql(star)
    print(f"  Table star: {sql}")
    
    # Star with EXCEPT
    star = b.star(except_=["password", "ssn"])
    sql = to_sql(star)
    print(f"  Star EXCEPT: {sql}")
    
    print("  ✓ All star tests passed!")

def test_set_operations():
    """Test set operation builders."""
    print("\nTesting set operations...")
    
    # Create test SELECTs
    s1 = Select(select_list=[SelectColumn(expression=b.col("a"))])
    s2 = Select(select_list=[SelectColumn(expression=b.col("b"))])
    
    # UNION
    u = b.union(s1, s2)
    assert u.operator == SetOperator.UNION
    assert u.all == False
    sql = to_sql(u)
    print(f"  UNION: {sql}")
    
    # UNION ALL
    u = b.union(s1, s2, all=True)
    assert u.all == True
    sql = to_sql(u)
    print(f"  UNION ALL: {sql}")
    
    print("  ✓ All set operation tests passed!")

def test_parameters():
    """Test parameter builders."""
    print("\nTesting query parameters...")
    
    # Named parameter
    p = b.param("user_id")
    assert p.name == "user_id"
    sql = to_sql(p)
    print(f"  Named parameter: {sql}")
    
    # Positional parameter
    p = b.positional_param(0)
    assert p.position == 0
    sql = to_sql(p)
    print(f"  Positional parameter: {sql}")
    
    print("  ✓ All parameter tests passed!")

def test_limit():
    """Test LIMIT builder."""
    print("\nTesting LIMIT builder...")
    
    # LIMIT only
    lc = b.limit(10)
    assert lc.limit.value == 10
    assert lc.offset is None
    sql = to_sql(lc)
    print(f"  LIMIT only: {sql}")
    
    # LIMIT with OFFSET
    lc = b.limit(10, offset=5)
    assert lc.limit.value == 10
    assert lc.offset.value == 5
    sql = to_sql(lc)
    print(f"  LIMIT with OFFSET: {sql}")
    
    print("  ✓ All LIMIT tests passed!")

def test_interval():
    """Test INTERVAL builder."""
    print("\nTesting INTERVAL literals...")
    
    # Simple interval
    i = b.interval(5, "DAY")
    assert "5 DAY" in str(i.value)
    
    # Range interval
    i = b.interval("10:20:30", "HOUR", "SECOND")
    assert i.from_part == "HOUR"
    assert i.to_part == "SECOND"
    
    print("  ✓ All INTERVAL tests passed!")

def test_json():
    """Test JSON builder."""
    print("\nTesting JSON literals...")
    
    # JSON from dict
    j = b.json({"name": "Alice", "age": 30})
    assert "Alice" in j.value
    
    # JSON from string
    j = b.json('{"valid": "json"}')
    assert j.value == '{"valid": "json"}'
    
    print("  ✓ All JSON tests passed!")

def main():
    """Run all tests."""
    print("=" * 60)
    print("PHASE 1 IMPLEMENTATION TESTS")
    print("=" * 60)
    
    try:
        test_group_by()
        test_order_by()
        test_star()
        test_set_operations()
        test_parameters()
        test_limit()
        test_interval()
        test_json()
        
        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED!")
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