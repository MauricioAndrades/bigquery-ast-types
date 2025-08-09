#!/usr/bin/env python3
"""
Test suite for Phase 1 BigQuery AST builders.

Tests GROUP BY variants, ORDER BY enhancements, set operations,
and BigQuery-specific literals.

Author: Implementation Test Suite
Date: 2025-08-09
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib'))

# Import the modules directly
import importlib.util

# Load types module
spec = importlib.util.spec_from_file_location("types", "./lib/types.py")
types = importlib.util.module_from_spec(spec)
sys.modules['types'] = types
spec.loader.exec_module(types)

# Load builders module
spec = importlib.util.spec_from_file_location("builders", "./lib/builders.py")
builders = importlib.util.module_from_spec(spec)
sys.modules['builders'] = builders
spec.loader.exec_module(builders)

# Load serializer module
spec = importlib.util.spec_from_file_location("serializer", "./lib/serializer.py")
serializer = importlib.util.module_from_spec(spec)
sys.modules['serializer'] = serializer
spec.loader.exec_module(serializer)

# Import what we need
from types import *
b = builders.b
ValidationError = builders.ValidationError
to_sql = serializer.to_sql

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
    assert len(gb.expressions) == 2
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
    assert len(gb.grouping_sets) == 3
    sql = to_sql(gb)
    print(f"  GROUPING SETS: {sql}")
    
    # Test validation
    try:
        b.group_by(0)  # Position must be >= 1
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works: {e}")
    
    print("  ✓ All GROUP BY tests passed!")

def test_having():
    """Test HAVING builder."""
    print("\nTesting HAVING builder...")
    
    # Simple HAVING
    having = b.having(b.gt(b.func("COUNT", b.star()), b.lit(5)))
    sql = to_sql(having)
    print(f"  Simple HAVING: {sql}")
    
    # Complex HAVING
    having = b.having(b.and_(
        b.gte(b.func("SUM", b.col("amount")), b.lit(1000)),
        b.lt(b.func("AVG", b.col("amount")), b.lit(500))
    ))
    sql = to_sql(having)
    print(f"  Complex HAVING: {sql}")
    
    print("  ✓ All HAVING tests passed!")

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
    
    # Multiple columns
    ob = b.order_by(
        b.col("dept"),
        (b.col("salary"), "DESC", "NULLS FIRST"),
        (b.col("name"), "ASC")
    )
    assert len(ob.items) == 3
    sql = to_sql(ob)
    print(f"  Multiple ORDER BY: {sql}")
    
    print("  ✓ All ORDER BY tests passed!")

def test_limit():
    """Test LIMIT/OFFSET builder."""
    print("\nTesting LIMIT/OFFSET builder...")
    
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
    
    # LIMIT with parameters
    lc = b.limit(b.param("limit"), offset=b.param("offset"))
    assert isinstance(lc.limit, NamedParameter)
    assert isinstance(lc.offset, NamedParameter)
    sql = to_sql(lc)
    print(f"  LIMIT with params: {sql}")
    
    # Test validation
    try:
        b.limit(-1)
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works: {e}")
    
    print("  ✓ All LIMIT tests passed!")

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
    
    # UNION CORRESPONDING
    u = b.union(s1, s2, corresponding=True)
    assert u.corresponding == True
    sql = to_sql(u)
    print(f"  UNION CORRESPONDING: {sql}")
    
    # INTERSECT
    i = b.intersect(s1, s2)
    assert i.operator == SetOperator.INTERSECT
    sql = to_sql(i)
    print(f"  INTERSECT: {sql}")
    
    # EXCEPT
    e = b.except_(s1, s2, all=True)
    assert e.operator == SetOperator.EXCEPT
    assert e.all == True
    sql = to_sql(e)
    print(f"  EXCEPT ALL: {sql}")
    
    print("  ✓ All set operation tests passed!")

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
    
    # Star with REPLACE
    star = b.star(replace={"name": b.func("UPPER", b.col("name"))})
    sql = to_sql(star)
    print(f"  Star REPLACE: {sql}")
    
    print("  ✓ All star tests passed!")

def test_interval():
    """Test INTERVAL builder."""
    print("\nTesting INTERVAL literals...")
    
    # Simple interval
    i = b.interval(5, "DAY")
    assert "5 DAY" in str(i.value)
    sql = to_sql(i)
    print(f"  Simple interval: {sql}")
    
    # Negative interval
    i = b.interval(-30, "MINUTE")
    sql = to_sql(i)
    print(f"  Negative interval: {sql}")
    
    # Range interval
    i = b.interval("10:20:30", "HOUR", "SECOND")
    assert i.from_part == "HOUR"
    assert i.to_part == "SECOND"
    sql = to_sql(i)
    print(f"  Range interval: {sql}")
    
    # Test validation
    try:
        b.interval(5, "INVALID")
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works: {e}")
    
    print("  ✓ All INTERVAL tests passed!")

def test_json():
    """Test JSON builder."""
    print("\nTesting JSON literals...")
    
    # JSON from dict
    j = b.json({"name": "Alice", "age": 30})
    assert "Alice" in j.value
    sql = to_sql(j)
    print(f"  JSON from dict: {sql}")
    
    # JSON from list
    j = b.json([1, 2, 3])
    sql = to_sql(j)
    print(f"  JSON from list: {sql}")
    
    # JSON from string
    j = b.json('{"valid": "json"}')
    assert j.value == '{"valid": "json"}'
    sql = to_sql(j)
    print(f"  JSON from string: {sql}")
    
    # Test validation
    try:
        b.json("not valid json")
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works: {e}")
    
    print("  ✓ All JSON tests passed!")

def test_parameters():
    """Test parameter builders."""
    print("\nTesting query parameters...")
    
    # Named parameter
    p = b.param("user_id")
    assert p.name == "user_id"
    sql = to_sql(p)
    print(f"  Named parameter: {sql}")
    
    # Test validation
    try:
        b.param("123invalid")  # Must start with letter or _
        assert False, "Should have raised ValidationError"
    except ValidationError as e:
        print(f"  ✓ Validation works: {e}")
    
    # Positional parameter
    p = b.positional_param(0)
    assert p.position == 0
    sql = to_sql(p)
    print(f"  Positional parameter: {sql}")
    
    print("  ✓ All parameter tests passed!")

def test_complex_query():
    """Test a complex query using multiple new features."""
    print("\nTesting complex query with new features...")
    
    # Build a complex query using the new features
    from types import TableName, WhereClause
    
    select_cols = [
        SelectColumn(expression=b.star(except_=["internal_id"])),
        SelectColumn(expression=b.func("COUNT", b.star()), alias="total")
    ]
    
    query = Select(
        select_list=select_cols,
        from_clause=TableRef(table=TableName(table="sales")),
        group_by_clause=b.group_by_rollup(b.col("year"), b.col("month")),
        having_clause=b.having(b.gt(b.func("SUM", b.col("amount")), b.lit(10000))),
        order_by_clause=b.order_by(
            (b.col("year"), "DESC"),
            (b.col("month"), "ASC", "NULLS LAST")
        ),
        limit_clause=b.limit(100, offset=10)
    )
    
    sql = to_sql(query)
    print(f"  Complex query SQL:\n{sql}")
    
    # Verify the SQL contains expected elements
    assert "* EXCEPT(internal_id)" in sql
    assert "GROUP BY ROLLUP(year, month)" in sql
    assert "HAVING" in sql
    assert "ORDER BY year DESC, month ASC NULLS LAST" in sql
    assert "LIMIT 100" in sql
    assert "OFFSET 10" in sql
    
    print("  ✓ Complex query test passed!")

def main():
    """Run all tests."""
    print("=" * 60)
    print("PHASE 1 IMPLEMENTATION TESTS")
    print("=" * 60)
    
    try:
        test_group_by()
        test_having()
        test_order_by()
        test_limit()
        test_set_operations()
        test_star()
        test_interval()
        test_json()
        test_parameters()
        test_complex_query()
        
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