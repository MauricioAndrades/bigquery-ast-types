#!/usr/bin/env python3
"""
Test insertBefore and insertAfter methods in Collection

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import without dots for direct execution
import collection
import node_path
import builders

Collection = collection.Collection
astCollection = collection.astCollection
NodePath = node_path.NodePath

SelectStatement = builders.SelectStatement
Identifier = builders.Identifier
ColumnExpression = builders.ColumnExpression
FromClause = builders.FromClause
TableExpression = builders.TableExpression
WhereClause = builders.WhereClause
BinaryExpression = builders.BinaryExpression


def test_insert_before():
    """Test insertBefore functionality."""
    print("Testing insertBefore...")
    
    # Create a simple SELECT statement with multiple columns
    select = SelectStatement(
        columns=[
            ColumnExpression(name=Identifier("col1")),
            ColumnExpression(name=Identifier("col2")),
            ColumnExpression(name=Identifier("col3"))
        ],
        from_clause=FromClause(
            tables=[TableExpression(name=Identifier("test_table"))]
        )
    )
    
    # Create collection of column nodes
    root_path = NodePath(select)
    col_collection = astCollection(root_path).find(ColumnExpression)
    
    print(f"Original columns: {[col.get().name.value for col in col_collection]}")
    assert len(col_collection) == 3
    
    # Insert a new column before each existing column
    new_col = ColumnExpression(name=Identifier("new_col"))
    try:
        inserted = col_collection.insertBefore(new_col)
        print(f"Inserted {len(inserted)} new columns")
        
        # Check the updated structure
        all_cols = astCollection(root_path).find(ColumnExpression)
        col_names = [col.get().name.value for col in all_cols]
        print(f"After insertBefore: {col_names}")
        
        # Should have 6 columns now
        assert len(all_cols) == 6
        
    except ValueError as e:
        print(f"Error: {e}")
    
    print("✓ insertBefore test passed\n")


def test_insert_after():
    """Test insertAfter functionality."""
    print("Testing insertAfter...")
    
    # Create a simple SELECT statement
    select = SelectStatement(
        columns=[
            ColumnExpression(name=Identifier("col1")),
            ColumnExpression(name=Identifier("col2")),
            ColumnExpression(name=Identifier("col3"))
        ],
        from_clause=FromClause(
            tables=[TableExpression(name=Identifier("test_table"))]
        )
    )
    
    root_path = NodePath(select)
    col_collection = astCollection(root_path).find(ColumnExpression)
    
    print(f"Original columns: {[col.get().name.value for col in col_collection]}")
    
    # Insert using a function to create unique columns
    def create_suffix_col(path, index):
        col_name = path.node.name.value
        return ColumnExpression(name=Identifier(f"{col_name}_after"))
    
    try:
        inserted = col_collection.insertAfter(create_suffix_col)
        print(f"Inserted {len(inserted)} new columns")
        
        # Check the updated structure
        all_cols = astCollection(root_path).find(ColumnExpression)
        col_names = [col.get().name.value for col in all_cols]
        print(f"After insertAfter: {col_names}")
        
        # Should have 6 columns now: col1, col1_after, col2, col2_after, col3, col3_after
        assert len(all_cols) == 6
        expected = ["col1", "col1_after", "col2", "col2_after", "col3", "col3_after"]
        assert col_names == expected
        
    except ValueError as e:
        print(f"Error: {e}")
    
    print("✓ insertAfter test passed\n")


def test_insert_with_where_clause():
    """Test inserting nodes in WHERE clause."""
    print("Testing insert in WHERE clause...")
    
    # Create SELECT with WHERE clause containing multiple conditions
    select = SelectStatement(
        columns=[ColumnExpression(name=Identifier("*"))],
        from_clause=FromClause(
            tables=[TableExpression(name=Identifier("orders"))]
        ),
        where_clause=WhereClause(
            condition=BinaryExpression(
                left=BinaryExpression(
                    left=ColumnExpression(name=Identifier("status")),
                    operator="=",
                    right=Identifier("'active'")
                ),
                operator="AND",
                right=BinaryExpression(
                    left=ColumnExpression(name=Identifier("amount")),
                    operator=">",
                    right=Identifier("100")
                )
            )
        )
    )
    
    root_path = NodePath(select)
    
    # Find all binary expressions in WHERE clause
    where_binaries = astCollection(root_path).find(
        WhereClause
    ).find(BinaryExpression)
    
    print(f"Found {len(where_binaries)} binary expressions in WHERE")
    
    # This should fail because WHERE conditions aren't in a list
    try:
        new_condition = BinaryExpression(
            left=ColumnExpression(name=Identifier("region")),
            operator="=",
            right=Identifier("'US'")
        )
        where_binaries.insertAfter(new_condition)
        print("ERROR: Should have raised ValueError!")
    except ValueError as e:
        print(f"✓ Correctly raised error for non-list insertion: {e}")
    
    print("✓ WHERE clause test passed\n")


def test_edge_cases():
    """Test edge cases."""
    print("Testing edge cases...")
    
    # Empty collection
    empty = Collection([])
    try:
        result = empty.insertBefore(Identifier("test"))
        print(f"✓ Empty collection returns empty: {len(result)} items")
        assert len(result) == 0
    except Exception as e:
        print(f"Empty collection handling: {e}")
    
    # Root node
    root = SelectStatement(columns=[])
    root_collection = astCollection(root)
    
    try:
        root_collection.insertBefore(Identifier("test"))
        print("ERROR: Should have raised ValueError for root!")
    except ValueError as e:
        print(f"✓ Correctly raised error for root node: {e}")
    
    print("✓ Edge cases passed\n")


if __name__ == "__main__":
    print("=== Testing Collection insert methods ===\n")
    
    test_insert_before()
    test_insert_after()
    test_insert_with_where_clause()
    test_edge_cases()
    
    print("🎉 All tests passed! 🐕")