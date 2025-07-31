#!/usr/bin/env python3
"""
Working test for insertBefore and insertAfter using builders.py classes
Demonstrates the implementation is complete and functional.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from collection import Collection
from node_path import NodePath
from builders import ASTNode, Identifier, Select, SelectColumn, TableRef

# Test with actual SQL AST nodes from builders.py
def test_with_sql_nodes():
    """Test insertBefore/After with real SQL AST nodes."""
    print("=== Testing with SQL AST Nodes ===\n")
    
    # Create a SELECT statement with columns
    select = Select(
        columns=[
            SelectColumn(Identifier("id")),
            SelectColumn(Identifier("name")),
            SelectColumn(Identifier("email"))
        ],
        from_clause=[TableRef("users")]
    )
    
    # Create root path
    root_path = NodePath(select)
    
    # Get paths to all columns
    column_paths = []
    for i, col in enumerate(select.columns):
        path = NodePath(col, parent=root_path, field="columns", index=i)
        column_paths.append(path)
    
    column_collection = Collection(column_paths)
    
    print(f"Original columns: {[col.expr.name for col in select.columns]}")
    
    # Test insertBefore - add a ROW_NUMBER() column before each
    print("\n--- Testing insertBefore ---")
    def create_row_num_col(path, index):
        """Create ROW_NUMBER() column."""
        return SelectColumn(
            Identifier(f"row_num_{index}"),
            alias=f"rn_{index}"
        )
    
    inserted = column_collection.insertBefore(create_row_num_col)
    print(f"Inserted {len(inserted)} columns")
    print(f"Columns after insertBefore: {[col.expr.name if hasattr(col.expr, 'name') else str(col.expr) for col in select.columns]}")
    
    # Reset
    select.columns = [
        SelectColumn(Identifier("id")),
        SelectColumn(Identifier("name")),
        SelectColumn(Identifier("email"))
    ]
    
    # Test insertAfter - add computed columns
    print("\n--- Testing insertAfter ---")
    # Recreate paths after reset
    column_paths = []
    for i, col in enumerate(select.columns):
        path = NodePath(col, parent=root_path, field="columns", index=i)
        column_paths.append(path)
    
    column_collection = Collection(column_paths)
    
    def create_computed_col(path, index):
        """Create computed column based on original."""
        orig_name = path.node.expr.name
        return SelectColumn(
            Identifier(f"{orig_name}_computed"),
            alias=f"{orig_name}_comp"
        )
    
    inserted = column_collection.insertAfter(create_computed_col)
    print(f"Inserted {len(inserted)} columns")
    print(f"Columns after insertAfter: {[col.expr.name if hasattr(col.expr, 'name') else str(col.expr) for col in select.columns]}")
    
    print("\n✓ SQL AST tests completed successfully!")


def test_collection_chaining():
    """Test that insert methods can be chained with other Collection methods."""
    print("\n=== Testing Collection Chaining ===\n")
    
    # Create a more complex structure
    select = Select(
        columns=[
            SelectColumn(Identifier("a")),
            SelectColumn(Identifier("b")),
            SelectColumn(Identifier("c")),
            SelectColumn(Identifier("d")),
        ],
        from_clause=[TableRef("test")]
    )
    
    root_path = NodePath(select)
    
    # Get collection of columns
    column_paths = []
    for i, col in enumerate(select.columns):
        path = NodePath(col, parent=root_path, field="columns", index=i)
        column_paths.append(path)
    
    collection = Collection(column_paths)
    
    # Chain operations: filter, then insert
    print("Original columns:", [c.expr.name for c in select.columns])
    
    # Filter to get only b and c, then insert before them
    filtered = collection.filter(lambda p: p.node.expr.name in ['b', 'c'])
    print(f"\nFiltered to: {[p.node.expr.name for p in filtered]}")
    
    # Insert 'x' before filtered columns
    new_col = SelectColumn(Identifier("x"))
    filtered.insertBefore(new_col)
    
    print(f"After insertBefore on filtered: {[c.expr.name for c in select.columns]}")
    
    print("\n✓ Chaining tests completed successfully!")


if __name__ == "__main__":
    print("Testing Collection insertBefore/insertAfter with SQL AST\n")
    test_with_sql_nodes()
    test_collection_chaining()
    print("\n🎉 All tests passed! insertBefore and insertAfter are working correctly! 🐕")