"""Tests for NodePath.insert_before behavior"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from lib.node_path import NodePath
from lib.types import Select, SelectColumn, Identifier, Literal, WhereClause, BinaryOp
from lib.builders import b


def test_insert_before_updates_indices():
    """Inserting before a node should update indices of existing paths."""
    # Build a simple SELECT with multiple columns
    select = Select(
        select_list=[
            SelectColumn(Identifier("a")),
            SelectColumn(Identifier("b")),
            SelectColumn(Identifier("c"))
        ]
    )
    root_path = NodePath(select)
    
    # Get original children paths - the select_list items
    original_children = root_path.get_children()
    
    # Find the SelectColumn children
    select_list_path = None
    for child in original_children:
        if child.field == "select_list":
            # This is one of the SelectColumn items
            select_list_path = child.parent
            break
    
    assert select_list_path is not None, "Could not find select_list"
    
    # Get the SelectColumn children
    columns = [c for c in original_children if c.field == "select_list"]
    assert len(columns) >= 3, f"Expected at least 3 columns but got: {len(columns)}"
    
    first = columns[0]
    second = columns[1]
    third = columns[2]
    
    # Insert a new column before the second one
    new_column = SelectColumn(Identifier("x"))
    new_path = second.insert_before(new_column)
    
    # Verify the returned NodePath points to the newly inserted node at index 1
    assert new_path.index == 1
    assert isinstance(new_path.node, SelectColumn)
    
    # Verify existing node indices are properly updated
    assert first.index == 0  # Should remain unchanged
    assert second.index == 2  # Should be incremented
    assert third.index == 3  # Should be incremented
    
    # Verify tree structure by getting fresh children
    fresh_columns = [c for c in root_path.get_children() if c.field == "select_list"]
    
    # Check we have 4 columns now
    assert len(fresh_columns) == 4
    assert fresh_columns[1].node == new_column
    
    # Verify indices
    indices = [c.index for c in fresh_columns]
    assert indices == [0, 1, 2, 3]


def test_insert_before_numeric_values():
    """Test with multiple literal values."""
    # Build a SELECT with literal values using the builder
    select = Select(
        select_list=[
            SelectColumn(b.lit(1)),
            SelectColumn(b.lit(2)),
            SelectColumn(b.lit(3))
        ]
    )
    root_path = NodePath(select)
    
    # Get the SelectColumn children
    columns = [c for c in root_path.get_children() if c.field == "select_list"]
    assert len(columns) >= 2, f"Expected at least 2 columns but got: {len(columns)}"
    second = columns[1]
    
    # Insert a new node before the second child
    new_column = SelectColumn(b.lit(99))
    new_path = second.insert_before(new_column)
    
    # The original path (for value 2) should have its index updated
    assert second.index == 2
    
    # The returned NodePath should point to the newly inserted node at index 1
    assert new_path.index == 1
    assert isinstance(new_path.node, SelectColumn)


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    test_insert_before_updates_indices()
    test_insert_before_numeric_values()
    print("All tests passed!")