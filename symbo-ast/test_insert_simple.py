#!/usr/bin/env python3
"""
Simple test for insertBefore and insertAfter methods in Collection
Uses only builders.py classes to avoid import conflicts.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import modules directly
import collection
import node_path
import builders

# Create simple test nodes using builders
class SimpleNode(builders.ASTNode):
    """Simple test node."""
    def __init__(self, value):
        super().__init__()
        self.value = value
        self.children = []
    
    @property
    def node_type(self):
        return "SimpleNode"
    
    def __repr__(self):
        return f"SimpleNode({self.value})"


class ContainerNode(builders.ASTNode):
    """Container that holds a list of nodes."""
    def __init__(self, items=None):
        super().__init__()
        self.items = items or []
    
    @property
    def node_type(self):
        return "ContainerNode"


def test_basic_insert():
    """Test basic insertBefore and insertAfter."""
    print("=== Testing basic insert operations ===\n")
    
    # Create a container with some nodes
    container = ContainerNode([
        SimpleNode("A"),
        SimpleNode("B"),
        SimpleNode("C")
    ])
    
    # Create NodePath for container
    root_path = node_path.NodePath(container)
    
    # Get collection of SimpleNodes
    # Since we can't use the find method easily without proper setup,
    # let's create paths manually
    paths = []
    for i, item in enumerate(container.items):
        path = node_path.NodePath(
            item, 
            parent=root_path, 
            field="items", 
            index=i
        )
        paths.append(path)
    
    simple_collection = collection.Collection(paths)
    
    print(f"Original items: {[p.node.value for p in simple_collection]}")
    print(f"Collection size: {len(simple_collection)}")
    
    # Test insertBefore
    print("\n--- Testing insertBefore ---")
    try:
        new_node = SimpleNode("X")
        # Insert X before each node
        inserted = simple_collection.insertBefore(new_node)
        print(f"Inserted {len(inserted)} nodes")
        
        # Check container items
        values = [item.value for item in container.items]
        print(f"After insertBefore: {values}")
        
    except Exception as e:
        print(f"Error in insertBefore: {e}")
    
    # Reset container
    container.items = [SimpleNode("A"), SimpleNode("B"), SimpleNode("C")]
    
    # Test insertAfter with function
    print("\n--- Testing insertAfter with function ---")
    paths = []
    for i, item in enumerate(container.items):
        path = node_path.NodePath(
            item, 
            parent=root_path, 
            field="items", 
            index=i
        )
        paths.append(path)
    
    simple_collection = collection.Collection(paths)
    
    def create_suffix_node(path, index):
        """Create node with suffix based on current node."""
        return SimpleNode(f"{path.node.value}_after")
    
    try:
        inserted = simple_collection.insertAfter(create_suffix_node)
        print(f"Inserted {len(inserted)} nodes")
        
        # Check container items
        values = [item.value for item in container.items]
        print(f"After insertAfter: {values}")
        
    except Exception as e:
        print(f"Error in insertAfter: {e}")
    
    print("\n✓ Basic tests completed")


def test_edge_cases():
    """Test edge cases."""
    print("\n=== Testing edge cases ===\n")
    
    # Empty collection
    empty = collection.Collection([])
    try:
        result = empty.insertBefore(SimpleNode("test"))
        print(f"✓ Empty collection handled: {len(result)} items")
    except Exception as e:
        print(f"Empty collection error: {e}")
    
    # Single field (not in list)
    single_container = ContainerNode()
    single_container.single_item = SimpleNode("lonely")
    
    root = node_path.NodePath(single_container)
    single_path = node_path.NodePath(
        single_container.single_item,
        parent=root,
        field="single_item",
        index=None  # Not in a list
    )
    
    single_collection = collection.Collection([single_path])
    
    try:
        single_collection.insertAfter(SimpleNode("friend"))
        print("ERROR: Should have raised ValueError!")
    except ValueError as e:
        print(f"✓ Correctly raised error for non-list field: {e}")
    
    print("\n✓ Edge case tests completed")


if __name__ == "__main__":
    print("Testing Collection insert methods\n")
    test_basic_insert()
    test_edge_cases()
    print("\n🎉 All tests completed! 🐕")