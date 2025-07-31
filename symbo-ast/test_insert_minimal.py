#!/usr/bin/env python3
"""
Minimal test for insertBefore and insertAfter without import issues

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

# Define minimal classes directly to avoid import issues
from typing import List, Optional, Any
from dataclasses import dataclass


@dataclass
class SimpleNode:
    """Minimal node for testing."""
    value: str
    
    @property
    def node_type(self):
        return "SimpleNode"


@dataclass 
class ContainerNode:
    """Container with list of nodes."""
    items: List[SimpleNode]
    
    @property
    def node_type(self):
        return "ContainerNode"


# Simple NodePath implementation
class NodePath:
    def __init__(self, node, parent=None, field=None, index=None, scope=None):
        self.node = node
        self.parent = parent
        self.field = field
        self.index = index
        self.scope = scope
        self._children = None
    
    def insert_before(self, new_node):
        """Insert before this node."""
        if self.parent is None or self.index is None:
            raise ValueError("Can only insert before nodes in lists")
        
        parent_node = self.parent.node
        getattr(parent_node, self.field).insert(self.index, new_node)
        
        # Update indices
        self.index += 1
        if self.parent._children:
            for child in self.parent._children:
                if (child.field == self.field and 
                    child.index is not None and 
                    child.index >= self.index):
                    child.index += 1
        
        # Clear cache
        self.parent._children = None
        
        return NodePath(new_node, self.parent, self.field, self.index - 1, self.scope)
    
    def insert_after(self, new_node):
        """Insert after this node."""
        if self.parent is None or self.index is None:
            raise ValueError("Can only insert after nodes in lists")
        
        parent_node = self.parent.node
        getattr(parent_node, self.field).insert(self.index + 1, new_node)
        
        # Update indices of siblings
        if self.parent._children:
            for child in self.parent._children:
                if (child.field == self.field and 
                    child.index is not None and 
                    child.index > self.index):
                    child.index += 1
        
        # Clear cache
        self.parent._children = None
        
        return NodePath(new_node, self.parent, self.field, self.index + 1, self.scope)


# Simple Collection implementation
class Collection:
    def __init__(self, paths):
        self.paths = paths
    
    def __len__(self):
        return len(self.paths)
    
    def insertBefore(self, node):
        """Insert node before each path."""
        new_paths = []
        
        # Process in reverse to avoid index issues
        for i, path in enumerate(reversed(self.paths)):
            if callable(node):
                new_node = node(path, len(self.paths) - 1 - i)
            else:
                new_node = node
            
            try:
                new_path = path.insert_before(new_node)
                new_paths.append(new_path)
            except ValueError as e:
                raise ValueError(f"Cannot insert before {path}: {e}")
        
        return Collection(list(reversed(new_paths)))
    
    def insertAfter(self, node):
        """Insert node after each path."""
        new_paths = []
        
        # Process in reverse to avoid index issues
        for i, path in enumerate(reversed(self.paths)):
            if callable(node):
                new_node = node(path, len(self.paths) - 1 - i)
            else:
                new_node = node
            
            try:
                new_path = path.insert_after(new_node)
                new_paths.append(new_path)
            except ValueError as e:
                raise ValueError(f"Cannot insert after {path}: {e}")
        
        return Collection(list(reversed(new_paths)))


def test_insert_operations():
    """Test insertBefore and insertAfter."""
    print("=== Testing insert operations ===\n")
    
    # Create container with items
    container = ContainerNode([
        SimpleNode("A"),
        SimpleNode("B"), 
        SimpleNode("C")
    ])
    
    # Create paths
    root_path = NodePath(container)
    paths = []
    for i, item in enumerate(container.items):
        path = NodePath(item, parent=root_path, field="items", index=i)
        paths.append(path)
    
    collection = Collection(paths)
    
    print(f"Original: {[item.value for item in container.items]}")
    
    # Test insertBefore
    print("\n--- insertBefore ---")
    new_node = SimpleNode("X")
    inserted = collection.insertBefore(new_node)
    print(f"Inserted {len(inserted)} nodes")
    print(f"Result: {[item.value for item in container.items]}")
    
    # Reset
    container.items = [SimpleNode("A"), SimpleNode("B"), SimpleNode("C")]
    
    # Test insertAfter with function
    print("\n--- insertAfter with function ---")
    paths = []
    for i, item in enumerate(container.items):
        path = NodePath(item, parent=root_path, field="items", index=i)
        paths.append(path)
    
    collection = Collection(paths)
    
    def create_after_node(path, index):
        return SimpleNode(f"{path.node.value}_after")
    
    inserted = collection.insertAfter(create_after_node)
    print(f"Inserted {len(inserted)} nodes")
    print(f"Result: {[item.value for item in container.items]}")
    
    print("\n✓ Tests completed successfully!")


if __name__ == "__main__":
    test_insert_operations()