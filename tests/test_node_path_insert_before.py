"""Tests for NodePath.insert_before behavior"""
from dataclasses import dataclass, field
from typing import List
import importlib
import pathlib
import sys
import types

# Provide a minimal ``lib`` package with a stub ``ASTNode`` so that we can
# import ``NodePath`` without pulling in the full, heavyweight type
# definitions (which are unnecessary for these tests and have unresolved
# dependencies).
lib_dir = pathlib.Path(__file__).resolve().parents[1] / "lib"
lib_pkg = types.ModuleType("lib")
lib_pkg.__path__ = [str(lib_dir)]
sys.modules.setdefault("lib", lib_pkg)

types_stub = types.ModuleType("lib.types")

class ASTNode:  # Minimal base class for tests
    def accept(self, visitor):  # pragma: no cover - not executed
        pass
    
    @property
    def node_type(self) -> str:  # pragma: no cover - simple attribute
        return self.__class__.__name__

types_stub.ASTNode = ASTNode
sys.modules.setdefault("lib.types", types_stub)

# Import NodePath after setting up the module stubs
NodePath = importlib.import_module("lib.node_path").NodePath


@dataclass
class DummyNode(ASTNode):
    """Simple AST node used for NodePath tests."""
    value: str
    children: List[ASTNode] = field(default_factory=list)


def test_insert_before_updates_indices():
    """Inserting before a node should update indices of existing paths."""
    # Build a simple tree ``root -> [a, b, c]``
    root = DummyNode("root", children=[DummyNode("a"), DummyNode("b"), DummyNode("c")])
    root_path = NodePath(root)
    
    # Get original children paths
    original_children = root_path.get_children()
    first = original_children[0]
    second = original_children[1]
    third = original_children[2]
    
    # Insert a new node before the second child
    new_node = DummyNode("x")
    new_path = second.insert_before(new_node)
    
    # Verify the returned NodePath points to the newly inserted node at index 1
    assert new_path.index == 1
    assert new_path.node.value == "x"
    
    # Verify existing node indices are properly updated
    assert first.index == 0  # Should remain unchanged
    assert second.index == 2  # Should be incremented
    assert third.index == 3  # Should be incremented
    
    # Verify tree structure by getting fresh children
    fresh_children = root_path.get_children()
    values = [c.node.value for c in fresh_children]
    indices = [c.index for c in fresh_children]
    
    assert values == ["a", "x", "b", "c"]
    assert indices == [0, 1, 2, 3]
    
    # Also verify the underlying data structure
    assert [c.value for c in root.children] == ["a", "x", "b", "c"]


def test_insert_before_numeric_values():
    """Test with numeric values to ensure compatibility."""
    @dataclass
    class NumericNode(ASTNode):
        value: int
        children: List[ASTNode] = field(default_factory=list)
    
    # Build a simple tree ``root -> [1, 2, 3]``
    root = NumericNode(0, children=[NumericNode(1), NumericNode(2), NumericNode(3)])
    root_path = NodePath(root)
    
    original_children = root_path.get_children()
    second = original_children[1]
    
    # Insert a new node before the second child
    new_node = NumericNode(99)
    new_path = second.insert_before(new_node)
    
    # Verify tree structure
    assert [c.value for c in root.children] == [1, 99, 2, 3]
    
    # The original path (for value 2) should have its index updated
    assert second.index == 2
    
    # The returned NodePath should point to the newly inserted node at index 1
    assert new_path.index == 1
    assert new_path.node.value == 99


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    test_insert_before_updates_indices()
    test_insert_before_numeric_values()
    print("All tests passed!")