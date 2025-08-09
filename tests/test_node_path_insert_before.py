from dataclasses import dataclass, field
from typing import List
import sys
import types
import os
import importlib

# Provide minimal stubs to import NodePath without full package dependencies
lib_module = types.ModuleType("lib")
lib_module.__path__ = [os.path.join(os.path.dirname(__file__), "..", "lib")]
sys.modules["lib"] = lib_module

types_module = types.ModuleType("lib.types")

class ASTNode:
    def __init__(self, *args, **kwargs):
        pass

types_module.ASTNode = ASTNode
sys.modules["lib.types"] = types_module

NodePath = importlib.import_module("lib.node_path").NodePath


@dataclass
class DummyNode(ASTNode):
    value: str
    children: List[ASTNode] = field(default_factory=list)

    def accept(self, visitor):
        pass


def test_insert_before_updates_indices():
    root = DummyNode("root", [DummyNode("a"), DummyNode("b"), DummyNode("c")])
    root_path = NodePath(root)
    children = root_path.get_children()

    first = children[0]
    second = children[1]
    third = children[2]

    new_node = DummyNode("x")
    new_path = second.insert_before(new_node)

    # Ensure returned path has correct index
    assert new_path.index == 1

    # Ensure existing nodes updated
    assert first.index == 0
    assert second.index == 2
    assert third.index == 3

    # Verify root children order and indices
    values = [c.node.value for c in root_path.get_children()]
    indices = [c.index for c in root_path.get_children()]
    assert values == ["a", "x", "b", "c"]
    assert indices == [0, 1, 2, 3]
