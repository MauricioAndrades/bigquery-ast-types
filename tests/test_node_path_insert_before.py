"""Tests for NodePath.insert_before behavior"""

from dataclasses import dataclass, field
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


types_stub.ASTNode = ASTNode
sys.modules.setdefault("lib.types", types_stub)

NodePath = importlib.import_module("lib.node_path").NodePath


@dataclass
class Dummy(ASTNode):
    """Simple AST node used for NodePath tests."""

    value: int
    children: list[ASTNode] = field(default_factory=list)

    @property
    def node_type(self) -> str:  # pragma: no cover - simple attribute
        return self.__class__.__name__


def test_insert_before_updates_indices():
    """Inserting before a node should update indices of existing paths."""

    # Build a simple tree ``root -> [1, 2, 3]``
    root = Dummy(0, children=[Dummy(1), Dummy(2), Dummy(3)])
    root_path = NodePath(root)
    original_children = root_path.get_children()
    second = original_children[1]

    # Insert a new node before the second child
    new_node = Dummy(99)
    new_path = second.insert_before(new_node)

    # Verify tree structure
    assert [c.value for c in root.children] == [1, 99, 2, 3]

    # The original path (for value 2) should have its index updated
    assert second.index == 2

    # The returned NodePath should point to the newly inserted node at index 1
    assert new_path.index == 1


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    test_insert_before_updates_indices()
    print("all tests passed")
