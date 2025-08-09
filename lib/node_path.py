"""
Enhanced NodePath for BigQuery AST

Provides path tracking with parent/child relationships,
field information, and scope tracking.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

from typing import Any, List, Optional, Dict, Union, Callable, Iterator
from dataclasses import dataclass, field as field_name

from .types import ASTNode
from .scope import Scope


@dataclass
class NodePath:
    """
    Enhanced wrapper for AST nodes with ancestry and context tracking.
    """

    node: ASTNode
    parent: Optional["NodePath"] = None
    field: Optional[str] = None  # Field name in parent
    index: Optional[int] = None  # Index if in a list
    scope: Optional[Scope] = None

    # Cache for performance
    _children: Optional[List["NodePath"]] = field_name(default=None, init=False, repr=False)
    _depth: Optional[int] = field_name(default=None, init=False, repr=False)
    _root: Optional["NodePath"] = field_name(default=None, init=False, repr=False)

    def __post_init__(self):
        """Initialize scope if not provided."""
        if self.scope is None and self.parent:
            self.scope = self.parent.scope
        elif self.scope is None:
            self.scope = Scope()

    @property
    def depth(self) -> int:
        """Get depth in tree (cached)."""
        if self._depth is None:
            self._depth = 0 if self.parent is None else self.parent.depth + 1
        return self._depth

    @property
    def root(self) -> "NodePath":
        """Get root of tree (cached)."""
        if self._root is None:
            self._root = self if self.parent is None else self.parent.root
        return self._root

    @property
    def ancestors(self) -> List["NodePath"]:
        """Get all ancestors from parent to root."""
        result = []
        current = self.parent
        while current:
            result.append(current)
            current = current.parent
        return result

    @property
    def path(self) -> str:
        """Get string representation of path from root."""
        parts = []
        current = self
        while current:
            if current.field:
                if current.index is not None:
                    parts.append(f"{current.field}[{current.index}]")
                else:
                    parts.append(current.field)
            current = current.parent
        return ".".join(reversed(parts)) if parts else "<root>"

    def get_children(self) -> List["NodePath"]:
        """Get all child NodePath objects (cached)."""
        if self._children is not None:
            return self._children

        children = []

        # Use vars() to discover children for both dataclasses and regular classes
        for field_name, value in vars(self.node).items():
            if field_name.startswith("_"):
                continue

            if value is None:
                continue

            # Handle lists
            if isinstance(value, list):
                for idx, item in enumerate(value):
                    if isinstance(item, ASTNode):
                        child_path = NodePath(
                            item,
                            parent=self,
                            field=field_name,
                            index=idx,
                            scope=self._get_child_scope(field_name, item),
                        )
                        children.append(child_path)
            # Handle single nodes
            elif isinstance(value, ASTNode):
                child_path = NodePath(
                    value,
                    parent=self,
                    field=field_name,
                    scope=self._get_child_scope(field_name, value),
                )
                children.append(child_path)

        self._children = children
        return children

    def _get_child_scope(self, field_name: str, child_node: ASTNode) -> Optional[Scope]:
        """Determine scope for child node."""
        # Create new scope for certain constructs
        if self._creates_new_scope(field_name, child_node):
            return Scope(parent=self.scope)
        return self.scope

    def _creates_new_scope(self, field_name: str, child_node: ASTNode) -> bool:
        """Check if this field/node creates a new scope."""
        # CTEs create new scope
        if field_name == "ctes" or child_node.node_type == "CTE":
            return True
        # Subqueries create new scope
        if child_node.node_type in ("SelectStatement", "SubqueryExpression"):
            return True
        # Functions with FROM clause create new scope
        if child_node.node_type == "TableFunction":
            return True
        return False

    def find_ancestor(
        self, predicate: Callable[["NodePath"], bool]
    ) -> Optional["NodePath"]:
        """Find first ancestor matching predicate."""
        current = self.parent
        while current:
            if predicate(current):
                return current
            current = current.parent
        return None

    def find_descendants(
        self, predicate: Callable[["NodePath"], bool]
    ) -> List["NodePath"]:
        """Find all descendants matching predicate."""
        results = []

        def visit(path: NodePath):
            if predicate(path):
                results.append(path)
            for child in path.get_children():
                visit(child)

        for child in self.get_children():
            visit(child)

        return results

    def replace(self, new_node: ASTNode) -> None:
        """Replace this node in the parent."""
        if self.parent is None:
            raise ValueError("Cannot replace root node")

        parent_node = self.parent.node

        if self.index is not None:
            # In a list
            if self.field is None:
                raise ValueError("Field name cannot be None when replacing a node in a list")
            getattr(parent_node, self.field)[self.index] = new_node
        else:
            # Single field
            if self.field is None:
                raise ValueError("Field name cannot be None when replacing a single field")
            setattr(parent_node, self.field, new_node)

        # Update our node reference
        self.node = new_node
        # Clear caches
        self._children = None
        if self.parent:
            self.parent._children = None

    def remove(self) -> None:
        """Remove this node from its parent."""
        if self.parent is None:
            raise ValueError("Cannot remove root node")

        if self.index is None:
            raise ValueError("Can only remove nodes from lists")

        parent_node = self.parent.node
        if self.field is None:
            raise ValueError("Field name cannot be None when removing a node from a list")
        getattr(parent_node, self.field).pop(self.index)

        # Update indices of siblings
        if self.parent._children:
            for child in self.parent._children:
                if (
                    child.field == self.field
                    and child.index is not None
                    and child.index > self.index
                ):
                    child.index -= 1

        # Clear parent's cache
        self.parent._children = None

    def insert_before(self, new_node: ASTNode) -> "NodePath":
        """Insert a node before this one in parent list."""
        if self.parent is None or self.index is None:
            raise ValueError("Can only insert before nodes in lists")

        parent_node = self.parent.node
        if self.field is None:
            raise ValueError("Field name cannot be None when inserting before a node in a list")
        insert_index = self.index
        getattr(parent_node, self.field).insert(insert_index, new_node)

        # Update indices of existing cached children using the original index
        if self.parent._children:
            for child in self.parent._children:
                if (
                    child.field == self.field
                    and child.index is not None
                    and child.index >= insert_index
                ):
                    child.index += 1

        # Update this path's index to reflect the insertion
        self.index = insert_index + 1

        # Clear parent's cache as structure changed
        self.parent._children = None

        # Return path for newly inserted node
        return NodePath(new_node, self.parent, self.field, insert_index, self.scope)

    def insert_after(self, new_node: ASTNode) -> "NodePath":
        """Insert a node after this one in parent list."""
        if self.parent is None or self.index is None:
            raise ValueError("Can only insert after nodes in lists")

        parent_node = self.parent.node
        getattr(parent_node, self.field).insert(self.index + 1, new_node)

        # Update indices of siblings
        if self.parent._children:
            for child in self.parent._children:
                if (
                    child.field == self.field
                    and child.index is not None
                    and child.index > self.index
                ):
                    child.index += 1

        # Clear parent's cache
        self.parent._children = None

        # Return path for new node
        return NodePath(new_node, self.parent, self.field, self.index + 1, self.scope)

    def siblings(self) -> List["NodePath"]:
        """Get sibling nodes (same parent and field)."""
        if not self.parent:
            return []

        return [
            child
            for child in self.parent.get_children()
            if child.field == self.field and child != self
        ]

    def is_first_child(self) -> bool:
        """Check if this is the first child in a list."""
        return self.index == 0 if self.index is not None else True

    def is_last_child(self) -> bool:
        """Check if this is the last child in a list."""
        if self.parent and self.index is not None:
            if self.field is None:
                raise ValueError("Field name cannot be None when checking last child in a list")
            parent_list = getattr(self.parent.node, self.field)
            return self.index == len(parent_list) - 1
        return True

    def walk(self) -> Iterator["NodePath"]:
        """Walk tree in pre-order traversal."""
        yield self
        for child in self.get_children():
            yield from child.walk()

    def __repr__(self) -> str:
        return f"<NodePath {self.node.node_type} at {self.path}>"

    def __eq__(self, other: Any) -> bool:
        """Paths are equal if they point to the same node instance."""
        return isinstance(other, NodePath) and self.node is other.node

    def __hash__(self) -> int:
        """Hash based on node identity."""
        return id(self.node)


# Helper functions
def create_path(node: ASTNode, scope: Optional[Scope] = None) -> NodePath:
    """Create a NodePath for an AST node."""
    return NodePath(node, scope=scope or Scope())


def get_node_at_path(root: NodePath, path: str) -> Optional[NodePath]:
    """Get node at string path like 'from_clause.tables.0.table'."""
    current = root

    for part in path.split("."):
        if "[" in part and "]" in part:
            # Handle array index
            field_name = part[: part.index("[")]
            index = int(part[part.index("[") + 1 : part.index("]")])

            found = False
            for child in current.get_children():
                if child.field == field_name and child.index == index:
                    current = child
                    found = True
                    break

            if not found:
                return None
        else:
            # Handle field name
            found = False
            for child in current.get_children():
                if child.field == part:
                    current = child
                    found = True
                    break

            if not found:
                return None

    return current
