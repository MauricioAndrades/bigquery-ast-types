"""
AST Collection - jQuery-like API for working with sets of AST nodes

Inspired by jscodeshift's Collection API, provides chainable methods
for finding, filtering, and transforming AST nodes.

Author: Little Bow Wow 🐕
Date: 2025-07-31
"""

from typing import List, Callable, Optional, Any, Union, TypeVar, Generic

try:
    # Try relative imports first (when used as module)
    from node_path import NodePath
    from visitor import BaseVisitor, visit
    from builders import ASTNode
except ImportError:
    # Fall back to absolute imports (when run directly)
    from node_path import NodePath
    from visitor import BaseVisitor, visit
    from builders import ASTNode

T = TypeVar("T", bound=ASTNode)


class Collection(Generic[T]):
    """
    A collection of NodePath objects with chainable API.
    Similar to jscodeshift's Collection.
    """

    def __init__(self, paths: List[NodePath]):
        self.paths = paths

    def __len__(self) -> int:
        return len(self.paths)

    def __iter__(self):
        return iter(self.paths)

    def __getitem__(self, index: int) -> NodePath:
        return self.paths[index]

    @property
    def length(self) -> int:
        """Alias for len()."""
        return len(self.paths)

    def size(self) -> int:
        """Number of paths in collection."""
        return len(self.paths)

    def at(self, index: int) -> Optional[NodePath]:
        """Get path at index."""
        if 0 <= index < len(self.paths):
            return self.paths[index]
        return None

    def get(self, index: int = 0) -> Optional[ASTNode]:
        """Get the node at index."""
        path = self.at(index)
        return path.node if path else None

    def nodes(self) -> List[ASTNode]:
        """Get all nodes."""
        return [path.node for path in self.paths]

    # Finding and Filtering

    def find(
        self, node_type: Union[str, type], predicate: Optional[Callable] = None
    ) -> "Collection":
        """
        Find all descendant nodes of the given type.

        Args:
            node_type: Either a string type name or a class
            predicate: Optional filter function
        """

        class FindVisitor(BaseVisitor):
            def __init__(self):
                self.found = []

            def visit(self, path):
                node = path.node
                type_match = False

                if isinstance(node_type, str):
                    type_match = type(node).__name__ == node_type
                else:
                    type_match = isinstance(node, node_type)

                if type_match:
                    if predicate is None or predicate(path):
                        self.found.append(path)

                self.generic_visit(path)

        visitor = FindVisitor()
        for path in self.paths:
            visit(path, visitor)

        return Collection(visitor.found)

    def filter(self, predicate: Callable[[NodePath], bool]) -> "Collection":
        """Filter paths by predicate."""
        return Collection([p for p in self.paths if predicate(p)])

    def filterType(self, node_type: Union[str, type]) -> "Collection":
        """Filter by node type."""
        if isinstance(node_type, str):
            return self.filter(lambda p: type(p.node).__name__ == node_type)
        else:
            return self.filter(lambda p: isinstance(p.node, node_type))

    def closest(
        self, node_type: Union[str, type], predicate: Optional[Callable] = None
    ) -> "Collection":
        """Find closest ancestor of given type."""
        results = []

        for path in self.paths:
            current = path.parent
            while current:
                type_match = False

                if isinstance(node_type, str):
                    type_match = type(current.node).__name__ == node_type
                else:
                    type_match = isinstance(current.node, node_type)

                if type_match:
                    if predicate is None or predicate(current):
                        results.append(current)
                        break

                current = current.parent

        return Collection(results)

    def parent(self) -> "Collection":
        """Get parent nodes."""
        parents = []
        seen = set()

        for path in self.paths:
            if path.parent and id(path.parent) not in seen:
                parents.append(path.parent)
                seen.add(id(path.parent))

        return Collection(parents)

    def children(self) -> "Collection":
        """Get all child nodes."""
        children = []
        for path in self.paths:
            children.extend(path.get_children())
        return Collection(children)

    # Traversal

    def forEach(self, callback: Callable[[NodePath, int], Any]) -> "Collection":
        """Execute callback for each path."""
        for i, path in enumerate(self.paths):
            callback(path, i)
        return self

    def map(self, callback: Callable[[NodePath, int], Any]) -> List[Any]:
        """Map over paths."""
        return [callback(path, i) for i, path in enumerate(self.paths)]

    def some(self, predicate: Callable[[NodePath], bool]) -> bool:
        """Check if any path matches predicate."""
        return any(predicate(path) for path in self.paths)

    def every(self, predicate: Callable[[NodePath], bool]) -> bool:
        """Check if all paths match predicate."""
        return all(predicate(path) for path in self.paths)

    # Manipulation

    def replaceWith(
        self, replacement: Union[ASTNode, Callable[[NodePath, int], ASTNode]]
    ) -> "Collection":
        """Replace all nodes."""
        for i, path in enumerate(self.paths):
            if callable(replacement):
                new_node = replacement(path, i)
            else:
                new_node = replacement
            path.replace(new_node)
        return self

    def remove(self) -> "Collection":
        """Remove all nodes."""
        # Remove in reverse order to avoid index issues
        for path in reversed(self.paths):
            path.remove()
        return Collection([])

    def insertBefore(self, node: Union[ASTNode, Callable[[NodePath, int], ASTNode]]) -> "Collection":
        """
        Insert node before each path in the collection.
        
        Args:
            node: Either an ASTNode to insert, or a function that returns 
                  an ASTNode based on the current path and index.
                  
        Returns:
            Collection of newly inserted NodePaths.
            
        Raises:
            ValueError: If any node cannot be inserted (e.g., root nodes or non-list nodes).
        """
        new_paths = []
        
        # Process in reverse order to avoid index shifting issues
        for i, path in enumerate(reversed(self.paths)):
            if callable(node):
                # Generate node based on current path
                new_node = node(path, len(self.paths) - 1 - i)
            else:
                # Use the same node (Note: this could be problematic if the same
                # node instance is inserted multiple times, consider cloning)
                new_node = node
                
            try:
                new_path = path.insert_before(new_node)
                new_paths.append(new_path)
            except ValueError as e:
                # Could either skip or raise, depending on desired behavior
                raise ValueError(f"Cannot insert before {path}: {e}")
                
        # Return in original order
        return Collection(list(reversed(new_paths)))

    def insertAfter(self, node: Union[ASTNode, Callable[[NodePath, int], ASTNode]]) -> "Collection":
        """
        Insert node after each path in the collection.
        
        Args:
            node: Either an ASTNode to insert, or a function that returns 
                  an ASTNode based on the current path and index.
                  
        Returns:
            Collection of newly inserted NodePaths.
            
        Raises:
            ValueError: If any node cannot be inserted (e.g., root nodes or non-list nodes).
        """
        new_paths = []
        
        # Process in reverse order to avoid index shifting issues
        for i, path in enumerate(reversed(self.paths)):
            if callable(node):
                # Generate node based on current path
                new_node = node(path, len(self.paths) - 1 - i)
            else:
                # Use the same node (Note: this could be problematic if the same
                # node instance is inserted multiple times, consider cloning)
                new_node = node
                
            try:
                new_path = path.insert_after(new_node)
                new_paths.append(new_path)
            except ValueError as e:
                # Could either skip or raise, depending on desired behavior
                raise ValueError(f"Cannot insert after {path}: {e}")
                
        # Return in original order
        return Collection(list(reversed(new_paths)))

    # Inspection

    def isEmpty(self) -> bool:
        """Check if collection is empty."""
        return len(self.paths) == 0

    def isNotEmpty(self) -> bool:
        """Check if collection has items."""
        return len(self.paths) > 0

    def hasClass(self, node_type: type) -> bool:
        """Check if any node is of given type."""
        return self.some(lambda p: isinstance(p.node, node_type))

    def getTypes(self) -> List[str]:
        """Get unique node types in collection."""
        types = set()
        for path in self.paths:
            types.add(type(path.node).__name__)
        return sorted(list(types))

    # Chaining helpers

    def first(self) -> "Collection":
        """Get first item as collection."""
        if self.paths:
            return Collection([self.paths[0]])
        return Collection([])

    def last(self) -> "Collection":
        """Get last item as collection."""
        if self.paths:
            return Collection([self.paths[-1]])
        return Collection([])

    def slice(self, start: int, end: Optional[int] = None) -> "Collection":
        """Slice the collection."""
        if end is None:
            return Collection(self.paths[start:])
        return Collection(self.paths[start:end])

    def eq(self, index: int) -> "Collection":
        """Get item at index as collection."""
        if 0 <= index < len(self.paths):
            return Collection([self.paths[index]])
        return Collection([])

    # Utilities

    def toArray(self) -> List[NodePath]:
        """Convert to list of paths."""
        return list(self.paths)

    def unique(self) -> "Collection":
        """Remove duplicate paths."""
        seen = set()
        unique_paths = []

        for path in self.paths:
            path_id = id(path.node)
            if path_id not in seen:
                seen.add(path_id)
                unique_paths.append(path)

        return Collection(unique_paths)

    def reverse(self) -> "Collection":
        """Reverse the collection."""
        return Collection(list(reversed(self.paths)))

    def sortBy(self, key_func: Callable[[NodePath], Any]) -> "Collection":
        """Sort collection by key function."""
        return Collection(sorted(self.paths, key=key_func))

    # Path utilities

    def getPath(self, field_path: str) -> "Collection":
        """
        Get nodes at a specific field path.
        E.g., 'body.statements.0' to get first statement in body
        """
        results = []

        for path in self.paths:
            current = path
            for part in field_path.split("."):
                if part.isdigit():
                    # Array index
                    children = current.get_children()
                    idx = int(part)
                    if 0 <= idx < len(children):
                        current = children[idx]
                    else:
                        current = None
                        break
                else:
                    # Field name
                    found = False
                    for child in current.get_children():
                        if child.field == part:
                            current = child
                            found = True
                            break
                    if not found:
                        current = None
                        break

            if current:
                results.append(current)

        return Collection(results)

    def getProp(self, prop_name: str) -> List[Any]:
        """Get property values from all nodes."""
        values = []
        for path in self.paths:
            if hasattr(path.node, prop_name):
                values.append(getattr(path.node, prop_name))
        return values


# Factory function
def astCollection(root: Union[ASTNode, NodePath, List[NodePath]]) -> Collection:
    """
    Create a collection from various inputs.

    Args:
        root: An AST node, NodePath, or list of NodePaths

    Returns:
        Collection instance
    """
    if isinstance(root, list):
        return Collection(root)
    elif isinstance(root, NodePath):
        return Collection([root])
    elif isinstance(root, ASTNode):
        return Collection([NodePath(root)])
    else:
        raise TypeError(f"Cannot create collection from {type(root)}")


# Aliases for common operations
def root(root_node: Union[ASTNode, NodePath, List[NodePath]]) -> Collection:
    """jQuery-style alias for astCollection with the provided root node.
    Args:
        root_node: The ASTNode, NodePath, or list of NodePaths to wrap in a Collection.
    Returns:
        Collection instance wrapping the root node(s).
    """
    return astCollection(root_node)
