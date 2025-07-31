class NodePath:
    """
    A wrapper for a ZetaSQL AST node, tracking ancestry and location.
    """
    def __init__(self, node, parent=None, field=None, index=None, scope=None):
        self.node = node
        self.parent = parent
        self.field = field
        self.index = index
        self.scope = scope

    def get_children(self):
        # Return child NodePath objects for all child AST nodes
        children = []
        for field_name in dir(self.node):
            value = getattr(self.node, field_name)
            # Heuristic: skip private and built-in attributes
            if field_name.startswith("_"):
                continue
            if isinstance(value, list):
                for idx, item in enumerate(value):
                    if hasattr(item, "__class__") and "Resolved" in item.__class__.__name__:
                        children.append(NodePath(item, self, field_name, idx))
            elif hasattr(value, "__class__") and "Resolved" in value.__class__.__name__:
                children.append(NodePath(value, self, field_name, None))
        return children

    def replace(self, new_node):
        # Replace this node in the parent with new_node
        if self.parent is None:
            raise Exception("Cannot replace root node")
        if self.index is not None:
            getattr(self.parent.node, self.field)[self.index] = new_node
        else:
            setattr(self.parent.node, self.field, new_node)
        self.node = new_node

    def remove(self):
        # Remove this node from its parent (if it's in a list)
        if self.parent is None or self.index is None:
            raise Exception("Can only remove nodes from lists in parent")
        del getattr(self.parent.node, self.field)[self.index]

    def __repr__(self):
        return f"<NodePath node={type(self.node).__name__} field={self.field} index={self.index}>"