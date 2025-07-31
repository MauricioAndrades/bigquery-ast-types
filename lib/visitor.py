from .node_path import NodePath


class BaseVisitor:
    def visit(self, path):
        method = getattr(self, f"visit_{type(path.node).__name__}", None)
        if method:
            return method(path)
        return self.generic_visit(path)

    def generic_visit(self, path):
        for child in path.get_children():
            self.visit(child)


def visit(ast_root, visitor):
    """
    Walk the tree starting at ast_root (NodePath or AST node), calling visitor methods.
    """
    path = ast_root if isinstance(ast_root, NodePath) else NodePath(ast_root)
    visitor.visit(path)
