# Placeholder: Add helpers for building new AST nodes as needed.
# You may need to adjust based on the actual ZetaSQL AST node constructors.

from zetasql import resolved_ast

def literal(value):
    """
    Build a ResolvedLiteral node.
    """
    return resolved_ast.ResolvedLiteral(value=value)

def table_scan(table_name):
    """
    Build a ResolvedTableScan node.
    """
    return resolved_ast.ResolvedTableScan(table_name=table_name)