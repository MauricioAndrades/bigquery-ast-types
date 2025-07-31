def is_scan(node):
    return "Scan" in node.__class__.__name__

def is_expression(node):
    return "Expr" in node.__class__.__name__ or "Expression" in node.__class__.__name__

def is_statement(node):
    return "Statement" in node.__class__.__name__ or "Stmt" in node.__class__.__name__