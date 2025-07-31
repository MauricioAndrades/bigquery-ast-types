from zsql_ast_transformer import parse_sql, NodePath, BaseVisitor, visit

def test_traversal():
    class CounterVisitor(BaseVisitor):
        def __init__(self):
            self.count = 0
        def visit_ResolvedLiteral(self, path):
            self.count += 1
            self.generic_visit(path)

    sql = "SELECT 1, 2, 3"
    ast = parse_sql(sql)
    visitor = CounterVisitor()
    path = NodePath(ast)
    visit(path, visitor)
    assert visitor.count == 3