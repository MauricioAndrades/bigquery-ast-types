from zsql_ast_transformer import parse_sql, NodePath, BaseVisitor, visit

class FindTablesVisitor(BaseVisitor):
    def __init__(self):
        self.tables = []

    def visit_ResolvedTableScan(self, path):
        # Assume the .table_name attribute exists (adjust as necessary)
        self.tables.append(path.node.table_name)
        self.generic_visit(path)

if __name__ == "__main__":
    sql = "SELECT * FROM my_dataset.my_table"
    ast = parse_sql(sql)
    path = NodePath(ast)
    visitor = FindTablesVisitor()
    visit(path, visitor)
    print("Tables found:", visitor.tables)