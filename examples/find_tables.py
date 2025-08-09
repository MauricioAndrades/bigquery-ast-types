from ..sqlglot.sqlglot import parse
from lib.visitor import BaseVisitor, visit

class FindTablesVisitor(BaseVisitor):
    def __init__(self):
        self.tables = []

    def visit_ResolvedTableScan(self, path):
        # Assume the .table_name attribute exists (adjust as necessary)
        self.tables.append(path.node.table_name)
        self.generic_visit(path)


if __name__ == "__main__":
    sql = "SELECT * FROM my_dataset.my_table"
    ast = parse(sql)
    visitor = FindTablesVisitor()
    visit(ast, visitor)
    print("Tables found:", visitor.tables)
