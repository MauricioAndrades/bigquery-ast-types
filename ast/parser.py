from zetasql import analyzer

def parse_sql(sql: str):
    """
    Parse a SQL string into a ZetaSQL AST (ResolvedStatement).
    """
    return analyzer.parse_statement(sql)