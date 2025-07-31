from zsql_ast_transformer import parse_sql

def test_parse_sql():
    sql = "SELECT 1"
    ast = parse_sql(sql)
    assert ast is not None