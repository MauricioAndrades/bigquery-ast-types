from zsql_ast_transformer import builders

def test_literal_builder():
    lit = builders.literal(100)
    assert hasattr(lit, "value")
    assert lit.value == 100

def test_table_scan_builder():
    scan = builders.table_scan("my_table")
    assert hasattr(scan, "table_name")
    assert scan.table_name == "my_table"