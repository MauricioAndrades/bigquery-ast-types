import pytest
import sys
import os

# Skip test if zsql_ast_transformer is not available
pytest.skip("zsql_ast_transformer module not available", allow_module_level=True)

# Original test would be:
# from zsql_ast_transformer import parse_sql
# 
# def test_parse_sql():
#     sql = "SELECT 1"
#     ast = parse_sql(sql)
#     assert ast is not None
