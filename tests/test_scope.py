from zsql_ast_transformer.scope import Scope


def test_scope_lookup():
    global_scope = Scope()
    global_scope.declare("table1", "Table1Object")
    child_scope = Scope(parent=global_scope)
    child_scope.declare("col1", "Column1Object")

    assert child_scope.lookup("col1") == "Column1Object"
    assert child_scope.lookup("table1") == "Table1Object"
    assert global_scope.lookup("col1") is None
    assert global_scope.is_global()
    assert not child_scope.is_global()
