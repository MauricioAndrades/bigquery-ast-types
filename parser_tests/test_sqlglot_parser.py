import os
import sys

# Ensure project and bundled sqlglot are on path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sqlglot')))

from parsers.sqlglot import (
    parse,
    Select,
    Insert,
    Update,
    CreateTable,
    JoinType,
    Subquery,
    Case,
)


def test_insert_values():
    stmt = parse("INSERT INTO t (a) VALUES (1)")
    assert isinstance(stmt, Insert)
    assert stmt.table.table.table == "t"
    assert [c.name for c in stmt.columns] == ["a"]
    assert stmt.values[0][0].value == 1


def test_update_where():
    stmt = parse("UPDATE t SET a=1 WHERE b=2")
    assert isinstance(stmt, Update)
    assert "a" in stmt.assignments
    assert stmt.where is not None


def test_create_table():
    stmt = parse("CREATE TABLE t (a INT64)")
    assert isinstance(stmt, CreateTable)
    assert stmt.table.table == "t"


def test_select_join():
    stmt = parse("SELECT a FROM t JOIN u ON t.id = u.id")
    assert isinstance(stmt, Select)
    assert len(stmt.joins) == 1
    assert stmt.joins[0].join_type == JoinType.INNER
    assert stmt.joins[0].table.table.table == "u"


def test_subquery_expression():
    stmt = parse("SELECT (SELECT 1) AS x")
    expr = stmt.select_list[0].expression
    assert isinstance(expr, Subquery)
    assert isinstance(expr.select, Select)


def test_case_expression():
    stmt = parse("SELECT CASE WHEN a=1 THEN 2 ELSE 3 END")
    expr = stmt.select_list[0].expression
    assert isinstance(expr, Case)
    assert len(expr.whens) == 1
