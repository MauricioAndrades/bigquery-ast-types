import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from parsers.sqlglot import SQLGlotParser
from lib.types import (
    Select,
    GroupByClause,
    HavingClause,
    OrderByClause,
    LimitClause,
)


def test_select_clause_parsing():
    sql = (
        "WITH cte AS (SELECT id FROM users) "
        "SELECT id, COUNT(*) AS c FROM cte WHERE active = TRUE "
        "GROUP BY id HAVING COUNT(*) > 1 ORDER BY id DESC LIMIT 10 OFFSET 5"
    )
    parser = SQLGlotParser()
    ast = parser.parse(sql)
    assert isinstance(ast, Select)
    assert ast.with_clause is not None
    assert isinstance(ast.group_by_clause, GroupByClause)
    assert isinstance(ast.having_clause, HavingClause)
    assert isinstance(ast.order_by_clause, OrderByClause)
    assert isinstance(ast.limit_clause, LimitClause)

