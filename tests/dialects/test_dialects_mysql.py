"""Tests for ormantism.dialects.mysql: F.concat, F.escape_for_like, connect."""

import pytest

from ormantism.dialects import MysqlDialect
from ormantism.expressions import FunctionExpression


def test_mysql_f_concat_returns_function_expression():
    d = MysqlDialect()
    expr = d.f.concat("a", "b")
    assert isinstance(expr, FunctionExpression)
    assert expr.symbol == "CONCAT"
    assert expr.arguments == ("a", "b")
    assert expr.sql == "CONCAT(?, ?)"
    assert expr.values == ("a", "b")


def test_mysql_f_escape_for_like():
    d = MysqlDialect()
    assert d.f.escape_for_like("%x_") == "\\%x\\_"


def test_mysql_connect(monkeypatch):
    """connect() runs and uses parsed URL; mock pymysql so body is covered without real DB."""
    import sys
    from unittest.mock import MagicMock

    mock_pymysql = MagicMock()
    monkeypatch.setitem(sys.modules, "pymysql", mock_pymysql)
    d = MysqlDialect()
    d.connect("mysql://user:pass@localhost/mydb")
    mock_pymysql.connect.assert_called_once()
    call_kw = mock_pymysql.connect.call_args[1]
    assert call_kw["host"] == "localhost"
    assert call_kw["user"] == "user"
    assert call_kw["password"] == "pass"
    assert call_kw["database"] == "mydb"
