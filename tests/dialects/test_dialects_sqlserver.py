"""Tests for ormantism.dialects.sqlserver: F.concat, F.escape_for_like, connect."""

import pytest

from ormantism.dialects import SqlserverDialect
from ormantism.expressions import FunctionExpression


def test_sqlserver_f_concat_returns_function_expression():
    d = SqlserverDialect()
    expr = d.f.concat("a", "b")
    assert isinstance(expr, FunctionExpression)
    assert expr.symbol == "CONCAT"
    assert expr.arguments == ("a", "b")


def test_sqlserver_f_escape_for_like():
    d = SqlserverDialect()
    assert d.f.escape_for_like("x%") == "x\\%"


def test_sqlserver_connect(monkeypatch):
    """connect() builds conn str and calls pyodbc.connect; mock pyodbc so body is covered."""
    import sys
    from unittest.mock import MagicMock

    mock_pyodbc = MagicMock()
    monkeypatch.setitem(sys.modules, "pyodbc", mock_pyodbc)
    d = SqlserverDialect()
    d.connect("sqlserver://localhost/db")
    mock_pyodbc.connect.assert_called_once()
    conn_str = mock_pyodbc.connect.call_args[0][0]
    assert "SERVER=localhost" in conn_str
    assert "DATABASE=db" in conn_str


def test_sqlserver_connect_with_port(monkeypatch):
    import sys
    from unittest.mock import MagicMock

    mock_pyodbc = MagicMock()
    monkeypatch.setitem(sys.modules, "pyodbc", mock_pyodbc)
    d = SqlserverDialect()
    d.connect("sqlserver://host:9999/mydb")
    conn_str = mock_pyodbc.connect.call_args[0][0]
    assert "host,9999" in conn_str or "SERVER=host,9999" in conn_str
