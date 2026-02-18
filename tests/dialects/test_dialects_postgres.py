"""Tests for ormantism.dialects.postgres: F.concat, F.escape_for_like, connect."""

import pytest

from ormantism.dialects import PostgresDialect
from ormantism.expressions import NaryOperatorExpression


def test_postgres_f_concat_returns_nary_operator():
    d = PostgresDialect()
    expr = d.f.concat("a", "b")
    assert isinstance(expr, NaryOperatorExpression)
    assert expr.symbol == "||"
    assert expr.sql == "(? || ?)"


def test_postgres_f_escape_for_like():
    d = PostgresDialect()
    assert d.f.escape_for_like("_%") == "\\_\\%"


def test_postgres_connect(monkeypatch):
    """connect() runs; mock psycopg2 so body is covered without real DB."""
    import sys
    from unittest.mock import MagicMock

    mock_psycopg2 = MagicMock()
    monkeypatch.setitem(sys.modules, "psycopg2", mock_psycopg2)
    d = PostgresDialect()
    d.connect("postgresql://user:pass@localhost/mydb")
    mock_psycopg2.connect.assert_called_once()
    call_kw = mock_psycopg2.connect.call_args[1]
    assert call_kw["host"] == "localhost"
    assert call_kw["user"] == "user"
    assert call_kw["password"] == "pass"
    assert call_kw["database"] == "mydb"
