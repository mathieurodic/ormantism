"""Tests for ormantism.dialects.sqlite: F.concat, F.escape_for_like, connect."""

import pytest

from ormantism.dialects import SqliteDialect
from ormantism.expressions import NaryOperatorExpression


def test_sqlite_f_concat_returns_nary_operator():
    d = SqliteDialect()
    expr = d.f.concat("a", "b", "c")
    assert isinstance(expr, NaryOperatorExpression)
    assert expr.symbol == "||"
    assert expr.arguments == ("a", "b", "c")
    assert expr.sql == "(? || ? || ?)"
    assert expr.values == ("a", "b", "c")


def test_sqlite_f_escape_for_like():
    d = SqliteDialect()
    assert d.f.escape_for_like("hello") == "hello"
    assert d.f.escape_for_like("50%") == "50\\%"
    assert d.f.escape_for_like("a_b") == "a\\_b"
    assert d.f.escape_for_like("\\") == "\\\\"
    assert d.f.escape_for_like("x%_\\y") == "x\\%\\_\\\\y"


def test_sqlite_connect_creates_connection(tmp_path):
    d = SqliteDialect()
    url = f"sqlite:///{tmp_path / 'test.db'}"
    conn = d.connect(url)
    conn.execute("SELECT 1")
    conn.close()
