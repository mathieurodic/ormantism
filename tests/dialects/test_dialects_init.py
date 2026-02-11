"""Tests for ormantism.dialects: get_dialect_for_scheme and supported schemes."""

import pytest

from ormantism.dialects import (
    get_dialect_for_scheme,
    SqliteDialect,
    MysqlDialect,
    PostgresDialect,
    SqlserverDialect,
)


def test_get_dialect_for_scheme_sqlite():
    d = get_dialect_for_scheme("sqlite")
    assert isinstance(d, SqliteDialect)


def test_get_dialect_for_scheme_normalizes_and_lowercases():
    d = get_dialect_for_scheme("SQLITE")
    assert isinstance(d, SqliteDialect)
    d = get_dialect_for_scheme("postgresql+psycopg2")
    assert isinstance(d, PostgresDialect)


def test_get_dialect_for_scheme_mysql():
    d = get_dialect_for_scheme("mysql")
    assert isinstance(d, MysqlDialect)


def test_get_dialect_for_scheme_postgresql():
    d = get_dialect_for_scheme("postgresql")
    assert isinstance(d, PostgresDialect)


def test_get_dialect_for_scheme_mssql():
    d = get_dialect_for_scheme("mssql")
    assert isinstance(d, SqlserverDialect)


def test_get_dialect_for_scheme_sqlserver():
    d = get_dialect_for_scheme("sqlserver")
    assert isinstance(d, SqlserverDialect)


def test_get_dialect_for_scheme_unsupported_raises():
    with pytest.raises(ValueError, match="Unsupported database scheme"):
        get_dialect_for_scheme("nosuch")
    with pytest.raises(ValueError, match="Unsupported database scheme"):
        get_dialect_for_scheme("oracle")


def test_get_dialect_for_scheme_empty_raises():
    with pytest.raises(ValueError, match="Unsupported database scheme"):
        get_dialect_for_scheme("")


def test_get_dialect_for_scheme_none_raises():
    with pytest.raises(ValueError, match="Unsupported database scheme"):
        get_dialect_for_scheme(None)
