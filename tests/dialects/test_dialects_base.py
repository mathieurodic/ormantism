"""Tests for ormantism.dialects.base: Dialect, _DialectF."""

import pytest

from ormantism.dialects.base import Dialect, _DialectF


def test_dialect_f_getattr_returns_callable():
    """dialect.f.concat etc. returns the F entry."""
    class D(Dialect):
        SUPPORTED_SCHEMA = ()
        F = {"concat": lambda *a: ("concat", a)}

        def connect(self, url):
            pass

    d = D()
    assert callable(d.f.concat)
    assert d.f.concat("a", "b") == ("concat", ("a", "b"))


def test_dialect_f_getattr_unknown_raises():
    """dialect.f.unknown raises AttributeError."""
    class D(Dialect):
        SUPPORTED_SCHEMA = ()
        F = {}

        def connect(self, url):
            pass

    d = D()
    with pytest.raises(AttributeError, match="unknown"):
        _ = d.f.unknown
