"""Tests for ormantism.utils.resolve_type (ForwardRef to Table subclass)."""

from typing import ForwardRef
import pytest

from ormantism.utils.resolve_type import resolve_type


def test_resolve_type_returns_non_forward_ref():
    assert resolve_type(int) is int
    assert resolve_type(str) is str


def test_resolve_type_returns_evaluated_forward_ref():
    ref = ForwardRef("Anything")
    ref.__forward_evaluated__ = True
    ref.__forward_value__ = str
    assert resolve_type(ref) is str


def test_resolve_type_resolves_forward_ref_by_table_name():
    from ormantism import Table

    class _ResolveHelper(Table):
        name: str = ""

    ref = ForwardRef("_ResolveHelper")
    assert resolve_type(ref) is _ResolveHelper
    ref2 = ForwardRef("_resolvehelper")
    assert resolve_type(ref2) is _ResolveHelper


def test_resolve_type_raises_unresolved_forward_ref():
    ref = ForwardRef("NoSuchTable")
    with pytest.raises(ValueError, match="Could not resolve"):
        resolve_type(ref)
