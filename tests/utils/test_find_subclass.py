"""Tests for ormantism.utils.find_subclass (find_subclass, _get_subclasses)."""

import pytest

from ormantism.utils.find_subclass import find_subclass, _get_subclasses


class _BaseA:
    pass


class _ChildA1(_BaseA):
    pass


class _ChildA2(_BaseA):
    pass


class _GrandChild(_ChildA1):
    pass


def test_find_subclass_returns_none_when_no_match():
    assert find_subclass(_BaseA, "NonExistent") is None


def test_find_subclass_returns_unique_subclass():
    assert find_subclass(_BaseA, "_ChildA1") is _ChildA1
    assert find_subclass(_BaseA, "_GrandChild") is _GrandChild


def test_find_subclass_raises_when_multiple_match():
    class _OtherChild(_BaseA):
        pass
    _OtherChild.__name__ = "_ChildA1"
    with pytest.raises(ValueError, match="More than one subclass"):
        find_subclass(_BaseA, "_ChildA1")
