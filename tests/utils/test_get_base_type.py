"""Tests for ormantism.utils.get_base_type and get_container_base_type."""

from types import GenericAlias
from typing import Optional
import pytest

from ormantism.utils.get_base_type import get_base_type, get_container_base_type


def test_get_base_type_union_with_none():
    t, args, optional = get_base_type(Optional[int])
    assert t is int
    assert optional is False


def test_get_base_type_type_union():
    t, args, optional = get_base_type(type | GenericAlias)
    assert t is type
    assert args == ()
    assert optional is False
    t2, args2, opt2 = get_base_type(type | None)
    assert t2 is type
    assert opt2 is False
    t3, args3, opt3 = get_base_type(type | GenericAlias | None)
    assert t3 is type
    assert opt3 is False


def test_get_base_type_union_without_none_raises():
    with pytest.raises(TypeError, match="union with None only"):
        get_base_type(int | str)
