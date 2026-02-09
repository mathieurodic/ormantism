"""Tests for ormantism.utils modules."""

import enum
from typing import Optional, Union, List, Dict, ForwardRef
import pytest
from pydantic import BaseModel

from ormantism.utils.find_subclass import find_subclass, _get_subclasses
from ormantism.utils.is_type_annotation import is_type_annotation
from ormantism.utils.make_hashable import make_hashable
from ormantism.utils.serialize import serialize
from ormantism.utils.resolve_type import resolve_type
from ormantism.utils.get_base_type import get_base_type, get_container_base_type


# --- find_subclass ---

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
    # Class __name__ is _ChildA1 (with underscore)
    assert find_subclass(_BaseA, "_ChildA1") is _ChildA1
    assert find_subclass(_BaseA, "_GrandChild") is _GrandChild


def test_find_subclass_raises_when_multiple_match():
    class _OtherChild(_BaseA):
        pass
    _OtherChild.__name__ = "_ChildA1"  # same name as _ChildA1
    with pytest.raises(ValueError, match="More than one subclass"):
        find_subclass(_BaseA, "_ChildA1")


# --- is_type_annotation ---

def test_is_type_annotation_bare_types():
    assert is_type_annotation(int) is True
    assert is_type_annotation(str) is True


def test_is_type_annotation_generic():
    assert is_type_annotation(list[int]) is True
    assert is_type_annotation(dict[str, int]) is True


def test_is_type_annotation_union():
    assert is_type_annotation(Optional[int]) is True
    assert is_type_annotation(Union[int, str]) is True


def test_is_type_annotation_rejects_string_in_union():
    assert is_type_annotation(Union[int, "not_a_type"]) is False


def test_is_type_annotation_optional_and_nested():
    """Cases inspired by the module's __main__ block."""
    assert is_type_annotation(Union[int, None]) is True
    assert is_type_annotation(Optional[list[str]]) is True
    assert is_type_annotation(Dict[str, list[int]]) is True
    assert is_type_annotation(Union[list[int], None]) is True


# --- make_hashable ---

def test_make_hashable_enum():
    class E(enum.Enum):
        A = 1
        B = 2
    assert make_hashable(E.A) == ("A", 1)


def test_make_hashable_pydantic_model():
    class M(BaseModel):
        x: int = 1
    assert make_hashable(M()) == (("x", 1),)


def test_make_hashable_dict_list_tuple():
    assert make_hashable({"a": 1, "b": 2}) == (("a", 1), ("b", 2))
    assert make_hashable([1, 2]) == (1, 2)
    assert make_hashable((1, 2)) == (1, 2)


def test_make_hashable_scalars():
    assert make_hashable(1) == 1
    assert make_hashable("x") == "x"
    assert make_hashable(None) is None


def test_make_hashable_forward_ref():
    fr = ForwardRef("SomeTable")
    assert make_hashable(fr) == "SomeTable"


def test_make_hashable_union_origin():
    # Union types are handled by the "classes" branch (get_origin is truthy) and returned as-is
    u = Union[int, str]
    result = make_hashable(u)
    assert result is u or result == u.__args__
    hash(result)  # must be hashable


def test_make_hashable_raises_for_unknown():
    with pytest.raises(ValueError, match="Cannot hash"):
        make_hashable(object())


def test_make_hashable_nested_pydantic_model_hashable():
    """Nested Pydantic model (from make_hashable.py __main__) is hashable."""
    from pydantic import Field

    class SubTest(BaseModel):
        bar: dict = {"a": 1, "b": 2, "c": {"d": 4, "e": 5}}

    class Test(BaseModel):
        foo: int = 42
        sub: SubTest = Field(default_factory=SubTest)

    test = Test()
    # __main__ does hash(make_hashable(test)); must not raise
    h = make_hashable(test)
    assert hash(h) == hash(h)


# --- serialize ---

def test_serialize_base_model():
    class M(BaseModel):
        a: int = 1
    assert serialize(M()) == {"a": 1}


def test_serialize_dict_list():
    assert serialize({"x": 1}) == {"x": 1}
    assert serialize([1, 2]) == [1, 2]


def test_serialize_enum():
    class E(enum.Enum):
        X = "x"
    assert serialize(E.X) == "X"


def test_serialize_datetime():
    import datetime
    d = datetime.datetime(2025, 1, 1, 12, 0, 0)
    assert serialize(d) == str(d)


def test_serialize_raises_unknown():
    with pytest.raises(ValueError):
        serialize(object())


# --- resolve_type ---

def test_resolve_type_returns_non_forward_ref():
    assert resolve_type(int) is int
    assert resolve_type(str) is str


def test_resolve_type_raises_unresolved_forward_ref():
    ref = ForwardRef("NoSuchTable")
    with pytest.raises(ValueError, match="Could not resolve"):
        resolve_type(ref)


# --- get_base_type ---

def test_get_base_type_union_with_none():
    t, args, optional = get_base_type(Optional[int])
    assert t is int
    assert optional is False


def test_get_base_type_type_union():
    """Cover type | GenericAlias / type | None branch in get_base_type."""
    from types import GenericAlias
    t, args, optional = get_base_type(type | GenericAlias)
    assert t is type
    # type | None: args = (type, type(None)), so None in args is False
    t2, args2, opt2 = get_base_type(type | None)
    assert t2 is type
    assert opt2 is False
