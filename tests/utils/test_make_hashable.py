"""Tests for ormantism.utils.make_hashable."""

import enum
import pytest
from pydantic import BaseModel, Field

from ormantism.utils.make_hashable import make_hashable


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
    from typing import ForwardRef
    fr = ForwardRef("SomeTable")
    assert make_hashable(fr) == "SomeTable"


def test_make_hashable_union_origin():
    from typing import Union
    u = Union[int, str]
    result = make_hashable(u)
    assert result is u or result == u.__args__
    hash(result)


def test_make_hashable_raises_for_unknown():
    with pytest.raises(ValueError, match="Cannot hash"):
        make_hashable(object())


def test_make_hashable_raises_for_unhashable_type():
    with pytest.raises(ValueError, match="Cannot hash"):
        make_hashable(b"bytes")


def test_make_hashable_nested_pydantic_model_hashable():
    class SubTest(BaseModel):
        bar: dict = {"a": 1, "b": 2, "c": {"d": 4, "e": 5}}

    class Test(BaseModel):
        foo: int = 42
        sub: SubTest = Field(default_factory=SubTest)

    test = Test()
    h = make_hashable(test)
    assert hash(h) == hash(h)
