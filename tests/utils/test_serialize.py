"""Tests for ormantism.utils.serialize."""

import datetime
import enum
import pytest
from pydantic import BaseModel

from ormantism.utils.serialize import serialize


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
    d = datetime.datetime(2025, 1, 1, 12, 0, 0)
    assert serialize(d) == str(d)


def test_serialize_raises_unknown():
    with pytest.raises(ValueError):
        serialize(object())
