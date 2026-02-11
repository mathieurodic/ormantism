"""Tests for Column.serialize() and Column hashability."""

import pytest
from pydantic import BaseModel

from ormantism.table import Table
from ormantism.column import Column


def test_field_hash():
    class T(Table):
        name: str

    info = T.model_fields["name"]
    f = Column.from_pydantic_info(T, "name", info)
    s = {f}
    assert f in s
    d = {f: 1}
    assert d[f] == 1


def test_column_eq_not_implemented_for_non_column():
    """Column.__eq__ returns NotImplemented when other is not a Column (line 242)."""
    class T(Table):
        name: str

    info = T.model_fields["name"]
    f = Column.from_pydantic_info(T, "name", info)
    assert f.__eq__(42) is NotImplemented
    assert f.__eq__("name") is NotImplemented


def test_serialize_list_of_references(setup_db):
    class B(Table, with_timestamps=True):
        value: int = 0

    class C(Table, with_timestamps=True):
        items: list[B] = []

    info = C.model_fields["items"]
    f = Column.from_pydantic_info(C, "items", info)
    b1 = B()
    b2 = B()
    result = f.serialize([b1, b2])
    assert result == [b1.id, b2.id]


def test_serialize_scalar_uses_utils_serialize():
    """Column.serialize for non-ref/non-JSON/non-type uses utils.serialize (line 272)."""
    class T(Table):
        name: str
        count: int = 0

    for name in ("name", "count"):
        info = T.model_fields[name]
        f = Column.from_pydantic_info(T, name, info)
        assert f.serialize("hello" if name == "name" else 42) == ("hello" if name == "name" else 42)

def test_serialize_type():
    class T(Table):
        kind: type

    info = T.model_fields["kind"]
    f = Column.from_pydantic_info(T, "kind", info)
    result = f.serialize(int)
    assert isinstance(result, dict)
    assert result.get("type") == "integer"


def test_serialize_exception_propagates():
    class B(Table):
        value: int = 0

    class C(Table):
        ref: B | None = None

    info = C.model_fields["ref"]
    f = Column.from_pydantic_info(C, "ref", info)
    with pytest.raises(AttributeError):
        f.serialize("not a B")
