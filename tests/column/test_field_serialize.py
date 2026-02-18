"""Tests for Column.serialize() and Column hashability."""

import pytest
from pydantic import BaseModel

from ormantism.table import Table
from ormantism.column import Column
from ormantism import JSON


def test_column_hash():
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


def test_serialize_reference_scalar_none():
    class B(Table):
        value: int = 0

    class C(Table):
        ref: B | None = None

    col = Column.from_pydantic_info(C, "ref", C.model_fields["ref"])
    assert col.serialize(None) is None


def test_serialize_reference_scalar(setup_db):
    class B(Table, with_timestamps=True):
        value: int = 0

    class C(Table, with_timestamps=True):
        ref: B | None = None

    b = B()
    col = Column.from_pydantic_info(C, "ref", C.model_fields["ref"])
    assert col.serialize(b) == b.id


def test_serialize_reference_list_empty():
    class B(Table):
        value: int = 0

    class C(Table):
        items: list[B] = []

    col = Column.from_pydantic_info(C, "items", C.model_fields["items"])
    assert col.serialize([]) == []


def test_serialize_polymorphic_reference_scalar(setup_db):
    class B(Table, with_timestamps=True):
        value: int = 0

    class Poly(Table, with_timestamps=True):
        target: Table | None = None

    b = B()
    col = Column.from_pydantic_info(Poly, "target", Poly.model_fields["target"])
    result = col.serialize(b)
    assert result == {"table": B._get_table_name(), "id": b.id}


def test_serialize_json():
    class T(Table):
        j: JSON

    col = Column.from_pydantic_info(T, "j", T.model_fields["j"])
    value = {"a": 1, "b": [2, 3]}
    import json
    assert json.loads(col.serialize(value)) == value


def test_serialize_for_filtering_param():
    class T(Table):
        name: str

    col = Column.from_pydantic_info(T, "name", T.model_fields["name"])
    assert col.serialize("hello", for_filtering=True) == "hello"
