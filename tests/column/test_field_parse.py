"""Tests for Column.parse(): JSON, set, tuple, BaseModel, scalars, type from schema, and errors."""

import enum
import pytest
from pydantic import BaseModel

from ormantism.table import Table
from ormantism.column import Column
from ormantism import JSON


def test_parse_json_non_string_returns_value():
    """parse(JSON) with non-string returns value as-is (line 274)."""
    class T(Table):
        j: JSON

    info = T.model_fields["j"]
    f = Column.from_pydantic_info(T, "j", info)
    value = [1, 2, 3]
    assert f.parse(value) == value


def test_parse_json_non_string_branch():
    """Ensure JSON column parse hits 'if not isinstance(value, str): return value' (line 272)."""
    from ormantism.column import JSON as JSONType

    class T(Table):
        name: str = ""

    f = Column(
        table=T,
        name="j",
        base_type=JSONType,
        secondary_type=None,
        full_type=JSONType,
        default=None,
        is_required=False,
        column_is_required=False,
        is_reference=False,
    )
    # Non-string value on JSON column returns as-is
    assert f.parse({"already": "dict"}) == {"already": "dict"}
    assert f.parse([1, 2]) == [1, 2]


def test_parse_json_decode_error_returns_value():
    class T(Table):
        j: JSON

    info = T.model_fields["j"]
    f = Column.from_pydantic_info(T, "j", info)
    assert f.parse("not valid json {{{") == "not valid json {{{"


def test_parse_enum():
    """parse() for enum columns returns self.base_type[value] (line 272)."""
    class E(enum.Enum):
        A = "a"
        B = "b"

    class T(Table):
        e: E

    info = T.model_fields["e"]
    f = Column.from_pydantic_info(T, "e", info)
    assert f.parse("A") is E.A
    assert f.parse("B") is E.B


def test_parse_dict_and_list():
    """parse() for dict/list columns uses json.loads (line 281)."""
    class T(Table):
        data: dict = {}
        items: list[str] = []

    info = T.model_fields["data"]
    f = Column.from_pydantic_info(T, "data", info)
    assert f.parse('{"a": 1}') == {"a": 1}
    info = T.model_fields["items"]
    f = Column.from_pydantic_info(T, "items", info)
    assert f.parse('["x", "y"]') == ["x", "y"]


def test_parse_set():
    class T(Table):
        tags: set[str]

    info = T.model_fields["tags"]
    f = Column.from_pydantic_info(T, "tags", info)
    result = f.parse('["a", "b"]')
    assert result == {"a", "b"}


def test_parse_tuple():
    class T(Table):
        pair: tuple[int, int]

    info = T.model_fields["pair"]
    f = Column.from_pydantic_info(T, "pair", info)
    result = f.parse("[1, 2]")
    assert result == (1, 2)


def test_parse_base_model():
    class Nested(BaseModel):
        x: int = 0
        y: str = ""

    class T(Table):
        n: Nested

    info = T.model_fields["n"]
    f = Column.from_pydantic_info(T, "n", info)
    result = f.parse('{"x": 1, "y": "two"}')
    assert isinstance(result, Nested)
    assert result.x == 1
    assert result.y == "two"


def test_parse_scalars():
    class T(Table):
        name: str
        count: int
        ratio: float
        flag: bool

    for name, raw, expected in [
        ("name", "hello", "hello"),
        ("count", "42", 42),
        ("ratio", "3.14", 3.14),
        ("flag", "1", True),
    ]:
        info = T.model_fields[name]
        f = Column.from_pydantic_info(T, name, info)
        assert f.parse(raw) == expected


def test_parse_type_from_json_schema():
    class T(Table):
        kind: type

    info = T.model_fields["kind"]
    f = Column.from_pydantic_info(T, "kind", info)
    schema = {"type": "integer"}
    result = f.parse(schema)
    assert result is int


def test_parse_type_from_json_string():
    class T(Table):
        kind: type

    info = T.model_fields["kind"]
    f = Column.from_pydantic_info(T, "kind", info)
    result = f.parse('{"type": "string"}')
    assert result is str


def test_parse_type_rebuild_pydantic_model():
    class T(Table):
        model_cls: type[BaseModel] | None = None

    info = T.model_fields["model_cls"]
    f = Column.from_pydantic_info(T, "model_cls", info)
    schema = {
        "type": "object",
        "title": "AdHoc",
        "properties": {"x": {"type": "integer"}},
        "required": ["x"],
    }
    result = f.parse(schema)
    assert isinstance(result, type)
    assert issubclass(result, BaseModel)
    assert result.__name__ == "AdHoc"
    inst = result(x=1)
    assert inst.x == 1


def test_parse_type_raises_when_not_dict():
    class T(Table):
        kind: type

    info = T.model_fields["kind"]
    f = Column.from_pydantic_info(T, "kind", info)
    with pytest.raises(ValueError, match="Type representation should be stored"):
        f.parse(123)


def test_parse_unknown_type_raises():
    class Unknown:
        pass

    class T(Table):
        name: str

    f = Column(
        table=T,
        name="x",
        base_type=Unknown,
        secondary_type=None,
        full_type=Unknown,
        default=None,
        is_required=True,
        column_is_required=True,
        is_reference=False,
    )
    with pytest.raises(ValueError, match="Cannot parse value"):
        f.parse("something")
