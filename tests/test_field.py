import enum
import json
from typing import Optional

import pytest
from pydantic import BaseModel, Field as PydanticField

from ormantism.table import Table
from ormantism.column import Column
from ormantism import JSON


def test_reference_type_non_reference():
    """reference_type returns None for non-reference fields."""
    class T(Table):
        name: str

    info = T.model_fields["name"]
    f = Column.from_pydantic_info(T, "name", info)
    assert f.reference_type is None


def test_reference_type_reference():
    """reference_type returns the referenced Table type for reference fields."""
    class B(Table):
        value: int = 0

    class C(Table):
        ref: B | None = None

    info = C.model_fields["ref"]
    f = Column.from_pydantic_info(C, "ref", info)
    assert f.reference_type is B


def test_column_base_type_reference():
    """column_base_type returns int for reference fields."""
    class B(Table):
        value: int = 0

    class C(Table):
        ref: B = None

    info = C.model_fields["ref"]
    f = Column.from_pydantic_info(C, "ref", info)
    assert f.column_base_type is int


def test_column_base_type_non_reference():
    """column_base_type returns base_type for non-reference fields."""
    class T(Table):
        name: str

    info = T.model_fields["name"]
    f = Column.from_pydantic_info(T, "name", info)
    assert f.column_base_type is str


def test_from_pydantic_info_dict_two_secondary_types():
    """from_pydantic_info handles dict[str, X] (base_type dict, 2 secondary types)."""
    class T(Table):
        data: dict[str, int] | None = None

    info = T.model_fields["data"]
    f = Column.from_pydantic_info(T, "data", info)
    assert f.base_type is dict
    assert f.secondary_type is str  # line 94 overwrites to secondary_types[0]


def test_from_pydantic_info_unsupported_union_raises():
    """from_pydantic_info raises ValueError for unsupported secondary_types_count."""
    with pytest.raises(ValueError, match="secondary_types|base_type"):
        class T(Table):
            x: tuple[str, int, float] | None = None


def test_from_pydantic_info():

    class Thing(Table):
        pass
    
    class Agent(Table):
        birthed_by: Optional["Agent"]
        name: str
        description: str | None
        thing: Thing
        system_input: str
        bot_name: str
        tools: list[str]
        max_iterations: int = 10
        temperature: float = 0.3
        with_input_improvement: bool = True
        conversation: list[str] = PydanticField(default_factory=list)

    fields = {
        name: Column.from_pydantic_info(Agent, name, info)
        for name, info in Agent.model_fields.items()
    }

    assert fields["birthed_by"] == Column(table=Agent,
                                         name="birthed_by",
                                         base_type=Agent,
                                         secondary_type=None,
                                         full_type=Optional[Agent],
                                         default=None,
                                         is_required=False,
                                         column_is_required=False,
                                         is_reference=True)
    assert fields["name"] == Column(table=Agent,
                                   name="name",
                                   base_type=str,
                                   secondary_type=None,
                                   full_type=str,
                                   default=None, 
                                   is_required=True,
                                   column_is_required=True,
                                   is_reference=False)
    assert fields["description"] == Column(table=Agent,
                                          name="description",
                                          base_type=str,
                                          secondary_type=None,
                                          full_type=Optional[str],
                                          default=None, 
                                          is_required=False,
                                          column_is_required=False,
                                          is_reference=False)
    assert fields["thing"] == Column(table=Agent,
                                    name="thing",
                                    base_type=Thing,
                                    secondary_type=None,
                                    full_type=Thing,
                                    default=None,
                                    is_required=True,
                                    column_is_required=True,
                                    is_reference=True)
    assert fields["with_input_improvement"] == Column(table=Agent,
                                                     name="with_input_improvement",
                                                     base_type=bool,
                                                     secondary_type=None,
                                                     full_type=bool,
                                                     default=True,
                                                     is_required=False,
                                                     column_is_required=True,
                                                     is_reference=False)


def test_sql_creations_scalar_reference_to_table():
    """sql_creations yields _table and _id for polymorphic (base_type Table) reference."""
    class B(Table):
        value: int = 0

    class Poly(Table):
        target: Table | None = None

    info = Poly.model_fields["target"]
    f = Column.from_pydantic_info(Poly, "target", info)
    sqls = list(f.sql_creations)
    assert any("target_table" in s for s in sqls)
    assert any("target_id" in s for s in sqls)


def test_sql_creations_list_of_table_references():
    """sql_creations yields _tables and _ids for list[Table] (polymorphic list)."""
    class Poly(Table):
        items: list[Table] = []

    info = Poly.model_fields["items"]
    f = Column.from_pydantic_info(Poly, "items", info)
    assert f.base_type is list
    assert f.secondary_type is Table
    sqls = list(f.sql_creations)
    assert any("items_tables" in s for s in sqls)
    assert any("items_ids" in s for s in sqls)


def test_sql_creations_list_of_table_references_explicit_field():
    """Cover branch secondary_type == Table by building Column with list and Table."""
    class Poly(Table):
        name: str = ""

    f = Column(
        table=Poly,
        name="items",
        base_type=list,
        secondary_type=Table,
        full_type=list[Table],
        default=None,
        is_required=False,
        column_is_required=False,
        is_reference=True,
    )
    sqls = list(f.sql_creations)
    assert any("items_tables" in s for s in sqls)
    assert any("items_ids" in s for s in sqls)


def test_sql_creations_enum_column():
    """sql_creations yields TEXT CHECK(...) for Enum columns."""
    class E(enum.Enum):
        A = "a"
        B = "b"

    class T(Table):
        e: E

    info = T.model_fields["e"]
    f = Column.from_pydantic_info(T, "e", info)
    sqls = list(f.sql_creations)
    assert len(sqls) == 1
    assert "e TEXT" in sqls[0]
    assert "CHECK" in sqls[0]
    assert "A" in sqls[0] and "B" in sqls[0]


def test_sql_creations_base_model_column():
    """sql_creations yields JSON for BaseModel (non-Table) columns."""
    class Nested(BaseModel):
        x: int = 0

    class T(Table):
        n: Nested

    info = T.model_fields["n"]
    f = Column.from_pydantic_info(T, "n", info)
    sqls = list(f.sql_creations)
    assert len(sqls) == 1
    assert "n JSON" in sqls[0]


def test_sql_creations_json_column():
    """sql_creations yields JSON DEFAULT 'null' for JSON type."""
    class T(Table):
        j: JSON

    info = T.model_fields["j"]
    f = Column.from_pydantic_info(T, "j", info)
    sqls = list(f.sql_creations)
    assert len(sqls) == 1
    assert "j JSON" in sqls[0]
    assert "null" in sqls[0]


def test_field_hash():
    """Column is hashable (e.g. usable in set or as dict key)."""
    class T(Table):
        name: str

    info = T.model_fields["name"]
    f = Column.from_pydantic_info(T, "name", info)
    s = {f}
    assert f in s
    d = {f: 1}
    assert d[f] == 1


def test_serialize_list_of_references(setup_db):
    """serialize converts list of Table instances to list of IDs."""
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


def test_serialize_type():
    """serialize converts type to JSON schema (to_json_schema)."""
    class T(Table):
        kind: type

    info = T.model_fields["kind"]
    f = Column.from_pydantic_info(T, "kind", info)
    result = f.serialize(int)
    assert isinstance(result, dict)
    assert result.get("type") == "integer"


def test_serialize_exception_propagates():
    """serialize re-raises when conversion fails."""
    class B(Table):
        value: int = 0

    class C(Table):
        ref: B | None = None

    info = C.model_fields["ref"]
    f = Column.from_pydantic_info(C, "ref", info)
    with pytest.raises(AttributeError):
        f.serialize("not a B")  # has no .id


def test_parse_json_non_string_returns_value():
    """parse for JSON field returns value as-is when not a string."""
    class T(Table):
        j: JSON

    info = T.model_fields["j"]
    f = Column.from_pydantic_info(T, "j", info)
    value = [1, 2, 3]
    assert f.parse(value) == value


def test_parse_json_decode_error_returns_value():
    """parse for JSON field returns value as-is on JSONDecodeError."""
    class T(Table):
        j: JSON

    info = T.model_fields["j"]
    f = Column.from_pydantic_info(T, "j", info)
    assert f.parse("not valid json {{{") == "not valid json {{{"


def test_parse_set():
    """parse for set field deserializes JSON to set."""
    class T(Table):
        tags: set[str]

    info = T.model_fields["tags"]
    f = Column.from_pydantic_info(T, "tags", info)
    result = f.parse('["a", "b"]')
    assert result == {"a", "b"}


def test_parse_tuple():
    """parse for tuple field deserializes JSON to tuple."""
    class T(Table):
        pair: tuple[int, int]

    info = T.model_fields["pair"]
    f = Column.from_pydantic_info(T, "pair", info)
    result = f.parse("[1, 2]")
    assert result == (1, 2)


def test_parse_base_model():
    """parse for BaseModel field deserializes JSON to model instance."""
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
    """parse for int/float/str/bool converts value."""
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
    """parse for type field deserializes dict via from_json_schema."""
    class T(Table):
        kind: type

    info = T.model_fields["kind"]
    f = Column.from_pydantic_info(T, "kind", info)
    schema = {"type": "integer"}
    result = f.parse(schema)
    assert result is int


def test_parse_type_from_json_string():
    """parse for type field parses JSON string then deserializes."""
    class T(Table):
        kind: type

    info = T.model_fields["kind"]
    f = Column.from_pydantic_info(T, "kind", info)
    result = f.parse('{"type": "string"}')
    assert result is str


def test_parse_type_rebuild_pydantic_model():
    """parse for type[BaseModel] field deserializes via rebuild_pydantic_model."""
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
    """parse for type field raises when value is not a dict."""
    class T(Table):
        kind: type

    info = T.model_fields["kind"]
    f = Column.from_pydantic_info(T, "kind", info)
    with pytest.raises(ValueError, match="Type representation should be stored"):
        f.parse(123)


def test_parse_unknown_type_raises():
    """parse raises ValueError for unsupported base_type."""
    class Unknown:
        pass

    class T(Table):
        name: str

    # Build a Column that looks like an unsupported type for parse
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


def test_sql_creations_unsupported_type_raises():
    """sql_creations raises TypeError for column_base_type with no SQL mapping."""
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
    with pytest.raises(TypeError, match="has no known conversion to SQL type"):
        list(f.sql_creations)
