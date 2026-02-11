"""Tests for Column.sql_creations: SQL column definitions for references, enums, JSON, BaseModel, and errors."""

import enum
import pytest
from pydantic import BaseModel

from ormantism.table import Table
from ormantism.column import Column
from ormantism import JSON


def test_sql_creations_scalar_reference_to_table():
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
    class T(Table):
        j: JSON

    info = T.model_fields["j"]
    f = Column.from_pydantic_info(T, "j", info)
    sqls = list(f.sql_creations)
    assert len(sqls) == 1
    assert "j JSON" in sqls[0]
    assert "null" in sqls[0]


def test_sql_is_json_true_for_json_stored_types():
    """Column.sql_is_json is True for BaseModel, list, dict, type, and JSON (lines 66-72)."""
    class Nested(BaseModel):
        x: int = 0

    class T(Table):
        n: Nested
        tags: list[str]
        meta: dict[str, str]
        kind: type
        j: JSON

    for name in ("n", "tags", "meta", "kind", "j"):
        info = T.model_fields[name]
        f = Column.from_pydantic_info(T, name, info)
        assert f.sql_is_json is True, name

    class S(Table):
        name: str

    info = S.model_fields["name"]
    f = Column.from_pydantic_info(S, "name", info)
    assert f.sql_is_json is False


def test_sql_creations_reference_non_list_tuple_set_raises():
    """sql_creations for reference with base_type not list/tuple/set raises (line 186)."""
    import collections.abc

    class B(Table):
        value: int = 0

    class T(Table):
        name: str = ""

    f = Column(
        table=T,
        name="items",
        base_type=collections.abc.Sequence,
        secondary_type=B,
        full_type=collections.abc.Sequence[B],
        default=None,
        is_required=False,
        column_is_required=False,
        is_reference=True,
    )
    with pytest.raises(Exception) as exc_info:
        list(f.sql_creations)
    assert exc_info.value.args[0] is collections.abc.Sequence


def test_sql_creations_unsupported_type_raises():
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
