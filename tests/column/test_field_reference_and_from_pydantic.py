"""Tests for Column.reference_type, column_base_type, and Column.from_pydantic_info()."""

from typing import Optional

import pytest
from pydantic import BaseModel, Field as PydanticField

from ormantism.table import Table
from ormantism.column import Column


def test_reference_type_list_reference():
    class B(Table):
        value: int = 0

    class C(Table):
        items: list[B] = []

    info = C.model_fields["items"]
    col = Column.from_pydantic_info(C, "items", info)
    assert col.reference_type is B


def test_reference_type_self_reference():
    class Agent(Table):
        birthed_by: Optional["Agent"] = None
        name: str

    info = Agent.model_fields["birthed_by"]
    col = Column.from_pydantic_info(Agent, "birthed_by", info)
    assert col.reference_type is Agent


def test_reference_type_non_reference():
    class T(Table):
        name: str

    info = T.model_fields["name"]
    f = Column.from_pydantic_info(T, "name", info)
    assert f.reference_type is None


def test_reference_type_reference():
    class B(Table):
        value: int = 0

    class C(Table):
        ref: B | None = None

    info = C.model_fields["ref"]
    f = Column.from_pydantic_info(C, "ref", info)
    assert f.reference_type is B


def test_column_base_type_reference():
    class B(Table):
        value: int = 0

    class C(Table):
        ref: B = None

    info = C.model_fields["ref"]
    f = Column.from_pydantic_info(C, "ref", info)
    assert f.column_base_type is int


def test_column_base_type_non_reference():
    class T(Table):
        name: str

    info = T.model_fields["name"]
    f = Column.from_pydantic_info(T, "name", info)
    assert f.column_base_type is str


def test_from_pydantic_info_dict_two_secondary_types():
    class T(Table):
        data: dict[str, int] | None = None

    info = T.model_fields["data"]
    f = Column.from_pydantic_info(T, "data", info)
    assert f.base_type is dict
    assert f.secondary_type is str


def test_from_pydantic_info_unsupported_union_raises():
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

    columns = {
        name: Column.from_pydantic_info(Agent, name, info)
        for name, info in Agent.model_fields.items()
    }

    assert columns["birthed_by"] == Column(table=Agent,
                                         name="birthed_by",
                                         base_type=Agent,
                                         secondary_type=None,
                                         full_type=Optional[Agent],
                                         default=None,
                                         is_required=False,
                                         column_is_required=False,
                                         is_reference=True)
    assert columns["name"] == Column(table=Agent,
                                   name="name",
                                   base_type=str,
                                   secondary_type=None,
                                   full_type=str,
                                   default=None,
                                   is_required=True,
                                   column_is_required=True,
                                   is_reference=False)
    assert columns["description"] == Column(table=Agent,
                                          name="description",
                                          base_type=str,
                                          secondary_type=None,
                                          full_type=Optional[str],
                                          default=None,
                                          is_required=False,
                                          column_is_required=False,
                                          is_reference=False)
    assert columns["thing"] == Column(table=Agent,
                                    name="thing",
                                    base_type=Thing,
                                    secondary_type=None,
                                    full_type=Thing,
                                    default=None,
                                    is_required=True,
                                    column_is_required=True,
                                    is_reference=True)
    assert columns["with_input_improvement"] == Column(table=Agent,
                                                     name="with_input_improvement",
                                                     base_type=bool,
                                                     secondary_type=None,
                                                     full_type=bool,
                                                     default=True,
                                                     is_required=False,
                                                     column_is_required=True,
                                                     is_reference=False)


def test_from_pydantic_info_scalar_str():
    class A(Table):
        name: str

    info = A.model_fields["name"]
    col = Column.from_pydantic_info(A, "name", info)
    assert col.name == "name"
    assert col.base_type is str
    assert col.secondary_type is None
    assert col.is_reference is False


def test_from_pydantic_info_reference_scalar():
    class B(Table):
        value: int = 0

    class C(Table):
        ref: B | None = None

    info = C.model_fields["ref"]
    col = Column.from_pydantic_info(C, "ref", info)
    assert col.is_reference is True
    assert col.reference_type is B
    assert col.name == "ref"


def test_from_pydantic_info_list_reference():
    class B(Table):
        value: int = 0

    class C(Table):
        items: list[B] = []

    info = C.model_fields["items"]
    col = Column.from_pydantic_info(C, "items", info)
    assert col.is_reference is True
    assert col.reference_type is B
    assert col.base_type is list
    assert col.secondary_type is B
    assert col.name == "items"
