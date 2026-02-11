"""Tests for Column.reference_type, column_base_type, and Column.from_pydantic_info()."""

from typing import Optional

import pytest
from pydantic import BaseModel, Field as PydanticField

from ormantism.table import Table
from ormantism.column import Column


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
