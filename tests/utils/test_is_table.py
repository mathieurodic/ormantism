"""Tests for ormantism.utils.is_table and is_polymorphic_table."""

import pytest

from ormantism.table import Table
from ormantism.utils.is_table import is_table, is_polymorphic_table


def test_is_table_true_for_table_subclass():
    class A(Table):
        name: str

    assert is_table(A) is True


def test_is_table_false_for_builtin():
    assert is_table(str) is False
    assert is_table(int) is False
    assert is_table(list) is False


def test_is_table_false_for_base_model():
    from pydantic import BaseModel

    class M(BaseModel):
        x: int

    assert is_table(M) is False


def test_is_polymorphic_table_true_for_base_table():
    assert is_polymorphic_table(Table) is True


def test_is_polymorphic_table_false_for_concrete_subclass():
    class A(Table):
        name: str

    assert is_polymorphic_table(A) is False


def test_is_polymorphic_table_false_for_non_table():
    assert is_polymorphic_table(str) is False
