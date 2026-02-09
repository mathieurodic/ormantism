"""Tests for ormantism.join_info.JoinInfo."""

from unittest.mock import patch
import pytest

from ormantism.table import Table
from ormantism.join_info import JoinInfo
from ormantism.field import Field


def test_join_info_add_children_non_reference_raises(setup_db):
    """add_children with a non-reference field raises ValueError."""
    class A(Table, with_timestamps=True):
        name: str

    class B(Table, with_timestamps=True):
        label: str
        parent: A | None = None

    info = JoinInfo(model=B)
    with pytest.raises(ValueError, match="not a reference"):
        info.add_children(["label"])


def test_join_info_add_children_generic_table_raises(setup_db):
    """add_children for a generic Table reference raises ValueError."""
    class A(Table, with_timestamps=True):
        name: str

    class B(Table, with_timestamps=True):
        ref: Table | None = None  # generic reference

    info = JoinInfo(model=B)
    with pytest.raises(ValueError, match="Generic reference cannot be preloaded"):
        info.add_children(["ref"])


def test_join_info_add_children_concrete_reference_registers_child(setup_db):
    """add_children with a concrete reference field registers the child (covers line 33)."""
    class A(Table, with_timestamps=True):
        name: str

    class B(Table, with_timestamps=True):
        label: str
        parent: A | None = None

    info = JoinInfo(model=B)
    info.add_children(["parent"])
    assert "parent" in info.children
    assert info.children["parent"].model is A


def test_join_info_get_tables_statements_default_from(setup_db):
    """get_tables_statements with no parent_alias yields FROM tablename."""
    class A(Table, with_timestamps=True):
        x: int = 0

    info = JoinInfo(model=A)
    statements = list(info.get_tables_statements())
    assert len(statements) >= 1
    assert statements[0].startswith("FROM ")
    assert "a" in statements[0].lower()


def test_join_info_get_columns_yields_column_pairs(setup_db):
    """get_columns yields (alias, column_expression) pairs."""
    class A(Table, with_timestamps=True):
        name: str

    info = JoinInfo(model=A)
    columns = list(info.get_columns())
    assert len(columns) >= 1
    for alias, expr in columns:
        assert isinstance(alias, str)
        assert isinstance(expr, str)
        assert alias and expr


def test_join_info_get_columns_unsupported_reference_type_raises(setup_db):
    """get_columns raises ValueError for a reference field that is not scalar or list/tuple/set (line 63)."""
    class A(Table, with_timestamps=True):
        name: str

    class B(Table, with_timestamps=True):
        value: int = 0

    # Field that is_reference=True but base_type is not list/tuple/set (e.g. dict)
    bad_field = Field(
        table=A,
        name="unsupported_ref",
        base_type=dict,
        secondary_type=B,
        full_type=dict,
        default=None,
        column_is_required=False,
        is_required=False,
        is_reference=True,
    )
    real_fields = A._get_fields()
    patched_fields = {**real_fields, "unsupported_ref": bad_field}

    info = JoinInfo(model=A)
    with patch.object(A, "_get_fields", return_value=patched_fields):
        with pytest.raises(ValueError):
            list(info.get_columns())


def test_join_info_get_columns_skips_child_with_non_reference_field(setup_db):
    """get_columns skips (continue) when a child key maps to a non-reference field (line 70)."""
    class A(Table, with_timestamps=True):
        name: str

    class B(Table, with_timestamps=True):
        value: int = 0

    class C(Table, with_timestamps=True):
        label: str = ""
        ref: B | None = None

    info = JoinInfo(model=C)
    info.add_children(["ref"])
    # Manually add a child for a non-reference field so we hit the continue branch
    info.children["label"] = JoinInfo(model=A)
    columns = list(info.get_columns())
    # Should still complete; we have ref columns and label column from the model
    assert any("ref" in alias for alias, _ in columns)
    assert any("label" in alias for alias, _ in columns)


def test_join_info_get_instance_list_reference_lazy_path(setup_db):
    """get_instance with list[ConcreteTable] not in children hits lazy path (lines 140, 148)."""
    class Child(Table, with_timestamps=True):
        x: int = 0

    class Parent(Table, with_timestamps=True):
        kids: list[Child] = []

    c1 = Child(x=1)
    c2 = Child(x=2)
    parent = Parent(kids=[c1, c2])
    # Load without preload so kids is not in join_info.children
    loaded = Parent.load(id=parent.id)
    assert loaded is not None
    # List reference not preloaded: stored as lazy
    assert "_lazy_joins" in loaded.__dict__
    assert "kids" in loaded._lazy_joins
    ref_types, ref_ids = loaded._lazy_joins["kids"]
    assert len(ref_types) == 2
    assert ref_types[0] is Child and ref_types[1] is Child
    assert ref_ids == [c1.id, c2.id]
