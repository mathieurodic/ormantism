"""Tests for Query JOIN building: join tree, SQL FROM/JOIN, column aliases, and list reference lazy paths."""

from unittest.mock import patch
import pytest

from ormantism.table import Table
from ormantism.query import Query
from ormantism.column import Column


def test_join_tree_non_reference_path_builds_without_join(setup_db):
    """Building SQL with a non-reference path (e.g. label) does not add a join; SQL builds."""
    class A(Table, with_timestamps=True):
        name: str = ""

    class B(Table, with_timestamps=True):
        label: str = ""
        parent: A | None = None

    root = B._root_expression()
    q = Query(table=B).select(root.label)
    rows, column_names = q._execute_with_column_names(q.sql, q.values)
    assert "label" in q.sql or any("label" in a for a in column_names)


def test_join_tree_generic_table_path_raises(setup_db):
    """Building SQL with a generic Table reference path raises ValueError."""
    class B(Table, with_timestamps=True):
        ref: Table | None = None

    root = B._root_expression()
    q = Query(table=B).select(root.ref)
    with pytest.raises(ValueError, match="Generic reference cannot be preloaded"):
        _ = q.sql


def test_join_tree_sql_contains_from_and_join(setup_db):
    """Generated SQL contains FROM tablename and can contain LEFT JOIN."""
    class A(Table, with_timestamps=True):
        x: int = 0

    q = Query(table=A)
    sql = q.sql
    assert sql.startswith("SELECT ")
    assert "FROM " in sql
    assert A._get_table_name().lower() in sql.lower()


def test_join_tree_sql_columns_have_aliases(setup_db):
    """Generated SELECT list has column AS alias pairs."""
    class A(Table, with_timestamps=True):
        name: str = ""

    q = Query(table=A)
    sql = q.sql
    assert " AS " in sql
    assert "id" in sql or "name" in sql


def test_join_tree_unsupported_reference_type_raises(setup_db):
    """Building columns for a reference field that is not scalar or list/tuple/set raises."""
    class A(Table, with_timestamps=True):
        name: str = ""

    class B(Table, with_timestamps=True):
        value: int = 0

    bad_field = Column(
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

    q = Query(table=A)
    with patch.object(A, "_get_fields", return_value=patched_fields):
        with pytest.raises(ValueError):
            _ = q.sql


def test_list_reference_lazy_path(setup_db, expect_lazy_loads):
    """Load without preload: list[ConcreteTable] is stored as lazy."""
    class Child(Table, with_timestamps=True):
        x: int = 0

    class Parent(Table, with_timestamps=True):
        kids: list[Child] = []

    c1 = Child(x=1)
    c2 = Child(x=2)
    parent = Parent(kids=[c1, c2])
    loaded = Parent.load(id=parent.id)
    assert loaded is not None
    assert "_lazy_joins" in loaded.__dict__
    assert "kids" in loaded._lazy_joins
    ref_types, ref_ids = loaded._lazy_joins["kids"]
    assert len(ref_types) == 2
    assert ref_types[0] is Child and ref_types[1] is Child
    assert ref_ids == [c1.id, c2.id]
    # Access kids triggers one lazy load (loads the list)
    _ = loaded.kids
    expect_lazy_loads.expect(1)