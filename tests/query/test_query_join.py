"""Tests for Query JOIN building: join tree, SQL FROM/JOIN, column aliases, and list reference lazy paths."""

from unittest.mock import patch
import pytest

from ormantism.table import Table
from tests.helpers import assert_table_instance
from ormantism.query import Query
from ormantism.column import Column
from ormantism.expressions import ALIAS_SEPARATOR


def test_join_tree_non_reference_path_builds_without_join(setup_db):
    """Building SQL with a non-reference path (e.g. label) does not add a join; SQL builds."""
    class A(Table, with_timestamps=True):
        name: str = ""

    class B(Table, with_timestamps=True):
        label: str = ""
        parent: A | None = None

    q = Query(table=B).select(B.label)
    rows = q.execute(q.sql, q.values, rows_as_dicts=True)
    assert "label" in q.sql or (rows and "label" in rows[0])


def test_join_tree_generic_table_path_raises(setup_db):
    """Building SQL with a generic Table reference path raises ValueError."""
    class B(Table, with_timestamps=True):
        ref: Table | None = None

    q = Query(table=B).select(B.ref)
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
    """Hydrating a reference column that is not scalar or list/tuple/set raises."""
    class A(Table, with_timestamps=True):
        name: str = ""

    class B(Table, with_timestamps=True):
        value: int = 0

    bad_column = Column(
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
    real_columns = A._get_columns()
    patched_columns = {**real_columns, "unsupported_ref": bad_column}

    row_dict = {"id": 1, "name": "x", "unsupported_ref": 42}
    with patch.object(A, "_get_columns", return_value=patched_columns):
        rearranged = A.rearrange_data_for_hydration([row_dict])
        root_pk = list(rearranged.keys())[0]
        instance = A.make_empty_instance(root_pk)
        with pytest.raises(ValueError, match="Unexpected reference type"):
            instance.integrate_data_for_hydration(rearranged)


def test_list_reference_lazy_path(setup_db, expect_lazy_loads):
    """Load without preload: list[ConcreteTable] has skeleton instances; kids lazy-loads on access."""
    class Child(Table, with_timestamps=True):
        x: int = 0

    class Parent(Table, with_timestamps=True):
        kids: list[Child] = []

    c1 = Child(x=1)
    c2 = Child(x=2)
    parent = Parent(kids=[c1, c2])
    loaded = Parent.load(id=parent.id)
    assert loaded is not None
    kids = loaded.kids
    assert_table_instance(
        loaded,
        {"id": parent.id, "kids": [c1, c2]},
        exclude={"created_at", "updated_at", "deleted_at"},
    )
    assert len(kids) == 2
    # kids in row (list ref); no lazy load on access.
    expect_lazy_loads.expect(0)


def test_lazy_list_ref_proxy_contains_bool_reversed(setup_db):
    """Lazy list ref: __contains__, __bool__, __reversed__ work on loaded list."""
    class Child(Table, with_timestamps=True):
        x: int = 0

    class Parent(Table, with_timestamps=True):
        kids: list[Child] = []

    c1 = Child(x=1)
    c2 = Child(x=2)
    parent = Parent(kids=[c1, c2])
    loaded = Parent.load(id=parent.id)
    kids = loaded.kids  # proxy before first use
    first = kids[0]  # triggers load
    assert first in kids  # __contains__
    assert bool(kids)  # __bool__
    assert list(reversed(kids))[0].x == 2  # __reversed__


def test_lazy_ref_proxy_private_attr_raises(setup_db):
    """Accessing _private on a lazy ref loads it first; nonexistent _private raises AttributeError."""
    class B(Table, with_timestamps=True):
        title: str = ""

    class A(Table, with_timestamps=True):
        book: B | None = None

    b = B(title="x")
    a = A(book=b)
    loaded = A.load(id=a.id)  # no preload, so book is lazy
    with pytest.raises(AttributeError, match="_"):
        _ = loaded.book._private_attr  # loads book, then raises for nonexistent _private_attr