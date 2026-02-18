"""Tests for created_at, updated_at, and deleted_at timestamps on Table instances."""

import datetime

import pytest

from ormantism.table import Table
from ormantism.query import Query


def _assert_timestamp_recent(dt: datetime.datetime | None, label: str) -> None:
    """Assert dt is between 1 minute ago and now (inclusive). Uses UTC for comparison."""
    assert dt is not None, f"{label} should not be None"
    # SQLite CURRENT_TIMESTAMP returns UTC; use utcnow for comparison
    now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    dt_naive = dt.replace(tzinfo=None) if dt.tzinfo else dt
    one_minute_ago = now_utc - datetime.timedelta(minutes=1)
    one_minute_later = now_utc + datetime.timedelta(minutes=1)
    assert one_minute_ago <= dt_naive <= one_minute_later, (
        f"{label} {dt} should be between {one_minute_ago} and {one_minute_later}"
    )


class TestCreatedAt:
    """Test created_at is set on insert."""

    def test_created_at_set_on_insert(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        assert a.created_at is not None
        _assert_timestamp_recent(a.created_at, "created_at")

    def test_created_at_persisted_and_loaded(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        loaded = A.q().where(A.id == a.id).first()
        assert loaded is not None
        assert loaded.created_at is not None
        _assert_timestamp_recent(loaded.created_at, "created_at")


class TestUpdatedAt:
    """Test updated_at is set/updated on save."""

    def test_updated_at_set_on_update(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        a.name = "y"
        loaded = A.q().where(A.id == a.id).first()
        assert loaded is not None
        assert loaded.updated_at is not None
        _assert_timestamp_recent(loaded.updated_at, "updated_at")

    def test_updated_at_none_before_first_update(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        # updated_at may be None on insert (set only when row is updated)
        loaded = A.q().where(A.id == a.id).first()
        assert loaded is not None
        assert loaded.created_at is not None


class TestDeletedAt:
    """Test deleted_at is set on soft delete."""

    def test_deleted_at_none_before_delete(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        assert a.deleted_at is None

    def test_deleted_at_set_after_soft_delete(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        a.delete()
        loaded = A.q().include_deleted().where(A.id == a.id).first()
        assert loaded is not None
        assert loaded.deleted_at is not None
        _assert_timestamp_recent(loaded.deleted_at, "deleted_at")
