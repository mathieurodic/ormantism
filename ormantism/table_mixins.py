"""Mixins for Table: primary key, soft delete, timestamps, versioning."""

import datetime
from typing import ClassVar

from .utils.supermodel import SuperModel


class _WithPrimaryKey(SuperModel):
    """Mixin that adds an auto-increment integer primary key `id`."""

    id: int = None
    # SQL fragment(s) for CREATE TABLE; collected by Table._get_table_sql_creations()
    TABLE_SQL_CREATIONS: ClassVar[list[str]] = [
        "id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT"
    ]


class _WithSoftDelete(SuperModel):
    """Mixin that adds soft delete via `deleted_at` timestamp."""

    deleted_at: datetime.datetime | None = None


class _WithCreatedAtTimestamp(SuperModel):
    """Mixin that adds a `created_at` timestamp set on insert."""

    created_at: datetime.datetime = None
    TABLE_SQL_CREATIONS: ClassVar[list[str]] = [
        "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
    ]


class _WithUpdatedAtTimestamp(SuperModel):
    """Mixin that adds an `updated_at` timestamp updated on save."""

    updated_at: datetime.datetime | None = None


class _WithTimestamps(_WithCreatedAtTimestamp, _WithSoftDelete, _WithUpdatedAtTimestamp):
    pass


class _WithVersion(_WithSoftDelete):
    """Mixin that adds a version counter for optimistic locking."""

    version: int = 0
