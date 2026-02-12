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
    """Mixin that adds timestamps and soft delete; default order is by created_at DESC."""

    @classmethod
    def _transform_query(cls, q):
        """Add default order by created_at DESC."""
        if not q.order_by_expressions:
            root = cls._expression
            q = q.order_by(root["created_at"].desc)
        return q


class _WithVersion(_WithSoftDelete):
    """Mixin that adds a version counter for optimistic locking."""

    version: int = 0

    @classmethod
    def _transform_query(cls, q):
        """Add default order by versioning_along columns and version DESC."""
        if not q.order_by_expressions:
            along = list(getattr(cls, "_VERSIONING_ALONG", ())) + ["version"]
            root = cls._expression
            exprs = []
            for name in along:
                col = root[name]
                exprs.append(col.desc if name == "version" else col)
            q = q.order_by(*exprs)
        return q
