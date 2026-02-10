"""Metaclass for Table: injects mixins (with_primary_key, with_timestamps)."""

from pydantic._internal._model_construction import ModelMetaclass

from .utils.supermodel import SuperModel
from .table_mixins import (
    _WithPrimaryKey,
    _WithCreatedAtTimestamp,
    _WithUpdatedAtTimestamp,
    _WithTimestamps,
    _WithVersion,
)


class TableMeta(ModelMetaclass):
    """Metaclass for Table: injects mixins (with_primary_key, with_timestamps)."""

    def __new__(mcs, name, bases, namespace,
                with_primary_key: bool = True,
                with_created_at_timestamp: bool = False,
                with_updated_at_timestamp: bool = False,
                with_timestamps: bool = False,
                versioning_along: tuple[str] = None,
                connection_name: str = None,
                **kwargs):
        # inherited behaviors
        default_bases: tuple[type[SuperModel]] = tuple()
        if with_primary_key:
            default_bases += (_WithPrimaryKey,)
        if with_updated_at_timestamp:
            default_bases += (_WithUpdatedAtTimestamp,)
        if with_created_at_timestamp:
            default_bases += (_WithCreatedAtTimestamp,)
        if with_timestamps:
            default_bases += (_WithTimestamps,)
        if versioning_along:
            default_bases += (_WithVersion,)
        # start building result
        result = super().__new__(
            mcs, name, bases + default_bases, namespace, **kwargs
        )
        # connection name
        if not connection_name:
            for base in bases:
                if base._CONNECTION_NAME:
                    connection_name = base._CONNECTION_NAME
        result._CONNECTION_NAME = connection_name
        # versioning
        if versioning_along is None:
            for base in bases:
                if getattr(base, "_VERSIONING_ALONG", None):
                    versioning_along = base._VERSIONING_ALONG
        result._VERSIONING_ALONG = versioning_along
        # read-only
        result._READ_ONLY_FIELDS = sum((tuple(base.model_fields.keys())
                                        for base in default_bases), start=())
        # here we go :)
        return result
