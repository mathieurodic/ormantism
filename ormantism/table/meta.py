"""Metaclass for Table: injects mixins and sets Column descriptors on the class."""

from pydantic._internal._model_construction import ModelMetaclass

from ..column import Column
from ..connection import _ConnectionDescriptor
from ..expressions import TableExpression
from ..utils.supermodel import SuperModel
from .mixins import (
    _WithPrimaryKey,
    _WithCreatedAtTimestamp,
    _WithUpdatedAtTimestamp,
    _WithTimestamps,
    _WithVersion,
)


class TableMeta(ModelMetaclass):
    """Metaclass for Table: injects mixins and attaches Column instances to the class."""

    def __new__(mcs, name, bases, namespace,
                with_primary_key: bool = True,
                with_created_at_timestamp: bool = False,
                with_updated_at_timestamp: bool = False,
                with_timestamps: bool = False,
                versioning_along: tuple[str] = None,
                connection_name: str = None,
                **kwargs):
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
        result = super().__new__(
            mcs, name, bases + default_bases, namespace, **kwargs
        )
        if not connection_name:
            for base in bases:
                cn = getattr(base, "_CONNECTION_NAME", None)
                if cn:
                    connection_name = cn
        result._CONNECTION_NAME = connection_name
        if versioning_along is None:
            for base in bases:
                if getattr(base, "_VERSIONING_ALONG", None):
                    versioning_along = base._VERSIONING_ALONG
        result._VERSIONING_ALONG = versioning_along
        result._READ_ONLY_COLUMNS = sum((tuple(base.model_fields.keys())
                                        for base in default_bases), start=())
        result._columns = {}
        for fname, info in result.model_fields.items():
            col = Column.from_pydantic_info(result, fname, info)
            result._columns[fname] = col
        root = TableExpression(table=result, parent=None, path=())
        for fname in result._columns:
            setattr(result, fname, root[fname])
        setattr(result, "_expression", root)
        setattr(result, "_connection", _ConnectionDescriptor())
        return result
