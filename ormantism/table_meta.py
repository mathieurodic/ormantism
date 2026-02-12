"""Metaclass for Table: injects mixins and sets Column descriptors on the class."""

from pydantic._internal._model_construction import ModelMetaclass

from .column import Column
from .connection import _ConnectionDescriptor
from .expressions import TableExpression
from .utils.supermodel import SuperModel
from .table_mixins import (
    _WithPrimaryKey,
    _WithCreatedAtTimestamp,
    _WithUpdatedAtTimestamp,
    _WithTimestamps,
    _WithVersion,
)


class TableMeta(ModelMetaclass):
    """Metaclass for Table: injects mixins and attaches Column instances to the class.

    After Pydantic builds the model, each model_fields entry becomes a Column
    stored both as a class attribute (TableSubClass.field_name = column) and
    in TableSubClass._columns (same objects).
    """

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
        result._READ_ONLY_COLUMNS = sum((tuple(base.model_fields.keys())
                                        for base in default_bases), start=())
        # Column instances: stored in _columns only (not as class attrs) so Pydantic
        # does not copy the descriptor into instance.__dict__; same objects in result._columns.
        result._columns = {}
        for fname, info in result.model_fields.items():
            col = Column.from_pydantic_info(result, fname, info)
            result._columns[fname] = col
        # Class-level attribute per column/relationship for query building (e.g. User.name, User.books).
        # Skip read-only fields (id, created_at, etc.) so instance.id returns the stored value, not an expression.
        root = TableExpression(table=result, parent=None, path=())
        for fname in result._columns:
            if fname in result._READ_ONLY_COLUMNS:
                continue
            setattr(result, fname, root[fname])
        # Class-level pk expression for query building (e.g. A.pk == 1), without shadowing instance.id.
        setattr(result, "pk", root["id"])
        # Root table expression for select(Model._expression) and helpers that take a TableExpression.
        setattr(result, "_expression", root)
        # Connection descriptor (set here so Pydantic does not treat _connection as a private attr).
        setattr(result, "_connection", _ConnectionDescriptor())
        return result
