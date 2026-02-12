"""Table model base for ORM CRUD and schema creation."""

import inspect
import warnings
from typing import ClassVar, Any
from functools import cache

from pydantic import BaseModel

from .utils.make_hashable import make_hashable
from .column import Column
from .table_meta import TableMeta


def _run_lazy_load(loader_info: tuple) -> Any:
    """Load a reference from (model, id) or (models, ids); emits N+1 warning."""
    model, identifier = loader_info
    if inspect.isclass(model) and issubclass(model, Table):
        return None if identifier is None else model.load(id=identifier)
    return [
        reference_type.load(id=reference_id)
        for reference_type, reference_id in zip(model, identifier)
    ]


from .table_mixins import (
    _WithPrimaryKey,
    _WithSoftDelete,
    _WithCreatedAtTimestamp,
    _WithUpdatedAtTimestamp,
    _WithTimestamps,
    _WithVersion,
)


class Table(metaclass=TableMeta):
    """Base class for ORM table models; provides DB operations and identity semantics."""

    model_config = {"arbitrary_types_allowed": True}
    _ensured_table_structure: ClassVar[bool] = False

    def __eq__(self, other: "Table"):
        """Compare by identity (hash); both must be the same class."""
        if not isinstance(other, self.__class__):
            raise ValueError(
                f"Comparing instances of different classes: {self.__class__} "
                f"and {other.__class__}"
            )
        return hash(self) == hash(other)

    def __hash__(self):
        """Hash for equality and use in sets/dicts."""
        # Ensure lazy fields are loaded so hash is consistent
        lazy_readonly = getattr(self, "_lazy_readonly", set())
        lazy_joins = getattr(self, "_lazy_joins", {})
        for name in list(lazy_readonly | lazy_joins.keys()):
            getattr(self, name)
        return hash(make_hashable(self))

    def __getattribute__(self, name: str) -> Any:
        """Lazy-load refs and read-only scalars on first access; then cache and return."""
        # Use object.__getattribute__ for internal attrs to avoid recursion
        try:
            lazy_joins = object.__getattribute__(self, "_lazy_joins")
        except AttributeError:
            lazy_joins = {}
        try:
            lazy_readonly = object.__getattribute__(self, "_lazy_readonly")
        except AttributeError:
            lazy_readonly = set()

        if name in lazy_joins:
            loader_info = lazy_joins[name]
            cls_name = type(self).__name__
            warnings.warn(
                f"Lazy loading '{name}' on {cls_name}: consider preloading "
                "(e.g. select or load with this path) to avoid N+1 queries.",
                UserWarning,
                stacklevel=2,
            )
            value = _run_lazy_load(loader_info)
            d = object.__getattribute__(self, "__dict__")
            d[name] = value
            del lazy_joins[name]
            return value

        if name in lazy_readonly:
            cls = type(self)
            pk = object.__getattribute__(self, "id")
            assert pk is not None, "id should always be present on table instance"
            from .query import Query
            field = cls._get_field(name)
            tbl = cls._get_table_name()
            col = field.column_name
            rows = Query(table=cls).execute(
                f"SELECT {col} FROM {tbl} WHERE id = ?",
                (pk,),
                ensure_structure=False,
            )
            raw = rows[0][0] if rows else None
            value = field.parse(raw)
            d = object.__getattribute__(self, "__dict__")
            d[name] = value
            lazy_readonly.discard(name)
            return value

        return object.__getattribute__(self, name)

    def __getattr__(self, name: str) -> Any:
        """Raise for unknown attributes (lazy fields are handled in __getattribute__)."""
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}") from None

    def __deepcopy__(self, memo):
        """Return self; table instances are treated as immutable for copy purposes."""
        return self

    # INSERT
    def on_after_create(self, init_data: dict):
        """Persist the instance to the database (INSERT) and set generated columns."""
        self.__class__.q().insert(instance=self, init_data=init_data)

    # UPDATE
    def on_before_update(self, new_data):
        """Apply changes to database."""
        self.check_read_only(new_data)
        self.q().where(id=self.id).update(**new_data)
        self._mark_readonly_lazy()

    # INSERT or SELECT / UPDATE
    @classmethod
    def load_or_create(cls, _search_fields=None, **data):
        """Load a row matching the data, or create one; optional _search_fields."""
        keys = _search_fields if _search_fields is not None else list(data.keys())
        on_conflict = [k for k in keys if k in data]
        if not on_conflict:
            return cls(**data)
        return cls.q().upsert(on_conflict=on_conflict, **data)

    ##

    @classmethod
    @cache
    def _has_field(cls, name: str) -> bool:
        """Return True if the table has a field (or column) with the given name."""
        return name in cls.model_fields

    @classmethod
    def _get_fields(cls) -> dict[str, Column]:
        """Return the mapping of field name to Column (set by TableMeta on the class)."""
        return getattr(cls, "_columns", {})

    @classmethod
    def _get_field(cls, name: str) -> Column:
        """Return the Column for the given name or column name; raises KeyError if missing."""
        columns = cls._get_fields()
        if name in columns:
            return columns[name]
        for col in columns.values():
            if col.column_name == name:
                return col
        if name.endswith("_table") and name[:-6] in columns:
            return columns[name[:-6]]
        raise KeyError(f"No such field for {cls.__name__}: {name}")

    @classmethod
    def _get_table_sql_creations(cls) -> list[str]:
        """Return CREATE TABLE column fragments from mixins (e.g. id, created_at).

        Collects TABLE_SQL_CREATIONS from the class MRO so mixins own their SQL.
        Deduplicates by column name (first token) so each column appears once.
        """
        seen_columns = set()
        statements = []
        for base in cls.__mro__:
            if hasattr(base, "TABLE_SQL_CREATIONS"):
                for stmt in base.TABLE_SQL_CREATIONS:
                    col = stmt.split()[0]
                    if col not in seen_columns:
                        seen_columns.add(col)
                        statements.append(stmt)
        return statements

    def check_read_only(self, data):
        """Check we are not attempting to alter read-only fields"""
        read_only_fields = list(set(data) & set(self._READ_ONLY_FIELDS))
        read_only_fields_count = len(read_only_fields)
        if read_only_fields_count:
            plural = "s" if read_only_fields_count > 1 else ""
            fields_str = ", ".join(read_only_fields)
            raise AttributeError(
                f"Cannot set read-only attribute{plural} of "
                f"{self.__class__.__name__}: {fields_str}"
            )

    @classmethod
    def process_data(cls, data: dict, for_filtering: bool = False) -> dict:
        """Convert dict to DB-ready form (serialize refs, expand to _id/_table)."""
        data = {**data}
        for name in list(data):
            value = data.pop(name)
            # is there no field for this name?
            try:
                field = cls._get_field(name)
            except KeyError as exc:
                raise ValueError(
                    f"Invalid key found in data for {cls.__name__}: {name}"
                ) from exc
            # so, there is.
            if field.is_reference:
                # scalar reference
                if field.secondary_type is None:
                    if field.base_type is Table:
                        data[f"{field.name}_table"] = (
                            value._get_table_name() if value else None
                        )
                    data[f"{field.name}_id"] = (
                        (value if isinstance(value, int) else value.id)
                        if value else None
                    )
                # list of references
                elif (isinstance(value, (list, tuple, set))
                        and issubclass(field.base_type, (list, tuple, set))):
                    if field.secondary_type == Table:
                        data[f"{field.name}_tables"] = [
                            referred._get_table_name() for referred in value
                        ]
                    data[f"{field.name}_ids"] = [referred.id for referred in value]
                # ?
                else:
                    raise NotImplementedError(
                        field.name, value, field.base_type, field.secondary_type
                    )
            # model
            elif isinstance(value, BaseModel):
                data[name] = value.model_dump(mode="json")
            # just some regular stuff
            else:
                data[name] = field.serialize(value, for_filtering=for_filtering)
        return data

    # DELETE
    def delete(self):
        """Delete the row (soft delete if _WithSoftDelete, else hard delete)."""
        self.q().where(id=self.id).delete()

    # SELECT
    @classmethod
    def q(cls) -> "Query":
        """Return a Query for this table. Use instead of load/load_all."""
        from .query import Query
        q = Query(table=cls)
        for c in cls.__mro__:
            transform = getattr(c, "_transform_query", Table._transform_query)
            q = transform.__func__(cls, q)
        return q

    @classmethod
    def _transform_query(cls, q: "Query") -> "Query":
        """Override in subclasses to customize the default Query. Identity by default."""
        return q

    @classmethod
    def load(cls, as_collection: bool = False,
             with_deleted=False, preload: str | list[str] = [],
             **criteria) -> "Table":
        """Load by criteria; supports preload paths and optional soft-delete."""
        warnings.warn(
            "load() is deprecated; use cls.q().where(...).first() or cls.q().where(...).all() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        q = cls.q()
        preload_list = [preload] if isinstance(preload, str) else preload
        for p in preload_list:
            q = q.select(p)
        if with_deleted and issubclass(cls, _WithSoftDelete):
            q = q.include_deleted()
        if criteria:
            q = q.where(**criteria)
        if as_collection:
            return q.all()
        return q.first()

    @classmethod
    def load_all(cls, **criteria) -> list["Table"]:
        """Load all rows matching the given criteria."""
        warnings.warn(
            "load_all() is deprecated; use cls.q().where(...).all() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return cls.load(as_collection=True, **criteria)

    # helper methods

    @classmethod
    def _get_table_name(cls) -> str:
        """Return the database table name (default: lowercased class name)."""
        return cls.__name__.lower()

    @classmethod
    def _root_expression(cls):
        """Return the root TableExpression for this table (for building query expressions)."""
        from .expressions import TableExpression
        return TableExpression(table=cls, parent=None, path=())

    @classmethod
    def get_column_expression(cls, name: str):
        """Return the ColumnExpression or TableExpression for the given field/column name (e.g. for query building)."""
        return cls._root_expression().get_column_expression(name)

    @classmethod
    def _suspend_validation(cls):
        """Replace __init__/__setattr__ so instances can be built without validation."""
        def __init__(self, *_args, **kwargs):
            self.__dict__.update(**kwargs)
            self.__pydantic_fields_set__ = set(cls.model_fields)
        def __setattr__(self, name, value):
            self.__dict__[name] = value
            return value
        __init__.__pydantic_base_init__ = True
        cls.__setattr_backup__ = cls.__setattr__
        cls.__setattr__ = __setattr__
        cls.__init_backup__ = cls.__init__
        cls.__init__ = __init__

    @classmethod
    def _resume_validation(cls):
        """Restore normal __init__/__setattr__ after _suspend_validation."""
        if hasattr(cls, "__init_backup__"):
            cls.__init__ = cls.__init_backup__
            cls.__setattr__ = cls.__setattr_backup__
            delattr(cls, "__init_backup__")
            delattr(cls, "__setattr_backup__")

    def _mark_readonly_lazy(self) -> None:
        """Mark read-only fields (except id) as lazy; values will fetch on first access."""
        cls = self.__class__
        lazy_names = {
            n for n in getattr(cls, "_READ_ONLY_FIELDS", ())
            if n != "id" and n in cls._get_fields()
        }
        if not lazy_names:
            return
        self._lazy_readonly = getattr(self, "_lazy_readonly", set()) | lazy_names
        for name in lazy_names:
            self.__dict__.pop(name, None)


# Re-export so existing imports from ormantism.table still work
__all__ = [
    "Table",
    "TableMeta",
    "_WithPrimaryKey",
    "_WithSoftDelete",
    "_WithCreatedAtTimestamp",
    "_WithUpdatedAtTimestamp",
    "_WithTimestamps",
    "_WithVersion",
]
