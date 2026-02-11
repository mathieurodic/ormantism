"""Table model base for ORM CRUD and schema creation."""

import inspect
import logging
import warnings
from typing import ClassVar
from functools import cache

from pydantic import BaseModel

from .utils.make_hashable import make_hashable
from .column import Column
from .table_meta import TableMeta
from .table_mixins import (
    _WithPrimaryKey,
    _WithSoftDelete,
    _WithCreatedAtTimestamp,
    _WithUpdatedAtTimestamp,
    _WithTimestamps,
    _WithVersion,
)

logger = logging.getLogger("ormantism")


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
        # Ensure lazy read-only fields are loaded so hash is consistent
        for name in getattr(self, "_lazy_readonly", set()):
            getattr(self, name)
        return hash(make_hashable(self))

    def __deepcopy__(self, memo):
        """Return self; table instances are treated as immutable for copy purposes."""
        return self

    # INSERT
    def save(self, init_data: dict):
        """Persist the instance to the database (INSERT) and rehydrate generated columns."""
        self.__class__.q().insert(instance=self, init_data=init_data)

    def on_after_create(self, init_data: dict):
        """Persist the instance to the database (INSERT) and set generated columns."""
        self.save(init_data)

    # UPDATE
    def on_before_update(self, new_data):
        """Apply changes to database."""
        from . import query
        query.update_instance(self, new_data)

    # INSERT or SELECT / UPDATE
    @classmethod
    def load_or_create(cls, _search_fields=None, **data):
        """Load a row matching the data, or create one; optional _search_fields."""
        # if restriction applies
        if _search_fields is None:
            searched_data = data
        else:
            searched_data = {key: data[key] for key in _search_fields if key in data}
        # return corresponding row if already exists
        loaded = cls.load(**searched_data)
        if loaded:
            logger.warning(data)
            def _changed(key, value):
                loaded_val = getattr(loaded, key)
                if value is None or loaded_val is None:
                    return value is not loaded_val
                return loaded_val != value
            changed_data = {key: value for key, value in data.items() if _changed(key, value)}
            changed_data = {}
            for name, value in data.items():
                field = cls._get_field(name)
                if field.is_reference:
                    if name not in loaded._lazy_joins:
                        if value is not None:
                            changed_data[name] = value.id
                            raise NotImplementedError(
                                "Unexpected: reference not in _lazy_joins"
                            )
                    elif value is None:
                        changed_data[name] = None
                    else:
                        foreign_key = loaded._lazy_joins[name]
                        if isinstance(foreign_key, int):
                            if foreign_key != value.id:
                                changed_data[name] = value.id
                        elif isinstance(foreign_key, tuple) and len(foreign_key) == 2:
                            if foreign_key[0] != value.__class__ or foreign_key[1] != value.id:
                                changed_data[name] = value
                        else:
                            raise ValueError("?!")

                elif getattr(loaded, name) != value:
                    if cls._get_field(name):
                        changed_data[name] = value
            logger.warning(changed_data)
            loaded.update(**changed_data)
            return loaded
        # build new item if not found
        return cls(**data)

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
    @cache
    def _get_non_default_fields(cls):
        """Return fields that are not read-only (e.g. not id, created_at from mixins)."""
        return {
            name: field
            for name, field in cls._get_fields().items()
            if name not in cls._READ_ONLY_FIELDS
        }

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

    def _execute_returning(self, sql: str, parameters=None, for_insertion=False):
        """Execute SQL with RETURNING and set parsed values on self."""
        from . import query
        query.apply_returning(self, sql, parameters, for_insertion=for_insertion)

    @classmethod
    def _create_table(cls, created=None):
        """Create the table and referenced tables if they do not exist."""
        from . import query
        query.create_table(cls, created)

    @classmethod
    def _add_columns(cls):
        """Add any missing columns to the existing table (SQLite ALTER TABLE)."""
        from . import query
        query.add_columns(cls)

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
        from . import query
        query.delete_instance(self)

    # SELECT
    @classmethod
    def q(cls) -> "Query":
        """Return a Query for this table. Use instead of load/load_all."""
        from .query import Query
        return Query(table=cls)

    @classmethod
    def load(cls, reverse_order: bool = True, as_collection: bool = False,
             with_deleted=False, preload: str | list[str] = None,
             **criteria) -> "Table":
        """Load by criteria; supports preload paths and optional soft-delete."""
        warnings.warn(
            "load() is deprecated; use cls.q().where(...).first() or cls.q().where(...).all() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        from .expressions import ColumnExpression
        from .query import Query
        preload_list = [preload] if isinstance(preload, str) else (preload or [])
        root = cls._root_expression()
        q = Query(table=cls)
        if preload_list:
            select_exprs = [root.get_column_expression("id")]
            for p in preload_list:
                e = q._resolve_user_path(p)
                if e is not None:
                    select_exprs.append(e)
            q = q.clone_query_with(select_expressions=select_exprs)
        if with_deleted and issubclass(cls, _WithSoftDelete):
            q = q.include_deleted()
        if criteria:
            stmts = []
            for k, v in criteria.items():
                field = cls._get_field(k)
                if field.is_reference and field.secondary_type is None:
                    col = root.get_column_expression(field.column_name)
                    val = v.id if (hasattr(v, "_get_table_name") and not isinstance(v, type)) else v
                    stmts.append(col == val)
                    # Generic ref (Table): also filter by _table so ref_id alone doesn't match another table's row
                    if field.base_type is Table and v is not None and hasattr(v, "_get_table_name") and not isinstance(v, type):
                        table_col = ColumnExpression(table_expression=root, name=f"{k}_table")
                        stmts.append(table_col == v._get_table_name())
                else:
                    stmts.append(getattr(root, k) == v)
            q = q.where(*stmts)
        if not reverse_order:
            if issubclass(cls, _WithTimestamps):
                q = q.order_by(root.get_column_expression("created_at"))
            elif issubclass(cls, _WithVersion):
                along = list(getattr(cls, "_VERSIONING_ALONG", ())) + ["version"]
                q = q.order_by(*[root.get_column_expression(a) for a in along])
            else:
                q = q.order_by(root.get_column_expression("id"))
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

    @classmethod
    def _add_lazy_loader(cls, name: str):
        """Attach a property on the class that loads the reference for `name` on first access."""
        def lazy_loader(self):
            if not name in self.__dict__:
                warnings.warn(
                    f"Lazy loading '{name}' on {cls.__name__}: consider preloading (e.g. select or load with this path) to avoid N+1 queries.",
                    UserWarning,
                    stacklevel=2,
                )
                model, identifier = self._lazy_joins[name]
                if inspect.isclass(model) and issubclass(model, Table):
                    value = None if identifier is None else model.load(id=identifier)
                else:
                    value = [reference_type.load(id=reference_id)
                             for reference_type, reference_id
                             in zip(model, identifier)]
                self.__dict__[name] = value
            return self.__dict__[name]
        setattr(cls, name, property(lazy_loader))

    @classmethod
    def _ensure_lazy_loaders(cls):
        """Ensure every reference field has a lazy-loading property."""
        if hasattr(cls, "_has_lazy_loaders"):
            return
        for name, field in cls._get_fields().items():
            if field.is_reference:
                cls._add_lazy_loader(name)
        cls._ensure_lazy_readonly_loaders()
        cls._has_lazy_loaders = True

    @classmethod
    def _add_lazy_readonly_loader(cls, name: str):
        """Attach a property that fetches a read-only scalar field on first access."""
        def lazy_loader(self):
            if name not in self.__dict__:
                pk = getattr(self, "id", None)
                if pk is None:
                    return None
                from .query import Query
                field = cls._get_field(name)
                tbl = cls._get_table_name()
                col = field.column_name
                rows = Query(table=cls).execute(
                    f"SELECT {col} FROM {tbl} WHERE id = ?",
                    (pk,),
                    ensure_structure=False,
                )
                value = rows[0][0] if rows else None
                parsed = field.parse(value)
                if parsed is None and field.default is not None:
                    parsed = field.default
                self.__dict__[name] = parsed
            return self.__dict__[name]
        setattr(cls, name, property(lazy_loader))

    @classmethod
    def _ensure_lazy_readonly_loaders(cls):
        """Ensure every read-only scalar field (except id) has a lazy-loading property."""
        if hasattr(cls, "_has_lazy_readonly_loaders"):
            return
        for name in getattr(cls, "_READ_ONLY_FIELDS", ()):
            if name == "id":
                continue
            if name in cls._get_fields():
                cls._add_lazy_readonly_loader(name)
        cls._has_lazy_readonly_loaders = True

    @classmethod
    def _mark_readonly_lazy(cls, instance: "Table") -> None:
        """Mark read-only fields (except id) as lazy; values will fetch on first access."""
        cls._ensure_lazy_readonly_loaders()
        lazy_names = {
            n for n in getattr(cls, "_READ_ONLY_FIELDS", ())
            if n != "id" and n in cls._get_fields()
        }
        if not lazy_names:
            return
        instance._lazy_readonly = getattr(instance, "_lazy_readonly", set()) | lazy_names
        for name in lazy_names:
            instance.__dict__.pop(name, None)


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
