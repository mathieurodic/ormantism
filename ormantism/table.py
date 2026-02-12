"""Table model base for ORM CRUD and schema creation."""

import json
import warnings
from collections import defaultdict
from typing import ClassVar, Any
from functools import cache

from pydantic import BaseModel

from .utils.make_hashable import make_hashable
from .expressions import ALIAS_SEPARATOR, ColumnExpression
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
        # Ensure lazy columns are loaded so hash is consistent
        return hash((self.__class__, self.id))

    def _load(self, names: tuple[str, ...]):
        """Load a value from the database for a given column name, using the current instance's identifier."""
        for name in names:
            col = type(self)._get_columns().get(name)
            if col is not None and col.is_reference:
                warnings.warn(
                    f"Lazy loading '{name}' on {type(self).__name__}: consider preloading "
                    f"(e.g. Model.q().select(Model.{name}).where(...))",
                    UserWarning,
                    stacklevel=2,
                )
                break
        rows = self.q().select(*names).where(id=self.id).rows(as_dicts=True)
        if not rows:
            return
        row = dict(rows[0])
        if "id" not in row:
            row["id"] = self.id
        self._load_row(row)
    
    def _load_row(self, row: dict[str, Any]):
        self.hydrate_with([row])

    def __getattribute__(self, name: str) -> Any:
        """Lazy-load columns on first access; derive from schema + __dict__."""
        d = object.__getattribute__(self, "__dict__")
        cls = type(self)
        try:
            column = cls._get_column(name)
        except KeyError:
            if name in d:
                return d[name]
            return object.__getattribute__(self, name)

        if name in d:
            val = d[name]
            # Pydantic may use cls.id etc. as defaults, so __dict__ can hold a ColumnExpression.
            # Treat that as "not loaded": return None for id, lazy-load others.
            if isinstance(val, ColumnExpression):
                if name == "id":
                    return None
                self._load((name,))
                return d.get(name)
            return val

        self._load((name,))
        return d.get(name)

    def __getattr__(self, name: str) -> Any:
        """Raise for unknown attributes (lazy columns are handled in __getattribute__)."""
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
        self.__dict__.pop("updated_at", None)

    @classmethod
    def load_or_create(cls, _search_fields=None, **data):
        """Load a row matching the data, or create one; optional _search_fields."""
        on_conflict = list(data) if _search_fields is None else _search_fields
        if not on_conflict:
            return cls(**data)
        return cls.q().upsert(on_conflict=on_conflict, **data)

    ##

    @classmethod
    @cache
    def _has_column(cls, name: str) -> bool:
        """Return True if the table has a column with the given name."""
        return name in cls.model_fields

    @classmethod
    def _get_columns(cls) -> dict[str, Column]:
        """Return the mapping of column name to Column (set by TableMeta on the class)."""
        return getattr(cls, "_columns", {})

    @classmethod
    def _get_column(cls, name: str) -> Column:
        """Return the Column for the given name; raises KeyError if missing."""
        columns = cls._get_columns()
        if name in columns:
            return columns[name]
        for col in columns.values():
            if col.name == name:
                return col
        raise KeyError(f"No such column for {cls.__name__}: {name}")

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
        """Check we are not attempting to alter read-only columns."""
        read_only_columns = list(set(data) & set(self._READ_ONLY_COLUMNS))
        read_only_count = len(read_only_columns)
        if read_only_count:
            plural = "s" if read_only_count > 1 else ""
            columns_str = ", ".join(read_only_columns)
            raise AttributeError(
                f"Cannot set read-only attribute{plural} of "
                f"{self.__class__.__name__}: {columns_str}"
            )

    @classmethod
    def make_empty_instance(cls, id: int) -> "Table":
        """Generate an empty instance of the table.
        Identifier is mandatory because it is needed to identify the instance."""
        cls._suspend_validation()
        instance = cls()
        instance.__dict__["id"] = id
        cls._resume_validation()
        return instance

    @staticmethod
    def rearrange_data_for_hydration(unparsed_data: list[dict[str, Any]]) -> dict[str, Any]:
        """Rearrange unparsed row data into a nested structure for hydration.

        All paths are relative to the root table. If Author has one Book, the full path
        from Author is ``"book"`` (not ``"author____book"``).

        Alias format: ``path{ALIAS_SEPARATOR}column`` (or ``column`` for root).
        - Path ``""``: root row (e.g. Author ``"id"``, ``"name"``).
        - Path ``"book"``: single ref (e.g. ``"book____id"``, ``"book____title"``).
        - Path ``"kids"``: collection ref (e.g. ``"kids____id"``, ``"kids____x"``).
        Each joined row contributes one nested row; different kids are distinguished by
        their ``id`` (pk). Repeated pks across rows are deduplicated.

        Joins always include the ``id`` column for each selected table, so
        ``pk = values.get("id")`` identifies each row.

        Returns ``{root_pk: {cols..., book: {pk: {cols...}}, kids: {pk: ...}, ...}}``.
        Example for Author with one book and two kids.
        unparsed_data (one row per JOIN result; root + book repeated, kids vary)::
            [{"id": 1, "name": "Alice", "book____id": 10, "book____title": "Python 101", "kids____id": 1, "kids____x": 10},
             {"id": 1, "name": "Alice", "book____id": 10, "book____title": "Python 101", "kids____id": 2, "kids____x": 20}]
        rearranged_data::
            {1: {"id": 1, "name": "Alice", "book": {10: {"id": 10, "title": "Python 101"}}, "kids": {1: {"id": 1, "x": 10}, 2: {"id": 2, "x": 20}}}}
        """
        deep_defaultdict = lambda: defaultdict(deep_defaultdict)
        rearranged_data = deep_defaultdict()
        for unparsed_row in unparsed_data:
            # Strip redundant FK columns when the reference is already joined and has data.
            # Keep the root FK when the joined row's id is None (LEFT JOIN with no match).
            keys = set(unparsed_row.keys())

            def should_strip(k: str) -> bool:
                joined_id_key = f"{k}{ALIAS_SEPARATOR}id"
                if joined_id_key not in keys:
                    return False
                # Only strip when joined row has actual data; keep FK when join returned NULL
                return unparsed_row.get(joined_id_key) is not None

            unparsed_row = {
                k: v for k, v in unparsed_row.items()
                if not (ALIAS_SEPARATOR not in k and should_strip(k))
            }
            data_per_table = deep_defaultdict()
            for alias, value in unparsed_row.items():
                if ALIAS_SEPARATOR in alias:
                    table_path, column_name = alias.rsplit(ALIAS_SEPARATOR, 1)
                else:
                    table_path, column_name = "", alias
                data_per_table[table_path][column_name] = value

            root_pk = data_per_table.get("", {}).get("id")
            if root_pk is None:
                continue

            for table_path in sorted(data_per_table.keys()):
                values = data_per_table[table_path]
                pk = values.get("id")
                if pk is None:
                    continue

                if table_path == "":
                    data = rearranged_data
                    target_pk = root_pk
                else:
                    data = rearranged_data[root_pk]
                    path_parts = table_path.split(ALIAS_SEPARATOR)
                    skip_path = False
                    for i, part in enumerate(path_parts):
                        data = data[part]
                        if i < len(path_parts) - 1:
                            parent_path = ALIAS_SEPARATOR.join(
                                path_parts[: i + 1]
                            )
                            parent_pk = data_per_table.get(parent_path, {}).get(
                                "id"
                            )
                            if parent_pk is None:
                                skip_path = True
                                break
                            data = data[parent_pk]
                    if skip_path:
                        continue
                    target_pk = pk

                if target_pk not in data:
                    data[target_pk] = deep_defaultdict()
                for key, value in values.items():
                    data[target_pk][key] = value
        return rearranged_data

    def integrate_data_for_hydration(self, rearranged_data: dict[str, Any]) -> None:
        """Integrate rearranged data into this instance, mutating it in place.

        Expects the structure produced by :meth:`rearrange_data_for_hydration`:
        ``{root_pk: {cols..., ref: {pk: {...}}, refs: {pk: {...}, pk2: {...}}, ...}}``.
        Exactly one root pk must be present (this instance's row).

        - Scalar columns: parsed via :meth:`Column.parse` and assigned to ``self``.
        - Single reference: ``ref: {pk: {cols...}}`` → one nested instance, recursively integrated.
        - Collection reference: ``refs: {pk: {...}, pk2: {...}}`` → list of nested instances.

        Example for Author with one book and two kids.
        rearranged_data::
            {1: {"id": 1, "name": "Alice", "book": {10: {"id": 10, "title": "Python 101"}}, "kids": {1: {"id": 1, "x": 10}, 2: {"id": 2, "x": 20}}}}
        Mutates ``self``: ``id=1``, ``name="Alice"``, ``book=<Book id=10>``, ``kids=[<Kid id=1>, <Kid id=2>]``.
        """
        if not rearranged_data: 
            return
        assert len(rearranged_data) == 1
        root_pk = list(rearranged_data.keys())[0]
        values = rearranged_data[root_pk]
        table = self.__class__
        for key, value in values.items():
            column = table._get_column(key)
            if column.is_reference:
                if column.secondary_type is not None and column.base_type not in (list, tuple, set):
                    raise ValueError("Unexpected reference type in integrate_data_for_hydration")
                if not isinstance(value, (dict, defaultdict)):
                    value = column.parse(value)
                else:
                    if column.is_collection:
                        nested_table = column.reference_type
                        nested_instances = []
                        for nested_key, nested_value in value.items():
                            nested_instance = nested_table.make_empty_instance(nested_key)
                            nested_instance.integrate_data_for_hydration({nested_key: nested_value})
                            nested_instances.append(nested_instance)
                        value = nested_instances
                    else:
                        nested_table = column.reference_type
                        nested_key, nested_value = next(iter(value.items()))
                        nested_instance = nested_table.make_empty_instance(nested_key)
                        nested_instance.integrate_data_for_hydration(value)
                        value = nested_instance
            else:
                value = column.parse(value)
            self.__dict__[key] = value
        for key, column in table._get_columns().items():
            if key not in values and column.is_reference and column.secondary_type is not None:
                self.__dict__[key] = []

    def hydrate_with(self, unparsed_data: list[dict[str, Any]]) -> None:
        """Hydrate a Table instance from a row dict mapping column alias to unparsed value."""
        rearranged_data = self.rearrange_data_for_hydration(unparsed_data)
        self.integrate_data_for_hydration(rearranged_data)
    
    def make_instance(self, unparsed_data: list[dict[str, Any]]) -> "Table":
        """Make a new instance of the table from unparsed data."""
        instance = self.make_empty_instance(id=None)
        instance.hydrate_with(unparsed_data)
        return instance

    @classmethod
    def process_data(cls, data: dict, for_filtering: bool = False) -> dict:
        """Convert dict to DB-ready form (serialize refs; 1 field ↔ 1 column)."""
        data = {**data}
        for name in list(data):
            value = data.pop(name)
            try:
                column = cls._get_column(name)
            except KeyError as exc:
                raise ValueError(
                    f"Invalid key found in data for {cls.__name__}: {name}"
                ) from exc
            # Refs: serialize to single column (id, [ids], or JSON)
            if column.is_reference:
                serialized = column.serialize(value, for_filtering=for_filtering)
                if column.sql_is_json and isinstance(serialized, (dict, list)):
                    serialized = json.dumps(serialized, ensure_ascii=False)
                data[name] = serialized
            elif isinstance(value, BaseModel):
                data[name] = value.model_dump(mode="json")
            else:
                data[name] = column.serialize(value, for_filtering=for_filtering)
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
    def get_column_expression(cls, name: str):
        """Return the ColumnExpression or TableExpression for the given column name (e.g. for query building)."""
        return cls._expression[name]

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
