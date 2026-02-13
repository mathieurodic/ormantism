"""Table model base for ORM CRUD and schema creation."""

import json
import warnings
from typing import ClassVar, Any
from functools import cache

from pydantic import BaseModel

from ..utils.make_hashable import make_hashable
from ..expressions import ColumnExpression
from ..column import Column
from .meta import TableMeta
from .mixins import _WithSoftDelete
from .hydratable import Hydratable


class Table(Hydratable, metaclass=TableMeta):
    """Base class for ORM table models; provides DB operations and identity semantics."""

    model_config = {"arbitrary_types_allowed": True}
    _ensured_table_structure: ClassVar[bool] = False

    def __eq__(self, other: "Table"):
        if not isinstance(other, self.__class__):
            raise ValueError(
                f"Comparing instances of different classes: {self.__class__} "
                f"and {other.__class__}"
            )
        return hash(self) == hash(other)

    def __hash__(self):
        raw_id = object.__getattribute__(self, "__dict__").get("id")
        if isinstance(raw_id, ColumnExpression):
            raise TypeError(
                "Cannot hash Table instance with unloaded primary key 'id'. "
                "Include 'id' in the select or load the row before using in sets/dicts."
            )
        return hash((self.__class__, raw_id))

    def __getattribute__(self, name: str) -> Any:
        d = object.__getattribute__(self, "__dict__")
        if name not in d:
            cls = object.__getattribute__(self, "__class__")
            if cls._has_column(name):
                d[name] = getattr(cls, name)
            else:
                return object.__getattribute__(self, name)
        value = d.get(name)
        if not isinstance(value, ColumnExpression):
            return value
        if name == "id":
            return None
        raw_id = object.__getattribute__(self, "__dict__").get("id")
        if isinstance(raw_id, ColumnExpression):
            raise ValueError(
                "Cannot lazy-load columns: primary key 'id' must be loaded first. "
                "Include 'id' in the select (e.g. Model.q().select('id', 'name').where(...))."
            )
        warnings.warn(
            f"Lazy loading '{name}' on {type(self).__name__}: consider preloading "
            f"(e.g. Model.q().select('{name}').where(...))",
            UserWarning,
            stacklevel=2,
        )
        rows = self.q().select(name).where(id=raw_id).rows(as_dicts=True)
        if not rows:
            raise ValueError(f"No row found for {name} with id {raw_id}")
        parsed_value = self.__class__._get_column(name).parse(rows[0].get(name))
        self.__dict__[name] = parsed_value
        return parsed_value

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}") from None

    def __deepcopy__(self, memo):
        return self

    def on_after_create(self, init_data: dict):
        self.__class__.q().insert(instance=self, init_data=init_data)

    def on_before_update(self, new_data):
        self.check_read_only(new_data)
        self.q().where(id=self.id).update(**new_data)
        self.__dict__.pop("updated_at", None)

    @classmethod
    def load_or_create(cls, _search_fields=None, **data):
        on_conflict = list(data) if _search_fields is None else _search_fields
        if not on_conflict:
            return cls(**data)
        return cls.q().upsert(on_conflict=on_conflict, **data)

    @classmethod
    @cache
    def _has_column(cls, name: str) -> bool:
        return name in cls.model_fields

    @classmethod
    def _get_columns(cls) -> dict[str, Column]:
        return getattr(cls, "_columns", {})

    @classmethod
    def _get_column(cls, name: str) -> Column:
        columns = cls._get_columns()
        if name in columns:
            return columns[name]
        for col in columns.values():
            if col.name == name:
                return col
        raise KeyError(f"No such column for {cls.__name__}: {name}")

    @classmethod
    def _get_table_sql_creations(cls) -> list[str]:
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
    def process_data(cls, data: dict, for_filtering: bool = False) -> dict:
        data = {**data}
        for name in list(data):
            value = data.pop(name)
            try:
                column = cls._get_column(name)
            except KeyError as exc:
                raise ValueError(
                    f"Invalid key found in data for {cls.__name__}: {name}"
                ) from exc
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

    def delete(self):
        self.q().where(id=self.id).delete()

    @classmethod
    def q(cls) -> "Query":
        from ..query import Query
        q = Query(table=cls)
        for c in cls.__mro__:
            transform = getattr(c, "_transform_query", Table._transform_query)
            q = transform.__func__(cls, q)
        return q

    @classmethod
    def _transform_query(cls, q: "Query") -> "Query":
        return q

    @classmethod
    def load(cls, as_collection: bool = False,
             with_deleted=False, preload: str | list[str] = [],
             **criteria) -> "Table":
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
        warnings.warn(
            "load_all() is deprecated; use cls.q().where(...).all() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return cls.load(as_collection=True, **criteria)

    @classmethod
    def _get_table_name(cls) -> str:
        return cls.__name__.lower()

    @classmethod
    def get_column_expression(cls, name: str):
        return cls._expression[name]
