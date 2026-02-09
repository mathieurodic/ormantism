"""Field metadata and SQL/serialization helpers for table columns."""

from __future__ import annotations
import enum
import json
import inspect
import datetime
from typing import Optional, Any, Iterable
from functools import cache
from dataclasses import dataclass, asdict

from pydantic import BaseModel
from pydantic.fields import FieldInfo as PydanticFieldInfo
from pydantic_core import PydanticUndefined

from .utils.get_base_type import get_base_type
from .utils.resolve_type import resolve_type
from .utils.rebuild_pydantic_model import rebuild_pydantic_model
from .utils.make_hashable import make_hashable
from .utils.supermodel import to_json_schema, from_json_schema
from .utils.serialize import serialize


# Define some custom types
# JSON: None | bool | int | float | str | list["JSON"] | dict[str, "JSON"]
JSON = Any


@dataclass
class Field:
    """Metadata for a single table column: types, defaults, and reference info."""

    table: type["Table"]
    name: str
    base_type: type
    secondary_type: Optional[type]
    full_type: type
    default: any
    is_required: bool
    column_is_required: bool
    is_reference: bool

    @property
    @cache
    def sql_is_json(self) -> bool:
        """True if this field is stored as JSON in the database."""
        if (issubclass(self.base_type, BaseModel)
                or self.base_type in (list, dict, type)
                or self.full_type == JSON):
            return True
        return False

    @property
    @cache
    def reference_type(self) -> type:
        """The referenced Table type for reference fields; None otherwise."""
        if not self.is_reference:
            return None
        if self.secondary_type is None:
            return self.base_type
        return self.secondary_type

    @property
    @cache
    def column_name(self):
        """Database column name (e.g. name_id for references)."""
        if self.is_reference:
            return f"{self.name}_id"
        return self.name

    @property
    @cache
    def column_base_type(self):
        """Python type used for the stored column (e.g. int for reference IDs)."""
        if self.is_reference:
            return int
        return self.base_type

    @classmethod
    def from_pydantic_info(cls, table: type["Table"], name: str,
                           info: PydanticFieldInfo):
        """Build a Field from a Pydantic field info for the given table and name."""
        from .table import Table
        resolved_type = resolve_type(info.annotation)
        base_type, secondary_types, column_is_required = get_base_type(
            resolved_type
        )
        none_type = type(None)
        secondary_types = [
            st for st in secondary_types if st is not none_type
        ]
        secondary_types_count = len(set(secondary_types))
        if secondary_types_count == 0:
            secondary_type = None
        elif base_type == dict and secondary_types_count == 2:
            secondary_type = secondary_types
        elif secondary_types_count == 1:
            secondary_type = secondary_types[0]
        else:
            raise ValueError(f"{table.__name__}.{name}: {secondary_types=} ({base_type=})")
        secondary_type = secondary_types[0] if secondary_types else None
        default = None if info.default == PydanticUndefined else info.default
        if info.default_factory:
            default = info.default_factory()

        def _is_reference(t):
            return inspect.isclass(t) and issubclass(t, Table)

        is_reference = _is_reference(base_type) or _is_reference(secondary_type)
        return cls(table=table,
                   name=name,
                   base_type=base_type,
                   secondary_type=secondary_type,
                   full_type=info.annotation,
                   default=default,
                   column_is_required=column_is_required,
                   is_required=column_is_required and info.is_required(),
                   is_reference=is_reference)

    @property
    def sql_creations(self) -> Iterable[str]:
        """Yield SQL column definition fragments (e.g. \"name TEXT NOT NULL\")."""
        # null, default
        sql_null = " NOT NULL" if self.column_is_required else ""
        if self.default is not None:
            serialized = self.serialize(self.default)
            if isinstance(serialized, (int, float)):
                serialized = str(serialized)
            else:
                if not isinstance(serialized, str):
                    serialized = json.dumps(serialized, ensure_ascii=False)
                serialized = "'" + serialized.replace("'", "''") + "'"
            sql_default = f" DEFAULT {serialized}"
        else:
            sql_default = ""

        # references
        if self.is_reference:
            from .table import Table
            # scalar reference
            if self.secondary_type is None:
                if self.base_type is Table:
                    yield f"{self.name}_table TEXT{sql_null}{sql_default}"
                yield f"{self.name}_id INTEGER{sql_null}{sql_default}"
            # list of references
            elif issubclass(self.base_type, (list, tuple, set)):
                if self.secondary_type == Table:
                    yield f"{self.name}_tables JSON{sql_null}{sql_default}"
                yield f"{self.name}_ids JSON{sql_null}{sql_default}"
            # whut?
            else:
                raise Exception(self.base_type)
            return

        # otherwise, only one column to create
        translate_type = {
            bool: "BOOLEAN",
            int: "INTEGER",
            float: "REAL",
            str: "TEXT",
            datetime.datetime: "TIMESTAMP",
            list: "JSON",
            set: "JSON",
            dict: "JSON",
            type[BaseModel]: "JSON",
            type: "JSON",
        }
        if (inspect.isclass(self.column_base_type)
                and issubclass(self.column_base_type, enum.Enum)):
            enum_members = list(self.column_base_type)
            names = "', '".join(e.name for e in enum_members)
            check = f"{self.column_name} in ('{names}')"
            sql = f"{self.column_name} TEXT CHECK({check})"
        elif (inspect.isclass(self.column_base_type)
                and issubclass(self.column_base_type, BaseModel)):
            sql = f"{self.column_name} JSON"
        elif self.column_base_type == JSON:
            sql = f"{self.column_name} JSON DEFAULT 'null'"
        elif self.column_base_type in translate_type:
            sql = f"{self.column_name} {translate_type[self.column_base_type]}"
        else:
            raise TypeError(
                f"Type `{self.column_base_type}` of "
                f"`{self.table.__name__}.{self.column_name}` has no known "
                "conversion to SQL type"
            )

        # final result
        yield sql + sql_null + sql_default


    def __hash__(self):
        """Hash for use in sets and as dict keys."""
        return hash(make_hashable(tuple(asdict(self).items())))

    # conversion

    def serialize(self, value: any, for_filtering: bool = False):
        """Convert a Python value to a database-ready form (e.g. JSON or ID)."""
        if self.is_reference:
            if self.secondary_type is None:
                return value.id if value else None
            return [v.id for v in value]
        if self.base_type == JSON:
            return json.dumps(value, ensure_ascii=False)
        if self.base_type == type:
            return to_json_schema(value)
        return serialize(value)

    def parse(self, value: any):
        """Convert a database value back to the field's Python type."""
        if value is None:
            return None
        if issubclass(self.base_type, enum.Enum):
            return self.base_type[value]
        if self.base_type == JSON:
            if not isinstance(value, str):
                return value
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        if self.base_type in (dict, list):
            return json.loads(value)
        if self.base_type in (set, tuple):
            return self.base_type(json.loads(value))
        if issubclass(self.base_type, BaseModel):
            return self.base_type(**json.loads(value))
        if self.base_type in (int, float, str, bool):
            return self.base_type(value)
        if self.base_type == datetime.datetime and isinstance(value, str):
            return datetime.datetime.fromisoformat(value)
        if self.base_type == type and not isinstance(value, type):
            if isinstance(value, str):
                value = json.loads(value)
            if not isinstance(value, dict):
                raise ValueError("Type representation should be stored as a `dict`")
            if self.full_type in (type[BaseModel], Optional[type[BaseModel]]):
                return rebuild_pydantic_model(value)
            return from_json_schema(value)
            # raise Exception(value)
        raise ValueError(
            f"Cannot parse value `{value}` of type `{type(value)}` "
            f"for field `{self.name}`"
        )
