"""Column metadata for Table models.

After a Table subclass is created, each model field is represented by a Column
instance stored in TableSubClass._columns: dict[str, Column]. The same objects
are referenced there (no duplicate Column per field). Column holds the same
metadata as the previous Field type (types, defaults, SQL, serialize/parse).
"""

from __future__ import annotations

import enum
import json
import inspect
import datetime
from typing import Optional, Any, Iterable
from functools import cache

from pydantic import BaseModel
from pydantic.fields import FieldInfo as PydanticFieldInfo
from pydantic_core import PydanticUndefined

from .utils.get_base_type import get_base_type
from .utils.resolve_type import resolve_type
from .utils.rebuild_pydantic_model import rebuild_pydantic_model
from .utils.make_hashable import make_hashable
from .utils.supermodel import to_json_schema, from_json_schema
from .utils.serialize import serialize


# Type alias for JSON-serializable values
JSON = Any


class Column:
    """Metadata for a single table column: types, defaults, SQL, serialize/parse.

    Stored in MyTable._columns["name"]; same API as the previous Field type.
    """

    def __init__(
        self,
        table: type["Table"],
        name: str,
        base_type: type,
        secondary_type: Optional[type],
        full_type: type,
        default: Any,
        is_required: bool,
        column_is_required: bool,
        is_reference: bool,
    ) -> None:
        self.table = table
        self.name = name
        self.base_type = base_type
        self.secondary_type = secondary_type
        self.full_type = full_type
        self.default = default
        self.is_required = is_required
        self.column_is_required = column_is_required
        self.is_reference = is_reference

    @property
    @cache
    def sql_is_json(self) -> bool:
        """True if this column is stored as JSON in the database."""
        if (
            issubclass(self.base_type, BaseModel)
            or self.base_type in (list, dict, type)
            or self.full_type == JSON
        ):
            return True
        return False

    @property
    @cache
    def reference_type(self) -> type | None:
        """The referenced Table type for reference columns; None otherwise."""
        if not self.is_reference:
            return None
        if self.secondary_type is None:
            return self.base_type
        return self.secondary_type

    @property
    @cache
    def column_name(self) -> str:
        """Database column name (e.g. name_id for references)."""
        if self.is_reference:
            return f"{self.name}_id"
        return self.name

    @property
    @cache
    def column_base_type(self) -> type:
        """Python type used for the stored column (e.g. int for reference IDs)."""
        if self.is_reference:
            return int
        return self.base_type

    @classmethod
    def from_pydantic_info(
        cls,
        table: type["Table"],
        name: str,
        info: PydanticFieldInfo,
    ) -> Column:
        """Build a Column from Pydantic field info for the given table and name."""
        # Avoid importing Table here (circular); treat as table if it has table-like attributes
        def _is_reference(t: type) -> bool:
            return (
                inspect.isclass(t)
                and getattr(t, "_get_table_name", None) is not None
                and getattr(t, "model_fields", None) is not None
            )
        resolved_type = resolve_type(info.annotation)
        base_type, secondary_types, column_is_required = get_base_type(resolved_type)
        none_type = type(None)
        secondary_types = [st for st in secondary_types if st is not none_type]
        secondary_types_count = len(set(secondary_types))
        if secondary_types_count == 0:
            secondary_type = None
        elif base_type == dict and secondary_types_count == 2:
            secondary_type = secondary_types
        elif secondary_types_count == 1:
            secondary_type = secondary_types[0]
        else:
            raise ValueError(
                f"{table.__name__}.{name}: secondary_types={secondary_types} base_type={base_type}"
            )
        secondary_type = secondary_types[0] if secondary_types else None
        default = None if info.default == PydanticUndefined else info.default
        if info.default_factory:
            default = info.default_factory()

        is_reference = _is_reference(base_type) or (
            secondary_type is not None and _is_reference(secondary_type)
        )
        return cls(
            table=table,
            name=name,
            base_type=base_type,
            secondary_type=secondary_type,
            full_type=info.annotation,
            default=default,
            column_is_required=column_is_required,
            is_required=column_is_required and info.is_required(),
            is_reference=is_reference,
        )

    @property
    def sql_creations(self) -> Iterable[str]:
        """Yield SQL column definition fragments (e.g. \"name TEXT NOT NULL\")."""
        # Only emit _table column for polymorphic ref (base_type is Table), not for concrete refs
        base_is_table = (
            getattr(self.base_type, "__name__", None) == "Table"
            and getattr(self.base_type, "_get_table_name", None) is not None
        )
        sec_is_table = (
            self.secondary_type is not None
            and getattr(self.secondary_type, "__name__", None) == "Table"
            and getattr(self.secondary_type, "_get_table_name", None) is not None
        )
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

        if self.is_reference:
            if self.secondary_type is None:
                if base_is_table:
                    yield f"{self.name}_table TEXT{sql_null}{sql_default}"
                yield f"{self.name}_id INTEGER{sql_null}{sql_default}"
            elif issubclass(self.base_type, (list, tuple, set)):
                if sec_is_table:
                    yield f"{self.name}_tables JSON{sql_null}{sql_default}"
                yield f"{self.name}_ids JSON{sql_null}{sql_default}"
            else:
                raise Exception(self.base_type)
            return

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
        if inspect.isclass(self.column_base_type) and issubclass(
            self.column_base_type, enum.Enum
        ):
            enum_members = list(self.column_base_type)
            names = "', '".join(e.name for e in enum_members)
            check = f"{self.column_name} in ('{names}')"
            sql = f"{self.column_name} TEXT CHECK({check})"
        elif inspect.isclass(self.column_base_type) and issubclass(
            self.column_base_type, BaseModel
        ):
            sql = f"{self.column_name} JSON"
        elif self.column_base_type == JSON:
            sql = f"{self.column_name} JSON DEFAULT 'null'"
        elif self.column_base_type in translate_type:
            sql = f"{self.column_name} {translate_type[self.column_base_type]}"
        else:
            raise TypeError(
                f"Type `{self.column_base_type}` of "
                f"`{self.table.__name__}.{self.column_name}` has no known conversion to SQL type"
            )
        yield sql + sql_null + sql_default

    def __hash__(self) -> int:
        return hash(
            make_hashable(
                (
                    self.table,
                    self.name,
                    self.base_type,
                    self.secondary_type,
                    self.full_type,
                    self.default,
                    self.is_required,
                    self.column_is_required,
                    self.is_reference,
                )
            )
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Column):
            return NotImplemented
        return (
            self.table is other.table
            and self.name == other.name
            and self.base_type is other.base_type
            and self.secondary_type is other.secondary_type
            and self.full_type == other.full_type
            and self.default == other.default
            and self.is_required == other.is_required
            and self.column_is_required == other.column_is_required
            and self.is_reference == other.is_reference
        )

    def serialize(self, value: Any, for_filtering: bool = False) -> Any:
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

    def parse(self, value: Any) -> Any:
        """Convert a database value back to the column's Python type."""
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
        raise ValueError(
            f"Cannot parse value `{value}` of type `{type(value)}` "
            f"for column `{self.name}`"
        )


__all__ = ["Column", "JSON"]
