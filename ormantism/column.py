"""Column metadata for Table models.

After a Table subclass is created, each model attribute is represented by a Column
instance stored in TableSubClass._columns: dict[str, Column]. The same objects
are referenced there (no duplicate Column per attribute). Column holds types,
defaults, SQL, serialize/parse.
"""

from __future__ import annotations

import enum
import json
import inspect
import datetime
from typing import Optional, Any, Iterable
from functools import cache

from pydantic import BaseModel, ConfigDict
from pydantic.fields import FieldInfo as PydanticFieldInfo
from pydantic_core import PydanticUndefined

from .utils.get_base_type import get_base_type
from .utils.get_table_by_name import get_table_by_name
from .utils.resolve_type import resolve_type
from .utils.is_table import is_table, is_polymorphic_table
from .utils.schema import rebuild_pydantic_model, serialize, to_json_schema, from_json_schema
from .utils.make_hashable import make_hashable


# Type alias for JSON-serializable values
JSON = Any


class Column(BaseModel):
    """Metadata for a single table column: types, defaults, SQL, serialize/parse.

    Stored in MyTable._columns["name"].
    """

    model_config = ConfigDict(frozen=True)

    table: Any  # type["Table"] - avoids circular import / forward ref
    name: str
    base_type: type
    secondary_type: Optional[type]
    full_type: Any  # Full annotation: list[str], X | None, type[BaseModel], etc.
    default: Any
    is_required: bool
    column_is_required: bool
    is_reference: bool

    @property
    @cache
    def sql_is_json(self) -> bool:
        """True if this column is stored as JSON in the database."""
        if self._is_polymorphic_ref:
            return True
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
    def is_collection(self) -> bool:
        """True if this is a collection reference (list/tuple/set of Table)."""
        return self.secondary_type is not None and self.base_type in (list, tuple, set)

    @property
    @cache
    def _is_polymorphic_ref(self) -> bool:
        """True if this is a polymorphic ref (Table or list[Table]) stored as JSON."""
        return is_polymorphic_table(self.base_type) or (
            self.secondary_type is not None and is_polymorphic_table(self.secondary_type)
        )

    @property
    @cache
    def column_base_type(self) -> type:
        """Python type used for the stored column (e.g. int for scalar ref, JSON for list ref)."""
        if self._is_polymorphic_ref:
            return dict  # JSON stored as dict/list
        if self.is_reference:
            return int if self.secondary_type is None else list  # scalar: int; list: JSON
        return self.base_type

    @classmethod
    def from_pydantic_info(
        cls,
        table: type["Table"],
        name: str,
        info: PydanticFieldInfo,
    ) -> Column:
        """Build a Column from Pydantic field info for the given table and attribute name."""
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

        is_reference = is_table(base_type) or (
            secondary_type is not None and is_table(secondary_type)
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
        """Yield SQL column definition fragments (e.g. \"name TEXT NOT NULL\"). One per column."""
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
            # Polymorphic refs: JSON {"table": "...", "id": N} or [{...}, ...]
            # Non-polymorphic scalar: INTEGER; non-polymorphic list: JSON [id1, id2, ...]
            if self._is_polymorphic_ref:
                yield f"{self.name} JSON{sql_null}{sql_default}"
            elif self.secondary_type is None:
                yield f"{self.name} INTEGER{sql_null}{sql_default}"
            elif self.base_type in (list, tuple, set):
                yield f"{self.name} JSON{sql_null}{sql_default}"
            else:
                raise TypeError(
                    f"Reference list must use list/tuple/set, not {self.base_type}"
                )
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
            check = f"{self.name} in ('{names}')"
            sql = f"{self.name} TEXT CHECK({check})"
        elif inspect.isclass(self.column_base_type) and issubclass(
            self.column_base_type, BaseModel
        ):
            sql = f"{self.name} JSON"
        elif self.column_base_type == JSON:
            sql = f"{self.name} JSON DEFAULT 'null'"
        elif self.column_base_type in translate_type:
            sql = f"{self.name} {translate_type[self.column_base_type]}"
        else:
            raise TypeError(
                f"Type `{self.column_base_type}` of "
                f"`{self.table.__name__}.{self.name}` has no known conversion to SQL type"
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
            if self._is_polymorphic_ref:
                if self.secondary_type is None:
                    # Scalar polymorphic: {"table": "book", "id": 42}
                    if value is None:
                        return None
                    return {"table": value._get_table_name(), "id": value.id}
                # List polymorphic: [{"table": "book", "id": 42}, ...]
                items = value or []
                return [{"table": v._get_table_name(), "id": v.id} for v in items]
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
            if self.is_reference and self.secondary_type is not None:
                return []
            return None
        if self.is_reference:
            if self._is_polymorphic_ref:
                if isinstance(value, str):
                    value = json.loads(value)
                # Polymorphic: create skeleton instances via make_empty_instance
                if value is None:
                    return None
                if isinstance(value, dict) and "table" in value and "id" in value:
                    ref_type = get_table_by_name(value["table"]) if value.get("table") else None
                    return ref_type.make_empty_instance(value["id"]) if ref_type else None
                if isinstance(value, list):
                    result = []
                    for it in value:
                        if isinstance(it, dict) and it.get("table") and it.get("id"):
                            ref_type = get_table_by_name(it["table"])
                            result.append(
                                ref_type.make_empty_instance(it["id"]) if ref_type else it
                            )
                        else:
                            result.append(it)
                    return result
                return value
            # Non-polymorphic: create skeleton instances
            if self.secondary_type is not None and isinstance(value, str):
                value = json.loads(value)
            if self.secondary_type is None:
                return self.reference_type.make_empty_instance(value)
            return [self.reference_type.make_empty_instance(v) for v in (value or [])]
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
