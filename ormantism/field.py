from __future__ import annotations
import enum
import types
import json
import inspect
import datetime
from typing import Optional, Any
from functools import cache
from dataclasses import dataclass, asdict

from pydantic import BaseModel
from pydantic.fields import FieldInfo as PydanticFieldInfo
from pydantic_core import PydanticUndefined

from .utils.get_base_type import get_base_type
from .utils.rebuild_pydantic_model import rebuild_pydantic_model
from .utils.make_hashable import make_hashable
from .utils.supermodel import to_json_schema, from_json_schema


# Which values are considered scalar
SCALARS = {
    bool: {"type": "boolean"},
    str: {"type": "string"},
    int: {"type": "integer"},
    float: {"type": "number"},
}


# Define some custom types
# JSON: None | bool | int | float | str | list["JSON"] | dict[str, "JSON"]
JSON = Any


@dataclass
class Field:
    table: "Table"
    name: str
    base_type: type
    full_type: type
    default: any
    is_required: bool
    column_is_required: bool
    is_reference: bool

    @property
    @cache
    def column_name(self):
        if self.is_reference:
            return f"{self.name}_id"
        return self.name

    @property
    @cache
    def column_base_type(self):
        if self.is_reference:
            return int
        return self.base_type

    @classmethod
    def from_pydantic_info(cls, table: "Table", name: str, info: PydanticFieldInfo):
        from .table import Table
        base_type, column_is_required = get_base_type(info.annotation)
        default = None if info.default == PydanticUndefined else info.default
        if info.default_factory:
            default = info.default_factory()
        return cls(table=table,
                   name=name,
                   base_type=base_type,
                   full_type=info.annotation,
                   default=default,
                   column_is_required=column_is_required,
                   is_required=column_is_required and info.is_required(),
                   is_reference=issubclass(base_type, Table))

    @property
    @cache
    def sql_declaration(self):
        translate_type = {
            bool: "BOOLEAN",
            int: "INTEGER",
            float: "REAL",
            str: "TEXT",
            datetime.datetime: "TIMESTAMP",
            list: "JSON",
            dict: "JSON",
            type[BaseModel]: "JSON",
            type: "JSON",
        }
        if inspect.isclass(self.column_base_type) and issubclass(self.column_base_type, enum.Enum):
            sql = f"{self.column_name} TEXT CHECK({self.column_name} in ('{"', '".join(e.name for e in self.column_base_type)}'))"
        elif inspect.isclass(self.column_base_type) and issubclass(self.column_base_type, BaseModel):
            sql = f"{self.column_name} JSON"
        elif self.column_base_type == JSON:
            sql = f"{self.column_name} JSON DEFAULT 'null'"
        elif self.column_base_type in translate_type:
            sql = f"{self.column_name} {translate_type[self.column_base_type]}"
        else:
            raise TypeError(f"Type `{self.column_base_type}` of `{self.table.__name__}.{self.column_name}` has no known conversion to SQL type")
        if self.column_is_required:
            sql += " NOT NULL"
        if self.default is not None:
            serialized = self.serialize(self.default)
            if isinstance(serialized, (int, float)):
                serialized = str(serialized)
            else:
                if not isinstance(serialized, str):
                    serialized = json.dumps(serialized, ensure_ascii=False)
                serialized = "'" + serialized.replace("'", "''") + "'"
            sql += f" DEFAULT {serialized}"
        return sql

    def __hash__(self):
        return hash(make_hashable(tuple(asdict(self).items())))

    # conversion

    def serialize(self, value: any):
        if self.base_type == JSON:
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float, str, type(None))):
            return value
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, enum.Enum):
            return value.name
        if self.is_reference:
            return value.id if value else None
        if isinstance(value, types.GenericAlias) or inspect.isclass(value):
            return to_json_schema(value)
        if isinstance(value, BaseModel):
            return value.model_dump_json()
        raise ValueError(f"Cannot serialize value `{value}` of type `{type(value)}` for field `{self.name}`")

    def parse(self, value: any):
        if value is None:
            return None
        if issubclass(self.base_type, enum.Enum):
            return self.base_type[value]
        if self.base_type == JSON:
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        if self.base_type in (dict, list):
            return json.loads(value)
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
        raise ValueError(f"Cannot parse value `{value}` of type `{type(value)}` for field `{self.name}`")
