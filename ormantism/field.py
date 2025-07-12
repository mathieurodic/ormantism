import enum
import json
import inspect
import datetime
from functools import cache
from dataclasses import dataclass, asdict

from pydantic import BaseModel as PydanticBaseModel
from pydantic.fields import FieldInfo as PydanticFieldInfo
from pydantic_core import PydanticUndefined

from .utils.get_base_type import get_base_type
from .utils.rebuild_pydantic_model import rebuild_pydantic_model


@dataclass
class Field:
    model: "Base"
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
    def from_pydantic_info(cls, name: str, info: PydanticFieldInfo):
        from .utils.get_base_type import get_base_type
        from .base import Base
        base_type, column_is_required = get_base_type(info.annotation)
        return cls(model=cls,
                   name=name,
                   base_type=base_type,
                   full_type=info.annotation,
                   default=None if info.default == PydanticUndefined else info.default,
                   column_is_required=column_is_required,
                   is_required=column_is_required and info.is_required(),
                   is_reference=issubclass(base_type, Base))

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
            type[PydanticBaseModel]: "JSON",
        }
        if inspect.isclass(self.column_base_type) and issubclass(self.column_base_type, enum.Enum):
            sql = f"{self.column_name} TEXT CHECK({self.column_name} in ('{"', '".join(e.value for e in self.column_base_type)}'))"
        elif inspect.isclass(self.column_base_type) and issubclass(self.column_base_type, PydanticBaseModel):
            sql = f"{self.column_name} JSON"
        elif self.full_type == type[PydanticBaseModel]:
            sql = f"{self.column_name} JSON"
        elif self.column_base_type in translate_type:
            sql = f"{self.column_name} {translate_type[self.column_base_type]}"
        else:
            raise TypeError(f"Type `{self.column_base_type}` of `{self.model.__name__}.{self.column_name}` has no known conversion to SQL type")
        if self.column_is_required:
            sql += " NOT NULL"
        return sql

    def __hash__(self):
        return hash(tuple(asdict(self).items()))

    # conversion

    def serialize(self, value: any):
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float, str, type(None))):
            return value
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False, indent=0)
        if isinstance(value, enum.Enum):
            return value.value
        if self.is_reference:
            return value.id if value else None
        if inspect.isclass(value) and issubclass(value, PydanticBaseModel):
            return json.dumps(value.model_json_schema(), ensure_ascii=False, indent=0)
        if isinstance(value, PydanticBaseModel):
            return value.model_dump_json()
        raise ValueError(f"Cannot serialize value `{value}` of type `{type(value)}` for field `{self.name}`")

    def parse(self, value: any):
        if value is None:
            return None
        if self.base_type in (int, float, str, bool):
            return self.base_type(value)
        if self.base_type == datetime.datetime and isinstance(value, str):
            return datetime.datetime.fromisoformat(value)
        if self.base_type in (dict, list):
            return json.loads(value)
        if self.full_type == type[PydanticBaseModel]:
            return rebuild_pydantic_model(json.loads(value))
        if issubclass(self.base_type, PydanticBaseModel):
            return self.base_type.model_construct(**json.loads(value))
        raise ValueError(f"Cannot parse value `{value}` of type `{type(value)}` for field `{self.name}`")


if __name__== "__main__":
    from typing import Optional
    from pydantic import Field as PydanticField
    from .base import Base

    class Thing(Base):
        pass
    class Agent(Base):
        birthed_by: Optional["Agent"]
        name: str
        description: str | None
        thing: Thing
        system_input: str
        bot_name: str
        tools: list[str]
        max_iterations: int = 10
        temperature: float = 0.3
        with_input_improvement: bool = True
        conversation: list[str] = PydanticField(default_factory=list)

    for name, info in Agent.model_fields.items():
        print()
        print(name)
        print(Field.from_pydantic_info(name, info))
        print()
