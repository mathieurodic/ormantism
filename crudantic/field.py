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
    is_reference: bool

    @classmethod
    def from_pydantic_info(cls, name: str, info: PydanticFieldInfo):
        from .utils.get_base_type import get_base_type
        from .base import Base
        base_type, is_required = get_base_type(info.annotation)
        return cls(model=cls,
                   name=name,
                   base_type=base_type,
                   full_type=info.annotation,
                   default=None if info.default == PydanticUndefined else info.default,
                   is_required=is_required and info.is_required(),
                   is_reference=issubclass(base_type, Base))

    @property
    @cache
    def sql_declaration(self):
        translate_type = {
            int: "INTEGER",
            float: "REAL",
            str: "TEXT",
            datetime.datetime: "TIMESTAMP",
            list: "JSON",
            dict: "JSON",
            type[PydanticBaseModel]: "JSON",
        }
        if issubclass(self.base_type, enum.Enum):
            result = f"{self.name} TEXT CHECK({self.name} in ('{"', '".join(e.value for e in self.base_type)}')) NOT NULL"
        elif issubclass(self.base_type, PydanticBaseModel):
            result = f"{self.name} JSON"
        elif self.full_type == type[PydanticBaseModel]:
            result = f"{self.name} JSON"
        elif self.base_type in translate_type:
            result = f"{self.name} {translate_type[self.base_type]}"
        else:
            raise TypeError(f"Type `{self.base_type}` of `{self.model.__name__}.{self.name}` has no known conversion to SQL type")
        if self.is_required:
            result += " NOT NULL"
        return result

    def __hash__(self):
        return hash(tuple(asdict(self).items()))

    # conversion

    def serialize(self, value: any):
        if isinstance(value, (int, float, str, type(None))):
            return value
        if isinstance(value, (list, dict)):
            return json.dumps(value)
        if isinstance(value, enum.Enum):
            return value.value
        if inspect.isclass(value) and issubclass(value, PydanticBaseModel):
            return json.dumps(value.model_json_schema())
        if isinstance(value, PydanticBaseModel):
            return value.model_dump_json()
        if self.is_reference:
            name += "_id"
            return value.id if value else None
        raise ValueError(f"Cannot serialize value `{value}` of type `{type(value)}` for field `{self.name}`")

    def parse(self, value: any):
        if value is None or self.base_type in (int, float, str):
            return value
        if self.base_type in (dict, list):
            return json.loads(value)
        if self.full_type == type[PydanticBaseModel]:
            return rebuild_pydantic_model(json.loads(value))
        if issubclass(self.base_type, PydanticBaseModel):
            return self.base_type.model_construct(**json.loads(value))
        raise ValueError(f"Cannot parse value `{value}` of type `{type(value)}` for field `{self.name}`")


if __name__== "__main__":
    from pydantic import Field as PydanticField
    from .base import Base

    class Thing(Base):
        pass
    class Agent(Base):
        birthed_by: "Agent" | None = None
        name: str
        description: str
        thing: Thing
        system_input: str
        bot_name: str
        tools: list[str]
        max_iterations: int = 10
        temperature: float = 0.3
        with_input_improvement: bool = True
        conversation: list[str] = PydanticField(default_factory=list)

    for name, info in Agent.model_fields.items():
        print(Field.from_pydantic_info(name, info))
