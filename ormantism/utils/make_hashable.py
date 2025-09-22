import inspect
import typing
import enum
from pydantic import BaseModel


def make_hashable(thing: any):
    # enums
    if isinstance(thing, enum.Enum):
        return (thing.name, thing.value)
    # pre-transform Pydantic model instances
    if isinstance(thing, BaseModel):
        thing = thing.model_dump()
    # dicts
    if isinstance(thing, dict):
        return tuple(
            (key, make_hashable(value))
            for key, value
            in sorted(thing.items(), key=lambda item: item[0])
        )
    # collections
    if isinstance(thing, (list, tuple, set)):
        return tuple(make_hashable(value) for value in thing)
    # scalar types
    if isinstance(thing, (int, float, str, type(None))):
        return thing
    # classes
    if inspect.isclass(thing) or isinstance(thing, type) or typing.get_origin(thing):
        return thing
    # forward refs
    if isinstance(thing, typing.ForwardRef):
        return thing.__forward_arg__
    # forward refs
    if typing.get_origin(thing) == typing.Union:
        return thing.__args__
    # other
    raise ValueError(f"Cannot hash `{thing}`, {type(thing)}, {typing.get_origin(thing)}")

if __name__ == "__main__":
    from pydantic import Field

    class SubTest(BaseModel):
        bar: dict = {"a": 1, "b": 2, "c": {"d": 4, "e": 5}}

    class Test(BaseModel):
        foo: int = 42
        sub: SubTest = Field(default_factory=SubTest)

    test = Test()
    print(hash(make_hashable(test)))
