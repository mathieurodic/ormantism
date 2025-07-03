import inspect
from pydantic import BaseModel


def make_hashable(thing: any):
    # pre-transform Pydantic models
    if inspect.isclass(thing) and issubclass(thing, BaseModel):
        thing = thing.model_json_schema()
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
    # other
    raise ValueError(f"Cannot hash `{thing}`")

if __name__ == "__main__":
    from pydantic import Field

    class SubTest(BaseModel):
        bar: dict = {"a": 1, "b": 2, "c": {"d": 4, "e": 5}}

    class Test(BaseModel):
        foo: int = 42
        sub: SubTest = Field(default_factory=SubTest)

    test = Test()
    print(hash(make_hashable(test)))
