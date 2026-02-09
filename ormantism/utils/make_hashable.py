"""Convert objects (including Pydantic models) to hashable, comparable form for hashing."""

import inspect
import typing
import enum
import datetime
from pydantic import BaseModel


def make_hashable(thing: any):
    """Return a hashable representation of thing (e.g. for use in hash() or as dict key)."""
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
    if isinstance(thing, (int, float, str, type(None), datetime.datetime)):
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
