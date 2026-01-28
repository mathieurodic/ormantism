from typing import Iterable


def _get_subclasses(base: type) -> Iterable[type]:
    for subclass in base.__subclasses__()[::-1]:
        yield from _get_subclasses(subclass)
        yield subclass


def find_subclass(base: type, name: str):
    subclasses = []
    for subclass in _get_subclasses(base):
        if subclass.__name__ == name:
            subclasses.append(subclass)
    if len(subclasses) > 1:
        raise ValueError(f"More than one subclass of `{base.__name__}` found with name `{name}`")
    if len(subclasses) == 0:
        return None
    return subclasses[0]
