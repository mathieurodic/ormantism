from typing import Iterable


def _get_subclasses(base: type) -> Iterable[type]:
    for subclass in base.__subclasses__()[::-1]:
        yield from _get_subclasses(subclass)
        yield subclass


def find_subclass(base: type, name: str):
    for subclass in _get_subclasses(base):
        if subclass.__name__ == name:
            return subclass
    return None
