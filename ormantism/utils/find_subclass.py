"""Discover subclasses by name for dynamic type resolution."""

from typing import Iterable


def _get_subclasses(base: type) -> Iterable[type]:
    """Recursively yield all subclasses of base in depth-first order."""
    for subclass in base.__subclasses__()[::-1]:
        yield from _get_subclasses(subclass)
        yield subclass


def find_subclass(base: type, name: str):
    """Return the unique subclass of base with __name__ == name, or None.

    Raises if multiple subclasses match.
    """
    subclasses = []
    for subclass in _get_subclasses(base):
        if subclass.__name__ == name:
            subclasses.append(subclass)
    if len(subclasses) > 1:
        raise ValueError(f"More than one subclass of `{base.__name__}` found with name `{name}`")
    if len(subclasses) == 0:
        return None
    return subclasses[0]
