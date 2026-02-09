"""Detect whether a value is a type annotation (for serialization and validation)."""

from typing import get_origin, get_args, Union


def is_type_annotation(annotation) -> bool:
    """Return True if annotation is a type or generic type (e.g. list[int], Union)."""
    origin = get_origin(annotation)
    args = get_args(annotation)

    # Bare types like `str`, `int`, `MyClass`
    if isinstance(annotation, type):
        return True

    # Generic types like list[int], dict[str, int], MyClass[...]
    if origin is not None and isinstance(origin, type):
        return True

    # Union types like Union[int, str, None]
    if origin is Union:
        return all(is_type_annotation(arg) for arg in args)

    return False
