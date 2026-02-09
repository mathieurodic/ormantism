"""Resolve table class by name (e.g. from stored polymorphic reference)."""

from typing import Iterable
from .find_subclass import _get_subclasses


def get_all_tables() -> Iterable[type["Table"]]:
    """Yield all Table subclasses in the application."""
    from ..table import Table
    for cls in _get_subclasses(Table):
        yield cls


def get_table_by_name(name: str) -> type["Table"]:
    """Return the Table subclass whose __name__ or _get_table_name() equals name."""
    for cls in get_all_tables():
        if name in (cls._get_table_name(), cls.__name__):
            return cls
