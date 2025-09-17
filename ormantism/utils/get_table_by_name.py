from typing import Iterable


def _get_subclasses(base: type) -> Iterable[type]:
    for subclass in base.__subclasses__()[::-1]:
        yield from _get_subclasses(subclass)
        yield subclass


def get_all_tables() -> Iterable[type["Table"]]:
    from ..table import Table
    for cls in _get_subclasses(Table):
        yield cls


def get_table_by_name(name: str) -> type["Table"]:
    for cls in get_all_tables():
        if cls._get_table_name() == name:
            return cls
