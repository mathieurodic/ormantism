"""Check if a type is a Table-like class (has _get_table_name and model_fields)."""

import inspect


def is_table(t: type) -> bool:
    """Return True if t is a Table subclass (can be used as a reference type)."""
    return (
        inspect.isclass(t)
        and getattr(t, "_get_table_name", None) is not None
        and getattr(t, "model_fields", None) is not None
    )


def is_polymorphic_table(t: type) -> bool:
    """Return True if t is the polymorphic base Table class (emits _table column)."""
    return is_table(t) and getattr(t, "__name__", None) == "Table"
