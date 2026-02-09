"""Resolve forward references to Table classes by name."""

from typing import ForwardRef
from .get_table_by_name import get_table_by_name


def resolve_type(reference_type: type):
    """Resolve a ForwardRef to a concrete type (e.g. Table subclass by name)."""
    if not isinstance(reference_type, ForwardRef):
        return reference_type
    if reference_type.__forward_evaluated__:
        return reference_type.__forward_value__
    cls = get_table_by_name(reference_type.__forward_arg__)
    if cls:
        return cls
    raise ValueError(f"Could not resolve {reference_type}")
