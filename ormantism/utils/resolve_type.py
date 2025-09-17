from typing import ForwardRef, Iterable


def _get_subclasses(base: type) -> Iterable[type]:
    for subclass in base.__subclasses__():
        yield subclass
        yield from _get_subclasses(subclass)


def resolve_type(reference_type: type):
    """Resolve forward reference"""
    if not isinstance(reference_type, ForwardRef):
        return reference_type
    if reference_type.__forward_evaluated__:
        return reference_type.__forward_value__
    from ..table import Table
    for table in _get_subclasses(Table):
        if table.__name__ == reference_type.__forward_arg__:
            return table
    raise ValueError(f"Could not resolve {reference_type}")
