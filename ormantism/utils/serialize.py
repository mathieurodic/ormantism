"""Recursive serialization of nested structures to JSON-serializable types.

DEPRECATED: Use ormantism.utils.schema instead.
"""

import warnings

from .schema import serialize

warnings.warn(
    "ormantism.utils.serialize is deprecated; use ormantism.utils.schema instead",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["serialize"]
