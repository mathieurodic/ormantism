"""Build Pydantic models from JSON Schema (e.g. for type fields).

DEPRECATED: Use ormantism.utils.schema instead.
"""

import warnings

from .schema import get_field_type, rebuild_pydantic_model

warnings.warn(
    "ormantism.utils.rebuild_pydantic_model is deprecated; use ormantism.utils.schema instead",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["get_field_type", "rebuild_pydantic_model"]
