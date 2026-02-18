"""SQL expression types for query building.

This module provides a tree of expression types used to build SELECT, WHERE,
and ORDER BY clauses in a SQLAlchemy-like style. Each Table subclass gets
class-level attributes per column/relationship (e.g. ``User.id``, ``User.books``,
``User.books.title``). Combine them with operators (``==``, ``<``, ``.in_(...)``)
and logic (``&``, ``|``). Each expression has a ``.sql`` property (SQL fragment
with ``?`` placeholders) and ``.values`` (tuple of bound values in the same order).
"""

from typing import Any

from ._bases import ArgumentedExpression, Expression
from .column import ColumnExpression
from .function import FunctionExpression
from .join_paths import collect_join_paths_from_expression
from .like import LikeExpression
from .nary_operator import NaryOperatorExpression
from .order import OrderExpression
from .table import ALIAS_SEPARATOR, TableExpression
from .unary_operator import UnaryOperatorExpression

# Avoid circular import; Table is only used for type hints and runtime model refs.
TableType = Any

# Resolve forward references for self-referential models
TableExpression.model_rebuild()
OrderExpression.model_rebuild()

__all__ = [
    "ALIAS_SEPARATOR",
    "ArgumentedExpression",
    "ColumnExpression",
    "Expression",
    "FunctionExpression",
    "LikeExpression",
    "NaryOperatorExpression",
    "OrderExpression",
    "TableExpression",
    "TableType",
    "UnaryOperatorExpression",
    "collect_join_paths_from_expression",
]
