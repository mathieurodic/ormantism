"""Utilities for collecting join paths from expression trees."""

from typing import Any

from ._bases import Expression
from .column import ColumnExpression
from .function import FunctionExpression
from .like import LikeExpression
from .nary_operator import NaryOperatorExpression
from .order import OrderExpression
from .table import TableExpression
from .unary_operator import UnaryOperatorExpression


def collect_join_paths_from_expression(expr: Expression) -> set[str]:
    """Collect dot-separated join paths (e.g. ``books``, ``books.author``) from an expression tree."""
    paths: set[str] = set()

    def walk(e: Any) -> None:
        if isinstance(e, ColumnExpression):
            if e.table_expression.path:
                paths.add(e.table_expression.path_str)
        elif isinstance(e, TableExpression):
            if e.path:
                paths.add(e.path_str)
        elif isinstance(e, OrderExpression):
            walk(e.column_expression)
        elif isinstance(e, (NaryOperatorExpression, UnaryOperatorExpression, FunctionExpression, LikeExpression)):
            for a in e.arguments:
                if isinstance(a, Expression):
                    walk(a)

    walk(expr)
    return paths
