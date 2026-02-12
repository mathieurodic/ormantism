"""LIKE expression."""

from typing import Any

from ._bases import ArgumentedExpression
from .function import FunctionExpression
from .nary_operator import NaryOperatorExpression


class LikeExpression(ArgumentedExpression):
    """LIKE expression (e.g. ``name LIKE '%John%'``)."""

    case_insensitive: bool = False
    fuzzy_start: bool = True
    fuzzy_end: bool = True
    escape_needle: bool = True
    """When True (default), the pattern is escaped for LIKE (%, _, \\) via dialect.f.escape_for_like."""

    @property
    def sql(self) -> str:
        """Build (column LIKE pattern_expr) so .sql and .values stay in sync."""
        assert len(self.arguments) == 2, "LikeExpression must have two arguments"
        haystack = self.arguments[0]
        needle = (
            self._dialect.f.escape_for_like(self.arguments[1])
            if self.escape_needle
            else self.arguments[1]
        )
        if self.case_insensitive:
            haystack = FunctionExpression(symbol="LOWER", arguments=(haystack,))
            needle = (
                needle.lower() if isinstance(needle, str) else
                FunctionExpression(symbol="LOWER", arguments=(needle,))
            )
        if self.fuzzy_start:
            needle = self._dialect.f.concat("%", needle)
        if self.fuzzy_end:
            needle = self._dialect.f.concat(needle, "%")
        sql = NaryOperatorExpression(symbol="LIKE", arguments=(haystack, needle)).sql
        return sql

    @property
    def values(self) -> tuple[Any, ...]:
        """Bound values for the composed LIKE (same composition as .sql so placeholder count matches)."""
        result = self._argument_to_values(self.arguments[0])
        if self.fuzzy_start:
            result += ("%",)
        result += self._argument_to_values(self.arguments[1])
        if self.fuzzy_end:
            result += ("%",)
        return result
