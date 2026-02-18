"""SQL function call expression."""

from ._bases import ArgumentedExpression


class FunctionExpression(ArgumentedExpression):
    """SQL function call: ``symbol(args...)`` (e.g. ``LOWER(name)``, ``POW(x, 2)``)."""

    @property
    def sql(self) -> str:
        if not self.symbol:
            raise ValueError("FunctionExpression must have a symbol")
        return self.symbol + "(" + ", ".join(map(self._argument_to_sql, self.arguments)) + ")"
