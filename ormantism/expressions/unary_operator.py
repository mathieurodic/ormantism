"""Unary operator expression."""

from ._bases import ArgumentedExpression


class UnaryOperatorExpression(ArgumentedExpression):
    """Single-argument operator, prefix or postfix (e.g. ``NOT x``, ``x IS NULL``)."""

    postfix: bool = False
    """If True, render as ``argument symbol``; else ``symbol argument``."""

    @property
    def sql(self) -> str:
        argument = self._argument_to_sql(self.arguments[0])
        if self.postfix:
            return f"{argument} {self.symbol}"
        return f"{self.symbol} {argument}"
