"""N-ary operator expression."""

from ._bases import ArgumentedExpression


class NaryOperatorExpression(ArgumentedExpression):
    """N-argument operator (e.g. ``=``, ``AND``, ``||`` for concat)."""

    @property
    def sql(self) -> str:
        if not self.symbol:
            raise ValueError("NaryOperatorExpression must have a symbol")
        if not self.arguments:
            raise ValueError("NaryOperatorExpression must have at least one argument")
        parts = tuple(map(self._argument_to_sql, self.arguments))
        return "(" + (" " + self.symbol + " ").join(parts) + ")"
