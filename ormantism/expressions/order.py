"""ORDER BY expression."""

from ._bases import Expression
from .column import ColumnExpression


class OrderExpression(Expression):
    """ORDER BY spec: one column and ascending or descending."""

    desc: bool = False
    column_expression: ColumnExpression

    @property
    def path_str(self) -> str:
        """Dot-separated path to the column (e.g. ``name``, ``books.title``)."""
        return self.column_expression.path_str

    @property
    def sql(self) -> str:
        """Column with ``DESC`` or ``ASC`` suffix."""
        return f"{self.column_expression.sql} {'DESC' if self.desc else 'ASC'}"

    @property
    def _dialect(self):
        """Dialect for this order expression (from column_expression._dialect)."""
        return self.column_expression._dialect
