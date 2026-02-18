"""Column expression for referencing a single column on a table."""

from functools import cached_property

from ._bases import Expression
from .table import ALIAS_SEPARATOR, TableExpression


class ColumnExpression(Expression):
    """Reference to a single column on a table (or joined table).

    Has no placeholders, so ``values`` is ``()``. Use ``sql_for_select`` when
    building a SELECT list so the column is given an alias for row mapping.
    """

    table_expression: TableExpression
    """The table (or join) this column belongs to."""
    name: str
    """Column name (e.g. ``id``, ``name``, ``author``)."""

    @property
    def path_str(self) -> str:
        """Dot-separated path to this column (e.g. ``id``, ``books.title``)."""
        return ".".join(self.table_expression.path + (self.name,))

    @property
    def root_table(self):
        """The root Table class for this column (from its table expression)."""
        return self.table_expression.root_table

    @cached_property
    def sql(self) -> str:
        """Qualified column (e.g. ``user____books.title``)."""
        return f"{self.table_expression.sql_alias}.{self.name}"

    @property
    def sql_for_select(self) -> str:
        """Column with AS alias for use in SELECT (e.g. ``t.title AS user____books____title``)."""
        return f"{self.sql} AS {self.table_expression.sql_alias}{ALIAS_SEPARATOR}{self.name}"

    @cached_property
    def desc(self):
        """Order by this column descending (for use in ``order_by(...)``)."""
        from .order import OrderExpression
        return OrderExpression(column_expression=self, desc=True)

    @property
    def _dialect(self):
        """Dialect for this column (from table_expression.table._connection.dialect)."""
        return self.table_expression.table._connection.dialect
