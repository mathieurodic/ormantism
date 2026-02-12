"""Table expression for FROM/JOIN references."""

from __future__ import annotations
from functools import cached_property
from typing import Any, Iterable

from pydantic import Field as PydanticField

from ._bases import Expression
from .function import FunctionExpression
from .nary_operator import NaryOperatorExpression
from .unary_operator import UnaryOperatorExpression
from ..utils.is_table import is_table

ALIAS_SEPARATOR = "____"
"""String used to join path segments in SQL aliases (e.g. ``user____books``)."""


class TableExpression(Expression):
    """Reference to a table in the FROM/JOIN chain.

    The root table has ``parent is None`` and ``path == ()``. A joined table
        has ``parent`` set and ``path`` as the sequence of column names from the root
    (e.g. ``("books", "author")``). Used to build aliases and JOIN clauses
    and to resolve column expressions.
    """

    parent: TableExpression | None = None
    """Parent table in the join chain; ``None`` for the root table."""
    table: Any  # Table subclass; avoids circular import.
    """The Table model this expression refers to."""
    path: tuple[str, ...] = PydanticField(default_factory=tuple)
    """Column names from root to this table (e.g. ``("books",)`` for ``User.books``)."""

    def __getitem__(self, name: str):
        from .column import ColumnExpression
        column = self.table._get_column(name)
        if column.is_reference:
            return TableExpression(parent=self,
                                   table=column.reference_type,
                                   path=self.path + (column.name,))
        return ColumnExpression(table_expression=self, name=column.name)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return self[name]

    @property
    def fk(self):
        """ColumnExpression for the FK that points to this table. Raises when no parent."""
        from .column import ColumnExpression
        if self.parent is None or not self.path:
            raise ValueError("fk only valid on joined table expressions")
        col = self.parent.table._get_column(self.path[-1])
        if not col.is_reference or col.secondary_type is not None:
            # actually, this should never happen (better safe than sorry)
            raise ValueError("fk only valid for scalar refs")
        return ColumnExpression(table_expression=self.parent, name=col.name)

    def __eq__(self, other: Any) -> NaryOperatorExpression:
        """Compare by id: User == user, Book.author == author, Ptr.ref == ref1 (incl. polymorphic)."""

        # enforce valid comparison types
        if not is_table(type(other)) and not isinstance(other, int) and not other is None:
            raise ValueError("Root table expression must be compared to a table instance, int, or None")

        # polymorphic FK comparison
        if self.table is None:
            # compare to root table instance
            if isinstance(other, int):
                raise ValueError("Polymorphic root table expression must be compared to a table instance, or None")
            if other is None:
                return self.fk.is_null()
            table_equality = FunctionExpression(
                symbol="json_extract",
                arguments=(self.fk, "$.table"),
            ) == other._get_table_name()
            id_equality = FunctionExpression(
                symbol="json_extract",
                arguments=(self.fk, "$.id"),
            ) == other.id
            return table_equality & id_equality

        # "normal" FK comparison (non-polymorphic)
        if is_table(type(other)) and not isinstance(other, self.table):
            raise ValueError("Root table expression must be compared to a table instance of the same type")
        column_expression = self["id"] if self.parent is None else self.fk
        if other is None:
            return column_expression.is_null()
        if isinstance(other, int):
            return column_expression == other
        return column_expression == other.id

    def __ne__(self, other: Any) -> NaryOperatorExpression:
        """Compare by id: User != user, Book.author != author, etc. (negation of __eq__)."""
        return (self == other).__not__()

    def is_null(self) -> UnaryOperatorExpression:
        """IS NULL on the FK column (for refs)."""
        return self == None

    def is_not_null(self) -> UnaryOperatorExpression:
        """IS NOT NULL on the FK column (for refs)."""
        return self != None

    is_ = __eq__

    def _isnull(self, isnull: bool) -> UnaryOperatorExpression:
        """For a relation table expression, use the parent's FK column for IS NULL / IS NOT NULL."""
        from .column import ColumnExpression
        if self.parent is not None and self.path:
            col = self.parent.table._get_column(self.path[-1])
            if col.is_reference:
                # FK column is ColumnExpression, not TableExpression
                fk_col = ColumnExpression(table_expression=self.parent, name=col.name)
                return fk_col._isnull(isnull)
        return super()._isnull(isnull)

    @property
    def path_str(self) -> str:
        """Dot-separated path (e.g. ``books``, ``books.author``)."""
        return ".".join(self.path)

    @property
    def root_table(self):
        """The root Table class for this table expression (walk up parent until root)."""
        te = self
        while te.parent is not None:
            te = te.parent
        return te.table

    @cached_property
    def sql_alias(self) -> str:
        """SQL alias for this table (e.g. ``user____books`` for path ``("books",)``)."""
        if self.parent:
            return f"{self.parent.sql_alias}{ALIAS_SEPARATOR}{self.path[-1]}"
        t = self.table
        if getattr(t, "__name__", None) == "Table" and getattr(t, "__module__", None) == "ormantism.table":
            raise ValueError(
                "Expressions must use a concrete Table subclass (e.g. MyModel.pk, MyModel.name), "
                "not the base Table class."
            )
        return t._get_table_name()

    @property
    def sql_declarations(self) -> Iterable[str]:
        if self.parent:
            assert self.path, "Path must not be empty"
            yield from self.parent.sql_declarations
            yield f"LEFT JOIN {self.table._get_table_name()} AS {self.sql_alias} ON {self.sql_alias}.id = {self.parent.sql_alias}.{self.path[-1]}"
        else:
            yield f"FROM {self.table._get_table_name()}"

    @property
    def _dialect(self):
        """Dialect for this table expression (from table._connection.dialect)."""
        return self.table._connection.dialect
