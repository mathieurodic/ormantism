"""SQL expression types for query building.

This module provides a tree of expression types used to build SELECT, WHERE,
and ORDER BY clauses in a SQLAlchemy-like style. Each Table subclass gets
class-level attributes per column/relationship (e.g. ``User.id``, ``User.books``,
``User.books.title``). Combine them with operators (``==``, ``<``, ``.in_(...)``)
and logic (``&``, ``|``). Each expression has a ``.sql`` property (SQL fragment
with ``?`` placeholders) and ``.values`` (tuple of bound values in the same order).
"""

from __future__ import annotations
from typing import Any, Iterable, Optional, Tuple
from functools import cached_property

from pydantic import BaseModel, Field as PydanticField

# Avoid circular import; Table is only used for type hints and runtime model refs.
TableType = Any

ALIAS_SEPARATOR = "____"
"""String used to join path segments in SQL aliases (e.g. ``user____books``)."""


class Expression(BaseModel):
    """Base type for all SQL expression nodes.

    Subclasses must implement the ``sql`` property. The default ``values``
    is an empty tuple; expression types that contain literals override it
    to return the bound values in the same order as ``?`` placeholders in ``sql``.
    """

    model_config = {"arbitrary_types_allowed": True}

    @property
    def sql(self) -> str:
        """SQL fragment for this expression, with ``?`` for bound parameters."""
        raise NotImplementedError("Subclasses must implement `sql` property")

    @property
    def values(self) -> tuple[Any, ...]:
        """Bound values for placeholders in ``sql``, in order."""
        return ()

    @property
    def _dialect(self):
        """Dialect in scope for this expression; subclasses override or resolve from arguments."""
        raise NotImplementedError("_dialect")

    def in_(self, other: Any) -> NaryOperatorExpression:
        """Build an IN expression (e.g. ``User.id.in_([1, 2, 3])``)."""
        return NaryOperatorExpression(symbol="IN", arguments=(self, other))

    def is_(self, other: Any) -> NaryOperatorExpression:
        """Build an IS expression (e.g. for ``IS NULL``)."""
        return NaryOperatorExpression(symbol="IS", arguments=(self, other))

    def is_null(self) -> UnaryOperatorExpression:
        """Build an IS NULL expression."""
        return UnaryOperatorExpression(symbol="IS NULL", arguments=(self,), postfix=True)

    def is_not(self, other: Any) -> NaryOperatorExpression:
        """Build an IS NOT expression."""
        return NaryOperatorExpression(symbol="IS NOT", arguments=(self, other))

    def is_not_null(self) -> UnaryOperatorExpression:
        """Build an IS NOT NULL expression."""
        return UnaryOperatorExpression(symbol="IS NOT NULL", arguments=(self,), postfix=True)

    def _isnull(self, isnull: bool) -> UnaryOperatorExpression:
        """Build IS NULL or IS NOT NULL according to the boolean (for where(**{field__isnull: True/False}))."""
        return self.is_null() if isnull else self.is_not_null()

    def _iexact(self, value: Any) -> NaryOperatorExpression:
        """Case-insensitive exact match (Django's iexact): LOWER(expr) = LOWER(value) for strings."""
        if isinstance(value, str):
            return FunctionExpression(symbol="LOWER", arguments=(self,)) == value.lower()
        return self == value

    def between(self, low: Any, high: Any|None = None) -> NaryOperatorExpression:
        """Inclusive range: (expr >= low) & (expr <= high). Used for Django-style value__range=(low, high)."""
        if high is None:
            assert isinstance(low, (tuple, list)), "low must be a tuple or list"
            assert len(low) == 2, "low must be a tuple or list of two values"
            low, high = low
        else:
            assert isinstance(high, (int, float)), "high must be a number"
        return (self >= low) & (self <= high)

    def __not__(self) -> Expression:
        """Build a NOT expression."""
        return UnaryOperatorExpression(symbol="NOT", arguments=(self,))

    def __and__(self, other: Any) -> NaryOperatorExpression:
        return NaryOperatorExpression(symbol="AND", arguments=(self, other))

    def __or__(self, other: Any) -> NaryOperatorExpression:
        return NaryOperatorExpression(symbol="OR", arguments=(self, other))

    def __add__(self, other: Any) -> NaryOperatorExpression:
        return NaryOperatorExpression(symbol="+", arguments=(self, other))

    def __sub__(self, other: Any) -> NaryOperatorExpression:
        return NaryOperatorExpression(symbol="-", arguments=(self, other))

    def __neg__(self) -> UnaryOperatorExpression:
        return UnaryOperatorExpression(symbol="-", arguments=(self,))

    def __pos__(self) -> UnaryOperatorExpression:
        return UnaryOperatorExpression(symbol="+", arguments=(self,))

    def __mul__(self, other: Any) -> NaryOperatorExpression:
        return NaryOperatorExpression(symbol="*", arguments=(self, other))

    def __truediv__(self, other: Any) -> NaryOperatorExpression:
        return NaryOperatorExpression(symbol="/", arguments=(self, other))

    def __mod__(self, other: Any) -> NaryOperatorExpression:
        return NaryOperatorExpression(symbol="%", arguments=(self, other))

    def __pow__(self, other: Any) -> FunctionExpression:
        return FunctionExpression(symbol="POW", arguments=(self, other))

    def __eq__(self, other: Any) -> NaryOperatorExpression:
        return NaryOperatorExpression(symbol="=", arguments=(self, other))

    def __ne__(self, other: Any) -> NaryOperatorExpression:
        return NaryOperatorExpression(symbol="!=", arguments=(self, other))

    def __lt__(self, other: Any) -> NaryOperatorExpression:
        return NaryOperatorExpression(symbol="<", arguments=(self, other))

    def __le__(self, other: Any) -> NaryOperatorExpression:
        return NaryOperatorExpression(symbol="<=", arguments=(self, other))

    def __gt__(self, other: Any) -> NaryOperatorExpression:
        return NaryOperatorExpression(symbol=">", arguments=(self, other))

    def __ge__(self, other: Any) -> NaryOperatorExpression:
        return NaryOperatorExpression(symbol=">=", arguments=(self, other))

    def like(self, pattern: str) -> LikeExpression:
        """Build a LIKE expression (exact pattern, e.g. ``User.name.like('John')``)."""
        return LikeExpression(symbol="LIKE", arguments=(self, pattern), fuzzy_start=False, fuzzy_end=False, escape_needle=False)

    def ilike(self, pattern: str) -> LikeExpression:
        """Build a case-insensitive LIKE expression."""
        return LikeExpression(symbol="LIKE", arguments=(self, pattern), fuzzy_start=False, fuzzy_end=False, case_insensitive=True, escape_needle=False)

    def startswith(self, prefix: str) -> LikeExpression:
        """Build a LIKE expression for prefix match (e.g. ``User.name.startswith('John')``)."""
        return LikeExpression(symbol="LIKE", arguments=(self, prefix), fuzzy_start=False, fuzzy_end=True)

    def istartswith(self, prefix: str) -> LikeExpression:
        """Build a case-insensitive prefix LIKE expression."""
        return LikeExpression(symbol="LIKE", arguments=(self, prefix), fuzzy_start=False, case_insensitive=True)

    def endswith(self, suffix: str) -> LikeExpression:
        """Build a LIKE expression for suffix match (e.g. ``User.name.endswith('son')``)."""
        return LikeExpression(symbol="LIKE", arguments=(self, suffix), fuzzy_end=False)

    def iendswith(self, suffix: str) -> LikeExpression:
        """Build a case-insensitive suffix LIKE expression."""
        return LikeExpression(symbol="LIKE", arguments=(self, suffix), fuzzy_end=False, case_insensitive=True)

    def contains(self, substring: str) -> LikeExpression:
        """Build a LIKE expression for substring match (e.g. ``User.name.contains('oh')``)."""
        return LikeExpression(symbol="LIKE", arguments=(self, substring), fuzzy_end=True)

    def icontains(self, substring: str) -> LikeExpression:
        """Build a case-insensitive substring LIKE expression."""
        return LikeExpression(symbol="LIKE", arguments=(self, substring), case_insensitive=True)

    def lower(self) -> FunctionExpression:
        """Build a LOWER function call."""
        return FunctionExpression(symbol="LOWER", arguments=(self,))

    def upper(self) -> FunctionExpression:
        """Build a UPPER function call."""
        return FunctionExpression(symbol="UPPER", arguments=(self,))

    def trim(self) -> FunctionExpression:
        """Build a TRIM function call."""
        return FunctionExpression(symbol="TRIM", arguments=(self,))

    def ltrim(self) -> FunctionExpression:
        """Build a LTRIM function call."""
        return FunctionExpression(symbol="LTRIM", arguments=(self,))

    def rtrim(self) -> FunctionExpression:
        """Build a RTRIM function call."""
        return FunctionExpression(symbol="RTRIM", arguments=(self,))


class ArgumentedExpression(Expression):
    """Base for expressions that have a symbol and a tuple of arguments.

    Used by function calls (e.g. ``LOWER(x)``) and operators (e.g. ``=``, ``AND``).
    ``values`` is the concatenation of literal argument values; nested expressions
    are recursed into.
    """

    symbol: str
    arguments: Tuple[Any, ...] = PydanticField(default_factory=tuple)

    @staticmethod
    def _argument_to_sql(argument: Any) -> str:
        """Render one argument as SQL: expression's ``sql`` or ``?`` for literals."""
        if isinstance(argument, Expression):
            return argument.sql
        return "?"

    @staticmethod
    def _argument_to_values(argument: Any) -> tuple[Any, ...]:
        """Collect values for one argument: recurse into expressions, else ``(argument,)``."""
        if isinstance(argument, Expression):
            return argument.values
        # Table instance: bind its primary key value, not the instance
        if not isinstance(argument, type) and hasattr(argument, "_get_table_name") and hasattr(argument, "id"):
            return (getattr(argument, "id", argument),)
        return (argument,)

    @property
    def values(self) -> tuple[Any, ...]:
        """All literal values from arguments, in order (recursing into nested expressions)."""
        return sum(map(self._argument_to_values, self.arguments), ())

    @property
    def _dialect(self):
        """Dialect from the first argument that has one (for use in e.g. self._dialect.f.concat(...))."""
        for a in self.arguments:
            if isinstance(a, Expression):
                try:
                    return a._dialect
                except NotImplementedError:
                    pass
        raise AttributeError("_dialect")


class FunctionExpression(ArgumentedExpression):
    """SQL function call: ``symbol(args...)`` (e.g. ``LOWER(name)``, ``POW(x, 2)``)."""

    @property
    def sql(self) -> str:
        if not self.symbol:
            raise ValueError("FunctionExpression must have a symbol")
        return self.symbol + "(" + ", ".join(map(self._argument_to_sql, self.arguments)) + ")"


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
        result = list(self._argument_to_values(self.arguments[0]))
        if self.fuzzy_start:
            result.append("%")
        result.extend(self._argument_to_values(self.arguments[1]))
        if self.fuzzy_end:
            result.append("%")
        return tuple(result)


class TableExpression(Expression):
    """Reference to a table in the FROM/JOIN chain.

    The root table has ``parent is None`` and ``path == ()``. A joined table
    has ``parent`` set and ``path`` as the sequence of field names from the root
    (e.g. ``("books", "author")``). Used to build aliases and JOIN clauses
    and to resolve column expressions.
    """

    parent: TableExpression | None = None
    """Parent table in the join chain; ``None`` for the root table."""
    table: Any  # Table subclass; avoids circular import.
    """The Table model this expression refers to."""
    path: tuple[str, ...] = PydanticField(default_factory=tuple)
    """Field names from root to this table (e.g. ``("books",)`` for ``User.books``)."""

    def get_column_expression(self, name: str) -> ColumnExpression | TableExpression:
        field = self.table._get_field(name)
        if field.is_reference:
            if name == field.column_name:
                return ColumnExpression(table_expression=self, name=field.column_name)
            return TableExpression(parent=self,
                                   table=field.reference_type,
                                   path=self.path + (name,))
        return ColumnExpression(table_expression=self, name=field.column_name)

    def __getattr__(self, name: str) -> ColumnExpression | TableExpression:
        """Chain: User.books.title -> (User.books).get_column_expression('title')."""
        if name.startswith("_"):
            raise AttributeError(name)
        return self.get_column_expression(name)

    def _isnull(self, isnull: bool) -> UnaryOperatorExpression:
        """For a relation table expression, use the parent's FK column for IS NULL / IS NOT NULL."""
        if self.parent is not None and self.path:
            field = self.parent.table._get_field(self.path[-1])
            if field.is_reference:
                fk_col = self.parent.get_column_expression(field.column_name)
                return fk_col._isnull(isnull)
        return super()._isnull(isnull)

    @property
    def path_str(self) -> str:
        """Dot-separated path (e.g. ``books``, ``books.author``)."""
        return ".".join(self.path)

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

    @cached_property
    def sql_declarations(self) -> Iterable[str]:
        if self.parent:
            assert self.path, "Path must not be empty"
            yield from self.parent.sql_declarations
            yield f"LEFT JOIN {self.table._get_table_name()} AS {self.sql_alias} ON {self.sql_alias}.id = {self.parent.sql_alias}.{self.path[-1]}_id"
        else:
            yield f"FROM {self.table._get_table_name()}"

    @property
    def _dialect(self):
        """Dialect for this table expression (from table._connection.dialect)."""
        return self.table._connection.dialect


class ColumnExpression(Expression):
    """Reference to a single column on a table (or joined table).

    Has no placeholders, so ``values`` is ``()``. Use ``sql_for_select`` when
    building a SELECT list so the column is given an alias for row mapping.
    """

    table_expression: TableExpression
    """The table (or join) this column belongs to."""
    name: str
    """Field name (e.g. ``id``, ``name``, ``author_id``)."""

    @property
    def path_str(self) -> str:
        """Dot-separated path to this column (e.g. ``id``, ``books.title``)."""
        return ".".join(self.table_expression.path + (self.name,))

    @cached_property
    def sql(self) -> str:
        """Qualified column (e.g. ``user____books.title``)."""
        return f"{self.table_expression.sql_alias}.{self.name}"

    @property
    def sql_for_select(self) -> str:
        """Column with AS alias for use in SELECT (e.g. ``t.title AS user____books____title``)."""
        return f"{self.sql} AS {self.table_expression.sql_alias}{ALIAS_SEPARATOR}{self.name}"

    @cached_property
    def desc(self) -> OrderExpression:
        """Order by this column descending (for use in ``order_by(...)``)."""
        return OrderExpression(column_expression=self, desc=True)

    @property
    def _dialect(self):
        """Dialect for this column (from table_expression.table._connection.dialect)."""
        return self.table_expression.table._connection.dialect


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


# Resolve forward references for self-referential models
TableExpression.model_rebuild()
OrderExpression.model_rebuild()

__all__ = [
    "TableExpression",
    "ColumnExpression",
    "OrderExpression",
    "LikeExpression",
    "NaryOperatorExpression",
    "collect_join_paths_from_expression",
]
