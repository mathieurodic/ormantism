"""Base expression types for SQL expression trees."""

from __future__ import annotations
from typing import Any, Tuple

from pydantic import BaseModel, Field as PydanticField


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

    def in_(self, other: Any):
        """Build an IN expression (e.g. ``User.id.in_([1, 2, 3])``)."""
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol="IN", arguments=(self, other))

    def is_(self, other: Any):
        """Build an IS expression (e.g. for ``IS NULL``)."""
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol="IS", arguments=(self, other))

    def is_null(self):
        """Build an IS NULL expression."""
        from .unary_operator import UnaryOperatorExpression
        return UnaryOperatorExpression(symbol="IS NULL", arguments=(self,), postfix=True)

    def is_not(self, other: Any):
        """Build an IS NOT expression."""
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol="IS NOT", arguments=(self, other))

    def is_not_null(self):
        """Build an IS NOT NULL expression."""
        from .unary_operator import UnaryOperatorExpression
        return UnaryOperatorExpression(symbol="IS NOT NULL", arguments=(self,), postfix=True)

    def _isnull(self, isnull: bool):
        """Build IS NULL or IS NOT NULL according to the boolean (for where(**{column__isnull: True/False}))."""
        return self.is_null() if isnull else self.is_not_null()

    def _iexact(self, value: Any):
        """Case-insensitive exact match (Django's iexact): LOWER(expr) = LOWER(value) for strings."""
        from .function import FunctionExpression
        if isinstance(value, str):
            return FunctionExpression(symbol="LOWER", arguments=(self,)) == value.lower()
        return self == value

    def between(self, low: Any, high: Any | None = None):
        """Inclusive range: (expr >= low) & (expr <= high). Used for Django-style value__range=(low, high)."""
        if high is None:
            assert isinstance(low, (tuple, list)), "low must be a tuple or list"
            assert len(low) == 2, "low must be a tuple or list of two values"
            low, high = low
        else:
            assert isinstance(high, (int, float)), "high must be a number"
        return (self >= low) & (self <= high)

    def __not__(self):
        """Build a NOT expression."""
        from .unary_operator import UnaryOperatorExpression
        return UnaryOperatorExpression(symbol="NOT", arguments=(self,))

    def __and__(self, other: Any):
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol="AND", arguments=(self, other))

    def __or__(self, other: Any):
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol="OR", arguments=(self, other))

    def __add__(self, other: Any):
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol="+", arguments=(self, other))

    def __sub__(self, other: Any):
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol="-", arguments=(self, other))

    def __neg__(self):
        from .unary_operator import UnaryOperatorExpression
        return UnaryOperatorExpression(symbol="-", arguments=(self,))

    def __pos__(self):
        from .unary_operator import UnaryOperatorExpression
        return UnaryOperatorExpression(symbol="+", arguments=(self,))

    def __mul__(self, other: Any):
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol="*", arguments=(self, other))

    def __truediv__(self, other: Any):
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol="/", arguments=(self, other))

    def __mod__(self, other: Any):
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol="%", arguments=(self, other))

    def __pow__(self, other: Any):
        from .function import FunctionExpression
        return FunctionExpression(symbol="POW", arguments=(self, other))

    def __eq__(self, other: Any):
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol="=", arguments=(self, other))

    def __ne__(self, other: Any):
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol="!=", arguments=(self, other))

    def __lt__(self, other: Any):
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol="<", arguments=(self, other))

    def __le__(self, other: Any):
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol="<=", arguments=(self, other))

    def __gt__(self, other: Any):
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol=">", arguments=(self, other))

    def __ge__(self, other: Any):
        from .nary_operator import NaryOperatorExpression
        return NaryOperatorExpression(symbol=">=", arguments=(self, other))

    def like(self, pattern: str):
        """Build a LIKE expression (exact pattern, e.g. ``User.name.like('John')``)."""
        from .like import LikeExpression
        return LikeExpression(symbol="LIKE", arguments=(self, pattern), fuzzy_start=False, fuzzy_end=False, escape_needle=False)

    def ilike(self, pattern: str):
        """Build a case-insensitive LIKE expression."""
        from .like import LikeExpression
        return LikeExpression(symbol="LIKE", arguments=(self, pattern), fuzzy_start=False, fuzzy_end=False, case_insensitive=True, escape_needle=False)

    def startswith(self, prefix: str):
        """Build a LIKE expression for prefix match (e.g. ``User.name.startswith('John')``)."""
        from .like import LikeExpression
        return LikeExpression(symbol="LIKE", arguments=(self, prefix), fuzzy_start=False, fuzzy_end=True)

    def istartswith(self, prefix: str):
        """Build a case-insensitive prefix LIKE expression."""
        from .like import LikeExpression
        return LikeExpression(symbol="LIKE", arguments=(self, prefix), fuzzy_start=False, case_insensitive=True)

    def endswith(self, suffix: str):
        """Build a LIKE expression for suffix match (e.g. ``User.name.endswith('son')``)."""
        from .like import LikeExpression
        return LikeExpression(symbol="LIKE", arguments=(self, suffix), fuzzy_end=False)

    def iendswith(self, suffix: str):
        """Build a case-insensitive suffix LIKE expression."""
        from .like import LikeExpression
        return LikeExpression(symbol="LIKE", arguments=(self, suffix), fuzzy_end=False, case_insensitive=True)

    def contains(self, substring: str):
        """Build a LIKE expression for substring match (e.g. ``User.name.contains('oh')``)."""
        from .like import LikeExpression
        return LikeExpression(symbol="LIKE", arguments=(self, substring), fuzzy_end=True)

    def icontains(self, substring: str):
        """Build a case-insensitive substring LIKE expression."""
        from .like import LikeExpression
        return LikeExpression(symbol="LIKE", arguments=(self, substring), case_insensitive=True)

    def lower(self):
        """Build a LOWER function call."""
        from .function import FunctionExpression
        return FunctionExpression(symbol="LOWER", arguments=(self,))

    def upper(self):
        """Build a UPPER function call."""
        from .function import FunctionExpression
        return FunctionExpression(symbol="UPPER", arguments=(self,))

    def trim(self):
        """Build a TRIM function call."""
        from .function import FunctionExpression
        return FunctionExpression(symbol="TRIM", arguments=(self,))

    def ltrim(self):
        """Build a LTRIM function call."""
        from .function import FunctionExpression
        return FunctionExpression(symbol="LTRIM", arguments=(self,))

    def rtrim(self):
        """Build a RTRIM function call."""
        from .function import FunctionExpression
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
