"""Base Dialect type: subclasses implement connect() for each engine."""

from abc import ABC, abstractmethod
from typing import Any, Callable, ClassVar

from pydantic import BaseModel


class _DialectF:
    """Helper for dialect.f: __getattr__ returns the callable from the dialect's F config."""

    __slots__ = ("_dialect",)

    def __init__(self, dialect: "Dialect") -> None:
        self._dialect = dialect

    def __getattr__(self, name: str) -> Callable[..., Any]:
        F = type(self._dialect).F  # pylint: disable=invalid-name
        if name in F:
            return F[name]
        raise AttributeError(name)


class Dialect(BaseModel, ABC):
    """Base for database dialects; subclasses implement connect() for a given URL."""

    model_config = {"arbitrary_types_allowed": True}

    SUPPORTED_SCHEMA: ClassVar[tuple[str, ...]] = ()
    """URL schemes this dialect handles (e.g. ('sqlite',), ('mssql', 'sqlserver'))."""

    F: ClassVar[dict[str, Callable[..., Any]]] = {}
    """Dialect-specific SQL helpers (e.g. concat). Access via dialect.f.concat(a, b, c)."""

    @property
    def f(self) -> _DialectF:
        """Access dialect-specific helpers by name (e.g. self.f.concat(a, b, c))."""
        return _DialectF(self)

    @abstractmethod
    def connect(self, url: str) -> Any:
        """Return a new raw driver connection for the given URL.

        The return value is engine-specific (e.g. sqlite3.Connection, pymysql.Connection).
        """
        ...  # pylint: disable=unnecessary-ellipsis
