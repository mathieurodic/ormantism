"""Database dialects: one class per engine (SQLite, MySQL, PostgreSQL, SQL Server)."""

from .base import Dialect
from .sqlite import SqliteDialect
from .mysql import MysqlDialect
from .postgres import PostgresDialect
from .sqlserver import SqlserverDialect

_DIALECT_CLASSES: tuple[type[Dialect], ...] = (
    SqliteDialect,
    MysqlDialect,
    PostgresDialect,
    SqlserverDialect,
)


def get_dialect_for_scheme(scheme: str) -> Dialect:
    """Return a Dialect instance for the given URL scheme (e.g. 'sqlite', 'mysql')."""
    normalized = (scheme or "").split("+")[0].lower()
    for dialect_cls in _DIALECT_CLASSES:
        if normalized in dialect_cls.SUPPORTED_SCHEMA:
            return dialect_cls()
    raise ValueError(f"Unsupported database scheme: {scheme}")


__all__ = [
    "Dialect",
    "SqliteDialect",
    "MysqlDialect",
    "PostgresDialect",
    "SqlserverDialect",
    "get_dialect_for_scheme",
]
