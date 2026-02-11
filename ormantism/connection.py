"""Database connection configuration and factory for MySQL, SQLite, and PostgreSQL."""

import logging
import inspect
import urllib.parse
from typing import Any, Callable, Optional, Union

from pydantic import BaseModel

from .dialects import Dialect, get_dialect_for_scheme

logger = logging.getLogger(__name__)


class Connection(BaseModel):
    """Holds a database URL and dialect and provides execute/execute_with_column_names against it."""

    url: str
    name: str = "default"
    dialect: Dialect

    @classmethod
    def from_url(cls, url: str | Any, name: str = "default") -> "Connection":
        """Build a Connection from a URL (or callable returning a URL). Does not register it."""
        if callable(url):
            url = url()
        if not isinstance(url, str):
            raise ValueError(
                "`url` should either be a `str`, or a callable returning a `str`"
            )
        parsed = urllib.parse.urlparse(url)
        dialect = get_dialect_for_scheme(parsed.scheme or "")
        return cls(url=url, name=name, dialect=dialect)

    def _get_raw_connection(self):
        """Return a new raw driver connection for this URL (used by transaction layer)."""
        return self.dialect.connect(self.url)

    def execute(
        self,
        sql: str,
        parameters: Optional[tuple[Any, ...] | list[Any]] = None,
    ) -> list[tuple[Any, ...]]:
        """Run SQL and return fetched rows."""
        from .transaction import transaction
        if parameters is None:
            parameters = ()
        with transaction(connection_name=self.name) as t:
            cursor = t.execute(sql, parameters)
            result = cursor.fetchall()
            cursor.close()
        return result

    def execute_with_column_names(
        self,
        sql: str,
        parameters: Optional[tuple[Any, ...] | list[Any]] = None,
    ) -> tuple[list[tuple[Any, ...]], list[str]]:
        """Execute SQL and return (rows, column_names). Column names from cursor.description."""
        from .transaction import transaction
        if parameters is None:
            parameters = ()
        with transaction(connection_name=self.name) as t:
            cursor = t.execute(sql, parameters)
            column_names = [d[0] for d in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            cursor.close()
        return rows, column_names


_connections: dict[str, Connection] = {}


def connect(database_url: Union[str, Callable[..., str]], name: Optional[str] = None):
    """Register a database URL for the default or named connection and log it."""
    conn_name = name if name is not None else "default"
    try:
        _connections[conn_name] = Connection.from_url(database_url, name=conn_name)
    except ValueError as e:
        if "url" in str(e).lower() and "callable" in str(e).lower():
            raise ValueError(
                "`database_url` should either be a `str`, or a method returning a `str`"
            ) from e
        raise

    if isinstance(database_url, str):
        url_representation = repr(database_url)
    else:
        if not callable(database_url):
            raise ValueError(
                "`database_url` should either be a `str`, or a method "
                "returning a `str`"
            )
        url_representation = (
            f"({database_url.__module__}:{database_url.__code__.co_firstlineno}"
            f":{database_url.__name__})"
        )

    if conn_name != "default":
        logger.warning("Set database %s to %s", repr(conn_name), url_representation)
    else:
        logger.warning("Set default database to %s", url_representation)
    stack_frames = (
        f"{frame_info.filename}:{frame_info.lineno}"
        for frame_info in inspect.stack()
        if ".venv" not in frame_info.filename
    )
    logger.info("\n".join(stack_frames))


def _get_connection(name=None):
    """Return a live database connection for the given name (thread-local where applicable)."""
    conn_name = name if name is not None else "default"
    if conn_name not in _connections:
        raise ValueError(f"No connection configured with name=`{conn_name}`")
    return _connections[conn_name]._get_raw_connection()


class _ConnectionDescriptor:
    """Descriptor for Table._connection: resolves by _CONNECTION_NAME."""

    def __get__(self, obj: Any, owner: type | None = None) -> Connection:
        if owner is None:
            owner = type(obj)
        name = getattr(owner, "_CONNECTION_NAME", "default") or "default"
        if name not in _connections:
            raise ValueError(f"No connection configured with name=`{name}`")
        return _connections[name]
