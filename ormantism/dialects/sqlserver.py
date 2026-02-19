"""SQL Server dialect."""

import urllib.parse
from typing import ClassVar

from ormantism.expressions import FunctionExpression

from .base import Dialect


class SqlserverDialect(Dialect):
    """Dialect for SQL Server (schemes mssql, sqlserver)."""

    SUPPORTED_SCHEMA: ClassVar[tuple[str, ...]] = ("mssql", "sqlserver")

    F: ClassVar[dict[str, callable]] = {
        "concat": lambda *args: FunctionExpression(symbol="CONCAT", arguments=args),
        "escape_for_like": lambda s: s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_"),
    }

    def connect(self, url: str):
        import pyodbc  # pylint: disable=import-outside-toplevel,import-error
        parsed = urllib.parse.urlparse(url)
        database = (parsed.path or "").lstrip("/") or None
        port = parsed.port or 1433
        server = parsed.hostname or "localhost"
        if port and port != 1433:
            server = f"{server},{port}"
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database or ''};"
            f"UID={parsed.username or ''};"
            f"PWD={parsed.password or ''}"
        )
        return pyodbc.connect(conn_str)
