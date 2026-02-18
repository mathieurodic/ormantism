"""SQLite dialect."""

import logging
import urllib.parse

from typing import ClassVar

from ormantism.expressions import NaryOperatorExpression

from .base import Dialect

logger = logging.getLogger(__name__)


class SqliteDialect(Dialect):
    """Dialect for SQLite (scheme sqlite)."""

    SUPPORTED_SCHEMA: ClassVar[tuple[str, ...]] = ("sqlite",)

    F: ClassVar[dict[str, callable]] = {
        "concat": lambda *args: NaryOperatorExpression(symbol="||", arguments=args),
        "escape_for_like": lambda s: s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_"),
    }

    def connect(self, url: str):
        import sqlite3
        parsed = urllib.parse.urlparse(url)
        path = (parsed.path or "")[1:] or parsed.hostname
        logger.critical("Connecting to SQLite database %s", path)
        conn = sqlite3.connect(path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
