"""PostgreSQL dialect."""

import urllib.parse
from typing import ClassVar

from ormantism.expressions import NaryOperatorExpression

from .base import Dialect


class PostgresDialect(Dialect):
    """Dialect for PostgreSQL (scheme postgresql)."""

    SUPPORTED_SCHEMA: ClassVar[tuple[str, ...]] = ("postgresql",)

    F: ClassVar[dict[str, callable]] = {
        "concat": lambda *args: NaryOperatorExpression(symbol="||", arguments=args),
    }

    def connect(self, url: str):
        import psycopg2
        parsed = urllib.parse.urlparse(url)
        return psycopg2.connect(
            host=parsed.hostname,
            user=parsed.username,
            password=parsed.password,
            database=(parsed.path or "")[1:] or None,
            port=parsed.port,
        )
