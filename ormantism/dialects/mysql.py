"""MySQL dialect."""

import urllib.parse
from typing import ClassVar

from ormantism.expressions import FunctionExpression

from .base import Dialect


class MysqlDialect(Dialect):
    """Dialect for MySQL (scheme mysql)."""

    SUPPORTED_SCHEMA: ClassVar[tuple[str, ...]] = ("mysql",)

    F: ClassVar[dict[str, callable]] = {
        "concat": lambda *args: FunctionExpression(symbol="CONCAT", arguments=args),
        "escape_for_like": lambda s: s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_"),
    }

    def connect(self, url: str):
        import pymysql  # pylint: disable=import-outside-toplevel,import-error
        parsed = urllib.parse.urlparse(url)
        return pymysql.connect(
            host=parsed.hostname,
            user=parsed.username,
            password=parsed.password,
            database=(parsed.path or "")[1:] or None,
            port=parsed.port,
        )
