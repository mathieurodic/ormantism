import logging
import inspect
import urllib


logger = logging.getLogger(__name__)


_urls: dict[str, str] = {}
def connect(database_url: str, name: str=None):
    _urls[name] = database_url

    # build rerpresentation for database URL
    if isinstance(database_url, str):
        url_representation = repr(database_url)
    else:
        if not callable(database_url):
            raise ValueError("`database_url` should either be a `str`, or a method returning a `str`")
        url_representation = f"({database_url.__module__}:{database_url.__code__.co_firstlineno}:{database_url.__name__})"

    # different messages, depending on provided name
    if name:
        logger.warning("Set database %s to %s", repr(name), url_representation)
    else:
        logger.warning("Set default database to %s", url_representation)
    logger.info("\n".join(f"{frame_info.filename}:{frame_info.lineno}"
                          for frame_info
                          in inspect.stack()
                          if ".venv" not in frame_info.filename))


def _get_connection(name=None):
    try:
        url = _urls[name]
    except KeyError as error:
        raise ValueError(f"No connection configured with name=`{name}`") from error

    if not isinstance(url, str):
        if callable(url):
            url = url()

    parsed_url = urllib.parse.urlparse(url)
    if parsed_url.scheme == "mysql":
        import pymysql

        # Establishing the connection
        connection = pymysql.connect(
            host=parsed_url.hostname,
            user=parsed_url.username,
            password=parsed_url.password,
            database=parsed_url.path[1:],
            port=parsed_url.port
        )
        return connection

    if parsed_url.scheme == "sqlite":
        import sqlite3

        # For SQLite, the database is usually a file path
        # Establishing the connection
        path = parsed_url.path[1:] or parsed_url.hostname
        logger.critical("Connecting to SQLite database %s", path)
        connection = sqlite3.connect(path)
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    if parsed_url.scheme == "postgresql":
        import psycopg2

        # Establishing the connection
        connection = psycopg2.connect(
            host=parsed_url.hostname,
            user=parsed_url.username,
            password=parsed_url.password,
            database=parsed_url.path[1:],
            port=parsed_url.port
        )
        return connection
