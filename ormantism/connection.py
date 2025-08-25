import urllib


_urls: dict[str, str] = {}
def connect(database_url: str, name: str=None):
    _urls[name] = database_url


def _get_connection(name=None):
    try:
        url = _urls[name]
    except KeyError as error:
        raise ValueError(f"No connection configured with name=`{name}`") from error

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
        connection = sqlite3.connect(parsed_url.path[1:] or parsed_url.hostname)
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
