import urllib


_urls: dict[str, str] = {}
def connect(database_url: str, name: str="default"):
    _urls[name] = database_url


def _get_connection(name="default"):
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
    

if __name__ == "__main__":

    connect("sqlite:////tmp/test0.sqlite3")
    connect("sqlite:////tmp/test1.sqlite3", name="alternative")

    print("\nTEST wrong name:")
    try:
        cn = _get_connection(name="nonexistent")
        print("Getting connection with wrong name failed to fail!")
    except ValueError:
        print("Getting connection with wrong name failed as expected.")

    print("\nTEST default:")
    c0 = _get_connection()
    c0.execute("CREATE TABLE foo(bar CHAR)")
    c0.execute("INSERT INTO foo(bar) VALUES ('Hello')")
    c0.commit()
    c0.close()
    count = _get_connection()[1].execute("SELECT COUNT(*) FROM foo").fetchone()[0]
    assert count == 1
    print("Good count.")

    print("\nTEST alternative:")
    c1 = _get_connection(name="alternative")
    c1.execute("CREATE TABLE foo2(bar CHAR)")
    c1.execute("INSERT INTO foo2(bar) VALUES ('Hello')")
    c1.execute("INSERT INTO foo2(bar) VALUES ('world')")
    c1.execute("INSERT INTO foo2(bar) VALUES ('!')")
    count = _get_connection(name="alternative")[1].execute("SELECT COUNT(*) FROM foo2").fetchone()[0]
    assert count == 0
    print("Good count before commit.")
    c1.commit()
    count = _get_connection(name="alternative")[1].execute("SELECT COUNT(*) FROM foo2").fetchone()[0]
    assert count == 3
    print("Good count after commit.")
