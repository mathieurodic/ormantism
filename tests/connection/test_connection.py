"""Tests for ormantism.connection: connect(), _get_connection(), and database URL handling."""

import os
import pytest
from ormantism.connection import _get_connection
from ormantism.connection import connect


def test_connect_rejects_non_string_non_callable():
    """connect() raises ValueError when database_url is neither str nor callable."""
    with pytest.raises(ValueError, match="database_url.*str.*or a method"):
        connect(123, name="bad")
    with pytest.raises(ValueError, match="database_url.*str.*or a method"):
        connect([], name="bad")


def test_get_connection_with_callable_url():
    """_get_connection resolves a callable database_url to a string URL."""
    os.makedirs("/tmp/ormantism-tests", exist_ok=True)
    path = "/tmp/ormantism-tests/test-callable.sqlite3"
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    def url_factory():
        return f"sqlite:///{path}"
    connect(url_factory, name="callable_db")
    conn = _get_connection(name="callable_db")
    conn.execute("CREATE TABLE callable_foo(bar CHAR)")
    conn.commit()
    conn.close()
    conn2 = _get_connection(name="callable_db")
    count = conn2.execute("SELECT COUNT(*) FROM callable_foo").fetchone()[0]
    assert count == 0  # new connection, empty table
    conn2.close()


def _setup(*names: tuple[str]):
    for name in names:
        os.makedirs("/tmp/ormantism-tests", exist_ok=True)
        path = f"/tmp/ormantism-tests/test-{name}.sqlite3"
        try:
            os.remove(path)
        except FileNotFoundError:pass
        connect(f"sqlite:///{path}", name=name)


def test_file_connection():
    _setup(None, "alternative")
    print("\nTEST wrong name:")
    try:
        cn = _get_connection(name="nonexistent")
        raise Exception("Getting connection with wrong name failed to fail!")
    except ValueError:
        print("Getting connection with wrong name failed as expected.")

    print("\nTEST default:")
    c0 = _get_connection()
    c0.execute("CREATE TABLE foo(bar CHAR)")
    c0.execute("INSERT INTO foo(bar) VALUES ('Hello')")
    c0.commit()
    c0.close()
    count = _get_connection().execute("SELECT COUNT(*) FROM foo").fetchone()[0]
    assert count == 1
    print("Good count.")

    print("\nTEST alternative:")
    c1 = _get_connection(name="alternative")
    c1.execute("CREATE TABLE foo2(bar CHAR)")
    c1.execute("INSERT INTO foo2(bar) VALUES ('Hello')")
    c1.execute("INSERT INTO foo2(bar) VALUES ('world')")
    c1.execute("INSERT INTO foo2(bar) VALUES ('!')")
    count = _get_connection(name="alternative").execute("SELECT COUNT(*) FROM foo2").fetchone()[0]
    assert count == 0
    print("Good count before commit.")
    c1.commit()
    count = _get_connection(name="alternative").execute("SELECT COUNT(*) FROM foo2").fetchone()[0]
    assert count == 3
    print("Good count after commit.")
