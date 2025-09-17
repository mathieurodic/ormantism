import os
from ormantism.connection import _get_connection
from ormantism.connection import connect


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
