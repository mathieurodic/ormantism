import os

import pytest
from ormantism.connection import connect
from ormantism.transaction import _transaction_managers


def _is_lazy_load_warning(record):
    """Match the lazy-load warning emitted by Table._add_lazy_loader."""
    return "Lazy loading" in str(record.message)


@pytest.fixture
def expect_lazy_loads(recwarn):
    """Fixture to assert how many lazy-load warnings were emitted during the test.

    Use after the code that may trigger lazy loads, e.g.:

        def test_something(setup_db, expect_lazy_loads):
            ...
            expect_lazy_loads.expect(2)   # exactly 2 lazy loads
            # or
            assert expect_lazy_loads.count == 2
    """
    class Helper:
        @property
        def count(self):
            return sum(1 for w in recwarn if _is_lazy_load_warning(w))

        def expect(self, n):
            c = self.count
            assert c == n, f"Expected {n} lazy load(s), got {c}"

    return Helper()


@pytest.fixture(scope="function")
def setup_db(request):
    """Setup a temporary SQLite database for each test.

    Uses an in-memory database by default. Set ORMANTISM_DB_FILE to use
    a file database (e.g. for debugging).
    """
    if os.environ.get("ORMANTISM_DB_FILE"):
        path = (
            f"/tmp/ormantism-tests/test-{request.function.__module__}"
            f"-{request.function.__name__}.sqlite3"
        )
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        connection_url = f"sqlite:///{path}"
    else:
        connection_url = "sqlite:///:memory:"
    connect(connection_url)
    _transaction_managers.clear()
    yield
