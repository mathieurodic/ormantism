import os
import pytest
from ormantism.connection import connect
from ormantism.transaction import _transaction_managers


@pytest.fixture(scope="function")
def setup_db(request):
    """Setup a temporary file SQLite database for each test."""
    path = (
        f"/tmp/ormantism-tests/test-{request.function.__module__}"
        f"-{request.function.__name__}.sqlite3"
    )
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    connection_url = f"sqlite:///{path}"
    connect(connection_url)
    _transaction_managers.clear()
    yield
