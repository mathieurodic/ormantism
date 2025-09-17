import os
import pytest
from ormantism.connection import connect


@pytest.fixture(scope="function")
def setup_db(request):
    """Setup a temporary file SQLite database for each test."""
    path = f"/tmp/ormantism-tests/test-{request.function.__module__}-{request.function.__name__}.sqlite3"
    import logging
    logging.critical(path)
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    connect(f"sqlite:///{path}")
    yield
