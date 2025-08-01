import os
import pytest
from ormantism.connection import connect


@pytest.fixture(scope="function")
def setup_db():
    """Setup an in-memory SQLite database for each test."""
    connect("sqlite:///:memory:")
    def method(*names: tuple[str]):
        for name in names:
            os.makedirs("/tmp/ormantism-tests", exist_ok=True)
            path = f"/tmp/ormantism-tests/test-{name}.sqlite3"
            try:
                os.remove(path)
            except FileNotFoundError:pass
            connect(f"sqlite:///{path}", name=name)
    yield method
