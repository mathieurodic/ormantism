import urllib.parse
from functools import cache
from contextlib import contextmanager


class DatabaseConnectionError(Exception):
    pass


class Database:

    AUTHORIZED_SCHEMAS = ("postgresql", "mysql", "sqlite")

    def __init__(self, database_url):
        self.database_url = database_url
        self.parsed_url = urllib.parse.urlparse(self.database_url)
        if self.parsed_url.scheme not in self.AUTHORIZED_SCHEMAS:
            raise DatabaseConnectionError(f"Unsupported database scheme: {self.parsed_url.scheme}; supported values are: {", ".join(self.AUTHORIZED_SCHEMAS)}.")

    @property
    @cache
    def connection(self):

        if self.parsed_url.scheme == "mysql":
            import pymysql

            # Establishing the connection
            connection = pymysql.connect(
                host=self.parsed_url.hostname,
                user=self.parsed_url.username,
                password=self.parsed_url.password,
                database=self.parsed_url.path[1:],
                port=self.parsed_url.port
            )
            return connection

        if self.parsed_url.scheme == "sqlite":
            import sqlite3

            # For SQLite, the database is usually a file path
            # Establishing the connection
            connection = sqlite3.connect(self.parsed_url.path[1:] or self.parsed_url.hostname)
            connection.execute("PRAGMA foreign_keys = ON")
            return connection

        if self.parsed_url.scheme == "postgresql":
            import psycopg2

            # Establishing the connection
            connection = psycopg2.connect(
                host=self.parsed_url.hostname,
                user=self.parsed_url.username,
                password=self.parsed_url.password,
                database=self.parsed_url.path[1:],
                port=self.parsed_url.port
            )
            return connection
    
    @contextmanager
    def transaction(self):
        """Context manager for database transactions with automatic rollback on exception."""
        conn = self.connection
        try:
            # Begin transaction
            yield conn
            # If we reach here, no exception occurred, so commit
            conn.commit()
        except Exception:
            # Roll back the transaction on any exception
            conn.rollback()
            raise  # Re-raise the exception
    
    def close(self):
        self.connection.close()


# Global database instance
_database = None


def connect(database_url: str):
    """Connect to a database. Use ":memory:" for in-memory database."""
    global _database
    _database = Database(database_url)
    return _database


def get_database():
    """Get the current database instance."""
    global _database
    if _database is None:
        raise RuntimeError("No database connection. Call `ormantism.connect(database_url)` first.")
    return _database


def transaction():
    """Convenience function to get transaction context manager."""
    return get_database().transaction()


if __name__ == "__main__":
    import sqlite3
    # Initialize database
    database = connect("sqlite:///:memory:")  # Use in-memory database for example
    
    # Create a test table
    database.connection.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE
        )
    """)
    
    # Successful transaction
    try:
        with transaction() as t:
            t.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Alice", "alice@example.com"))
            t.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Bob", "bob@example.com"))
            # Transaction will be committed automatically
        print("Transaction committed successfully")
    except Exception as e:
        print(f"Transaction failed: {e}")
    
    # Failed transaction (will be rolled back)
    try:
        with transaction() as t:
            t.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Charlie", "charlie@example.com"))
            # This will cause a UNIQUE constraint violation
            t.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("David", "alice@example.com"))
    except sqlite3.IntegrityError as e:
        print(f"Transaction rolled back due to error: {e}")
    
    # Check what data actually got inserted
    cursor = database.connection.execute("SELECT * FROM users")
    print("Users in database:", cursor.fetchall())
    
    database.close()
