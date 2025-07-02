# database.py
import sqlite3
from contextlib import contextmanager


class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path)
    
    @contextmanager
    def transaction(self):
        """Context manager for database transactions with automatic rollback on exception."""
        conn = self.connection
        try:
            # Begin transaction (sqlite3 starts transaction automatically on first SQL statement)
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
    """Connect to a database. Use ':memory:' for in-memory database."""
    global _database
    _database = Database(database_url)
    return _database


def get_database():
    """Get the current database instance."""
    global _database
    if _database is None:
        raise RuntimeError("No database connection. Call nanorm.connect(database_url) first.")
    return _database


def transaction():
    """Convenience function to get transaction context manager."""
    return get_database().transaction()


if __name__ == "__main__":
    # Initialize database
    database = connect(":memory:")  # Use in-memory database for example
    
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
