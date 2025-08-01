from ormantism.transaction import TransactionManager, TransactionError


def test_transaction():

    def connect():
        """Mock connection factory"""
        import sqlite3
        c = sqlite3.connect(":memory:")
        c.execute("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name CHAR, age INTEGER)")
        c.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, product CHAR)")
        c.execute("CREATE TABLE inventory (id INTEGER PRIMARY KEY AUTOINCREMENT, quantity INTEGER, product CHAR)")
        return c
    
    # Create transaction manager
    tm = TransactionManager(connect)

    # For debugging
    def get_users_names():
        with tm.transaction() as t:
            cursor = t.execute("SELECT name FROM users")
            return list(row[0] for row in cursor.fetchall())

    # Example 1: Simple transaction
    print("-- Simple Transaction --")
    with tm.transaction() as t:
        t.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
        t.execute("UPDATE users SET age = ? WHERE name = ?", (25, "Alice"))
    assert get_users_names() == ["Alice"]

    # Example 2: Nested transactions
    print("\n-- Nested Transactions --")
    with tm.transaction() as outer:
        outer.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))
        
        with tm.transaction() as inner:
            inner.execute("INSERT INTO orders (user_id, product) VALUES (?, ?)", (1, "Laptop"))
            inner.execute("UPDATE inventory SET quantity = quantity - 1 WHERE product = ?", ("Laptop",))
            inner.execute("SELECT * FROM orders")
        
        # This would raise an exception if uncommented:
        # outer.execute("SELECT * FROM orders")  # Would fail because inner transaction is no longer active
    assert get_users_names() == ["Alice", "Bob"]

    # Example 3: Demonstrating the level restriction
    print("\n-- Level Restriction --")
    try:
        with tm.transaction() as t1:
            with tm.transaction() as t2:
                # This will raise an exception because t1 is a higher level
                t1.execute("SELECT * FROM users")
                raise AssertionError("This should have raised a TransactionError!")
    except TransactionError as e:
        print(f"Caught expected error: {e}")

    # Example 4: Error handling with rollback
    print("\n-- Error Handling --")
    with tm.transaction() as t1:
        try:
            t1.execute("INSERT INTO users (name) VALUES (?)", ("Charlie",))
            with tm.transaction() as t2:
                t2.execute("INSERT INTO users (name) VALUES (?)", ("David",))
                raise ValueError("Testing if exception causes rollback as expected")
        except ValueError as e:
            print(f"Caught error: {e}")
            print("Transaction was rolled back automatically")
    assert get_users_names() == ["Alice", "Bob", "Charlie"]