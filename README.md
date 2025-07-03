# Ormantism

A tiny, simple ORM built on top of Pydantic.

When you need to perform simple CRUD operations with minimal code.

Offers support for PostgreSQL, MySQL, SQLite (database URL syntax is the same as in SQLAlchemy).

## Features

- **Simple Model Declaration**: Define your models using familiar Pydantic syntax
- **Automatic Table Creation**: Tables are created automatically when first accessed
- **Lazy Loading**: Relationships are loaded on-demand for optimal performance
- **Transaction Support**: Built-in transaction management with automatic rollback
- **Preloading**: Efficiently load related data with JOIN queries
- **Optional Timestamps**: Add created_at, updated_at, deleted_at fields automatically

## Installation

```bash
pip install ormantism
```

## Quick Start

### 1. Connect to Database

```python
import ormantism

# Connect to a file database
ormantism.connect("sqlite:///my_app.db")

# Or use in-memory database for testing
ormantism.connect("sqlite://:memory:")

# MySQL
ormantism.connect("mysql://login:password@host:port/database")

# PostgresSQL
ormantism.connect("postgresql://login:password@host:port/database")
```

### 2. Define Models

```python
from ormantism import Base
from typing import Optional

class User(Base):
    name: str
    email: str
    age: Optional[int] = None

class Post(Base, with_timestamps=True):
    title: str
    content: str
    author: User
```

### 3. Create and Save Records

```python
# Create a user
user = User(name="Alice", email="alice@example.com", age=30)
# The record is automatically saved to the database

# Create a post linked to the user
post = Post(title="My First Post", content="Hello World!", author=user)
```

### 4. Query Records

```python
# Load by ID
user = User.load(id=1)

# Load by criteria
user = User.load(name="Alice")
user = User.load(email="alice@example.com")

# Load latest post from alice@example.com
latest_post = Post.load(user_id=user.id, last_created=True)

# Load all records
users = User.load_all()

# Load with criteria
users_named_alice = User.load_all(name="Alice")
```

### 5. Update Records

```python
user = User.load(id=1)
user.age = 31  # Automatically saved to database
# or
user.update(age=31, email="alice.updated@example.com")
```

### 6. Delete Records

```python
user = User.load(id=1)
user.delete()
```

## Advanced Features

### Timestamps

Add automatic timestamp tracking to your models:

```python
class Post(Base, with_timestamps=True):
    title: str
    content: str
```

This adds `created_at`, `updated_at`, and `deleted_at` fields. Soft deletes are used when timestamps are enabled.

### Relationships and Lazy Loading

```python
class Author(Base):
    name: str

class Book(Base):
    title: str
    author: Author

# Create records
author = Author(name="Jane Doe")
book = Book(title="My Book", author=author)

# Lazy loading - author is loaded from DB when accessed
book = Book.load(id=1)
print(book.author.name)  # Database query happens here
```

### Preloading (Eager Loading)

Avoid N+1 queries by preloading relationships:

```python
# Load book with author in a single query
book = Book.load(id=1, preload="author")
print(book.author.name)  # No additional database query

# Preload nested relationships
book = Book.load(id=1, preload="author.publisher")

# Preload multiple relationships
book = Book.load(id=1, preload=["author", "category"])
```

### Transactions

```python
from ormantism import transaction

try:
    with transaction() as t:
        user1 = User(name="Alice", email="alice@example.com")
        user2 = User(name="Bob", email="bob@example.com")
        # Both users are saved automatically
        # Transaction commits when exiting the context
except Exception:
    # Transaction is automatically rolled back on any exception
    pass
```

### Querying Examples

```python
# Load single record
user = User.load(name="Alice")
latest_user = User.load(last_created=True)

# Load multiple records
all_users = User.load_all()
users_named_alice = User.load_all(name="Alice")

# Include soft-deleted records (when using timestamps)
all_including_deleted = User.load_all(with_deleted=True)
```

## Model Definition

### Basic Model

```python
class User(Base):
    name: str
    email: str
    age: int = 25  # Default value
    bio: Optional[str] = None  # Nullable field
```

### With Timestamps

```python
class Post(Base, with_timestamps=True):
    title: str
    content: str
    # Automatically adds: created_at, updated_at, deleted_at
```

### Supported Field Types

- `int`, `float`, `str`
- `Optional[T]` for nullable fields
- `list`, `dict` (stored as JSON)
- `datetime.datetime`
- `enum.Enum`
- Pydantic models (stored as JSON)
- References to other Base models

### Relationships

```python
class Category(Base):
    name: str

class Post(Base):
    title: str
    category: Category  # Foreign key relationship
    tags: Optional[Category] = None  # Nullable relationship
```

## API Reference

### Base Class Methods

#### Creating Records
- `Model()` - Create and automatically save a new record

#### Querying
- `Model.load(**criteria)` - Load single record
- `Model.load(last_created=True)` - Load most recently created record
- `Model.load_all(**criteria)` - Load multiple records
- `Model.load(preload="relationship")` - Eager load relationships
- `Model.load(with_deleted=True)` - Include soft-deleted records

#### Updating
- `instance.update(**kwargs)` - Update multiple fields
- `instance.field = value` - Update single field (auto-saves)

#### Deleting
- `instance.delete()` - Delete record (soft delete if timestamps enabled)

### Database Functions

- `ormantism.connect(database_url)` - Connect to database
- `ormantism.transaction()` - Get transaction context manager

## Limitations

- **Simple Queries**: Complex queries may require raw SQL
- **No Migrations**: Schema changes require manual handling
- **Basic Relationships**: Only supports simple foreign key relationships

## Requirements

- Python 3.12+
- Pydantic

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
