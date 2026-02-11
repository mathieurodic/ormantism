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
- **Load-or-create**: Find by criteria or create in one call
- **Versioning**: Optional versioning along specified fields

## Installation

```bash
pip install ormantism
```

SQLite works out of the box. For MySQL or PostgreSQL, install the corresponding optional dependency:

```bash
# MySQL
pip install ormantism[mysql]

# PostgreSQL
pip install ormantism[postgresql]

# Both
pip install ormantism[mysql,postgresql]
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

# PostgreSQL
ormantism.connect("postgresql://login:password@host:port/database")
```

### 2. Define Models

```python
from ormantism import Table
from typing import Optional

class User(Table):
    name: str
    email: str
    age: Optional[int] = None

class Post(Table, with_timestamps=True):
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

# Load latest post by this author
latest_post = Post.load(author=user, last_created=True)

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

### Load or create

Find a row by criteria or create it in one call. Use `_search_fields` to limit which fields are used for the lookup; other fields are then used to update the row if it already exists, or to populate a new row if not.

```python
# Create or reuse by name; update value if name already exists
user = User.load_or_create(_search_fields=("name",), name="Alice", email="alice@example.com")
user2 = User.load_or_create(_search_fields=("name",), name="Alice", email="new@example.com")  # same row, email updated
```

### Timestamps

Add automatic timestamp tracking to your models:

```python
class Post(Table, with_timestamps=True):
    title: str
    content: str
```

This adds `created_at`, `updated_at`, and `deleted_at` fields. Soft deletes are used when timestamps are enabled.

### Versioning

Use `versioning_along=(...)` so that updates that change those fields create a new row instead of updating in place (useful for history).

```python
class Document(Table, versioning_along=("name",)):
    name: str
    content: str

d1 = Document(name="foo", content="v1")
d2 = Document(name="foo", content="v2")  # New row; same name, different content
```

### Relationships and Lazy Loading

```python
class Author(Table):
    name: str

class Book(Table):
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
class User(Table):
    name: str
    email: str
    age: int = 25  # Default value
    bio: Optional[str] = None  # Nullable field
```

### With Timestamps

```python
class Post(Table, with_timestamps=True):
    title: str
    content: str
    # Automatically adds: created_at, updated_at, deleted_at
```

### Supported Field Types

- `int`, `float`, `str`
- `Optional[T]` for nullable fields
- `list`, `dict` (stored as JSON)
- `ormantism.JSON` — arbitrary JSON (dict, list, primitives); stored as a JSON column
- `datetime.datetime`
- `enum.Enum`
- Pydantic models (stored as JSON)
- References to other Table models (single or `list[...]`); optional and self-referential refs supported

### Relationships

```python
class Category(Table):
    name: str

class Post(Table):
    title: str
    category: Category  # Foreign key relationship
    tags: Optional[Category] = None  # Nullable relationship
```

### Self-referential and list relationships

```python
from typing import Optional
from pydantic import Field

class Node(Table):
    parent: Optional["Node"] = None  # Self-reference
    name: str

class Parent(Table):
    name: str
    children: list["Parent"] = Field(default_factory=list)  # List of references
```

### JSON and generic reference fields

```python
from ormantism import Table, JSON

class WithJSON(Table):
    j: JSON  # Arbitrary JSON (dict, list, primitives); stored as JSON column

# Generic reference (any Table subclass); cannot be preloaded
class Ptr(Table):
    ref: Table
```

## API Reference

### Table class methods

#### Creating and loading
- `Model(**data)` - Create and automatically save a new record
- `Model.load_or_create(_search_fields=("name",), **data)` - Load one matching the given fields, or create; only `_search_fields` are used for the lookup; other fields can update the row if it already exists
- `Model.load(**criteria)` - Load single record
- `Model.load(last_created=True)` - Load most recently created record
- `Model.load(as_collection=True, **criteria)` - Load as list (no LIMIT 1)
- `Model.load_all(**criteria)` - Load multiple records
- `Model.load(preload="relationship")` or `preload=["a", "b"]` - Eager load relationships (not supported for generic `Table` references)
- `Model.load(with_deleted=True)` - Include soft-deleted records

#### Updating
- `instance.update(**kwargs)` - Update multiple fields
- `instance.field = value` - Update single field (auto-saves)

#### Deleting
- `instance.delete()` - Delete record (soft delete if timestamps enabled)

### Database and transaction

- `ormantism.connect(database_url)` - Connect to database
- `ormantism.transaction(connection_name=...)` - Get transaction context manager (optional `connection_name` for multi-connection setups)

### Table class options

- `Table(..., with_timestamps=True)` - Add created_at, updated_at, deleted_at and soft deletes
- `Table(..., with_created_at_timestamp=True, with_timestamps=False)` - Only created_at
- `Table(..., with_updated_at_timestamp=True, with_timestamps=False)` - Only updated_at
- `Table(..., versioning_along=("name",))` - Version rows by these fields (creates new row on update when these change)
- `Table(..., connection_name="custom_conn")` - Use a named connection (inherited by subclasses)

## Code reference

For a full **code reference** of the library (all classes and methods with definition and usage locations), see **[ormantism/REFERENCE.md](ormantism/REFERENCE.md)**. It lists every public class (e.g. `Table`, `Field`, `QueryJoin`, `Transaction`, `SuperModel`) and method with their source file and line numbers and where they are used—useful when navigating the codebase or contributing.

## Limitations

- **Simple Queries**: Complex queries may require raw SQL
- **No Migrations**: New columns are added automatically when the model gains fields; dropping or renaming columns, or changing column types, requires manual handling
- **Basic Relationships**: Only supports simple foreign key relationships

See **[TODO.md](TODO.md)** for planned improvements and contribution ideas.

## Requirements

- Python 3.12+
- Pydantic
- For MySQL: install with `pip install ormantism[mysql]` (uses `pymysql`)
- For PostgreSQL: install with `pip install ormantism[postgresql]` (uses `psycopg2`)
- SQLite is supported with no extra dependencies

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For possible improvements and known TODOs, see **[TODO.md](TODO.md)** at the root of the project.
