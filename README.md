# Ormantism

A lightweight ORM built on Pydantic for simple CRUD with minimal code. Use it when you need straightforward database access without the overhead of a full-featured ORM.

**Supported backends:** SQLite (built-in), MySQL, PostgreSQL. Database URLs use the same style as SQLAlchemy.

---

## Features

- **Pydantic-based models** — Define tables with type hints and optional defaults
- **Auto table creation** — Tables are created on first use; new columns are added when the model gains fields
- **Relationships** — Single and list references to other tables; lazy loading by default
- **Preloading** — Eager-load relations with JOINs to avoid N+1 queries
- **Timestamps** — Optional `created_at` / `updated_at` / `deleted_at` and soft deletes
- **Versioning** — Optional row versioning so updates can create new rows instead of overwriting
- **Load-or-create** — Find by criteria or create in one call, with control over which fields are used for the lookup
- **Transactions** — Context manager with automatic commit/rollback

---

## Installation

```bash
pip install ormantism
```

SQLite works with no extra dependencies. For MySQL or PostgreSQL, install the corresponding extra:

```bash
pip install ormantism[mysql]      # pymysql
pip install ormantism[postgresql]  # psycopg2
pip install ormantism[mysql,postgresql]
```

**Requires:** Python 3.12+, Pydantic 2.x.

---

## Quick start

### Connect

```python
import ormantism

ormantism.connect("sqlite:///my_app.db")
# or: sqlite://:memory:  |  mysql://user:pass@host/db  |  postgresql://user:pass@host/db
```

### Define models

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

### Create and query

```python
# Create (saved automatically)
user = User(name="Alice", email="alice@example.com", age=30)
post = Post(title="Hello", content="World", author=user)

# Load by id or criteria
user = User.load(id=1)
user = User.load(name="Alice")
posts = Post.load_all(author=user)

# Update
user.age = 31   # auto-saved
# or
user.update(age=31, email="alice@new.com")

# Delete (soft delete if with_timestamps=True)
user.delete()
```

---

## Model options

### Timestamps and soft delete

```python
class Post(Table, with_timestamps=True):
    title: str
    content: str
# Adds: created_at, updated_at, deleted_at. delete() becomes soft delete.
```

Only some timestamps:

```python
class Log(Table, with_created_at_timestamp=True, with_timestamps=False):
    message: str
```

### Versioning

When specified fields change on update, a new row is created instead of updating in place:

```python
class Document(Table, versioning_along=("name",)):
    name: str
    content: str

doc = Document(name="foo", content="v1")
doc = Document(name="foo", content="v2")  # New row; same name, new content
```

### Named connection

```python
class Remote(Table, connection_name="secondary"):
    ...
```

---

## Field types

- **Scalars:** `int`, `float`, `str`, `bool`, `datetime.datetime`, `enum.Enum`
- **Nullable:** `Optional[T] = None`
- **Defaults:** `age: int = 0`
- **JSON:** `list`, `dict`, or `ormantism.JSON` (arbitrary JSON in a column)
- **Relations:** `Author` (single), `Optional[Author]`, `list[Child]`
- **Generic reference:** `ref: Table` (any table; cannot be preloaded)
- **Pydantic models:** Stored as JSON

### Relationships

```python
class Category(Table):
    name: str

class Post(Table):
    title: str
    category: Category
    tags: Optional[Category] = None

# Self-reference and list of refs
class Node(Table):
    parent: Optional["Node"] = None
    name: str

class Parent(Table):
    name: str
    children: list["Child"] = []
```

---

## Loading and preloading

### Basic loading

```python
User.load(id=1)
User.load(name="Alice")
User.load(last_created=True)
User.load_all()
User.load_all(name="Alice")
User.load_all(with_deleted=True)   # Include soft-deleted when using timestamps
```

### Preload relations (eager loading)

```python
book = Book.load(id=1, preload="author")
book = Book.load(id=1, preload="author.publisher")
book = Book.load(id=1, preload=["author", "category"])
```

Without preload, `book.author` triggers a lazy load (and a warning in some setups). Preloading fetches in one query with JOINs.

### Load or create

Find by given fields or create; other fields update the row if it exists or set values on create:

```python
user = User.load_or_create(_search_fields=("name",), name="Alice", email="alice@example.com")
# Same row, email updated:
user2 = User.load_or_create(_search_fields=("name",), name="Alice", email="new@example.com")
```

---

## Transactions

```python
from ormantism import transaction

with transaction() as t:
    User(name="Alice", email="alice@example.com")
    User(name="Bob", email="bob@example.com")
# Commits on exit; rolls back on exception
```

Use `transaction(connection_name="...")` when using a named connection.

---

## API summary

### Table: create and persist

- `Model(**kwargs)` — Create and save a row
- `instance.update(**kwargs)` — Update fields and save
- `instance.field = value` — Assign and auto-save
- `instance.delete()` — Delete (soft if timestamps enabled)

### Table: loading

- `Model.load(**criteria)` — One row (or None)
- `Model.load(last_created=True)` — Most recently created
- `Model.load_all(**criteria)` — List of rows
- `Model.load(..., preload="rel")` / `preload=["a","b"]` — Eager load relations
- `Model.load(..., with_deleted=True)` — Include soft-deleted
- `Model.load_or_create(_search_fields=(...), **data)` — Load by search fields or create; other fields update or populate

### Connection and transaction

- `ormantism.connect(url)` — Set default connection (SQLAlchemy-style URL)
- `ormantism.transaction(connection_name=...)` — Context manager for transactions

### Table class options

| Option | Effect |
|--------|--------|
| `with_timestamps=True` | Add created_at, updated_at, deleted_at; soft delete |
| `with_created_at_timestamp=True` | Only created_at |
| `with_updated_at_timestamp=True` | Only updated_at |
| `versioning_along=("field",)` | New row when these fields change on update |
| `connection_name="name"` | Use named connection (inherited by subclasses) |

---

## Code reference

For a full **code reference** (classes and methods with file/line and usages), see **[ormantism/REFERENCE.md](ormantism/REFERENCE.md)**.

---

## Limitations

- **Queries** — Complex filters may require raw SQL or the lower-level `Query` API.
- **Migrations** — New columns are added automatically; dropping/renaming columns or changing types is not automated (see [TODO.md](TODO.md)).
- **Relations** — Single and list references; no built-in many-to-many tables.

---

## License and contributing

**License:** MIT.

Contributions are welcome. See **[TODO.md](TODO.md)** for ideas and planned improvements.


