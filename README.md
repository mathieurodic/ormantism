# Ormantism

A lightweight ORM built on Pydantic for simple CRUD with minimal code. Use it when you need straightforward database access without the overhead of a full-featured ORM.

**Supported backends:** SQLite (built-in), MySQL, PostgreSQL. Database URLs use the same style as SQLAlchemy.

---

## Features

- **Pydantic-based models** — Define tables with type hints and optional defaults
- **Auto table creation** — Tables are created on first use; new columns are added when the model gains fields
- **Relationships** — Single and list references to other tables; lazy loading by default
- **Preloading** — Eager-load relations with JOINs to avoid N+1 queries
- **Fluent Query API** — `Model.q().where(...).select(...).order_by(...).first()` / `.all()`
- **Timestamps** — Optional `created_at` / `updated_at` / `deleted_at` and soft deletes
- **Versioning** — Optional row history (append-only by key) via soft-deleted previous versions
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

class User(Table, with_timestamps=True):
    name: str
    email: str
    age: Optional[int] = None

class Post(Table, with_timestamps=True):
    title: str
    content: str
    author: User | None = None
```

### Create and query

```python
# Create (saved automatically)
user = User(name="Alice", email="alice@example.com", age=30)
post = Post(title="Hello", content="World", author=user)

# Query: one row
user = User.q().where(User.id == 1).first()
user = User.q().where(name="Alice").first()

# Query: all matching rows
posts = Post.q().where(author=user).all()

# Update
user.age = 31   # auto-saved on assignment
user.update(age=31, email="alice@new.com")

# Delete (soft delete when with_timestamps=True)
user.delete()
```

---

## Query API

The primary way to query is `Model.q()`, which returns a fluent `Query` builder. Chain methods and end with `.first()`, `.all()`, or iterate.

### Basic usage

```python
# One row or None
user = User.q().where(User.id == 1).first()
user = User.q().where(name="Alice").first()

# All matching rows
users = User.q().where(age__gte=18).all()
users = list(User.q().where(name="Bob"))

# Limit and offset
users = User.q().limit(10).all()
page = User.q().offset(20).limit(10).all()
```

### Where: expression-style and Django-style

**Expression-style** — SQLAlchemy-like, using model attributes and operators:

```python
User.q().where(User.name == "Alice").first()
User.q().where(User.age >= 18, User.email.is_not_null()).all()
User.q().where(Post.author.name.icontains("smith")).all()   # filter by related column
```

**Django-style kwargs** — `field__lookup=value`:

```python
User.q().where(name="Alice")                    # exact (default)
User.q().where(name__icontains="alice")         # case-insensitive contains
User.q().where(age__gte=18, age__lt=65)         # gt, gte, lt, lte
User.q().where(name__in=["Alice", "Bob"])       # IN
User.q().where(name__range=(1, 10))             # BETWEEN
User.q().where(author__isnull=True)             # IS NULL
User.q().where(book__title__contains="Python")  # nested path
```

Supported lookups: `exact`, `iexact`, `lt`, `lte`, `gt`, `gte`, `in`, `range`, `isnull`, `contains`, `icontains`, `startswith`, `istartswith`, `endswith`, `iendswith`, `like`, `ilike`.

### Select and preload

Use `select()` to choose which columns/relations to fetch. Relations in `select()` are eager-loaded (JOINs), avoiding N+1 lazy loads.

```python
# Preload a relation (all columns from root + author)
book = Book.q().select("author").where(Book.id == 1).first()
book.author  # no lazy load

# Preload nested path
book = Book.q().select("author.publisher").where(Book.id == 1).first()

# Multiple relations
users = User.q().select("profile", "posts").where(User.active == True).all()

# Expression-style
User.q().select(User.name, User.book.title).where(...)
```

Without `select()` for a relation, accessing `row.author` triggers a lazy load (and a warning).

### Order, limit, offset

```python
User.q().order_by(User.name).all()           # ascending
User.q().order_by(User.created_at.desc).all()  # descending
User.q().order_by(User.name, User.id).all()  # multiple columns

User.q().limit(10).offset(20).all()
```

### Soft-deleted rows

For tables with `with_timestamps=True`, soft-deleted rows are excluded by default. Include them with:

```python
User.q().include_deleted().where(User.id == 1).first()
```

### Query execution

| Method | Returns |
|--------|---------|
| `.first()` | One `Model` or `None` |
| `.all(limit=N)` | List of `Model` |
| `list(q)` | Same as `.all()` |
| `for row in q:` | Iterate (lazy) |

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

Ormantism supports a lightweight **row-history / version series** mode.

When `versioning_along` is set, rows with the same values for those fields form a series.

Any change to a versioned instance (either via attribute assignment or `instance.update(...)`) will:
1. **Insert a new row** with an incremented `version` (new `id`)
2. **Soft-delete** the previous “current” row in the series (`deleted_at` is set)
3. Mutate the *same Python instance* so it now points to the new row (its `id` changes)

Fields listed in `versioning_along` are treated as **immutable** (changing them raises).

```python
class Document(Table, versioning_along=("name",)):
    name: str
    content: str

doc = Document(name="foo", content="v1")
doc = Document(name="foo", content="v2")  # New row; same name, version increments

doc.content = "v3"   # New row again (id changes)
doc.update(content="v4")  # New row again
```

Querying:
- Default queries exclude soft-deleted rows. For a versioned series, that means you’ll see only the latest (current) row.
- Use `include_deleted()` to fetch full history.

Backend note: version assignment currently relies on `UPDATE ... RETURNING` support (works on PostgreSQL and SQLite ≥ 3.35; may not work on some MySQL setups).

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
class Category(Table, with_timestamps=True):
    name: str

class Post(Table, with_timestamps=True):
    title: str
    category: Category | None = None
    tags: list[Category] = []

# Self-reference
class Node(Table, with_timestamps=True):
    parent: Optional["Node"] = None
    name: str
```

---

## Load or create

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

with transaction():
    User(name="Alice", email="alice@example.com")
    User(name="Bob", email="bob@example.com")
# Commits on exit; rolls back on exception
```

Use `transaction(connection_name="...")` when using a named connection.

---

## API summary

### Table: create and persist

- `Model(**kwargs)` — Create and save a row
- `instance.field = value` — Assign and auto-save
- `instance.update(**kwargs)` — Update fields and save
- `instance.delete()` — Delete (soft if timestamps enabled)

### Table: query builder

- `Model.q()` — Return a `Query` for this table (supports `_transform_query` from mixins)

### Query: fluent chain

- `q.where(*exprs, **kwargs)` — Filter (expressions and/or Django-style kwargs)
- `q.filter(...)` — Alias for `where`
- `q.select(*paths)` — Preload relations (e.g. `"author"`, `"author.publisher"`)
- `q.order_by(*exprs)` — ORDER BY (e.g. `User.name`, `User.created_at.desc`)
- `q.limit(n)` / `q.offset(n)` — Pagination
- `q.include_deleted()` — Include soft-deleted rows
- `q.first()` — One row or None
- `q.all(limit=N)` — List of rows
- `q.update(**kwargs)` — Update matched rows
- `q.delete()` — Delete matched rows

### Table: load or create

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
| `versioning_along=("field",)` | Enable row history series keyed by these fields (copy-on-write on update; previous becomes soft-deleted) |
| `connection_name="name"` | Use named connection (inherited by subclasses) |

---

## Deprecated: load and load_all

`Model.load(**criteria)` and `Model.load_all(**criteria)` are deprecated. Use the Query API instead:

```python
# Instead of: User.load(id=1)
User.q().where(id=1).first()

# Instead of: User.load_all(name="Alice")
User.q().where(name="Alice").all()

# Instead of: Book.load(id=1, preload="author")
Book.q().select("author").where(Book.id == 1).first()

# Instead of: User.load_all(with_deleted=True)
User.q().include_deleted().all()
```

---

## Code reference

For a full **code reference** (classes and methods with descriptions, file/line, and usages), see **[ormantism/REFERENCE.md](ormantism/REFERENCE.md)**.

---

## Limitations

- **Migrations** — New columns are added automatically; dropping/renaming columns or changing types is not automated (see [TODO.md](TODO.md)).
- **Relations** — Single and list references; no built-in many-to-many tables.
- **Generic references** — `ref: Table` cannot be preloaded (JOIN not supported).

---

## License and contributing

**License:** MIT.

Contributions are welcome. See **[TODO.md](TODO.md)** for ideas and planned improvements.
