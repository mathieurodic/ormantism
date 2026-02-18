# TODO and possible improvements

Ongoing and potential improvements for the Ormantism project. See [README.md](README.md) for usage and overview.

---

## Features

### Query and SQL

- **Aggregates**: No `GROUP BY`, `HAVING`, or aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX` beyond the existing `count()`). Add `q.aggregate(field, fn)` or `q.group_by(...).annotate(...)` for grouped queries.
- **Subqueries in WHERE**: No support for `EXISTS (SELECT ...)` or `field IN (SELECT ...)` in predicates. Would enable richer filtering (e.g. "users who have at least one post").
- **Raw SQL / escape hatch**: No way to run raw SQL or inject a custom fragment into a query. Useful for dialect-specific functions or one-off queries.
- **Regex lookups**: The Django-style `regex` and `iregex` lookups are not implemented (DB-specific; would need dialect hooks).
- **Distinct**: No `DISTINCT` on selected columns; only `COUNT(DISTINCT id)` internally when JOINs are used.
- **Upsert**: `upsert` with `on_conflict` exists; ensure it is documented and tested across dialects.

### Relations and schema

- **Many-to-many**: No built-in many-to-many tables (e.g. association table for tags ↔ posts). Users must model the join table explicitly.
- **Generic references** (`ref: Table`): Cannot be preloaded (JOIN not supported). Consider adding optional preload if the concrete type is known at query time.
- **Column renames / drops**: Schema changes beyond adding columns are manual. A migration helper or schema-diff tool could detect and optionally apply renames, drops, or type changes.

### Bulk and performance

- **Bulk insert**: No batch insert (e.g. `Model.bulk_create([...])`). Inserting many rows requires creating instances one by one.
- **Lazy iteration**: `.all()` loads everything into memory. Consider a true cursor/streaming mode for large result sets (e.g. `q.iterate()` that yields rows without materializing the full list).
- **Connection pooling**: Connections are created per use; no pooling. For high-throughput apps, external pooling (e.g. PgBouncer) might be needed.

### Async and concurrency

- **Async drivers**: All DB access is synchronous. Async drivers (e.g. asyncpg, aiomysql) could be a later addition behind an optional API (`ormantism[async]`).
- **Retry / backoff**: No built-in retry for transient failures (connection drops, deadlocks). Users must implement their own.

---

## Reliability

### Error handling and validation

- **In-code TODO**: In `ormantism/utils/supermodel.py` (around line 238) there is a `# TODO: better validation here` when applying updates for type-annotation fields. Replace with proper validation or document the current behaviour.
- **Bare except**: In `tests/connection/test_connection.py`, avoid bare `except`; use specific exception types (e.g. `except FileNotFoundError`).
- **Connection errors**: Ensure connection failures and transaction rollbacks surface clear errors (e.g. `TransactionError` with cause).

### Security and operations

- **Connection logging**: `ormantism/connection.py` logs the database URL (or its representation) and the call stack on `connect()`. In production this can leak sensitive URLs or paths. Consider reducing log level (e.g. debug only), redacting passwords, or making this configurable.
- **SQLite path handling**: For `sqlite:///path`, the code uses `parsed_url.path[1:]`. Document or verify behaviour on Windows (e.g. absolute paths) and in edge cases (e.g. empty path).

### Testing and CI

- **Test environment**: `tests/conftest.py` uses `/tmp` and `request.function` when `ORMANTISM_TESTS_USE_DB_FILES` is set. Consider using pytest's `tmp_path` for DB paths to improve portability.
- **Test style**: In `tests/connection/test_connection.py`, replace `print()` with assertions or structured logging.
- **CI matrix**: `pyproject.toml` has `requires-python = ">=3.12"`. CI only runs 3.12. Consider adding 3.13 (and optionally 3.12 + 3.13 matrix) to catch compatibility issues.
- **Optional test deps**: Add an optional dependency group in `pyproject.toml` for tests (e.g. `pytest`, `pytest-cov`) so contributors can install with `pip install -e ".[tests]"`.
- **Dialect coverage**: Ensure tests run against MySQL, PostgreSQL, and SQLite in CI where feasible (or document which are tested).

### Type safety

- **Type hints**: Tighten types where they are loose or wrong:
  - `ormantism/transaction.py`: use `typing.Callable` instead of `callable` for type hints (e.g. `connection_factory: callable`).
  - `ormantism/utils/make_hashable.py` and `ormantism/utils/schema.py`: use `typing.Any` instead of lowercase `any`.
- **Type checking**: No mypy (or similar) config. If type hints are improved, consider adding mypy in CI and in optional dev dependencies.

---

## Code quality and tooling

- **Pre-commit**: No pre-commit config. Optionally add a `.pre-commit-config.yaml` (e.g. pylint, pytest, or a formatter) so contributors get consistent checks locally.
- **.gitignore**: Consider adding `.pytest_cache/`, `dist/`, and `.mypy_cache/` if you adopt those tools.

---

## Documentation and contributing

- **Contributing**: The README now links to [TODO.md](TODO.md). Optionally add issue/PR templates and a short “Development setup” (install with `[tests]`, run pytest, run pylint).
- **Limitations**: The README already lists limitations (no migrations, simple queries, etc.). Optionally add a short “Future work” or “Improvements” pointer to this file.

---

*If you pick an item to work on, consider removing or updating it here and opening a PR.*
