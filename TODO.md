# TODO and possible improvements

Ongoing and potential improvements for the Ormantism project. See [README.md](README.md) for usage and overview.

---

## Code quality

- **In-code TODO**: In `ormantism/utils/supermodel.py` (around line 238) there is a `# TODO: better validation here` when applying updates for type-annotation fields. Replace with proper validation or document the current behaviour.
- **Type hints**: Tighten types where they are loose or wrong:
  - `ormantism/transaction.py`: use `typing.Callable` instead of `callable` for type hints (e.g. `connection_factory: callable`).
  - `ormantism/utils/make_hashable.py` and `ormantism/utils/serialize.py`: use `typing.Any` instead of lowercase `any`.

---

## Testing

- **Test environment**: `tests/conftest.py` uses `/tmp` and `request.function` when `ORMANTISM_DB_FILE` is set. Consider using pytest’s `tmp_path` for DB paths to improve portability.
- **Test style**: In `tests/connection/test_connection.py`, replace `print()` with assertions or structured logging, and avoid bare `except` where possible (e.g. `except FileNotFoundError:pass`).
- **CI matrix**: `pyproject.toml` has `requires-python = ">=3.12"`. CI only runs 3.12. Consider adding 3.13 (and optionally 3.12 + 3.13 matrix) to catch compatibility issues.
- **Optional test deps**: Add an optional dependency group in `pyproject.toml` for tests (e.g. `pytest`, `pytest-cov`) so contributors can install with `pip install -e ".[tests]"`.

---

## Security and operations

- **Connection logging**: `ormantism/connection.py` logs the database URL (or its representation) and the call stack on `connect()`. In production this can leak sensitive URLs or paths. Consider reducing log level (e.g. debug only), redacting passwords, or making this configurable.
- **SQLite path handling**: For `sqlite:///path`, the code uses `parsed_url.path[1:]`. Document or verify behaviour on Windows (e.g. absolute paths) and in edge cases (e.g. empty path).

---

## Documentation and contributing

- **Contributing**: The README now links to [TODO.md](TODO.md). Optionally add issue/PR templates and a short “Development setup” (install with `[tests]`, run pytest, run pylint).
- **Limitations**: The README already lists limitations (no migrations, simple queries, etc.). Optionally add a short “Future work” or “Improvements” pointer to this file.

---

## Tooling and repo hygiene

- **Pre-commit**: No pre-commit config. Optionally add a `.pre-commit-config.yaml` (e.g. pylint, pytest, or a formatter) so contributors get consistent checks locally.
- **Type checking**: No mypy (or similar) config. If type hints are improved, consider adding mypy in CI and in optional dev dependencies.
- **.gitignore**: Consider adding `.pytest_cache/`, `dist/`, and `.mypy_cache/` if you adopt those tools.

---

## Features and design (optional / longer term)

- **Migrations**: README states there are no migrations (new columns added automatically; drops/renames/type changes are manual). A small migration or schema-diff helper could be a future improvement.
- **Async**: All DB access is synchronous. Async drivers (e.g. asyncpg, aiomysql) could be a later addition behind an optional API.

---

*If you pick an item to work on, consider removing or updating it here and opening a PR.*
