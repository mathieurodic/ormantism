"""Table schema: create_table, add_columns."""

import inspect
import logging
import sqlite3
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .base import Table

logger = logging.getLogger("ormantism")


def create_table(model: "type[Table]", created: Optional[set["type[Table]"]] = None) -> None:
    """Create the table and referenced tables if they do not exist."""
    from ..query import Query
    from .base import Table

    if created is None:
        created = set()
    created.add(model)
    for field in model._get_columns().values():
        if field.is_reference:
            for t in (field.base_type, field.secondary_type):
                if inspect.isclass(t) and issubclass(t, Table) and t != Table and t not in created:
                    create_table(t, created)
    statements = list(model._get_table_sql_creations())
    statements += sum(
        (
            list(field.sql_creations)
            for field in model._get_columns().values()
            if field.name not in ("created_at", "id")
        ),
        start=[],
    )
    statements += [
        f"FOREIGN KEY ({name}) REFERENCES {field.base_type._get_table_name()}(id)"
        for name, field in model._get_columns().items()
        if field.is_reference
        and field.secondary_type is None
        and field.base_type != Table
    ]
    stmt_join = ",\n  ".join(statements)
    sql = f"CREATE TABLE IF NOT EXISTS {model._get_table_name()} (\n  {stmt_join})"
    Query(table=model).execute(sql, (), ensure_structure=False)


def add_columns(model: "type[Table]") -> None:
    """Add any missing columns to the existing table (SQLite ALTER TABLE)."""
    from ..query import Query

    tbl = model._get_table_name()
    rows = Query(table=model).execute(
        f"SELECT name FROM pragma_table_info('{tbl}')",
        (),
        ensure_structure=False,
    )
    columns_names = {name for name, in rows}
    mixin_column_names = {
        name
        for stmt in model._get_table_sql_creations()
        for name in (stmt.split()[0],)
    }
    new_fields = [
        field
        for field in model._get_columns().values()
        if field.name not in columns_names
        and field.name not in mixin_column_names
    ]
    for field in new_fields:
        logger.info("ADD COLUMN %s.%s", field.table.__name__, field.name)
        for sql_creation in field.sql_creations:
            try:
                Query(table=model).execute(
                    f"ALTER TABLE {model._get_table_name()} ADD COLUMN " + sql_creation,
                    (),
                    ensure_structure=False,
                )
            except sqlite3.OperationalError as error:
                if "duplicate column name" not in error.args[0]:
                    raise
