"""Query builder and execution for Table models.

This module provides a fluent Query API to build and run SELECT queries with
optional JOINs (preload paths), WHERE criteria, ORDER BY, and LIMIT. It also
ensures table structure exists before running queries and hydrates result rows
into Table instances with lazy-loaded references where not preloaded.
"""

from __future__ import annotations

import inspect
import json
import logging
import sqlite3
from collections import defaultdict
from typing import Any, Optional, Iterable

from pydantic import BaseModel, Field

from .table import Table
from .expressions import (
    ALIAS_SEPARATOR,
    ArgumentedExpression,
    Expression,
    NaryOperatorExpression,
    UnaryOperatorExpression,
    FunctionExpression,
    LikeExpression,
    TableExpression,
    ColumnExpression,
    OrderExpression,
    collect_join_paths_from_expression,
)

# Django-style lookup -> Expression method for Query.where(**kwargs) / filter(**kwargs).
# See: https://docs.djangoproject.com/en/stable/ref/models/querysets/#field-lookups
# Not implemented: regex, iregex (DB-specific; would need dialect hooks).
_WHERE_LOOKUP_MAP: dict[str, str] = {
    "exact": "__eq__",
    "iexact": "_iexact",
    "lt": "__lt__",
    "lte": "__le__",
    "gt": "__gt__",
    "gte": "__ge__",
    "in": "in_",
    "range": "between",
    "isnull": "_isnull",
    "icontains": "icontains",
    "contains": "contains",
    "istartswith": "istartswith",
    "startswith": "startswith",
    "iendswith": "iendswith",
    "endswith": "endswith",
    "like": "like",
    "ilike": "ilike",
}
from .table_mixins import (
    _WithSoftDelete,
    _WithCreatedAtTimestamp,
    _WithTimestamps,
    _WithVersion,
    _WithUpdatedAtTimestamp,
)
from .utils.get_table_by_name import get_table_by_name

logger = logging.getLogger("ormantism")


def create_table(model: type[Table], created: Optional[set[type[Table]]] = None) -> None:
    """Create the table and referenced tables if they do not exist.

    Uses Query.execute(..., ensure_structure=False) to avoid recursion.
    """
    if created is None:
        created = set()
    created.add(model)
    for field in model._get_fields().values():
        if field.is_reference:
            for t in (field.base_type, field.secondary_type):
                if inspect.isclass(t) and issubclass(t, Table) and t != Table and t not in created:
                    create_table(t, created)
    statements = list(model._get_table_sql_creations())
    statements += sum(
        (
            list(field.sql_creations)
            for field in model._get_fields().values()
            if field.name not in ("created_at", "id")
        ),
        start=[],
    )
    statements += [
        f"FOREIGN KEY ({name}_id) REFERENCES {field.base_type._get_table_name()}(id)"
        for name, field in model._get_fields().items()
        if field.is_reference
        and field.secondary_type is None
        and field.base_type != Table
    ]
    stmt_join = ",\n  ".join(statements)
    sql = f"CREATE TABLE IF NOT EXISTS {model._get_table_name()} (\n  {stmt_join})"
    Query(table=model).execute(sql, (), ensure_structure=False)


def add_columns(model: type[Table]) -> None:
    """Add any missing columns to the existing table (SQLite ALTER TABLE)."""
    tbl = model._get_table_name()
    rows = Query(table=model).execute(
        f"SELECT name FROM pragma_table_info('{tbl}')",
        (),
        ensure_structure=False,
    )
    columns_names = {name for name, in rows}
    # Never ALTER-add columns that mixins provide via TABLE_SQL_CREATIONS (id, created_at)
    mixin_column_names = {
        name
        for stmt in model._get_table_sql_creations()
        for name in (stmt.split()[0],)  # first token is column name
    }
    new_fields = [
        field
        for field in model._get_fields().values()
        if field.column_name not in columns_names
        and field.column_name not in mixin_column_names
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


def _select_paths_from_expressions(
    table: type[Table], exprs: list[ColumnExpression | TableExpression]
) -> set[str]:
    """Collect path strings from select expressions."""
    return {e.path_str for e in exprs if e is not None}


def _collect_table_expressions(query: "Query", path_strings: Iterable[str]) -> list[TableExpression]:
    """Collect root and joined TableExpressions from path strings. Root first, then children in path_str order.

    Raises ValueError for generic Table reference (same as before).
    """
    root = query.table._root_expression()
    seen_paths: set[str] = set()
    result: list[TableExpression] = [root]

    for path_str in path_strings:
        parts = path_str.split(".")
        for i in range(1, len(parts) + 1):
            prefix = ".".join(parts[:i])
            if prefix in seen_paths:
                continue
            seen_paths.add(prefix)
            try:
                e = query.resolve(prefix)
            except (AttributeError, ValueError, KeyError):
                continue
            if not isinstance(e, TableExpression):
                continue
            # Validate: segment must be a reference and not generic Table
            parent = root.table if i == 1 else e.parent.table
            seg = parts[i - 1]
            field = parent._get_field(seg)
            if not field.is_reference:
                continue
            if field.reference_type == Table:
                raise ValueError(f"Generic reference cannot be preloaded: {path_str}")
            # Avoid using "e in result": Expression.__eq__ returns a NaryOperatorExpression, so use path
            if not any(te.path == e.path and te.table is e.table for te in result):
                result.append(e)
    return result


def _yield_columns_for_table_expression(te: TableExpression):
    """Yield (alias, sql_expr) for every column of this table (same order as model._get_fields())."""
    model = te.table
    alias_prefix = te.sql_alias
    for field in model._get_fields().values():
        if field.is_reference:
            if field.secondary_type is None:
                if field.base_type == Table:
                    yield f"{alias_prefix}{ALIAS_SEPARATOR}{field.name}_table", f"{alias_prefix}.{field.name}_table"
                yield f"{alias_prefix}{ALIAS_SEPARATOR}{field.name}_id", f"{alias_prefix}.{field.name}_id"
            elif issubclass(field.base_type, (list, tuple, set)):
                if field.secondary_type == Table:
                    yield f"{alias_prefix}{ALIAS_SEPARATOR}{field.name}_tables", f"{alias_prefix}.{field.name}_tables"
                yield f"{alias_prefix}{ALIAS_SEPARATOR}{field.name}_ids", f"{alias_prefix}.{field.name}_ids"
            else:
                raise ValueError()
        else:
            yield f"{alias_prefix}{ALIAS_SEPARATOR}{field.column_name}", f"{alias_prefix}.{field.column_name}"


def _sql_from_join(table_expressions: list[TableExpression]) -> str:
    """Build FROM and LEFT JOIN clauses from table expressions (root first, then each child's JOIN)."""
    if not table_expressions:
        return ""
    root = table_expressions[0]
    lines = [next(iter(root.sql_declarations))]
    for te in table_expressions[1:]:
        decls = list(te.sql_declarations)
        if decls:
            lines.append(decls[-1])
    return "\n".join(lines)


class Query(BaseModel):
    """Fluent query builder for a Table: SELECT, JOINs, WHERE, ORDER BY, LIMIT, and execution.

    State is expression-only: select_expressions, where_expressions, order_by_expressions, limit, offset.
    SELECT and FROM/JOIN are built from TableExpression and ColumnExpression (expressions module).
    """

    model_config = {"arbitrary_types_allowed": True}

    table: type[Table]

    """The Table model this query targets."""
    select_expressions: list[Expression] = Field(default_factory=list, exclude=True)
    """Column/table expressions for SELECT and preload."""
    where_expressions: list[Expression] = Field(default_factory=list, exclude=True)
    """Expression-based WHERE (Expression instances)."""
    order_by_expressions: list[OrderExpression] = Field(default_factory=list, exclude=True)
    """OrderExpression instances for ORDER BY. Empty means use table default."""
    offset_value: Optional[int] = None
    """Optional OFFSET (stored to avoid shadowing the offset() method)."""
    limit_value: Optional[int] = None
    """Optional LIMIT (stored to avoid shadowing the limit() method)."""
    with_deleted: bool = False
    """If True, include soft-deleted rows (only valid for _WithSoftDelete tables)."""

    def _validate_expression_root(self, column: Expression) -> None:
        """Walk expression tree and raise ValueError if any ColumnExpression/TableExpression has a different root."""
        if isinstance(column, (ColumnExpression, TableExpression)):
            if column.root_table is not self.table:
                raise ValueError(
                    f"Expression has root table {column.root_table.__name__} but query is for {self.table.__name__}; "
                    f"use the query's table for all expressions (e.g. {self.table.__name__}.book.title)"
                )
        elif isinstance(column, OrderExpression):
            self._validate_expression_root(column.column_expression)
        elif isinstance(column, ArgumentedExpression):
            for a in column.arguments:
                if isinstance(a, Expression):
                    self._validate_expression_root(a)

    @property
    def _connection(self):
        """Connection for this query's table (from table._connection)."""
        return self.table._connection

    @property
    def _dialect(self):
        """Dialect for this query's table (from table._connection.dialect)."""
        return self.table._connection.dialect

    def ensure_table_structure(self) -> None:
        """Ensure the table exists and has all model columns.

        Calls create_table() and add_columns(), then sets table._ensured_table_structure.
        No-op for the base Table class or when already ensured.
        """
        if self.table == Table:
            return
        if getattr(self.table, "_ensured_table_structure", False):
            return
        create_table(self.table)
        add_columns(self.table)
        self.table._ensured_table_structure = True

    def execute(
        self,
        sql: str,
        parameters: Optional[tuple[Any, ...] | list[Any]] = None,
        ensure_structure: bool = True,
        rows_as_dicts: bool = False,
    ):
        """Run raw SQL and return rows (low-level).

        For query results as Table instances, use iteration, all(), get(), or first()
        instead. This method optionally ensures table structure and uses the table's connection.

        Args:
            sql: Full SQL statement.
            parameters: Bound parameters for placeholders; default ().
            ensure_structure: If True, ensure table and columns exist before running.
            rows_as_dicts: If True, return list of dicts; otherwise list of tuples.

        Returns:
            List of row tuples or list of row dicts, depending on rows_as_dicts.
        """
        if parameters is None:
            parameters = ()
        if ensure_structure:
            self.ensure_table_structure()
        return self._connection.execute(sql, parameters, rows_as_dicts=rows_as_dicts)

    def clone_query_with(self, **changes) -> Query:
        """Return a new Query with the same state except for the given overrides.

        Args:
            **changes: Field names and values to set on the clone (e.g. sql_limit="10").

        Returns:
            A new Query instance.
        """
        d = self.model_dump(exclude={"select_expressions", "where_expressions", "order_by_expressions"})
        d["select_expressions"] = list(self.select_expressions)
        d["where_expressions"] = list(self.where_expressions)
        d["order_by_expressions"] = list(self.order_by_expressions)
        for k, v in changes.items():
            if k in d:
                d[k] = v
        return type(self)(**d)

    def get_alias_for_path(self, path: list[str], parent_alias: Optional[str] = None) -> str:
        """Return the table alias for the table that contains the column at path (e.g. ['book','title'] -> 'tbl____book')."""
        parent = parent_alias or self.table._get_table_name()
        walk = path[:-1]  # path to table (exclude column name); for root column ['name'], walk = []
        if not walk:
            return parent
        return parent + ALIAS_SEPARATOR + ALIAS_SEPARATOR.join(walk)

    def resolve(
        self, column: str | ColumnExpression | TableExpression
    ) -> ColumnExpression | TableExpression:
        """Resolve a path string or validate an expression. Strings are resolved from the query's root.
        ColumnExpression/TableExpression are validated to have the same root as the query.

        Raises ValueError if a passed expression has a different root table.
        """
        if isinstance(column, str):
            path_str = column.replace("__", ".")
            parts = path_str.split(".")
            e: ColumnExpression | TableExpression = self.table._root_expression()
            for p in parts:
                if isinstance(e, TableExpression):
                    e = e.get_column_expression(p)
                else:
                    raise ValueError(
                        f"Cannot resolve path '{path_str}': '{p}' is not a table (column expressions cannot be traversed)"
                    )
            assert isinstance(e, (ColumnExpression, TableExpression))
            return e
        if isinstance(column, (ColumnExpression, TableExpression)):
            self._validate_expression_root(column)
            return column
        raise TypeError(
            f"resolve requires str, ColumnExpression, or TableExpression; got {type(column)}"
        )

    def _where_kwargs_to_expressions(self, kwargs: dict[str, Any]) -> list[Expression]:
        """Convert Django-style where(**kwargs) into a list of expressions (ANDed by caller)."""
        result: list[Expression] = []
        for key, value in kwargs.items():
            parts = key.split("__")
            if parts[-1] in _WHERE_LOOKUP_MAP:
                lookup = parts[-1]
                path_str = ".".join(parts[:-1]) if len(parts) > 1 else ""
            else:
                lookup = "exact"
                path_str = ".".join(parts)
            if not path_str.strip():
                raise ValueError(f"where kwargs key {key!r} must include a field path (e.g. name__icontains or foo)")
            expr = self.resolve(path_str)
            # For relation (TableExpression) + isnull, delegate to expr._isnull (uses FK column)
            if isinstance(expr, TableExpression) and lookup == "isnull":
                result.append(expr._isnull(value))
                continue
            # For relation + exact, compare FK to value (pk or instance)
            if isinstance(expr, TableExpression) and lookup == "exact":
                original_value = value
                if not isinstance(value, type) and hasattr(value, "_get_table_name") and hasattr(value, "id"):
                    value = getattr(value, "id", value)
                parent = expr.parent
                if parent is not None and expr.path:
                    field = parent.table._get_field(expr.path[-1])
                    if field.is_reference:
                        fk_col = parent.get_column_expression(field.column_name)
                        result.append(fk_col == value)
                        # Generic Table ref: also filter by _table so ref_id alone doesn't match another table's row
                        if (
                            field.base_type is Table
                            and original_value is not None
                            and hasattr(original_value, "_get_table_name")
                            and not isinstance(original_value, type)
                        ):
                            table_col = ColumnExpression(
                                table_expression=parent, name=f"{field.name}_table"
                            )
                            result.append(table_col == original_value._get_table_name())
                        continue
            # Column (or fallback) lookups
            meth = _WHERE_LOOKUP_MAP.get(lookup)
            if meth is None:
                if lookup == "isnull":
                    result.append(expr.is_null() if value else expr.is_not_null())
                else:
                    raise ValueError(f"Unknown lookup: {lookup!r}")
            else:
                result.append(getattr(expr, meth)(value))
        return result

    def select(self, *columns: type[Table] | str | ColumnExpression | TableExpression) -> Query:
        """Set select/preload. Pass the table class for all fields (e.g. User), path strings (e.g. 'name', 'books.title'), or column/table expressions (e.g. User.id, User.books.title)."""
        if not columns:
            root = self.table._root_expression()
            columns = (root.get_column_expression("id"),)
        normalized = []
        for c in columns:
            if isinstance(c, type) and issubclass(c, Table):
                if c is not self.table:
                    raise ValueError("Cannot select another table directly")
                normalized.append(c._expression)
            elif isinstance(c, (str, ColumnExpression, TableExpression)):
                normalized.append(self.resolve(c))
            else:
                raise TypeError(
                    f"select requires Table class, str, ColumnExpression, or TableExpression; got {type(c)}"
                )
        return self.clone_query_with(select_expressions=self.select_expressions + normalized)

    def where(self, *statements: Expression, **kwargs: Any) -> Query:
        """Apply filters using SQLAlchemy-like expressions and/or Django-style kwargs.

        Examples:
            where(User.id == 12)
            where(name__icontains="e", value__lt=42)
            where(foo="bar")
        """
        for stmt in statements:
            self._validate_expression_root(stmt)
        new_exprs = list(self.where_expressions) + list(statements)
        if kwargs:
            new_exprs.extend(self._where_kwargs_to_expressions(kwargs))
        return self.clone_query_with(where_expressions=new_exprs)

    def filter(self, *statements: Expression, **kwargs: Any) -> Query:
        """Alias for where(). Apply filters using expressions and/or Django-style kwargs."""
        return self.where(*statements, **kwargs)

    def include_deleted(self) -> Query:
        """Include soft-deleted rows in results.

        Returns:
            New Query with with_deleted=True.

        Raises:
            NotImplementedError: If the table does not use _WithSoftDelete.
        """
        if not issubclass(self.table, _WithSoftDelete):
            raise NotImplementedError("include_deleted only applies to tables with soft delete")
        return self.clone_query_with(with_deleted=True)

    def order_by(self, *orders: type[Table] | OrderExpression | ColumnExpression | TableExpression) -> Query:
        """Set ORDER BY. Pass the table class (e.g. User) to order by pk, or expressions (e.g. User.name, User.name.desc)."""
        order_by_expressions = list(self.order_by_expressions)
        for order in orders:
            if isinstance(order, type) and issubclass(order, Table):
                if order is not self.table:
                    raise ValueError("Cannot order by another table")
                order = order._expression
            if isinstance(order, OrderExpression):
                self._validate_expression_root(order)
                order_by_expressions.append(order)
            elif isinstance(order, (ColumnExpression, TableExpression)):
                resolved = self.resolve(order)
                if isinstance(resolved, TableExpression):
                    resolved = resolved.get_column_expression("id")
                order_by_expressions.append(OrderExpression(column_expression=resolved, desc=False))
            else:
                raise TypeError(f"order_by requires table class, OrderExpression, ColumnExpression, or TableExpression; got {type(order)}")
        return self.clone_query_with(order_by_expressions=order_by_expressions)

    def limit(self, limit: int) -> Query:
        """Set LIMIT to the given integer."""
        return self.clone_query_with(limit_value=limit)

    def offset(self, offset: int) -> Query:
        """Set OFFSET to the given integer."""
        return self.clone_query_with(offset_value=offset)

    # --- SQL-generating methods (sql_*) ---

    def sql_select_and_join(self, extra_paths: Optional[Iterable[str]] = None) -> tuple[str, str]:
        """Build full SELECT clause and FROM/JOIN clause. Column names come from cursor.description at execution."""
        all_paths = _select_paths_from_expressions(self.table, self.select_expressions)
        if extra_paths:
            all_paths = set(all_paths) | set(extra_paths)
        table_expressions = _collect_table_expressions(self, all_paths)
        columns = []
        for te in table_expressions:
            columns.extend(_yield_columns_for_table_expression(te))
        statements = [f"{expr} AS {alias}" for alias, expr in columns]
        sql_select = ", ".join(statements) if statements else ""
        from_join = _sql_from_join(table_expressions)
        return sql_select, from_join

    def sql_order(self) -> str:
        """ORDER BY clause from order_by_expressions, or id DESC when empty."""
        if not self.order_by_expressions:
            tbl = self.table._get_table_name()
            return f"{tbl}.id DESC"
        return ", ".join(o.sql for o in self.order_by_expressions)

    @property
    def sql_where(self) -> str:
        """Return WHERE clause (including leading newline) or empty string if no conditions."""
        conditions = []
        if issubclass(self.table, _WithTimestamps) and not self.with_deleted:
            conditions.append(f"{self.table._get_table_name()}.deleted_at IS NULL")
        for expression in self.where_expressions:
            conditions.append(expression.sql)
        if not conditions:
            return ""
        return "\nWHERE " + "\nAND ".join(conditions)

    @property
    def sql(self) -> str:
        """Return the compiled SQL string for this query.

        When LIMIT or OFFSET is set, uses a subquery so that limit/offset apply to root rows
        (not to the joined result), avoiding wrong pagination with JOINs.
        """
        extra_paths = set()
        for e in self.where_expressions:
            extra_paths.update(collect_join_paths_from_expression(e))
        for o in self.order_by_expressions:
            extra_paths.update(collect_join_paths_from_expression(o))
        sql_select, from_join = self.sql_select_and_join(extra_paths=extra_paths)
        tbl = self.table._get_table_name()
        order_clause = self.sql_order()
        where_clause = self.sql_where

        use_subquery = self.limit_value is not None or self.offset_value is not None
        if use_subquery:
            subquery_tables = _collect_table_expressions(self, extra_paths)
            subquery_from = _sql_from_join(subquery_tables)
            subquery = f"SELECT {tbl}.id\n{subquery_from}{where_clause}"
            subquery += "\nORDER BY " + order_clause
            if self.limit_value is not None:
                subquery += "\nLIMIT " + str(self.limit_value)
            if self.offset_value is not None:
                subquery += "\nOFFSET " + str(self.offset_value)
            sql = "SELECT " + sql_select + "\n" + from_join
            sql += f"\nWHERE {tbl}.id IN (\n{subquery}\n)"
            sql += "\nORDER BY " + order_clause
        else:
            sql = "SELECT " + sql_select + "\n" + from_join
            sql += where_clause
            sql += "\nORDER BY " + order_clause
        return sql

    @property
    def values(self) -> tuple[Any, ...]:
        """Return the bound values for this query's placeholders (same order as ? in sql)."""
        if not self.where_expressions:
            return ()
        values = []
        for expression in self.where_expressions:
            values.extend(expression.values)
        return tuple(values)

    def instance_from_row(self, row: dict[str, Any]) -> Table:
        """Build a Table instance from a row dict mapping column alias to value.

        Alias format uses ALIAS_SEPARATOR; path
        from root is deduced by splitting alias and dropping the first segment (root table name).
        """
        root_alias = self.table._get_table_name()
        data = defaultdict(lambda: defaultdict(dict))

        for alias, value in row.items():
            parts = alias.split(ALIAS_SEPARATOR)
            path = parts[1:] if parts and parts[0] == root_alias else parts
            if not path:
                continue
            d = data
            for p in path[:-1]:
                d = d[p]
            d[path[-1]] = value

        preload_paths = _select_paths_from_expressions(self.table, self.select_expressions)
        return self._instance_from_data(self.table, dict(data), preload_paths)

    def _instance_from_data(
        self,
        model: type[Table],
        data: dict,
        preload_paths: set[str],
        path_prefix: str = "",
    ) -> Table:
        """
        Hydrate a Table instance recursively from nested dictionary data.

        Args:
            model: Table subclass being instantiated.
            data: Flat or nested dictionary representing row and joined/ref data.
            preload_paths: Set of dot-path strings indicating which referenced fields
                are preloaded (as opposed to lazy).
            path_prefix: The current path from the root (used for recursive preloads).

        Returns:
            An instance of model, with all direct values set, related fields either populated
            (if preloaded) or set up for lazy load, and ._lazy_joins indicating unresolved refs.

        Notes:
            - Handles both direct references and collection references (e.g., lists of related rows).
            - Supports partial row shapes where not all relationships are hydrated.
            - Ensures lazy loaders for unresolved relationships; no premature loading.
        """
        _lazy_joins: dict[str, Any] = {}  # For unresolved refs, track what to lazy-load later
        _lazy_readonly: set[str] = set()   # Read-only scalars not fetched; will lazy-load on access

        # Iterate through each field of the Table model
        for name, field in model._get_fields().items():

            if field.is_reference:
                # Handle single object relationships (e.g., foreign keys)
                if field.secondary_type is None:
                    if field.base_type == Table:
                        # Polymorphic reference: pop the table name if present
                        reference_table = data.pop(f"{name}_table", None)
                        reference_type = get_table_by_name(reference_table) if reference_table else None
                    else:
                        reference_type = field.reference_type

                    # Get id value for reference; check for preloaded data
                    reference_id = data.pop(f"{name}_id", None)

                    if reference_id is None:
                        # No value available; treat as null
                        data[name] = None
                    elif name in data and isinstance(data.get(name), dict):
                        # Preloaded related data; hydrate recursively
                        data[name] = self._instance_from_data(
                            reference_type,
                            data[name],
                            preload_paths,
                            f"{path_prefix}.{name}".lstrip("."),
                        )
                    else:
                        # Not preloaded: set up for lazy loading
                        data.pop(name, None)  # Remove placeholder dict if present
                        _lazy_joins[name] = (reference_type, reference_id)

                # Handle collections of references (e.g., Many-to-Many or One-to-Many)
                elif issubclass(field.base_type, (list, tuple, set)):
                    references_ids = data.pop(f"{name}_ids", None)
                    # If ids were serialized, parse them
                    if isinstance(references_ids, str):
                        references_ids = json.loads(references_ids)
                    if references_ids is None:
                        references_ids = []

                    if field.secondary_type == Table:
                        # Polymorphic multiple references: pop and decode table names
                        references_tables = data.pop(f"{name}_tables", None)
                        if isinstance(references_tables, str):
                            references_tables = json.loads(references_tables)
                        references_types = list(map(get_table_by_name, references_tables or []))
                    else:
                        # All references are same type
                        references_types = [field.reference_type] * len(references_ids)

                    segment = f"{path_prefix}.{name}".lstrip(".")
                    if not references_ids:
                        # No related values
                        data[name] = []
                    elif (
                        segment in preload_paths
                        or any(p.startswith(segment + ".") for p in preload_paths)
                    ):
                        # Preloaded: hydrate each referenced instance
                        data[name] = [
                            ref_type.load(id=rid)
                            for ref_type, rid in zip(references_types, references_ids)
                        ]
                    else:
                        # Non-preloaded: leave empty list, record for lazy load
                        data[name] = []
                        _lazy_joins[name] = (references_types, references_ids)
                else:
                    raise ValueError("Unexpected reference type in _instance_from_data")
            else:
                # Normal (non-reference) field
                if (
                    name in getattr(model, "_READ_ONLY_FIELDS", ())
                    and name != "id"
                    and name not in data
                ):
                    # Read-only field not fetched: will lazy-load on access
                    _lazy_readonly.add(name)
                else:
                    data[name] = field.parse(data.get(name))

        model._ensure_lazy_loaders()        # Ensure property accessors for lazy refs
        model._suspend_validation()         # Avoid validation until instance built

        instance = model(**data)            # Hydrate a Table instance
        instance.__dict__.update(data)      # Attach all data for non-pydantic usage
        instance._lazy_joins = _lazy_joins  # Save what still needs to be lazy-loaded
        instance._lazy_readonly = _lazy_readonly

        # Remove placeholders for unresolved refs so property can fetch on access
        for name in _lazy_joins:
            if name in instance.__dict__:
                val = instance.__dict__[name]
                if val is None or val == []:
                    del instance.__dict__[name]

        for name in _lazy_readonly:
            instance.__dict__.pop(name, None)

        model._resume_validation()          # Turn validation back on

        return instance

    def __iter__(self) -> Iterable[Table]:
        """Execute the query and yield each row as a Table instance (self.table)."""
        self.table._ensure_lazy_loaders()
        rows = self.execute(self.sql, self.values, rows_as_dicts=True)
        for row_dict in rows:
            yield self.instance_from_row(row_dict)

    def all(self, limit: Optional[int] = None) -> list[Table]:
        """Return all matching rows as a list of Table instances (self.table).

        Args:
            limit: Optional maximum number of rows (applies LIMIT to the query).

        Returns:
            List of instances of self.table; may be empty.
        """
        q = self
        if limit is not None:
            q = q.limit(limit)
        return list(q)

    def update(self, **new_values) -> None:
        """Update all rows matched by this query with the given field values.

        Respects the query's WHERE (and soft-delete filter). Sets updated_at
        when the table uses _WithUpdatedAtTimestamp.

        Args:
            **new_values: Field names and values to set.

        Raises:
            AttributeError: If any key is a read-only field (e.g. id, created_at).
        """
        if not new_values:
            return
        read_only = set(new_values) & set(getattr(self.table, "_READ_ONLY_FIELDS", ()))
        if read_only:
            raise AttributeError(
                f"Cannot set read-only attribute(s) of {self.table.__name__}: {', '.join(read_only)}"
            )
        set_data = self.table.process_data(new_values)
        if not set_data:
            return
        tbl = self.table._get_table_name()
        sql = f"UPDATE {tbl}\nSET " + ", ".join(f"{k} = ?" for k in set_data)
        if issubclass(self.table, _WithUpdatedAtTimestamp):
            sql += ", updated_at = CURRENT_TIMESTAMP"
        sql += self.sql_where
        values = list(set_data.values())
        values.extend(self.values)
        self.execute(sql, tuple(values))

    def delete(self) -> None:
        """Delete all rows matched by this query (soft delete if _WithSoftDelete, else hard delete)."""
        tbl = self.table._get_table_name()
        if issubclass(self.table, _WithSoftDelete):
            sql = f"UPDATE {tbl} SET deleted_at = CURRENT_TIMESTAMP{self.sql_where}"
            self.execute(sql, self.values)
        else:
            sql = f"DELETE FROM {tbl}{self.sql_where}"
            self.execute(sql, self.values)

    def get_one(self, *statements: Expression, **kwargs: Any) -> Table:
        """Return a single Table instance (self.table) or None.

        Args:
            ensure_one_result: If True, require exactly one row: raise ValueError
                when zero or more than one result; return the instance when one.

        Returns:
            One instance of self.table, or None if no row and ensure_one_result is False.

        Raises:
            ValueError: If ensure_one_result is True and there are zero or multiple results.
        """
        rows = self.where(*statements, **kwargs).all(limit=2)
        if len(rows) == 0:
            raise ValueError("Query returned no results")
        if len(rows) > 1:
            raise ValueError("Query returned more than one result")
        return rows[0]

    def first(self) -> Optional[Table]:
        """Return the first matching row as a Table instance (self.table), or None if no results."""
        q = self.limit(1)
        for row in q:
            return row
        return None

    def upsert(
        self,
        on_conflict: list[str],
        **data: Any,
    ) -> Table:
        """Insert a row, or update if a row matching on_conflict columns already exists.

        Uses SELECT + UPDATE/INSERT (driver-agnostic, no UNIQUE constraint required).

        Args:
            on_conflict: Field names that form the match criteria (insert or update).
            **data: Field values to insert or update.

        Returns:
            The upserted Table instance (inserted or updated row).
        """
        cls = self.table
        read_only = set(data) & set(getattr(cls, "_READ_ONLY_FIELDS", ()))
        if read_only:
            raise AttributeError(
                f"Cannot set read-only attribute(s) of {cls.__name__}: {', '.join(read_only)}"
            )
        for name in on_conflict:
            if name not in data:
                raise ValueError(
                    f"on_conflict field {name!r} must be present in data"
                )

        search_kwargs = {name: data[name] for name in on_conflict}
        existing = cls.q().where(**search_kwargs).first()
        if existing:
            update_data = {k: v for k, v in data.items() if k not in getattr(cls, "_READ_ONLY_FIELDS", ())}
            if update_data:
                cls.q().where(cls.get_column_expression("id") == existing.id).update(**update_data)
            return cls.q().where(cls.get_column_expression("id") == existing.id).first()
        return cls(**data)

    def insert(
        self,
        instance: Optional[Table] = None,
        init_data: Optional[dict[str, Any]] = None,
    ) -> Table:
        """Insert a row into the table and return the instance with generated columns rehydrated.

        When instance is provided (e.g. from Table constructor via on_after_create), persists it
        and sets PK, created_at, etc. on the instance. When instance is None, creates an empty
        instance and inserts DEFAULT VALUES.
        """
        cls = self.table
        init_data = init_data if init_data is not None else {}

        if instance is None:
            cls._suspend_validation()
            try:
                instance = cls()
            finally:
                cls._resume_validation()

        if instance.id is not None and instance.id >= 0:
            return instance

        instance.check_read_only(init_data)

        if isinstance(instance, _WithVersion):
            tbl = cls._get_table_name()
            sql = f"UPDATE {tbl} SET deleted_at = CURRENT_TIMESTAMP WHERE deleted_at IS NULL"
            values = []
            for name, value in init_data.items():
                if name not in cls._VERSIONING_ALONG:
                    continue
                if value is None:
                    sql += f" AND {name} IS NULL"
                else:
                    sql += f" AND {name} = ?"
                    values.append(value)
            sql += " RETURNING version"
            rows = self.execute(sql, values, ensure_structure=True)
            init_data["version"] = (max(version for version, in rows) + 1) if rows else 0

        exclude = set(cls.model_fields)
        include = set()
        processed_data = cls.process_data(init_data)
        formatted_data = {}

        for name, field in cls._get_fields().items():
            if (
                not field.is_reference
                and name not in cls._READ_ONLY_FIELDS
                and field.default is not None
            ):
                include.add(name)
                if name not in processed_data:
                    object.__setattr__(instance, name, field.default)

        for name, value in processed_data.items():
            include.add(name)
            if name in exclude:
                exclude.remove(name)
            else:
                formatted_data[name] = value

        for name, field in cls._get_fields().items():
            if name not in include or name in exclude or field.is_reference:
                continue
            if name not in processed_data and field.default is not None:
                instance_value = field.default
            else:
                instance_value = getattr(instance, name)
            formatted_data[name] = field.serialize(instance_value)

        if formatted_data:
            tbl = cls._get_table_name()
            sql = (
                f"INSERT INTO {tbl} ({', '.join(formatted_data.keys())})\n"
                f"VALUES ({', '.join('?' for _ in formatted_data.values())})\n"
                "RETURNING id"
            )
            rows = self.execute(sql, list(formatted_data.values()), ensure_structure=True)
        else:
            tbl = cls._get_table_name()
            sql = f"INSERT INTO {tbl} DEFAULT VALUES\nRETURNING id"
            rows = self.execute(sql, [], ensure_structure=True)
        id_field = cls._get_field("id")
        object.__setattr__(instance, "id", id_field.parse(rows[0][0]))
        instance._mark_readonly_lazy()

        for name, field in cls._get_fields().items():
            if (
                not field.is_reference
                and name not in cls._READ_ONLY_FIELDS
                and field.default is not None
                and name not in processed_data
            ):
                object.__setattr__(instance, name, field.default)

        if hasattr(instance, "__post_init__"):
            instance.__post_init__()

        return instance


__all__ = ["Query", "ALIAS_SEPARATOR"]
