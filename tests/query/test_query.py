"""Tests for ormantism.query: Query, ensure_table_structure, run_sql, select/where/order/limit, update/delete, instance_from_row, versioning, polymorphic refs."""

import pytest
from ormantism.table import Table
from ormantism.query import (
    Query,
    ALIAS_SEPARATOR,
    add_columns,
    ensure_table_structure,
    run_sql,
)


def _pk(inst: Table):
    """Return the instance's stored primary key (from __dict__); never return an Expression."""
    from ormantism.query import _stored_pk
    return _stored_pk(inst)


class TestEnsureTableStructure:
    """Test ensure_table_structure runs once per model and creates table/columns."""

    def test_ensure_table_structure_creates_table(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        ensure_table_structure(A)
        # Table exists: we can load
        a = A(name="x")
        assert a.id is not None

    def test_ensure_table_structure_idempotent(self, setup_db):
        class B(Table, with_timestamps=True):
            value: int = 0

        ensure_table_structure(B)
        ensure_table_structure(B)
        b = B()
        assert b.id is not None

    def test_ensure_table_structure_base_table_no_op(self, setup_db):
        """ensure_table_structure(Table) is a no-op and does not raise."""
        ensure_table_structure(Table)


class TestQueryBasics:
    """Test Query construction, execute, clone."""

    def test_query_initial_select_is_pk(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        q = Query(table=A)
        assert q.select_expressions
        assert q.select_expressions[0].path_str == "id"
        # Query runs and returns rows (SELECT built from join)
        list(q)

    def test_execute_returns_rows(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        A(name="a1")
        A(name="a2")
        q = Query(table=A)
        sql = f"SELECT id FROM {A._get_table_name()} ORDER BY id"
        rows = q.execute(sql)
        assert len(rows) >= 2

    def test_clone_query_with(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        q = Query(table=A).limit(5)
        q2 = q.clone_query_with(limit_value=10)
        assert q2.table is A
        assert q2.limit_value == 10
        assert q.limit_value == 5

    def test_query_dialect_property(self, setup_db):
        """Query._dialect returns table._connection.dialect (query.py ~483)."""
        class A(Table, with_timestamps=True):
            name: str = ""

        q = Query(table=A)
        d = q._dialect
        assert d is A._connection.dialect


class TestQuerySelect:
    """Test select() and iteration with columns/preload."""

    def test_select_expands_columns(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""
            value: int = 0

        A(name="x", value=1)
        q = Query(table=A).select(A.pk, A.name, A.value)
        rows = list(q)
        assert len(rows) == 1
        assert rows[0].id == 1
        assert rows[0].name == "x"
        assert rows[0].value == 1

    def test_select_with_preload(self, setup_db):
        class B(Table, with_timestamps=True):
            value: int = 42

        class C(Table, with_timestamps=True):
            links_to: B | None = None

        b = B()
        C(links_to=b)
        q = Query(table=C).select(C.pk, C.links_to).where(C.get_column_expression("links_to_id") == _pk(b))
        rows = list(q)
        assert len(rows) >= 1
        assert rows[0].links_to is not None
        assert rows[0].links_to.id == b.id


class TestQueryWhere:
    """Test where() filtering."""

    def test_where_id(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="unique")
        q = Query(table=A).where(A.pk == _pk(a))
        row = q.first()
        assert row is not None
        assert row.id == a.id
        assert row.name == "unique"

    def test_where_criteria(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        A(name="x")
        A(name="y")
        q = Query(table=A).where(A.name == "y")
        row = q.first()
        assert row is not None
        assert row.name == "y"

    def test_where_multiple_expressions_combined_with_and(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""
            value: int = 0

        A(name="a", value=1)
        A(name="a", value=2)
        q = Query(table=A).where(A.name == "a", A.value == 1)
        row = q.first()
        assert row is not None
        assert row.name == "a"
        assert row.value == 1


class TestQueryWhereKwargsAndFilter:
    """Django-style where(**kwargs) and filter alias."""

    def test_where_exact_kwarg(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""
            value: int = 0

        A(name="foo", value=1)
        A(name="bar", value=2)
        q = Query(table=A).where(name="bar")
        row = q.first()
        assert row is not None
        assert row.name == "bar"

    def test_where_icontains_and_lt(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""
            value: int = 0

        A(name="Alice", value=10)
        A(name="Bob", value=30)
        A(name="Charlie", value=25)
        q = Query(table=A).where(name__icontains="e", value__lt=42)
        rows = list(q)
        assert len(rows) >= 1
        for r in rows:
            assert "e" in r.name.lower()
            assert r.value < 42

    def test_where_and_expression_combined(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""
            value: int = 0

        A(name="x", value=1)
        A(name="y", value=1)
        q = Query(table=A).where(A.value == 1, name="y")
        row = q.first()
        assert row is not None
        assert row.name == "y"
        assert row.value == 1

    def test_filter_alias(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        A(name="a")
        A(name="b")
        q = Query(table=A).filter(name="b")
        row = q.first()
        assert row is not None
        assert row.name == "b"

    def test_where_nested_path(self, setup_db):
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        b = B(title="Python")
        A(book=b)
        q = Query(table=A).where(book__title__contains="Py")
        row = q.first()
        assert row is not None
        assert row.book.title == "Python"

    def test_where_iexact(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        A(name="Hello")
        A(name="HELLO")
        A(name="hello")
        A(name="other")
        q = Query(table=A).where(name__iexact="hello")
        rows = list(q)
        assert len(rows) == 3
        assert {r.name for r in rows} == {"Hello", "HELLO", "hello"}

    def test_where_range(self, setup_db):
        class A(Table, with_timestamps=True):
            value: int = 0

        for i in (1, 5, 10, 15, 20):
            A(value=i)
        q = Query(table=A).where(value__range=(5, 15))
        rows = list(q)
        assert len(rows) == 3
        assert {r.value for r in rows} == {5, 10, 15}

    def test_where_empty_path_raises(self, setup_db):
        """where(**{'__exact': 5}) raises because path is empty (query.py ~581)."""
        class A(Table, with_timestamps=True):
            name: str = ""

        q = Query(table=A)
        with pytest.raises(ValueError, match="field path"):
            q.where(**{"__exact": 5})

    def test_where_relation_exact_fk_comparison(self, setup_db):
        """where(book=instance) and where(book__exact=id) use FK column (query.py 589-597)."""
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        b = B(title="x")
        A(book=b)
        row = Query(table=A).where(book=b).first()
        assert row is not None and row.book is not None and row.book.id == b.id
        row2 = Query(table=A).where(book__exact=b.id).first()
        assert row2 is not None and row2.book is not None

    def test_where_relation_isnull(self, setup_db):
        """relation__isnull uses TableExpression._isnull (FK column IS NULL / IS NOT NULL)."""
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        b = B(title="x")
        A(book=b)
        A(book=None)
        q_null = Query(table=A).where(book__isnull=True)
        rows_null = list(q_null)
        assert len(rows_null) == 1
        assert rows_null[0].book is None
        q_not_null = Query(table=A).where(book__isnull=False)
        rows_not_null = list(q_not_null)
        assert len(rows_not_null) == 1
        assert rows_not_null[0].book is not None


class TestQueryOrderAndLimit:
    """Test order_by and limit."""

    def test_order_by(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        A(name="c")
        A(name="a")
        A(name="b")
        q = Query(table=A).order_by(A.name).limit(3)
        rows = list(q)
        names = [r.name for r in rows]
        assert names == sorted(names)

    def test_order_by_desc(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        A(name="first")
        A(name="second")
        q = Query(table=A).order_by(A.pk.desc).limit(2)
        rows = list(q)
        ids = [r.id for r in rows]
        assert ids == sorted(ids, reverse=True)

    def test_order_by_table_expression_orders_by_that_table_pk(self, setup_db):
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        b1 = B(title="first")
        b2 = B(title="second")
        A(book=b1)
        A(book=b2)
        # Order by the related table expression (resolves to book.id)
        q = Query(table=A).select(A.pk, A.book).order_by(A.book).limit(5)
        rows = list(q)
        assert len(rows) == 2
        assert [r.id for r in rows] == [1, 2]
        assert rows[0].book.id == b1.id
        assert rows[1].book.id == b2.id

    def test_default_order_without_timestamps_uses_pk_desc(self, setup_db):
        """Table without timestamps: default ORDER BY is pk DESC."""
        class A(Table, with_timestamps=False):
            name: str = ""

        A(name="z")
        A(name="a")
        q = Query(table=A)
        tbl = A._get_table_name()
        expected_sql = (
            f"SELECT {tbl}.id AS {tbl}{ALIAS_SEPARATOR}id, {tbl}.name AS {tbl}{ALIAS_SEPARATOR}name\n"
            f"FROM {tbl}\n"
            f"ORDER BY {tbl}.id DESC"
        )
        assert q.sql == expected_sql
        rows = list(q)
        assert len(rows) == 2
        # ORDER BY id DESC: higher id first
        assert rows[0].id == 2
        assert rows[0].name == "a"
        assert rows[1].id == 1
        assert rows[1].name == "z"

    def test_limit(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        for i in range(5):
            A(name=f"n{i}")
        q = Query(table=A).limit(2)
        rows = list(q)
        assert len(rows) == 2
        assert all(r.id >= 1 and r.name in [f"n{i}" for i in range(5)] for r in rows)
        assert rows[0].id != rows[1].id


class TestQueryIncludeDeleted:
    """Test include_deleted for soft-delete tables."""

    def test_include_deleted_raises_without_soft_delete(self, setup_db):
        class A(Table, with_timestamps=False):
            name: str = ""

        q = Query(table=A)
        with pytest.raises(NotImplementedError, match="include_deleted"):
            q.include_deleted()

    def test_include_deleted_returns_deleted_rows(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        a.delete()
        # Default: not found
        q_default = Query(table=A).where(A.pk == _pk(a))
        assert q_default.first() is None
        # With include_deleted: found
        q_with = Query(table=A).include_deleted().where(A.pk == _pk(a))
        found = q_with.first()
        assert found is not None
        assert found.id == a.id


class TestQueryAllGetFirst:
    """Test all(), get(), first()."""

    def test_all(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        A(name="a")
        A(name="b")
        q = Query(table=A)
        results = q.all()
        assert isinstance(results, list)
        assert len(results) == 2
        assert all(isinstance(r, A) for r in results)
        ids = {r.id for r in results}
        assert ids == {1, 2}
        assert {r.name for r in results} == {"a", "b"}
        for r in results:
            if r.id == 1:
                assert r.name == "a"
            else:
                assert r.name == "b"

    def test_all_with_limit(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        for _ in range(5):
            A(name="x")
        q = Query(table=A)
        results = q.all(limit=3)
        assert len(results) == 3
        assert all(r.id >= 1 and r.name == "x" for r in results)
        assert len({r.id for r in results}) == 3

    def test_first_returns_one_or_none(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="only")
        q = Query(table=A).where(A.pk == _pk(a))
        assert q.first() is not None
        q_none = Query(table=A).where(A.pk == 999999)
        assert q_none.first() is None

    def test_get_returns_first(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="g")
        q = Query(table=A).where(A.pk == _pk(a))
        assert q.get() is not None
        assert q.get().id == a.id

    def test_get_ensure_one_result_zero_raises(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        q = Query(table=A).where(A.pk == 999999)
        with pytest.raises(ValueError, match="no results"):
            q.get(ensure_one_result=True)

    def test_get_ensure_one_result_two_raises(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        A(name="a")
        A(name="b")
        q = Query(table=A)
        with pytest.raises(ValueError, match="more than one"):
            q.get(ensure_one_result=True)

    def test_get_ensure_one_result_one_returns(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="single")
        q = Query(table=A).where(A.pk == _pk(a))
        row = q.get(ensure_one_result=True)
        assert row is not None
        assert row.id == a.id


class TestQueryUpdate:
    """Test update() on query."""

    def test_update_changes_rows(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""
            value: int = 0

        A(name="x", value=1)
        A(name="x", value=2)
        q = Query(table=A).where(A.name == "x")
        q.update(value=10)
        rows = list(Query(table=A).where(A.name == "x"))
        assert len(rows) == 2
        assert sorted(r.id for r in rows) == [1, 2]
        assert all(r.value == 10 for r in rows)

    def test_update_read_only_raises(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        q = Query(table=A)
        with pytest.raises(AttributeError, match="read-only"):
            q.update(id=999)


class TestQueryExecuteReturning:
    """Test execute_returning."""

    def test_execute_returning_returns_rows(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="r")
        tbl = A._get_table_name()
        q = Query(table=A)
        # SQLite doesn't support RETURNING in all versions; just run a SELECT
        rows = q.execute_returning(f"SELECT id, name FROM {tbl} WHERE id = ?", (_pk(a),))
        assert len(rows) == 1
        assert rows[0][0] == a.id
        assert rows[0][1] == "r"


class TestQueryIteration:
    """Test __iter__ and integration with Table.load semantics."""

    def test_iter_yields_table_instances(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        A(name="i1")
        A(name="i2")
        q = Query(table=A)
        count = 0
        for row in q:
            assert isinstance(row, A)
            count += 1
            if count >= 2:
                break
        assert count >= 2


class TestQueryJoin:
    """Test join-tree behavior via Query: path validation, get_alias_for_path."""

    def test_select_non_reference_column_builds_without_join(self, setup_db):
        """Selecting a non-reference column (e.g. name) does not add a join; SQL builds successfully."""
        class A(Table, with_timestamps=True):
            name: str = ""

        q = Query(table=A).select(A.name)
        assert "name" in q.sql

    def test_add_children_generic_table_reference_raises(self, setup_db):
        class A(Table, with_timestamps=True):
            ref: Table | None = None

        q = Query(table=A).select(A.ref)  # generic reference
        with pytest.raises(ValueError, match="Generic reference cannot be preloaded"):
            _ = q.sql

    def test_get_alias_for_path_root_column_returns_parent_alias(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        q = Query(table=A)
        assert q.get_alias_for_path(["name"]) == A._get_table_name()

    def test_get_alias_for_path_nested_returns_joined_alias(self, setup_db):
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        q = Query(table=A).select(A.book)
        alias = q.get_alias_for_path(["book", "title"])
        assert "book" in alias
        assert alias == f"{A._get_table_name()}{ALIAS_SEPARATOR}book"


class TestResolveUserPathAndSelectString:
    """Query._resolve_user_path and Query.select with path strings."""

    def test_resolve_user_path_root_column(self, setup_db):
        """Query._resolve_user_path('name') returns the root column expression."""
        class A(Table, with_timestamps=True):
            name: str = ""

        expr = Query(table=A)._resolve_user_path("name")
        assert expr.path_str == "name"
        assert "name" in Query(table=A).select(expr).sql

    def test_resolve_user_path_nested_dot(self, setup_db):
        """Query._resolve_user_path('book.title') returns the nested column expression."""
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        expr = Query(table=A)._resolve_user_path("book.title")
        assert expr.path_str == "book.title"
        assert "title" in Query(table=A).select(expr).sql and "JOIN" in Query(table=A).select(expr).sql

    def test_resolve_user_path_nested_double_underscore(self, setup_db):
        """Query._resolve_user_path('book__title') is treated as 'book.title'."""
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        expr = Query(table=A)._resolve_user_path("book__title")
        assert expr.path_str == "book.title"

    def test_resolve_user_path_traverse_column_raises(self, setup_db):
        """_resolve_user_path('name.foo') raises: cannot traverse column expression (query.py ~563)."""
        class A(Table, with_timestamps=True):
            name: str = ""

        q = Query(table=A)
        with pytest.raises(ValueError, match="not a table"):
            q._resolve_user_path("name.foo")

    def test_select_column_expression_direct(self, setup_db):
        """select(ColumnExpression) appends expression directly (query.py ~618 else branch)."""
        class A(Table, with_timestamps=True):
            name: str = ""

        col = A.get_column_expression("name")
        q = Query(table=A).select(col)
        assert "name" in q.sql
        rows = list(q)
        assert all(hasattr(r, "name") for r in rows)

    def test_select_string_root_column(self, setup_db):
        """select('name') builds the same as select(A.name)."""
        class A(Table, with_timestamps=True):
            name: str = ""

        q = Query(table=A).select("name")
        assert "name" in q.sql
        rows = list(q)
        assert all(hasattr(r, "name") for r in rows)

    def test_select_string_nested_column(self, setup_db):
        """select('book.title') builds JOIN and returns rows."""
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        b = B(title="t1")
        A(book=b)
        q = Query(table=A).select("book.title")
        assert "JOIN" in q.sql
        rows = list(q)
        assert len(rows) >= 1
        assert rows[0].book is not None and rows[0].book.title == "t1"


class TestCompiledSqlAndValues:
    """Test q.sql / q.values and subquery when LIMIT/OFFSET set."""

    def test_sql_compiled_and_values_no_where_no_limit_exact(self, setup_db):
        """Exact SQL and values for a minimal query (no WHERE, no LIMIT)."""
        class A(Table, with_timestamps=False):
            name: str = ""

        q = Query(table=A)
        sql, values = q.sql, q.values
        tbl = A._get_table_name()
        expected_sql = (
            f"SELECT {tbl}.id AS {tbl}{ALIAS_SEPARATOR}id, {tbl}.name AS {tbl}{ALIAS_SEPARATOR}name\n"
            f"FROM {tbl}\n"
            f"ORDER BY {tbl}.id DESC"
        )
        assert sql == expected_sql
        assert values == ()

    def test_sql_compiled_and_values_where_exact(self, setup_db):
        """Exact SQL and values for a query with one WHERE expression."""
        class A(Table, with_timestamps=False):
            name: str = ""

        q = Query(table=A).where(A.name == "x")
        sql, values = q.sql, q.values
        tbl = A._get_table_name()
        expected_sql = (
            f"SELECT {tbl}.id AS {tbl}{ALIAS_SEPARATOR}id, {tbl}.name AS {tbl}{ALIAS_SEPARATOR}name\n"
            f"FROM {tbl}\n"
            f"WHERE ({tbl}.name = ?)\n"
            f"ORDER BY {tbl}.id DESC"
        )
        assert sql == expected_sql
        assert values == ("x",)

    def test_sql_compiled_and_values_where_two_expressions_exact(self, setup_db):
        """Exact SQL and values for WHERE with two expressions (AND)."""
        class A(Table, with_timestamps=False):
            name: str = ""
            value: int = 0

        q = Query(table=A).where(A.name == "a", A.value == 42)
        sql, values = q.sql, q.values
        tbl = A._get_table_name()
        expected_sql = (
            f"SELECT {tbl}.id AS {tbl}{ALIAS_SEPARATOR}id, {tbl}.name AS {tbl}{ALIAS_SEPARATOR}name, "
            f"{tbl}.value AS {tbl}{ALIAS_SEPARATOR}value\n"
            f"FROM {tbl}\n"
            f"WHERE ({tbl}.name = ?)\n"
            f"AND ({tbl}.value = ?)\n"
            f"ORDER BY {tbl}.id DESC"
        )
        assert sql == expected_sql
        assert values == ("a", 42)

    def test_sql_compiled_and_values_with_limit_exact(self, setup_db):
        """Exact SQL and values for LIMIT (subquery form)."""
        class A(Table, with_timestamps=False):
            name: str = ""

        q = Query(table=A).limit(2)
        sql, values = q.sql, q.values
        tbl = A._get_table_name()
        subquery = (
            f"SELECT {tbl}.id\n"
            f"FROM {tbl}\n"
            f"ORDER BY {tbl}.id DESC\n"
            "LIMIT 2"
        )
        expected_sql = (
            f"SELECT {tbl}.id AS {tbl}{ALIAS_SEPARATOR}id, {tbl}.name AS {tbl}{ALIAS_SEPARATOR}name\n"
            f"FROM {tbl}\n"
            f"WHERE {tbl}.id IN (\n{subquery}\n)\n"
            f"ORDER BY {tbl}.id DESC"
        )
        assert sql == expected_sql
        assert values == ()

    def test_sql_compiled_and_values_with_offset_exact(self, setup_db):
        """Exact SQL and values for OFFSET (subquery form)."""
        class A(Table, with_timestamps=False):
            name: str = ""

        q = Query(table=A).offset(1)
        sql, values = q.sql, q.values
        tbl = A._get_table_name()
        subquery = (
            f"SELECT {tbl}.id\n"
            f"FROM {tbl}\n"
            f"ORDER BY {tbl}.id DESC\n"
            "OFFSET 1"
        )
        expected_sql = (
            f"SELECT {tbl}.id AS {tbl}{ALIAS_SEPARATOR}id, {tbl}.name AS {tbl}{ALIAS_SEPARATOR}name\n"
            f"FROM {tbl}\n"
            f"WHERE {tbl}.id IN (\n{subquery}\n)\n"
            f"ORDER BY {tbl}.id DESC"
        )
        assert sql == expected_sql
        assert values == ()

    def test_sql_compiled_and_values_where_and_limit_exact(self, setup_db):
        """Exact SQL and values for WHERE + LIMIT (subquery has WHERE)."""
        class A(Table, with_timestamps=False):
            name: str = ""

        q = Query(table=A).where(A.name == "x").limit(1)
        sql, values = q.sql, q.values
        tbl = A._get_table_name()
        subquery = (
            f"SELECT {tbl}.id\n"
            f"FROM {tbl}\n"
            f"WHERE ({tbl}.name = ?)\n"
            f"ORDER BY {tbl}.id DESC\n"
            "LIMIT 1"
        )
        expected_sql = (
            f"SELECT {tbl}.id AS {tbl}{ALIAS_SEPARATOR}id, {tbl}.name AS {tbl}{ALIAS_SEPARATOR}name\n"
            f"FROM {tbl}\n"
            f"WHERE {tbl}.id IN (\n{subquery}\n)\n"
            f"ORDER BY {tbl}.id DESC"
        )
        assert sql == expected_sql
        assert values == ("x",)

    def test_compiled_sql_executes_same_as_iteration(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        A(name="c1")
        A(name="c2")
        q = Query(table=A).where(A.name == "c1").limit(1)
        sql, values = q.sql, q.values
        rows = q.execute(sql, values)
        assert len(rows) == 1
        inst = q.first()
        assert inst is not None
        assert inst.id == 1
        assert inst.name == "c1"


class TestQueryOffset:
    """Test offset() applies correctly via subquery."""

    def test_offset_with_limit_returns_correct_count(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        for i in range(5):
            A(name=f"n{i}")
        q = Query(table=A).order_by(A.pk).limit(2).offset(1)
        rows = list(q)
        assert len(rows) == 2
        # ORDER BY id ASC, OFFSET 1 LIMIT 2 -> ids 2 and 3
        assert rows[0].id == 2
        assert rows[0].name == "n1"
        assert rows[1].id == 3
        assert rows[1].name == "n2"
        tbl = A._get_table_name()
        subquery = (
            f"SELECT {tbl}.id\n"
            f"FROM {tbl}\n"
            f"WHERE {tbl}.deleted_at IS NULL\n"
            f"ORDER BY {tbl}.id ASC\n"
            "LIMIT 2\n"
            "OFFSET 1"
        )
        expected_sql = (
            f"SELECT {tbl}.updated_at AS {tbl}{ALIAS_SEPARATOR}updated_at, {tbl}.deleted_at AS {tbl}{ALIAS_SEPARATOR}deleted_at, "
            f"{tbl}.created_at AS {tbl}{ALIAS_SEPARATOR}created_at, {tbl}.id AS {tbl}{ALIAS_SEPARATOR}id, "
            f"{tbl}.name AS {tbl}{ALIAS_SEPARATOR}name\n"
            f"FROM {tbl}\n"
            f"WHERE {tbl}.id IN (\n{subquery}\n)\n"
            f"ORDER BY {tbl}.id ASC"
        )
        assert q.sql == expected_sql


class TestRunSqlAndEnsureStructureCoverage:
    """Tests to cover run_sql, add_columns, and related paths in query.py."""

    def test_run_sql_with_parameters_none(self, setup_db):
        """run_sql(model, sql, None) uses empty tuple for parameters (covers parameters is None branch)."""
        class A(Table, with_timestamps=True):
            name: str = ""

        ensure_table_structure(A)
        rows = run_sql(A, "SELECT 1 AS x", parameters=None)
        assert len(rows) == 1
        assert rows[0][0] == 1

    def test_add_columns_adds_missing_column(self, setup_db):
        """ensure_table_structure adds missing column when table exists with fewer columns (covers add_columns)."""
        class A(Table, with_timestamps=True):
            name: str = ""
            value: int = 0

        tbl = A._get_table_name()
        # Create table without "value" column so add_columns will ALTER TABLE
        run_sql(
            A,
            f"CREATE TABLE IF NOT EXISTS {tbl} (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
            "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP, "
            "deleted_at TIMESTAMP, name TEXT)",
            ensure_structure=False,
        )
        # Clear so ensure_table_structure will run create_table + add_columns
        if getattr(A, "_CHECKED_TABLE_EXISTENCE", False):
            delattr(A, "_CHECKED_TABLE_EXISTENCE")
        ensure_table_structure(A)
        a = A(name="x", value=42)
        assert a.id == 1
        assert a.name == "x"
        assert a.value == 42

    def test_add_columns_duplicate_column_ignored(self, setup_db):
        """add_columns when column already exists catches OperationalError (covers 141-143)."""
        class A(Table, with_timestamps=True):
            name: str = ""

        ensure_table_structure(A)
        A(name="one")
        add_columns(A)
        row = Query(table=A).first()
        assert row is not None
        assert row.id == 1
        assert row.name == "one"


class TestVersionedTableDefaultOrder:
    """Default ORDER BY for _WithVersion tables (covers sql_order versioning branch)."""

    def test_versioned_table_default_order_in_sql(self, setup_db):
        class Doc(Table, versioning_along=("name",)):
            name: str = ""

        ensure_table_structure(Doc)
        q = Query(table=Doc)
        sql = q.sql
        assert "version" in sql
        assert "DESC" in sql
        assert Doc._get_table_name() in sql


class TestGetAliasForPathCoverage:
    """get_alias_for_path with parent_alias (covers non-default parent_alias branch)."""

    def test_get_alias_for_path_with_parent_alias(self, setup_db):
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        q = Query(table=A)
        alias = q.get_alias_for_path(["book", "title"], parent_alias="mytbl")
        assert alias == "mytbl" + ALIAS_SEPARATOR + "book"


class TestQueryUpdateCoverage:
    """update() no-op and empty set_data branches."""

    def test_update_with_no_kwargs_returns_early(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        A(name="one")
        q = Query(table=A)
        q.update()
        rows = list(q)
        assert len(rows) == 1
        assert rows[0].id == 1
        assert rows[0].name == "one"

    def test_update_with_empty_set_data_returns_early(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        A(name="x")
        q = Query(table=A)
        q.update()
        rows = list(Query(table=A))
        assert len(rows) == 1
        assert rows[0].id == 1
        assert rows[0].name == "x"


class TestInstanceFromRowAndHydrationCoverage:
    """instance_from_row skip root-only alias; preloaded list ref; nullable ref."""

    def test_instance_from_row_skips_alias_equal_to_root_only(self, setup_db):
        """Row key that equals root table name (no separator) is skipped (path empty)."""
        class A(Table, with_timestamps=False):
            name: str = ""

        A(name="x")
        q = Query(table=A).select(A.pk, A.name)
        rows = list(q)
        assert len(rows) == 1
        assert rows[0].id == 1
        assert rows[0].name == "x"
        tbl = A._get_table_name()
        row_dict = {tbl: 999, f"{tbl}{ALIAS_SEPARATOR}id": 1, f"{tbl}{ALIAS_SEPARATOR}name": "x"}
        inst = q.instance_from_row(row_dict)
        assert inst.id == 1
        assert inst.name == "x"

    def test_lazy_list_ref_loads_with_correct_ids(self, setup_db, expect_lazy_loads):
        """Load without preload: list ref is lazy; on access we get instances with correct ids."""
        class Child(Table, with_timestamps=True):
            x: int = 0

        class Parent(Table, with_timestamps=True):
            kids: list[Child] = []

        c1 = Child(x=10)
        c2 = Child(x=20)
        p = Parent(kids=[c1, c2])
        assert p.id == 1
        loaded = Parent.load(id=p.id)
        assert loaded is not None
        assert loaded.id == 1
        assert "kids" in loaded._lazy_joins
        kids = loaded.kids
        assert len(kids) == 2
        ids = sorted(k.id for k in kids)
        assert ids == [1, 2]
        vals = sorted(k.x for k in kids)
        assert vals == [10, 20]
        expect_lazy_loads.expect(1)

    @pytest.mark.skip(reason="preload for list refs in load() not yet supported (join column naming)")
    def test_preload_list_ref_avoids_lazy(self, setup_db, expect_lazy_loads):
        """Load with preload='kids': accessing kids must not trigger lazy load."""
        class Child(Table, with_timestamps=True):
            x: int = 0

        class Parent(Table, with_timestamps=True):
            kids: list[Child] = []

        c1 = Child(x=10)
        c2 = Child(x=20)
        p = Parent(kids=[c1, c2])
        loaded = Parent.load(id=p.id, preload="kids")
        assert loaded is not None
        kids = loaded.kids
        assert len(kids) == 2
        assert sorted(k.id for k in kids) == [1, 2]
        expect_lazy_loads.expect(0)

    def test_load_row_with_nullable_ref_none(self, setup_db):
        """Row with ref_id None produces instance with ref=None (reference_id is None branch)."""
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        a = A(book=None)
        assert a.id == 1
        q = Query(table=A).select(A.pk, A.book).where(A.pk == 1)
        row = q.first()
        assert row is not None
        assert row.id == 1
        assert row.book is None

    def test_instance_from_row_with_explicit_null_ref(self, setup_db):
        """instance_from_row with row containing ref_id key set to None (covers 741-742)."""
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        tbl = A._get_table_name()
        row_dict = {
            f"{tbl}{ALIAS_SEPARATOR}id": 1,
            f"{tbl}{ALIAS_SEPARATOR}book_id": None,
        }
        q = Query(table=A)
        inst = q.instance_from_row(row_dict)
        assert inst.id == 1
        assert inst.book is None


class TestOrderByTypeError:
    """order_by with invalid type raises TypeError (covers _to_order_expression raise)."""

    def test_order_by_invalid_type_raises(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        q = Query(table=A)
        with pytest.raises(TypeError, match="OrderExpression, ColumnExpression, or TableExpression"):
            q.order_by(123)

    def test_order_by_table_expression_uses_pk(self, setup_db):
        """order_by(TableExpression) normalizes to PK column (355-357)."""
        class A(Table, with_timestamps=True):
            name: str = ""

        q = Query(table=A).order_by(A)
        sql = q.sql
        assert "ORDER BY" in sql
        assert "id" in sql

    def test_order_by_relation_table_expression(self, setup_db):
        """order_by(TableExpression for relation) uses that table's PK (query.py ~662)."""
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        q = Query(table=A).select(A.pk, A.book).order_by(A.book).limit(2)
        sql = q.sql
        assert "ORDER BY" in sql
        assert "book" in sql or "id" in sql

    def test_to_order_expression_table_expression_direct(self, setup_db):
        """_to_order_expression with TableExpression (350-357) - called directly."""
        from ormantism.expressions import OrderExpression
        from ormantism.query import _to_order_expression

        class A(Table, with_timestamps=True):
            name: str = ""

        o = _to_order_expression(A, A._expression)
        assert o.column_expression is not None
        assert o.column_expression.path_str == "id"

        o2 = _to_order_expression(A, o)
        assert o2 is o

        col = A.get_column_expression("name")
        o3 = _to_order_expression(A, col)
        assert o3.column_expression is col

        with pytest.raises(TypeError, match="OrderExpression, ColumnExpression, or TableExpression"):
            _to_order_expression(A, 123)


class TestSelectWithNoArguments:
    """select() called with no arguments uses PK column (covers 551-553)."""

    def test_select_empty_uses_pk_then_iterates(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        A(name="x")
        q = Query(table=A).select()
        rows = list(q)
        assert len(rows) == 1
        assert rows[0].id == 1
        assert rows[0].name == "x"


class TestUpdateAndDeleteInstanceCoverage:
    """update_instance and delete_instance (soft and hard) via save/delete."""

    def test_assign_after_create_calls_update_instance(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="one")
        assert a.id == 1
        a.name = "two"
        loaded = A.load(id=1)
        assert loaded is not None
        assert loaded.id == 1
        assert loaded.name == "two"

    def test_delete_soft_sets_deleted_at(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        assert a.id == 1
        a.delete()
        assert Query(table=A).first() is None
        found = Query(table=A).include_deleted().where(A.pk == 1).first()
        assert found is not None
        assert found.id == 1

    def test_delete_hard_removes_row(self, setup_db):
        class A(Table, with_timestamps=False):
            name: str = ""

        a = A(name="x")
        assert a.id == 1
        a.delete()
        assert Query(table=A).first() is None


class TestInsertInstanceCoverage:
    """insert_instance branches: versioned insert, default-only insert."""

    def test_versioned_insert_sets_version(self, setup_db):
        class Doc(Table, versioning_along=("name",)):
            name: str | None = None
            version: int = 0

        d = Doc(name="v1")
        assert d.id == 1
        assert getattr(d, "version", None) is not None
        loaded = Doc.load(id=1)
        assert loaded.id == 1
        assert loaded.name == "v1"

    def test_insert_all_defaults_uses_default_values_sql(self, setup_db):
        class A(Table, with_timestamps=False):
            name: str = "default"
            value: int = 42

        a = A()
        assert a.id == 1
        assert a.name == "default"
        assert a.value == 42
        row = Query(table=A).first()
        assert row is not None
        assert row.id == 1
        assert row.name == "default"
        assert row.value == 42


class TestColumnNameForPath:
    """_column_name_for_path with path length > 1 (324, 329-336)."""

    def test_column_name_for_path_nested_returns_column_name(self, setup_db):
        from ormantism.query import _column_name_for_path

        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        assert _column_name_for_path(A, ["book", "title"]) == "title"
        assert _column_name_for_path(A, ["book"]) == "book_id"
        assert _column_name_for_path(A, []) == "id"


class TestEnsureTableStructureCacheCoverage:
    """ensure_table_structure early return when _CHECKED_TABLE_EXISTENCE (158)."""

    def test_ensure_table_structure_second_call_returns_early(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        ensure_table_structure(A)
        A(name="x")
        ensure_table_structure.cache_clear()
        ensure_table_structure(A)
        row = Query(table=A).first()
        assert row is not None
        assert row.id == 1
        assert row.name == "x"


class TestApplyReturningParametersNone:
    """apply_returning with parameters=None uses [] (176-177)."""

    def test_apply_returning_parameters_none(self, setup_db):
        from ormantism.query import apply_returning

        class A(Table, with_timestamps=False):
            name: str = "a"

        ensure_table_structure(A)
        a = A.__new__(A)
        a.__dict__.update({"name": "a"})
        apply_returning(a, "INSERT INTO a DEFAULT VALUES", None, for_insertion=True)
        assert a.id == 1


class TestVersionedInsertBranches:
    """Versioned insert: IS NULL (195), version assign (222, 224), default after insert (254)."""

    def test_versioned_insert_with_null_along_and_default_field(self, setup_db):
        class Doc(Table, versioning_along=("name",)):
            name: str | None = None
            content: str = ""

        d = Doc(name=None)
        assert d.id == 1
        assert d.version >= 0
        assert d.content == ""
        loaded = Doc.load(id=1)
        assert loaded.id == 1
        assert loaded.name is None
        assert loaded.content == ""

    def test_versioned_insert_two_along_fields_null_and_set(self, setup_db):
        """Versioning with (name, key): None and set, and existing row so rows non-empty (195, 213, 222, 224)."""
        class Doc(Table, versioning_along=("name", "key")):
            name: str | None = None
            key: str = ""
            content: str = ""

        d1 = Doc(name=None, key="k1")
        assert d1.id == 1
        d2 = Doc(name=None, key="k1")
        assert d2.id == 2
        assert getattr(d2, "version", 0) >= 0
        loaded = Doc.load(id=2)
        assert loaded.id == 2
        assert loaded.name is None
        assert loaded.key == "k1"


class TestUpdateAndDeleteInstanceDirect:
    """Direct update_instance and delete_instance calls (278, 287, 291, 302-315)."""

    def test_update_instance_direct(self, setup_db):
        from ormantism.query import update_instance

        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="first")
        assert a.id == 1
        update_instance(a, {"name": "second"})
        loaded = A.load(id=1)
        assert loaded.id == 1
        assert loaded.name == "second"

    def test_update_instance_empty_process_data_returns_early(self, setup_db):
        """update_instance when process_data returns {} hits early return (287)."""
        from ormantism.query import update_instance

        _empty = [False]

        class A(Table, with_timestamps=True):
            name: str = ""

            @classmethod
            def process_data(cls, data: dict, for_filtering: bool = False) -> dict:
                if _empty[0]:
                    return {}
                return super().process_data(data, for_filtering)

        a = A(name="x")
        assert a.id == 1
        _empty[0] = True
        try:
            update_instance(a, {"name": "y"})
        finally:
            _empty[0] = False
        loaded = A.load(id=1)
        assert loaded.name == "x"

    def test_delete_instance_soft_then_hard(self, setup_db):
        from ormantism.query import delete_instance

        class Soft(Table, with_timestamps=True):
            name: str = ""

        class Hard(Table, with_timestamps=False):
            name: str = ""

        s = Soft(name="s")
        assert s.id == 1
        delete_instance(s)
        assert Query(table=Soft).first() is None
        assert Query(table=Soft).include_deleted().where(Soft.pk == 1).first().id == 1

        h = Hard(name="h")
        assert h.id == 1
        delete_instance(h)
        assert Query(table=Hard).first() is None


class TestCollectTableExpressionsExcept:
    """_collect_table_expressions with invalid path (350-357, 384)."""

    def test_collect_table_expressions_skips_invalid_path(self, setup_db):
        from ormantism.query import _collect_table_expressions

        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        result = _collect_table_expressions(Query(table=A), ["id", "nosuch.nothing"])
        assert len(result) == 1
        assert result[0].table is A


class TestPolymorphicRefAndJoinCoverage:
    """Polymorphic ref yield (414), multi-table join (429)."""

    def test_select_root_with_polymorphic_ref_yields_table_column(self, setup_db):
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            ref: Table | None = None

        ensure_table_structure(A)
        q = Query(table=A).select(A)
        sql = q.sql
        assert "ref_table" in sql
        assert "ref_id" in sql

    def test_select_joined_table_builds_join_sql(self, setup_db):
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        q = Query(table=A).select(A.pk, A.book.title)
        sql = q.sql
        assert "LEFT JOIN" in sql
        assert "book" in sql.lower()

    def test_select_overlapping_paths_hits_seen_paths_continue(self, setup_db):
        """Select book.title and book.id so prefix 'book' is seen twice (384)."""
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        q = Query(table=A).select(A.book.title, A.book.id)
        sql = q.sql
        assert "book" in sql

    def test_select_non_pk_adds_pk_to_paths(self, setup_db):
        """Select only name so _select_paths_from_expressions adds pk (350-357)."""
        class A(Table, with_timestamps=True):
            name: str = ""

        q = Query(table=A).select(A.name)
        sql = q.sql
        assert "id" in sql
        assert "name" in sql

    def test_yield_columns_list_table_ref_in_sql(self, setup_db):
        """Build SQL for model with list[Table]; only select id so we yield _tables (418)."""
        class B(Table, with_timestamps=True):
            x: int = 0

        class A(Table, with_timestamps=True):
            items: list[Table] = []

        ensure_table_structure(A)
        ensure_table_structure(B)
        q = Query(table=A).select(A.pk)
        sql = q.sql
        assert "items_ids" in sql
        assert "items_tables" in sql


class TestPolymorphicListRefHydration:
    """List ref with secondary_type Table: JSON ids/tables (772, 776-779)."""

    def test_instance_from_row_polymorphic_list_ref(self, setup_db):
        class B(Table, with_timestamps=True):
            x: int = 0

        class A(Table, with_timestamps=True):
            items: list[Table] = []

        ensure_table_structure(A)
        ensure_table_structure(B)
        b1 = B(x=10)
        b2 = B(x=20)
        a = A(items=[b1, b2])
        assert a.id == 1
        tbl = A._get_table_name()
        row_dict = {
            f"{tbl}{ALIAS_SEPARATOR}id": 1,
            f"{tbl}{ALIAS_SEPARATOR}items_ids": "[1, 2]",
            f"{tbl}{ALIAS_SEPARATOR}items_tables": '["b", "b"]',
        }
        q = Query(table=A).select(A.pk, A.items)
        inst = q.instance_from_row(row_dict)
        assert inst.id == 1
        assert len(inst.items) == 2
        assert inst.items[0].id == 1
        assert inst.items[1].id == 2
        assert inst.items[0].x == 10
        assert inst.items[1].x == 20


class TestQueryCoverageHelpers:
    """Targeted tests for query.py branches: _stored_pk, _pk_name, _sql_from_join, insert defaults, list ref no ids."""

    def test_stored_pk_returns_none_when_id_missing_or_expression(self, setup_db):
        """_stored_pk returns None when instance.id is not in __dict__ or is an Expression (line 201)."""
        from ormantism.query import _stored_pk

        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        assert a.id is not None
        assert _stored_pk(a) == a.id

        empty = A.__new__(A)
        empty.__dict__.clear()
        assert _stored_pk(empty) is None

    def test_pk_name_returns_id_when_no_read_only_fields(self, setup_db):
        """_pk_name returns 'id' when table has no _READ_ONLY_FIELDS or it is empty (line 321)."""
        from ormantism.query import _pk_name

        class A(Table, with_timestamps=True):
            name: str = ""

        assert _pk_name(A) == "id"
        orig = getattr(A, "_READ_ONLY_FIELDS", None)
        try:
            A._READ_ONLY_FIELDS = ()
            assert _pk_name(A) == "id"
        finally:
            if orig is not None:
                A._READ_ONLY_FIELDS = orig

    def test_sql_from_join_empty_returns_empty_string(self):
        """_sql_from_join([]) returns '' (line 426)."""
        from ormantism.query import _sql_from_join

        assert _sql_from_join([]) == ""

    def test_insert_instance_applies_default_for_field_not_in_init_data(self, setup_db):
        """insert_instance sets default on instance for field not in processed_data (lines 251, 275)."""
        from ormantism.query import insert_instance

        class A(Table, with_timestamps=True):
            name: str = ""
            optional: int = 42

        ensure_table_structure(A)
        a = A.__new__(A)
        a.__dict__.update({"name": "x", "id": None})
        insert_instance(a, {"name": "x"})
        assert a.id is not None
        assert getattr(a, "optional", None) == 42

    def test_insert_instance_calls_post_init(self, setup_db):
        """insert_instance calls __post_init__ when present (query.py ~292)."""
        from ormantism.query import insert_instance

        post_init_called = []

        class A(Table, with_timestamps=True):
            name: str = ""

            def __post_init__(self):
                post_init_called.append(True)

        ensure_table_structure(A)
        a = A.__new__(A)
        a.__dict__.update({"name": "x", "id": None})
        insert_instance(a, {"name": "x"})
        assert a.id is not None
        assert post_init_called == [True]

    def test_instance_from_row_list_ref_with_no_ids_key_uses_empty_list(self, setup_db):
        """When row has no name_ids key, list ref gets references_ids=None then [] (line 769)."""
        class Child(Table, with_timestamps=True):
            x: int = 0

        class Parent(Table, with_timestamps=True):
            kids: list[Child] = []

        ensure_table_structure(Parent)
        ensure_table_structure(Child)
        p = Parent()
        assert p.id == 1
        tbl = Parent._get_table_name()
        row_dict = {f"{tbl}{ALIAS_SEPARATOR}id": 1}
        q = Query(table=Parent).select(Parent.pk)
        inst = q.instance_from_row(row_dict)
        assert inst.id == 1
        assert inst.kids == []

    def test_select_paths_adds_pk_when_not_in_expressions(self, setup_db):
        """_select_paths_from_expressions adds pk to paths when missing (line 364)."""
        from ormantism.query import _select_paths_from_expressions

        class A(Table, with_timestamps=True):
            name: str = ""

        paths = _select_paths_from_expressions(A, [A.name])
        assert "id" in paths
        assert "name" in paths

    def test_collect_table_expressions_skips_column_expression(self, setup_db):
        """_collect_table_expressions skips path that resolves to ColumnExpression (line 394)."""
        from ormantism.query import _collect_table_expressions

        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        result = _collect_table_expressions(Query(table=A), ["book.title"])
        assert len(result) == 2
        assert result[0].table is A
        assert result[1].table is B
        assert result[1].path_str == "book"


class TestQueryUpdateEmptySetData:
    """Query.update() when process_data returns {} (868)."""

    def test_update_empty_set_data_returns_early(self, setup_db):
        _return_empty = [False]

        class A(Table, with_timestamps=True):
            name: str = ""

            @classmethod
            def process_data(cls, data: dict, for_filtering: bool = False) -> dict:
                if _return_empty[0]:
                    return {}
                return super().process_data(data, for_filtering)

        ensure_table_structure(A)
        A(name="one")
        q = Query(table=A)
        _return_empty[0] = True
        try:
            q.update(name="two")
        finally:
            _return_empty[0] = False
        row = Query(table=A).first()
        assert row is not None
        assert row.id == 1
        assert row.name == "one"
