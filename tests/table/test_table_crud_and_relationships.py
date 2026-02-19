"""Tests for Table CRUD, timestamps, relationships, load-on-access, and versioning."""

import pytest
from ormantism.table import Table
from tests.helpers import assert_table_instance


class TestBasicTableOperations:
    """Test basic table operations without relationships."""

    def test_table_with_timestamps_false(self, setup_db):
        class A(Table, with_timestamps=False):
            pass

        columns = A._get_columns()
        assert 'id' in columns
        assert 'created_at' not in columns
        assert 'updated_at' not in columns
        assert 'deleted_at' not in columns

    def test_table_with_timestamps_true(self, setup_db):
        class TTWTT(Table, with_timestamps=True):
            value: int = 42

        columns = TTWTT._get_columns()
        assert 'id' in columns
        assert 'created_at' in columns
        assert 'updated_at' in columns
        assert 'deleted_at' in columns

        b = TTWTT()
        assert b.value == 42
        assert b.id is not None

        b.value = 69
        loaded_b = TTWTT.load(id=b.id)
        assert loaded_b.value == 69


class TestTableRelationships:
    """Test table relationships and foreign keys."""

    def test_table_with_foreign_key(self, setup_db):
        class B(Table, with_timestamps=True):
            value: int = 42

        class C(Table, with_timestamps=True):
            links_to: B|None = None

        c_columns = C._get_columns()
        assert 'links_to' in c_columns
        assert c_columns['links_to'].is_reference

        b = B()
        assert b.id is not None
        assert b.created_at is not None

        c = C(links_to=b)
        assert_table_instance(
            c,
            {"id": c.id, "links_to": b, "created_at": c.created_at, "updated_at": c.updated_at, "deleted_at": c.deleted_at},
        )


class TestLazyLoading:
    """Test lazy loading functionality."""

    def test_explicit_preloading(self, setup_db, expect_lazy_loads):
        class B(Table, with_timestamps=True):
            value: int = 42

        class C(Table, with_timestamps=True):
            links_to: B = None

        b = B()
        c = C(links_to = b)

        loaded_c = C.load(id=c.id, preload="links_to")
        assert loaded_c is not None
        assert_table_instance(
            loaded_c,
            {"id": c.id, "links_to": b},
            exclude={"created_at", "updated_at", "deleted_at"},
        )
        expect_lazy_loads.expect(0)

    def test_load_on_access(self, setup_db, expect_lazy_loads):
        class B(Table, with_timestamps=True):
            value: int = 42

        class C(Table, with_timestamps=True):
            links_to: B = None

        b = B()
        c = C(links_to = b)

        loaded_c = C.load(id=c.id)
        assert loaded_c is not None
        assert_table_instance(
            loaded_c,
            {"id": c.id, "links_to": b},
            exclude={"created_at", "updated_at", "deleted_at"},
        )
        first_access = loaded_c.links_to
        second_access = loaded_c.links_to
        if first_access is not None:
            assert second_access.id == first_access.id
        # links_to gets skeleton from initial hydration (FK id in row); no lazy load on access.
        expect_lazy_loads.expect(0)


class TestCompanyEmployeeExample:
    """Test the company-employee relationship example."""

    def test_company_employee_operations(self, setup_db):
        class Company(Table, with_timestamps=True):
            name: str = ""

        class Employee(Table, with_timestamps=True):
            name: str = ""
            company: Company | None = None

        c1 = Company(name="Acme")
        c2 = Company(name="Globex")
        e1 = Employee(name="Alice", company=c1)
        e2 = Employee(name="Bob", company=c1)
        e3 = Employee(name="Carol", company=c2)

        assert_table_instance(
            e1,
            {"id": e1.id, "name": "Alice", "company": c1},
            exclude={"created_at", "updated_at", "deleted_at"},
        )
        assert_table_instance(
            e3,
            {"id": e3.id, "name": "Carol", "company": c2},
            exclude={"created_at", "updated_at", "deleted_at"},
        )

        c5 = Company(name="Initech")
        c5.name += " II"
        loaded = Company.load(id=c5.id)
        assert loaded is not None
        assert_table_instance(
            loaded,
            {"id": c5.id, "name": "Initech II"},
            exclude={"created_at", "updated_at", "deleted_at"},
        )


class TestVersioning:
    """Test versioning_along and version field."""

    def test_versioning_along_fields(self, setup_db):
        class Document(Table, versioning_along=("name",)):
            name: str
            content: str

        d1 = Document(name="foo", content="azertyuiop")
        assert d1.name == "foo"
        assert d1.content == "azertyuiop"

        d2 = Document(name="foo", content="azertyuiopqsdfghjlm")
        assert d2.name == "foo"
        assert d2.content == "azertyuiopqsdfghjlm"

        original_content = d2.content
        d2.content += " :)"
        assert d2.content == original_content + " :)"


class TestVersioningSemantics:
    """Stronger assertions around versioned series: ids, version increments, soft-delete, updates."""

    def test_versioning_creates_new_rows_and_soft_deletes_previous(self, setup_db):
        import datetime

        class Doc(Table, versioning_along=("name",)):
            name: str
            content: str

        d1 = Doc(name="a", content="v1")
        assert_table_instance(
            d1,
            {
                "id": 1,
                "deleted_at": None,
                "version": 0,
                "name": "a",
                "content": "v1",
            },
        )

        d2 = Doc(name="a", content="v2")
        assert_table_instance(
            d2,
            {
                "id": 2,
                "deleted_at": None,
                "version": 1,
                "name": "a",
                "content": "v2",
            },
        )

        # Default query should only see current row.
        rows_current = Doc.q().where(name="a").all()
        assert len(rows_current) == 1
        assert rows_current[0].id == 2
        assert rows_current[0].version == 1

        # With include_deleted: both versions are visible.
        rows_all = Doc.q().include_deleted().where(name="a").all()
        assert len(rows_all) == 2
        # Order is deterministic due to default ordering: name ASC, version DESC.
        assert [r.version for r in rows_all] == [1, 0]
        assert [r.id for r in rows_all] == [2, 1]

        older = rows_all[1]
        assert older.deleted_at is not None
        assert isinstance(older.deleted_at, datetime.datetime)

    def test_versioning_series_are_independent(self, setup_db):
        class Doc(Table, versioning_along=("name",)):
            name: str
            content: str

        a1 = Doc(name="a", content="a1")
        a2 = Doc(name="a", content="a2")
        b1 = Doc(name="b", content="b1")

        assert a1.version == 0
        assert a2.version == 1
        assert b1.version == 0

    def test_assignment_creates_new_version(self, setup_db):
        import datetime

        class Doc(Table, versioning_along=("name",)):
            name: str
            content: str

        doc = Doc(name="a", content="v1")
        old_id = doc.id
        assert doc.version == 0

        doc.content = "v2"
        assert doc.id != old_id
        assert doc.version == 1
        assert doc.content == "v2"

        history = Doc.q().include_deleted().where(name="a").all()
        assert len(history) == 2
        assert {r.version for r in history} == {0, 1}
        old = next(r for r in history if r.version == 0)
        assert old.id == old_id
        assert old.deleted_at is not None
        assert isinstance(old.deleted_at, datetime.datetime)

    def test_update_method_creates_new_version(self, setup_db):
        class Doc(Table, versioning_along=("name",)):
            name: str
            content: str

        doc = Doc(name="a", content="v1")
        old_id = doc.id
        assert doc.version == 0

        doc.update(content="v2")
        assert doc.id != old_id
        assert doc.version == 1
        assert doc.content == "v2"

    def test_versioning_along_fields_are_immutable(self, setup_db):
        class Doc(Table, versioning_along=("name",)):
            name: str
            content: str

        doc = Doc(name="a", content="v1")
        with pytest.raises(AttributeError, match="versioning_along"):
            doc.name = "b"
        with pytest.raises(AttributeError, match="versioning_along"):
            doc.update(name="b")
