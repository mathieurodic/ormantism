"""Tests for Table CRUD, timestamps, relationships, lazy loading, and versioning."""

import pytest
from ormantism.table import Table


class TestBasicTableOperations:
    """Test basic table operations without relationships."""

    def test_table_with_timestamps_false(self, setup_db):
        class A(Table, with_timestamps=False):
            pass

        fields = A._get_fields()
        assert 'id' in fields
        assert 'created_at' not in fields
        assert 'updated_at' not in fields
        assert 'deleted_at' not in fields

    def test_table_with_timestamps_true(self, setup_db):
        class TTWTT(Table, with_timestamps=True):
            value: int = 42

        fields = TTWTT._get_fields()
        assert 'id' in fields
        assert 'created_at' in fields
        assert 'updated_at' in fields
        assert 'deleted_at' in fields

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

        c_fields = C._get_fields()
        assert 'links_to' in c_fields
        assert c_fields['links_to'].is_reference

        b = B()
        assert b.id is not None
        assert b.created_at is not None

        c = C(links_to = b)
        assert c.links_to.id == b.id


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
        assert loaded_c.id == c.id

        linked_b = loaded_c.links_to
        if linked_b:
            assert linked_b.id == b.id
        expect_lazy_loads.expect(0)

    def test_lazy_loading(self, setup_db, expect_lazy_loads):
        class B(Table, with_timestamps=True):
            value: int = 42

        class C(Table, with_timestamps=True):
            links_to: B = None

        b = B()
        c = C(links_to = b)

        loaded_c = C.load(id=c.id)
        assert loaded_c is not None
        assert loaded_c.id == c.id

        first_access = loaded_c.links_to
        second_access = loaded_c.links_to

        if first_access is not None:
            assert second_access.id == first_access.id
        expect_lazy_loads.expect(1)


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

        assert e1.company.id == c1.id
        assert e3.company.id == c2.id

        c5 = Company(name="Initech")
        c5.name += " II"
        loaded = Company.load(id=c5.id)
        assert loaded is not None
        assert " II" in loaded.name


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
