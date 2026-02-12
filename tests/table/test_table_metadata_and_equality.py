"""Tests for Table metadata (_get_columns, _get_table_name, _get_column), options inheritance, __eq__, __hash__, __deepcopy__."""

import copy
import pytest
from ormantism.table import Table


class TestTableMetadata:
    """Test table metadata and helper methods."""

    def test_table_name_generation(self, setup_db):
        class MyTestTable(Table):
            name: str

        assert MyTestTable._get_table_name() == "mytesttable"

    def test_field_information(self, setup_db):
        class TestTable(Table, with_timestamps=True):
            name: str
            value: int = 42

        columns = TestTable._get_columns()

        assert 'name' in columns
        assert 'value' in columns
        assert 'id' in columns
        assert 'created_at' in columns

    def test_get_column_by_name(self, setup_db):
        class B(Table, with_timestamps=True):
            value: int = 0

        class C(Table, with_timestamps=True):
            links_to: B | None = None

        column = C._get_column("links_to")
        assert column.name == "links_to"
        assert column.is_reference

    def test_get_column_raises_for_unknown(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str

        with pytest.raises(KeyError, match="No such column"):
            A._get_column("nonexistent")

    def test_has_column(self, setup_db):
        """_has_column returns True for existing column, False for missing."""
        class A(Table, with_timestamps=True):
            name: str = ""

        assert A._has_column("name") is True
        assert A._has_column("id") is True
        assert A._has_column("nonexistent") is False

    def test_check_read_only_raises(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str

        a = A(name="x")
        with pytest.raises(AttributeError, match="read-only"):
            a.check_read_only({"id": 999})
        with pytest.raises(AttributeError, match="read-only"):
            a.check_read_only({"created_at": None})

    def test_process_data_invalid_key_raises(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str

        with pytest.raises(ValueError, match="Invalid key"):
            A.process_data({"not_a_column": 1})

    def test_load_as_collection(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""
            value: int = 0

        A(name="a1")
        A(name="a2")
        rows = A.load(as_collection=True)
        assert isinstance(rows, list)
        assert len(rows) >= 2
        for r in rows:
            assert isinstance(r, A)


class TestTableMetaOptions:
    """Test TableMetaclass options and inheritance."""

    def test_with_created_at_timestamp_only(self, setup_db):
        class A(Table, with_created_at_timestamp=True, with_timestamps=False):
            name: str = ""

        assert "created_at" in A._get_columns()
        assert "deleted_at" not in A._get_columns()
        assert "updated_at" not in A._get_columns()

    def test_with_updated_at_timestamp_only(self, setup_db):
        class A(Table, with_updated_at_timestamp=True, with_timestamps=False):
            name: str = ""

        assert "updated_at" in A._get_columns()
        assert "created_at" not in A._get_columns()
        assert "deleted_at" not in A._get_columns()

    def test_inherit_connection_name_from_base(self, setup_db):
        class Base(Table, connection_name="custom_conn"):
            name: str = ""

        class Child(Base):
            value: int = 0

        assert Child._CONNECTION_NAME == "custom_conn"

    def test_inherit_versioning_along_from_base(self, setup_db):
        class Base(Table, versioning_along=("key",)):
            key: str = ""
            body: str = ""

        class Child(Base):
            extra: str = ""

        assert Child._VERSIONING_ALONG == ("key",)


class TestTableEqualityAndCopy:
    """Test __eq__, __hash__, __deepcopy__."""

    def test_eq_raises_for_different_class(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        class B(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        b = B(name="x")
        with pytest.raises(ValueError, match="Comparing instances of different classes"):
            a == b

    def test_eq_same_class_different_hash(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a1 = A(name="a")
        a2 = A(name="b")
        assert a1 != a2

    def test_eq_same_class_same_hash(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        loaded = A.load(id=a.id)
        assert loaded is not None
        assert a == loaded

    def test_hash(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        s = {a}
        assert a in s
        assert hash(a) == hash(a)

    def test_deepcopy_returns_self(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        c = copy.deepcopy(a)
        assert c is a


class TestTableLazyReadonly:
    """Table __getattr__ for lazy read-only scalars."""

    def test_getattr_lazy_readonly_fetches_and_caches(self, setup_db):
        """__getattribute__ for lazy readonly fetches from DB and caches (created_at path)."""
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        assert a.id is not None
        # After update, _mark_readonly_lazy can leave created_at as lazy (not in __dict__)
        a._mark_readonly_lazy()
        assert "created_at" not in a.__dict__
        created = getattr(a, "created_at")
        assert created is not None
        assert "created_at" in a.__dict__  # now cached
