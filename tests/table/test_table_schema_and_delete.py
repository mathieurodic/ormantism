"""Tests for Table schema (_create_table, _add_columns), process_data, delete(), and load() ordering."""

import sqlite3
import pytest
from unittest.mock import patch
from pydantic import BaseModel

from ormantism.table import Table


class TestCreateTableAndAddColumns:
    """Test _create_table and _add_columns edge cases."""

    def test_create_table_skips_fk_for_base_type_table(self, setup_db):
        class Poly(Table, with_timestamps=True):
            target: Table | None = None

        Poly._create_table()
        Poly._add_columns()

    def test_create_table_creates_referenced_table_first(self, setup_db):
        class Refd(Table, with_timestamps=True):
            value: int = 0

        class Refing(Table, with_timestamps=True):
            ref: Refd | None = None

        Refing._create_table()
        r = Refd()
        i = Refing(ref=r)
        assert i.ref is not None and i.ref.id == r.id

    def test_add_columns_ignores_duplicate_column_error(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        A._create_table()
        A._add_columns()
        A._add_columns()

    def test_add_columns_reraises_other_operational_error(self, setup_db):
        from ormantism.query import Query

        class A(Table, with_timestamps=True):
            name: str = ""

        A._create_table()
        A._add_columns()
        original_execute = Query.execute

        def execute_raise_on_alter(self, sql, parameters=None, ensure_structure=True):
            if "ALTER" in sql:
                raise sqlite3.OperationalError("other error")
            if "pragma_table_info" in sql:
                return [("id",), ("created_at",), ("updated_at",), ("deleted_at",)]
            return original_execute(self, sql, parameters, ensure_structure)

        with patch.object(Query, "execute", execute_raise_on_alter):
            with pytest.raises(sqlite3.OperationalError):
                A._add_columns()


class TestProcessData:
    """Test process_data branches (base_type==Table, list refs, BaseModel value)."""

    def test_process_data_scalar_reference_to_table_base(self, setup_db):
        class B(Table, with_timestamps=True):
            value: int = 0

        class Poly(Table, with_timestamps=True):
            target: Table | None = None

        b = B()
        out = Poly.process_data({"target": b})
        assert out["target_id"] == b.id
        assert out["target_table"] == B._get_table_name()

    def test_process_data_list_of_references(self, setup_db):
        class B(Table, with_timestamps=True):
            value: int = 0

        class C(Table, with_timestamps=True):
            items: list[B] = []

        b1 = B()
        b2 = B()
        out = C.process_data({"items": [b1, b2]})
        assert out["items_ids"] == [b1.id, b2.id]

    def test_process_data_base_model_value(self, setup_db):
        class Nested(BaseModel):
            x: int = 0
            y: str = ""

        class A(Table, with_timestamps=True):
            name: str = ""
            data: dict = {}

        out = A.process_data({"data": Nested(x=1, y="two")})
        assert out["data"] == {"x": 1, "y": "two"}

    def test_process_data_list_ref_non_list_raises(self, setup_db):
        """process_data raises NotImplementedError when list ref gets non-list value (line 237)."""
        class B(Table, with_timestamps=True):
            value: int = 0

        class C(Table, with_timestamps=True):
            items: list[B] = []

        with pytest.raises(NotImplementedError):
            C.process_data({"items": 42})
        with pytest.raises(NotImplementedError):
            C.process_data({"items": "not a list"})


class TestCheckReadOnly:
    """Test check_read_only branches (plural message for multiple fields, line 135)."""

    def test_check_read_only_plural_when_multiple_fields(self, setup_db):
        """check_read_only with multiple read-only keys uses plural 'attributes' (line 135)."""
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        with pytest.raises(AttributeError, match="read-only attributes"):
            a.check_read_only({"id": 1, "created_at": None})
        with pytest.raises(AttributeError, match="read-only attribute"):  # singular when one
            a.check_read_only({"id": 999})


class TestGetFieldByColumnName:
    """Test _get_field by column name and _table suffix (lines 118, 134)."""

    def test_get_field_by_table_suffix(self, setup_db):
        """_get_field('ref_table') returns the ref column when table has that reference (line 118)."""
        class B(Table, with_timestamps=True):
            value: int = 0

        class Poly(Table, with_timestamps=True):
            target: Table | None = None

        col = Poly._get_field("target_table")
        assert col.name == "target"
        assert col.is_reference

    def test_get_field_raises_for_unknown_name(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        with pytest.raises(KeyError, match="No such field"):
            A._get_field("nosuch")


class TestDelete:
    """Test delete() soft vs hard."""

    def test_delete_soft(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        a.delete()
        loaded = A.load(id=a.id)
        assert loaded is None
        loaded_all = A.load(id=a.id, with_deleted=True)
        assert loaded_all is not None

    def test_delete_hard(self, setup_db):
        class A(Table, with_timestamps=False):
            name: str = ""

        a = A(name="x")
        a.delete()
        loaded = A.load(id=a.id)
        assert loaded is None


class TestLoadOrderAndVersioning:
    """Test load() ORDER BY for versioned and minimal tables."""

    def test_load_order_by_version_when_versioned(self, setup_db):
        class Doc(Table, versioning_along=("name",)):
            name: str = ""
            content: str = ""

        Doc(name="a", content="1")
        Doc(name="a", content="2")
        rows = Doc.load(as_collection=True)
        assert len(rows) >= 2

    def test_load_order_by_id_when_no_timestamps_no_version(self, setup_db):
        class A(Table, with_timestamps=False):
            name: str = ""

        A(name="first")
        A(name="second")
        rows = A.load(as_collection=True)
        assert len(rows) >= 2
        ids = [r.id for r in rows]
        assert ids == sorted(ids, reverse=True)
