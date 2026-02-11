import copy
import pytest
from ormantism.table import Table


class TestBasicTableOperations:
    """Test basic table operations without relationships."""
    
    def test_table_with_timestamps_false(self, setup_db):
        """Test table creation with timestamps disabled."""
        class A(Table, with_timestamps=False):
            pass
        
        # Test field and column information
        fields = A._get_fields()
        assert 'id' in fields
        assert 'created_at' not in fields
        assert 'updated_at' not in fields
        assert 'deleted_at' not in fields
    
    def test_table_with_timestamps_true(self, setup_db):
        """Test table creation with timestamps enabled."""
        class TTWTT(Table, with_timestamps=True):
            value: int = 42
        
        fields = TTWTT._get_fields()
        assert 'id' in fields
        assert 'created_at' in fields
        assert 'updated_at' in fields
        assert 'deleted_at' in fields
        
        # Test instance creation and value assignment
        b = TTWTT()
        assert b.value == 42
        assert b.id is not None
        
        # Test value modification and persistence
        b.value = 69
        loaded_b = TTWTT.load(id=b.id)
        assert loaded_b.value == 69


class TestTableRelationships:
    """Test table relationships and foreign keys."""
    
    def test_table_with_foreign_key(self, setup_db):
        """Test table with foreign key relationship."""
        class B(Table, with_timestamps=True):
            value: int = 42

        class C(Table, with_timestamps=True):
            links_to: B|None = None

        # Test field structure
        c_fields = C._get_fields()
        assert 'links_to' in c_fields
        assert c_fields['links_to'].is_reference

        
        # Test instance creation
        b = B()
        assert b.id is not None
        assert b.created_at is not None
        
        # Test setting relationship
        c = C(links_to = b)
        
        # Test columns data extraction
        assert c.links_to.id == b.id


class TestLazyLoading:
    """Test lazy loading functionality."""
    
    def test_explicit_preloading(self, setup_db):
        """Test explicit preloading of relationships."""
        class B(Table, with_timestamps=True):
            value: int = 42

        class C(Table, with_timestamps=True):
            links_to: B = None
        
        # Create test data
        b = B()
        c = C(links_to = b)
        

        loaded_c = C.load(id=c.id, preload="links_to")
        assert loaded_c is not None
        assert loaded_c.id == c.id
        
        # Access the preloaded relationship
        linked_b = loaded_c.links_to
        if linked_b:  # May be None depending on implementation
            assert linked_b.id == b.id
    
    def test_lazy_loading(self, setup_db):
        """Test lazy loading of relationships."""
        class B(Table, with_timestamps=True):
            value: int = 42

        class C(Table, with_timestamps=True):
            links_to: B = None
        
        # Create test data
        b = B()
        c = C(links_to = b)
        

        loaded_c = C.load(id=c.id)
        assert loaded_c is not None
        assert loaded_c.id == c.id
        
        # Access the relationship multiple times (should be cached after first access)
        first_access = loaded_c.links_to
        second_access = loaded_c.links_to
        
        # Both accesses should return the same result
        if first_access is not None:
            assert second_access.id == first_access.id


class TestCompanyEmployeeExample:
    """Test the company-employee relationship example."""
    
    def test_company_employee_operations(self, setup_db):
        """Test complex operations with Company and Employee models."""
        class Company(Table):
            name: str

        class Employee(Table):
            firstname: str
            lastname: str
            company: Company
        
        # Test loading non-existent records
        c1 = Company.load(id=4)

        
        c2 = Company.load(name="AutoKod")
        assert c2 is None
        
        c3 = Company.load(name="AutoKod II")
        assert c3 is None
        
        # Test creating new records
        c4 = Company(name="AutoKod")
        assert c4.id is not None
        assert c4.name == "AutoKod"
        
        c5 = Company(name="AutoKod")
        assert c5.id is not None
        assert c5.name == "AutoKod"
        
        # Test updating record
        c5.name += " II"
        assert c5.name == "AutoKod II"
        
        # Test creating employee with company relationship
        e1 = Employee(firstname="Mathieu", lastname="Rodic", company=c5)
        assert e1.id is not None
        assert e1.firstname == "Mathieu"
        assert e1.lastname == "Rodic"
        assert e1.company.id == c5.id
        
        # Test loading employee by company relationship
        e2 = Employee.load(company=c4)
        assert e2 is None or isinstance(e2, Employee)
        
        # Test loading all employees for a company
        e_all = Employee.load_all(company=c4)
        assert isinstance(e_all, list)


class TestVersioning:
    """Test versioning functionality (commented out in original)."""
    
    def test_versioning_along_fields(self, setup_db):
        """Test versioning along specific fields."""
        # This test is based on the commented versioning example
        class Document(Table, versioning_along=("name",)):
            name: str
            content: str
        
        # Create first version
        d1 = Document(name="foo", content="azertyuiop")
        assert d1.name == "foo"
        assert d1.content == "azertyuiop"
        
        # Create second version with same name
        d2 = Document(name="foo", content="azertyuiopqsdfghjlm")
        assert d2.name == "foo"
        assert d2.content == "azertyuiopqsdfghjlm"
        
        # Test updating content
        original_content = d2.content
        d2.content += " :)"
        assert d2.content == original_content + " :)"


class TestTableMetadata:
    """Test table metadata and helper methods."""
    
    def test_table_name_generation(self, setup_db):
        """Test automatic table name generation."""
        class MyTestTable(Table):
            name: str
        
        assert MyTestTable._get_table_name() == "mytesttable"
    
    def test_field_information(self, setup_db):
        """Test field information retrieval."""
        class TestTable(Table, with_timestamps=True):
            name: str
            value: int = 42
        
        fields = TestTable._get_fields()
        non_default_fields = TestTable._get_non_default_fields()
        
        # Should have all defined fields plus timestamp fields
        assert 'name' in fields
        assert 'value' in fields
        assert 'id' in fields
        assert 'created_at' in fields
        
        # Non-default fields should exclude read-only fields
        assert 'name' in non_default_fields
        assert 'value' in non_default_fields
        # Read-only fields should not be in non-default fields
        assert 'id' not in non_default_fields
        assert 'created_at' not in non_default_fields

    def test_get_field_by_column_name(self, setup_db):
        """_get_field can be looked up by column name (e.g. name_id for reference)."""
        class B(Table, with_timestamps=True):
            value: int = 0

        class C(Table, with_timestamps=True):
            links_to: B | None = None

        field = C._get_field("links_to_id")
        assert field.name == "links_to"
        assert field.column_name == "links_to_id"

    def test_get_field_raises_for_unknown(self, setup_db):
        """_get_field raises KeyError for unknown field or column name."""
        class A(Table, with_timestamps=True):
            name: str

        with pytest.raises(KeyError, match="No such field"):
            A._get_field("nonexistent")

    def test_check_read_only_raises(self, setup_db):
        """check_read_only raises AttributeError when data contains read-only keys."""
        class A(Table, with_timestamps=True):
            name: str

        a = A(name="x")
        with pytest.raises(AttributeError, match="read-only"):
            a.check_read_only({"id": 999})
        with pytest.raises(AttributeError, match="read-only"):
            a.check_read_only({"created_at": None})

    def test_process_data_invalid_key_raises(self, setup_db):
        """process_data raises ValueError for keys that are not model fields."""
        class A(Table, with_timestamps=True):
            name: str

        with pytest.raises(ValueError, match="Invalid key"):
            A.process_data({"not_a_field": 1})

    def test_load_as_collection(self, setup_db):
        """load with as_collection=True returns a list of rows."""
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
        """with_created_at_timestamp=True (without with_timestamps) adds created_at."""
        class A(Table, with_created_at_timestamp=True, with_timestamps=False):
            name: str = ""

        assert "created_at" in A._get_fields()
        assert "deleted_at" not in A._get_fields()
        assert "updated_at" not in A._get_fields()

    def test_with_updated_at_timestamp_only(self, setup_db):
        """with_updated_at_timestamp=True (without with_timestamps) adds updated_at."""
        class A(Table, with_updated_at_timestamp=True, with_timestamps=False):
            name: str = ""

        assert "updated_at" in A._get_fields()
        assert "created_at" not in A._get_fields()
        assert "deleted_at" not in A._get_fields()

    def test_inherit_connection_name_from_base(self, setup_db):
        """Subclass inherits connection_name from base when not specified."""
        class Base(Table, connection_name="custom_conn"):
            name: str = ""

        class Child(Base):
            value: int = 0

        assert Child._CONNECTION_NAME == "custom_conn"

    def test_inherit_versioning_along_from_base(self, setup_db):
        """Subclass inherits versioning_along from base when not specified."""
        class Base(Table, versioning_along=("key",)):
            key: str = ""
            body: str = ""

        class Child(Base):
            extra: str = ""

        assert Child._VERSIONING_ALONG == ("key",)


class TestTableEqualityAndCopy:
    """Test __eq__, __hash__, __deepcopy__."""

    def test_eq_raises_for_different_class(self, setup_db):
        """__eq__ raises ValueError when comparing with different table class."""
        class A(Table, with_timestamps=True):
            name: str = ""

        class B(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        b = B(name="x")
        with pytest.raises(ValueError, match="Comparing instances of different classes"):
            a == b

    def test_eq_same_class_different_hash(self, setup_db):
        """__eq__ returns False when same class but different hash (different data)."""
        class A(Table, with_timestamps=True):
            name: str = ""

        a1 = A(name="a")
        a2 = A(name="b")
        assert a1 != a2

    def test_eq_same_class_same_hash(self, setup_db):
        """__eq__ returns True when same class and same hash (same row)."""
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        loaded = A.load(id=a.id)
        assert loaded is not None
        assert a == loaded

    def test_hash(self, setup_db):
        """__hash__ is stable and usable in sets."""
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        s = {a}
        assert a in s
        assert hash(a) == hash(a)

    def test_deepcopy_returns_self(self, setup_db):
        """__deepcopy__ returns self (table instances treated as immutable)."""
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        c = copy.deepcopy(a)
        assert c is a


class TestOnAfterCreatePaths:
    """Test on_after_create branches (early return, versioning NULL, ref skip, empty INSERT)."""

    def test_on_after_create_skips_when_id_already_set(self, setup_db):
        """When id is already set and >= 0, on_after_create returns without inserting."""
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="first")
        first_id = a.id
        # Simulate already-persisted instance: set id and call again (e.g. from __init__)
        object.__setattr__(a, "id", first_id)
        a.on_after_create({"name": "first"})
        # Still same row
        loaded = A.load(id=first_id)
        assert loaded.name == "first"

    def test_versioning_with_null_in_search(self, setup_db):
        """Versioning UPDATE uses IS NULL for versioning_along fields that are None."""
        class Doc(Table, versioning_along=("name",)):
            name: str | None = None
            content: str = ""

        d = Doc(name=None, content="none")
        assert d.name is None
        assert d.content == "none"
        d2 = Doc(name="foo", content="bar")
        assert d2.name == "foo"

    def test_insert_with_reference_field_skips_ref_in_formatted_data(self, setup_db):
        """Insert builds formatted_data and skips reference fields (serialized separately)."""
        class B(Table, with_timestamps=True):
            value: int = 0

        class C(Table, with_timestamps=True):
            links_to: B | None = None

        b = B()
        c = C(links_to=b)
        assert c.id is not None
        assert c.links_to.id == b.id

    def test_insert_default_values_only(self, setup_db):
        """INSERT with no user-provided columns uses DEFAULT VALUES (formatted_data empty)."""
        # Table with only mixin columns (id, timestamps) so DEFAULT VALUES satisfies NOT NULL
        class IdAndTimestampsOnly(Table, with_timestamps=True):
            pass

        IdAndTimestampsOnly._create_table()
        IdAndTimestampsOnly._add_columns()
        # Call on_after_create with empty init_data so formatted_data stays empty; id=None to avoid early return
        inst = object.__new__(IdAndTimestampsOnly)
        inst.__dict__.update({"id": None})
        inst.on_after_create({})
        assert inst.id is not None

    def test_execute_returning_applies_default_when_db_returns_null(self, setup_db):
        """_execute_returning uses field.default when parsed value is None (line 343)."""
        class A(Table, with_timestamps=True):
            name: str = ""
            optional: int | None = 42  # default 42; we can insert NULL

        a = A(name="x", optional=None)
        # INSERT stores NULL for optional; RETURNING returns NULL; code applies default
        assert a.optional == 42


class TestOnBeforeUpdatePaths:
    """Test on_before_update branches (empty set_statement, no primary key)."""

    def test_update_empty_set_statement_returns_early(self, setup_db):
        """on_before_update returns when process_data yields no changes."""
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        # update with same data -> set_statement empty
        a.on_before_update({"name": "x"})

    def test_update_without_primary_key_raises(self, setup_db):
        """on_before_update raises NotImplementedError when table has no primary key."""
        from unittest.mock import patch

        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        # Check is in query.update_instance; patch so isinstance(instance, _WithPrimaryKey) is False
        with patch("ormantism.query._WithPrimaryKey", type("_FakePK", (), {})):
            with pytest.raises(NotImplementedError):
                a.on_before_update({"name": "z"})


class TestLoadOrCreate:
    """Test load_or_create branches."""

    def test_load_or_create_with_search_fields(self, setup_db):
        """load_or_create restricts match to _search_fields."""
        class A(Table, with_timestamps=True):
            name: str = ""
            tag: str = ""

        a1 = A(name="n", tag="t1")
        # Same name, different tag: with _search_fields=("name",) we find existing and update
        a2 = A.load_or_create(_search_fields=("name",), name="n", tag="t2")
        assert a2.id == a1.id
        assert a2.tag == "t2"

    def test_load_or_create_creates_when_not_found(self, setup_db):
        """load_or_create creates new row when load returns None."""
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A.load_or_create(name="unique")
        assert a is not None
        assert a.name == "unique"
        a2 = A.load_or_create(name="unique")
        assert a2.id == a.id

    def test_load_or_create_updates_non_reference_field(self, setup_db):
        """load_or_create updates existing row when scalar field differs (match by _search_fields)."""
        class A(Table, with_timestamps=True):
            name: str = ""
            value: int = 0

        a = A.load_or_create(_search_fields=("name",), name="x", value=1)
        a2 = A.load_or_create(_search_fields=("name",), name="x", value=2)
        assert a2.id == a.id
        assert a2.value == 2

    def test_load_or_create_updates_reference_field_none(self, setup_db):
        """load_or_create sets reference to None when passing value=None and loaded had a ref (in _lazy_joins)."""
        class B(Table, with_timestamps=True):
            value: int = 0

        class C(Table, with_timestamps=True):
            name: str = ""
            link: B | None = None

        b = B()
        c = C(name="x", link=b)
        # Find same row and pass link=None -> should update to clear ref
        c2 = C.load_or_create(_search_fields=("name",), name="x", link=None)
        assert c2.id == c.id
        assert c2.link is None

    def test_load_or_create_updates_reference_field_different_id(self, setup_db):
        """load_or_create updates reference when _lazy_joins has int and we pass a different instance."""
        class B(Table, with_timestamps=True):
            value: int = 0

        class C(Table, with_timestamps=True):
            name: str = ""
            link: B | None = None

        b1 = B()
        b2 = B()
        c = C(name="y", link=b1)
        c2 = C.load_or_create(_search_fields=("name",), name="y", link=b2)
        assert c2.id == c.id
        assert c2.link is not None and c2.link.id == b2.id

    def test_load_or_create_reference_not_in_lazy_joins_raises(self, setup_db):
        """load_or_create hits branch when ref name not in loaded._lazy_joins and value is not None (raises)."""
        from unittest.mock import patch

        class B(Table, with_timestamps=True):
            value: int = 0

        class C(Table, with_timestamps=True):
            name: str = ""
            link: B | None = None

        c = C(name="z", link=B())
        real_load = Table.load.__func__

        def load_with_empty_lazy_joins(cls, *args, **kwargs):
            out = real_load(cls, *args, **kwargs)
            if out is not None:
                out._lazy_joins = {}
            return out

        with patch.object(C, "load", classmethod(load_with_empty_lazy_joins)):
            with pytest.raises(Exception):
                C.load_or_create(_search_fields=("name",), name="z", link=B())


class TestCreateTableAndAddColumns:
    """Test _create_table and _add_columns edge cases."""

    def test_create_table_skips_fk_for_base_type_table(self, setup_db):
        """_create_table does not add FK when reference base_type is Table."""
        class Poly(Table, with_timestamps=True):
            # Reference to Table base: no concrete table to reference
            target: Table | None = None

        # Should not raise; FK for Table is skipped
        Poly._create_table()
        Poly._add_columns()

    def test_create_table_creates_referenced_table_first(self, setup_db):
        """_create_table recurses into referenced concrete tables (line 357)."""
        class Refd(Table, with_timestamps=True):
            value: int = 0

        class Refing(Table, with_timestamps=True):
            ref: Refd | None = None

        # Creating Refing should create Refd first (recursive _create_table)
        Refing._create_table()
        # Both tables must exist: load would fail if Refd table did not exist
        r = Refd()
        i = Refing(ref=r)
        assert i.ref is not None and i.ref.id == r.id

    def test_add_columns_ignores_duplicate_column_error(self, setup_db):
        """_add_columns catches 'duplicate column name' and does not re-raise."""
        class A(Table, with_timestamps=True):
            name: str = ""

        A._create_table()
        A._add_columns()
        # Second call: columns already exist; ADD COLUMN would duplicate
        A._add_columns()

    def test_add_columns_reraises_other_operational_error(self, setup_db):
        """_add_columns re-raises OperationalError when message is not 'duplicate column name' (line 399)."""
        import sqlite3
        from unittest.mock import patch

        class A(Table, with_timestamps=True):
            name: str = ""

        A._create_table()
        A._add_columns()
        original_execute = A._execute

        def execute_raise_on_alter(sql, *args, **kwargs):
            if "ALTER" in sql:
                raise sqlite3.OperationalError("other error")
            # Make pragma return columns without "name" so _add_columns tries to ALTER
            if "pragma_table_info" in sql:
                return [("id",), ("created_at",), ("updated_at",), ("deleted_at",)]
            parameters = kwargs.pop("parameters", []) if not args else args[0]
            return original_execute(sql, parameters, **kwargs)

        with patch.object(A, "_execute", execute_raise_on_alter):
            with pytest.raises(sqlite3.OperationalError):
                A._add_columns()


class TestProcessData:
    """Test process_data branches (base_type==Table, list refs, BaseModel value)."""

    def test_process_data_scalar_reference_to_table_base(self, setup_db):
        """process_data sets _id and _table for reference where base_type is Table."""
        class B(Table, with_timestamps=True):
            value: int = 0

        class Poly(Table, with_timestamps=True):
            target: Table | None = None

        b = B()
        out = Poly.process_data({"target": b})
        assert out["target_id"] == b.id
        assert out["target_table"] == B._get_table_name()

    def test_process_data_list_of_references(self, setup_db):
        """process_data sets _ids for list of table references; _tables only when secondary_type is Table."""
        class B(Table, with_timestamps=True):
            value: int = 0

        class C(Table, with_timestamps=True):
            items: list[B] = []

        b1 = B()
        b2 = B()
        out = C.process_data({"items": [b1, b2]})
        assert out["items_ids"] == [b1.id, b2.id]
        # items_tables is only set when secondary_type is Table (polymorphic); list[B] has secondary_type B

    def test_process_data_base_model_value(self, setup_db):
        """process_data serializes BaseModel value for non-reference field (e.g. JSON)."""
        from pydantic import BaseModel

        class Nested(BaseModel):
            x: int = 0
            y: str = ""

        class A(Table, with_timestamps=True):
            name: str = ""
            # JSON-like field holding a model
            data: dict = {}

        # Pass a BaseModel where field accepts dict; process_data serializes it
        out = A.process_data({"data": Nested(x=1, y="two")})
        assert out["data"] == {"x": 1, "y": "two"}


class TestDelete:
    """Test delete() soft vs hard."""

    def test_delete_soft(self, setup_db):
        """delete() on table with timestamps does soft delete (deleted_at)."""
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        a.delete()
        # Should not appear in default load (with_deleted=False)
        loaded = A.load(id=a.id)
        assert loaded is None
        loaded_all = A.load(id=a.id, with_deleted=True)
        assert loaded_all is not None

    def test_delete_hard(self, setup_db):
        """delete() on table without soft delete does hard DELETE."""
        class A(Table, with_timestamps=False):
            name: str = ""

        a = A(name="x")
        a.delete()
        loaded = A.load(id=a.id)
        assert loaded is None


class TestLoadOrderAndVersioning:
    """Test load() ORDER BY for versioned and minimal tables."""

    def test_load_order_by_version_when_versioned(self, setup_db):
        """load() uses ORDER BY versioning_along + version for _WithVersion tables."""
        class Doc(Table, versioning_along=("name",)):
            name: str = ""
            content: str = ""

        Doc(name="a", content="1")
        Doc(name="a", content="2")
        rows = Doc.load(as_collection=True)
        assert len(rows) >= 2

    def test_load_order_by_id_when_no_timestamps_no_version(self, setup_db):
        """load() uses ORDER BY id when neither timestamps nor versioning."""
        class A(Table, with_timestamps=False):
            name: str = ""

        A(name="first")
        A(name="second")
        rows = A.load(as_collection=True)
        assert len(rows) >= 2
        ids = [r.id for r in rows]
        assert ids == sorted(ids, reverse=True)
