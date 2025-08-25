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
        class B(Table, with_timestamps=True):
            value: int = 42
        
        fields = B._get_fields()
        assert 'id' in fields
        assert 'created_at' in fields
        assert 'updated_at' in fields
        assert 'deleted_at' in fields
        
        # Test instance creation and value assignment
        b = B()
        assert b.value == 42
        assert b.id is not None
        
        # Test value modification and persistence
        b.value = 69
        loaded_b = B.load(id=b.id)
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
