"""Tests for Table lifecycle: on_after_create, on_before_update, and load_or_create."""

import pytest
from ormantism.table import Table


class TestOnAfterCreatePaths:
    """Test on_after_create branches (early return, versioning NULL, ref skip, empty INSERT)."""

    def test_on_after_create_skips_when_id_already_set(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="first")
        first_id = a.id
        object.__setattr__(a, "id", first_id)
        a.on_after_create({"name": "first"})
        loaded = A.load(id=first_id)
        assert loaded.name == "first"

    def test_versioning_with_null_in_search(self, setup_db):
        class Doc(Table, versioning_along=("name",)):
            name: str | None = None
            content: str = ""

        d = Doc(name=None, content="none")
        assert d.name is None
        assert d.content == "none"
        d2 = Doc(name="foo", content="bar")
        assert d2.name == "foo"

    def test_insert_with_reference_field_skips_ref_in_formatted_data(self, setup_db):
        class B(Table, with_timestamps=True):
            value: int = 0

        class C(Table, with_timestamps=True):
            links_to: B | None = None

        b = B()
        c = C(links_to=b)
        assert c.id is not None
        assert c.links_to.id == b.id

    def test_insert_default_values_only(self, setup_db):
        class IdAndTimestampsOnly(Table, with_timestamps=True):
            pass

        IdAndTimestampsOnly._create_table()
        IdAndTimestampsOnly._add_columns()
        inst = object.__new__(IdAndTimestampsOnly)
        inst.__dict__.update({"id": None})
        inst.on_after_create({})
        assert inst.id is not None

    def test_insert_sets_id_and_makes_readonly_lazy(self, setup_db):
        """Query.insert returns id and marks read-only fields lazy."""
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="direct")
        assert a.id is not None
        assert a.name == "direct"

    def test_insert_applies_defaults_for_fields_not_in_init_data(self, setup_db):
        """Fields with defaults not in init_data get default before insert."""
        class A(Table, with_timestamps=True):
            name: str = ""
            optional: int | None = 42

        a = A(name="x")
        assert a.optional == 42


class TestOnBeforeUpdatePaths:
    """Test on_before_update branches (empty set_statement)."""

    def test_update_empty_set_statement_returns_early(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A(name="x")
        a.on_before_update({"name": "x"})


class TestLoadOrCreate:
    """Test load_or_create branches."""

    def test_load_or_create_with_search_fields(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""
            tag: str = ""

        a1 = A(name="n", tag="t1")
        a2 = A.load_or_create(_search_fields=("name",), name="n", tag="t2")
        assert a2.id == a1.id
        assert a2.tag == "t2"

    def test_load_or_create_creates_when_not_found(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        a = A.load_or_create(name="unique")
        assert a is not None
        assert a.name == "unique"
        a2 = A.load_or_create(name="unique")
        assert a2.id == a.id

    def test_load_or_create_updates_non_reference_field(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""
            value: int = 0

        a = A.load_or_create(_search_fields=("name",), name="x", value=1)
        a2 = A.load_or_create(_search_fields=("name",), name="x", value=2)
        assert a2.id == a.id
        assert a2.value == 2

    def test_load_or_create_updates_reference_field_none(self, setup_db):
        class B(Table, with_timestamps=True):
            value: int = 0

        class C(Table, with_timestamps=True):
            name: str = ""
            link: B | None = None

        b = B()
        c = C(name="x", link=b)
        c2 = C.load_or_create(_search_fields=("name",), name="x", link=None)
        assert c2.id == c.id
        assert c2.link is None

    def test_load_or_create_updates_reference_field_different_id(self, setup_db):
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

    def test_load_or_create_polymorphic_ref_tuple_updates(self, setup_db):
        """load_or_create when _lazy_joins[name] is (class, id) and value differs (lines 95-96)."""
        class B1(Table, with_timestamps=True):
            tag: str = ""

        class B2(Table, with_timestamps=True):
            tag: str = ""

        class Poly(Table, with_timestamps=True):
            key: str = ""
            ref: Table | None = None

        b1 = B1(tag="one")
        b2 = B2(tag="two")
        p = Poly(key="k", ref=b1)
        # Load without preload so ref is in _lazy_joins as (B1, id)
        loaded = Poly.load(key="k")
        assert "ref" in loaded._lazy_joins
        assert loaded._lazy_joins["ref"][0] is B1
        # Update via load_or_create with same-type ref but different id (hits 94-95)
        b1_other = B1(tag="other")
        p2 = Poly.load_or_create(_search_fields=("key",), key="k", ref=b1_other)
        assert p2.id == p.id
        assert p2.ref is not None and p2.ref.id == b1_other.id
