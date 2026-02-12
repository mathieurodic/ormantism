"""Tests for Table foreign key fields: specific refs, generic refs, list refs, and preload/lazy loading."""

from typing import Optional

import pytest
from pydantic import Field
from ormantism import Table


def test_specific_foreign_key(setup_db, expect_lazy_loads):

    class Node(Table):
        parent: Optional["Node"] = None
        name: str

    grandparent = Node(name="grandparent")
    parent = Node(name="parent", parent=grandparent)
    child = Node(name="child")
    child.parent = parent
    assert grandparent.parent is None
    assert parent.parent.id == grandparent.id
    assert child.parent.id == parent.id

    for preload in ([], ["parent"]):
        grandparent = Node.load(name="grandparent", preload=preload)
        assert grandparent.parent is None
        parent = Node.load(name="parent", preload=preload)
        assert parent.parent.id == grandparent.id
        child = Node.load(name="child", preload=preload)
        assert child.parent.id == parent.id
    # First iteration (no preload) triggers 2 lazy loads (parent.parent, child.parent); second (preload) triggers 0.
    expect_lazy_loads.expect(2)


@pytest.mark.xfail(reason="load(id=..., preload=[...]) joins but hydration expects nested data; flat row not converted")
def test_specific_foreign_key_preload_avoids_lazy(setup_db, expect_lazy_loads):
    """When preloading by id, accessing the relationship must not trigger lazy load."""
    class Node(Table):
        parent: Optional["Node"] = None
        name: str

    grandparent = Node(name="grandparent")
    parent = Node(name="parent", parent=grandparent)
    child = Node(name="child", parent=parent)
    # Load by id with preload so refs are joined and not lazy-loaded
    loaded_gp = Node.load(id=grandparent.id, preload=["parent"])
    loaded_parent = Node.load(id=parent.id, preload=["parent"])
    loaded_child = Node.load(id=child.id, preload=["parent"])
    assert loaded_gp.parent is None
    assert loaded_parent.parent.id == grandparent.id
    assert loaded_child.parent.id == parent.id
    expect_lazy_loads.expect(0)


def test_generic_foreign_key(setup_db):
    
    class Ref1(Table):
        foo: int
    
    class Ref2(Table):
        bar: int
        
    class Ptr(Table):
        ref: Table

    # creation

    ref1 = Ref1(foo=42)
    ref2 = Ref2(bar=101)
    pointer1 = Ptr(ref=ref1)
    pointer2 = Ptr(ref=ref2)

    # retrieval

    with pytest.raises(ValueError, match="Generic reference cannot be preloaded: ref"):
        pointer1 = Ptr.load(ref=ref1, preload="ref")

    pointer1 = Ptr.load(ref=ref1)
    assert pointer1.ref.id == ref1.id
    assert pointer1.ref.__class__ == Ref1
    pointer2 = Ptr.load(ref=ref2)
    assert pointer2.ref.id == ref2.id
    assert pointer2.ref.__class__ == Ref2

    # update

    pointer2.ref = ref1
    pointer2_id = pointer2.id

    # retrieval

    pointer2 = Ptr.load(id=pointer2_id)
    assert pointer2.ref.id == ref1.id
    assert pointer2.ref.__class__ == Ref1


def test_specific_foreign_key_list(setup_db, expect_lazy_loads):

    class Parent(Table):
        name: str
        children: list["Parent"] = Field(default_factory=list)

    n1 = Parent(name="node1")
    n2 = Parent(name="node2")
    n3 = Parent(name="node3", children=[n1, n2])

    assert isinstance(n3.children, list)
    assert len(n3.children) == 2
    assert isinstance(n3.children[0], Parent)

    n3 = Parent.load(name="node3")

    assert isinstance(n3.children, list)
    assert len(n3.children) == 2
    assert isinstance(n3.children[0], Parent)
    expect_lazy_loads.expect(1)


@pytest.mark.skip(reason="preload for list refs in load() not yet supported (join column naming)")
def test_specific_foreign_key_list_preload_avoids_lazy(setup_db, expect_lazy_loads):
    """When preloading children, accessing .children must not trigger lazy load."""
    class Parent(Table):
        name: str
        children: list["Parent"] = Field(default_factory=list)

    n1 = Parent(name="node1")
    n2 = Parent(name="node2")
    n3 = Parent(name="node3", children=[n1, n2])
    n3 = Parent.load(name="node3", preload="children")
    assert isinstance(n3.children, list)
    assert len(n3.children) == 2
    assert isinstance(n3.children[0], Parent)
    expect_lazy_loads.expect(0)


def test_generic_foreign_key_list():

    class Reference1(Table):
        foo: int

    class Reference2(Table):
        bar: float

    class Reference3(Table):
        foobar: str

    class Pointer(Table):
        ref: list[Table]

    # creation

    reference1 = Reference1(foo=42)
    reference2 = Reference2(bar=3.14)
    reference3 = Reference3(foobar="hello")
    pointer = Pointer(ref=[reference1, reference2])

    # retrieval

    with pytest.raises(ValueError, match="Generic reference cannot be preloaded: ref"):
        pointer = Pointer.load(preload="ref")

    pointer = Pointer.load()
    assert pointer.ref[0].id == reference1.id
    assert pointer.ref[0].__class__ == Reference1
    assert pointer.ref[0].foo == 42
    assert pointer.ref[1].id == reference2.id
    assert pointer.ref[1].__class__ == Reference2
    assert pointer.ref[1].bar == 3.14

    # update

    pointer.ref = [reference3, reference2, reference1]

    # retrieval

    pointer = Pointer.load()
    assert len(pointer.ref) == 3
    assert pointer.ref[0].id == reference3.id
    assert pointer.ref[0].__class__ == Reference3
    assert pointer.ref[0].foobar == "hello"
    assert pointer.ref[1].id == reference2.id
    assert pointer.ref[1].__class__ == Reference2
    assert pointer.ref[1].bar == 3.14
    assert pointer.ref[2].id == reference1.id
    assert pointer.ref[2].__class__ == Reference1
    assert pointer.ref[2].foo == 42


class TestLazyRefCoverage:
    """Coverage for lazy ref loading via __getattribute__."""

    def test_lazy_ref_private_attr_raises(self, setup_db):
        """Accessing nonexistent _private on a lazy ref raises AttributeError."""
        class B(Table, with_timestamps=True):
            title: str = ""

        class A(Table, with_timestamps=True):
            book: B | None = None

        b = B(title="x")
        a = A(book=b)
        loaded = A.q().select(A.pk).where(A.pk == a.id).first()
        assert loaded is not None
        with pytest.raises(AttributeError, match="_"):
            _ = loaded.book._private_thing  # loads book, then raises for nonexistent _private_thing

    def test_lazy_list_ref_contains_bool_reversed(self, setup_db):
        """Lazy list ref: __contains__, __bool__, __reversed__ work on loaded list."""
        class Child(Table, with_timestamps=True):
            x: int = 0

        class Parent(Table, with_timestamps=True):
            kids: list[Child] = []

        c1 = Child(x=1)
        c2 = Child(x=2)
        p = Parent(kids=[c1, c2])
        loaded = Parent.q().select(Parent.pk).where(Parent.pk == p.id).first()
        assert loaded is not None
        assert c1 in loaded.kids
        assert bool(loaded.kids) is True
        assert list(reversed(loaded.kids)) == [c2, c1]

