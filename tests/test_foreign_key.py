from typing import Optional
import pytest
from ormantism.table import Table


def test_specific_foreign_key(setup_db):

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


def test_generic_foreign_key():
    
    class Ref1(Table):
        foo: int
    
    class Ref2(Table):
        bar: int
        
    class Pointer(Table):
        ref: Table

    # creation

    ref1 = Ref1(foo=42)
    ref2 = Ref2(bar=101)
    pointer1 = Pointer(ref=ref1)
    pointer2 = Pointer(ref=ref2)

    # retrieval

    with pytest.raises(ValueError, match="Generic reference cannot be preloaded: ref"):
        pointer1 = Pointer.load(ref=ref1, preload="ref")

    pointer1 = Pointer.load(ref=ref1)
    assert pointer1.ref.id == ref1.id
    assert pointer1.ref.__class__ == Ref1
    pointer2 = Pointer.load(ref=ref2)
    assert pointer2.ref.id == ref2.id
    assert pointer2.ref.__class__ == Ref2

    # update

    pointer2.ref = ref1
    pointer2_id = pointer2.id

    # retrieval

    pointer2 = Pointer.load(id=pointer2_id)
    assert pointer2.ref.id == ref1.id
    assert pointer2.ref.__class__ == Ref1
