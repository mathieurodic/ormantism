from typing import Optional

import pytest
from pydantic import Field
from ormantism import Table


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


def test_specific_foreign_key_list(setup_db):

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
