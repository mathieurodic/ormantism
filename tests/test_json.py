from ormantism import Table, JSON


def test_update(setup_db):
    
    class WithJSON(Table):
        j: JSON
    
    # creation & retrieval

    j = [{"url": "https://example.com"}]

    w = WithJSON(j=j)
    assert w.j == j

    w = WithJSON.load()
    assert w.j == j

    w = WithJSON.load(j=j)
    assert w.j == j

    # update!

    j2 = [{"url": "https://example.org"}]

    w.j = j2
    assert w.j == j2

    w = WithJSON.load(j=j2)
    assert w.j == j2
