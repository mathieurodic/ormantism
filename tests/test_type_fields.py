from ormantism import Table


def test_type_fields(setup_db):
    
    class TableWithType(Table):
        name: str
        type: type

    t1 = TableWithType(name="integer", type=int)

    assert t1.name == "integer"
    assert t1.type == int
    # t1.type = str
    # assert t1.type == str
