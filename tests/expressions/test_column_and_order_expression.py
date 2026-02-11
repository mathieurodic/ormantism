"""Tests for ormantism.expressions.ColumnExpression and OrderExpression."""

from ormantism.table import Table
from ormantism.expressions import ColumnExpression, OrderExpression, TableExpression


def test_column_expression_sql_and_values(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("name")
    assert col.sql == "user.name"
    assert col.values == ()


def test_column_expression_sql_for_select(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("name")
    assert col.sql_for_select == "user.name AS user____name"


def test_column_expression_desc(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("name")
    order = col.desc
    assert isinstance(order, OrderExpression)
    assert order.desc is True
    assert order.column_expression is col


def test_order_expression_asc(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("name")
    order = OrderExpression(column_expression=col, desc=False)
    assert order.sql == "user.name ASC"


def test_order_expression_desc(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("name")
    order = OrderExpression(column_expression=col, desc=True)
    assert order.sql == "user.name DESC"
    assert order.path_str == "name"


def test_table_expression_dialect(setup_db):
    """TableExpression._dialect returns table._connection.dialect."""
    class User(Table, with_timestamps=True):
        name: str

    root = User._expression
    assert isinstance(root, TableExpression)
    d = root._dialect
    assert d is User._connection.dialect


def test_order_expression_dialect(setup_db):
    """OrderExpression._dialect delegates to column_expression._dialect."""
    class User(Table, with_timestamps=True):
        name: str

    order = User.name.desc
    assert order._dialect is User._expression._dialect
