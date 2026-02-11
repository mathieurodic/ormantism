"""Tests for expression integration with Table: class-level column attributes and __getattr__ chaining."""

from ormantism.table import Table
from ormantism.expressions import TableExpression, ColumnExpression


def test_table_class_has_column_attributes(setup_db):
    """Table subclasses get class-level .name as ColumnExpression; id via root.get_column_expression."""
    class User(Table, with_timestamps=True):
        name: str

    root = User._root_expression()
    id_expr = root.get_column_expression("id")
    assert isinstance(id_expr, ColumnExpression)
    assert id_expr.name == "id"
    assert isinstance(User.name, ColumnExpression)
    assert User.name.name == "name"


def test_table_expression_getattr_chains(setup_db):
    """TableExpression.__getattr__ allows Book.author.name style chaining."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    assert isinstance(Book.author, TableExpression)
    assert Book.author.table is Author
    name_expr = Book.author.name
    assert isinstance(name_expr, ColumnExpression)
    assert name_expr.name == "name"
