"""Tests for ormantism.expressions.TableExpression: aliases, get_column_expression, sql_declarations."""

import pytest

from ormantism.table import Table
from ormantism.expressions import TableExpression, ColumnExpression


def test_table_expression_root_alias(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    assert root.sql_alias == "user"


def test_table_expression_joined_alias(setup_db):
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    root = TableExpression(table=Book, parent=None, path=())
    author_table = TableExpression(table=Author, parent=root, path=("author",))
    assert author_table.sql_alias == "book____author"


def test_table_expression_get_column_expression_scalar(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    col = root.get_column_expression("name")
    assert isinstance(col, ColumnExpression)
    assert col.name == "name"
    assert col.table_expression is root


def test_table_expression_get_column_expression_reference(setup_db):
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    root = TableExpression(table=Book, parent=None, path=())
    author_expr = root.get_column_expression("author")
    assert isinstance(author_expr, TableExpression)
    assert author_expr.table is Author
    assert author_expr.path == ("author",)
    assert author_expr.parent is root


def test_table_expression_sql_declarations_root(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    decls = list(root.sql_declarations)
    assert decls == ["FROM user"]


def test_table_expression_sql_declarations_with_join(setup_db):
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    root = TableExpression(table=Book, parent=None, path=())
    author_expr = TableExpression(table=Author, parent=root, path=("author",))
    decls = list(author_expr.sql_declarations)
    assert decls[0] == "FROM book"
    assert "JOIN author AS book____author" in decls[1]
    assert "ON book____author.id = book.author_id" in decls[1]


def test_table_expression_sql_declarations_empty_path_asserts(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    bad = TableExpression(table=User, parent=root, path=())
    with pytest.raises(AssertionError, match="Path must not be empty"):
        list(bad.sql_declarations)


def test_table_expression_getattr_underscore_raises(setup_db):
    """TableExpression.__getattr__ raises AttributeError for names starting with _ (line 213)."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    with pytest.raises(AttributeError, match="_private"):
        _ = root._private


def test_table_expression_sql_alias_base_table_raises(setup_db):
    """TableExpression with table=base Table raises when accessing sql_alias (line 228)."""
    from ormantism.table import Table
    from ormantism.expressions import TableExpression

    root = TableExpression(table=Table, parent=None, path=())
    with pytest.raises(ValueError, match="concrete Table subclass"):
        _ = root.sql_alias
