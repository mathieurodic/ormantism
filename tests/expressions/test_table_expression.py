"""Tests for ormantism.expressions.TableExpression: aliases, get_column_expression, sql_declarations."""

import pytest

from ormantism.table import Table
from ormantism.expressions import (
    TableExpression,
    ColumnExpression,
    NaryOperatorExpression,
    UnaryOperatorExpression,
    FunctionExpression,
)


def test_table_expression_root_alias(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    assert User._expression.sql_alias == "user"


def test_table_expression_joined_alias(setup_db):
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    author_table = TableExpression(table=Author, parent=Book._expression, path=("author",))
    assert author_table.sql_alias == "book____author"


def test_table_expression_get_column_expression_scalar(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("name")
    assert isinstance(col, ColumnExpression)
    assert col.name == "name"
    assert col.table_expression.table is User and col.table_expression.path == ()


def test_table_expression_get_column_expression_reference(setup_db):
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    author_expr = Book.get_column_expression("author")
    assert isinstance(author_expr, TableExpression)
    assert author_expr.table is Author
    assert author_expr.path == ("author",)
    assert author_expr.parent.table is Book and author_expr.parent.path == ()


def test_table_expression_isnull_relation_uses_fk_column(setup_db):
    """TableExpression._isnull on a relation uses parent's FK column for IS NULL / IS NOT NULL."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    author_expr = Book.get_column_expression("author")
    is_null_expr = author_expr._isnull(True)
    assert "author" in is_null_expr.sql
    assert "IS NULL" in is_null_expr.sql
    is_not_null_expr = author_expr._isnull(False)
    assert "author" in is_not_null_expr.sql
    assert "IS NOT NULL" in is_not_null_expr.sql


def test_table_expression_sql_declarations_root(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    decls = list(User._expression.sql_declarations)
    assert decls == ["FROM user"]


def test_table_expression_sql_declarations_with_join(setup_db):
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    author_expr = TableExpression(table=Author, parent=Book._expression, path=("author",))
    decls = list(author_expr.sql_declarations)
    assert decls[0] == "FROM book"
    assert "JOIN author AS book____author" in decls[1]
    assert "ON book____author.id = book.author" in decls[1]


def test_table_expression_sql_declarations_empty_path_asserts(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    bad = TableExpression(table=User, parent=User._expression, path=())
    with pytest.raises(AssertionError, match="Path must not be empty"):
        list(bad.sql_declarations)


def test_table_expression_getattr_underscore_raises(setup_db):
    """TableExpression.__getattr__ raises AttributeError for names starting with _ (line 213)."""
    class User(Table, with_timestamps=True):
        name: str

    with pytest.raises(AttributeError, match="_private"):
        _ = User._expression._private


def test_table_expression_sql_alias_base_table_raises(setup_db):
    """TableExpression with table=base Table raises when accessing sql_alias (line 228)."""
    from ormantism.table import Table
    from ormantism.expressions import TableExpression

    root = TableExpression(table=Table, parent=None, path=())
    with pytest.raises(ValueError, match="concrete Table subclass"):
        _ = root.sql_alias


# --- fk property ---


def test_fk_on_root_expression_raises(setup_db):
    """TableExpression.fk raises ValueError when called on a root expression (no parent)."""
    class User(Table, with_timestamps=True):
        name: str

    with pytest.raises(ValueError, match="fk only valid on joined"):
        _ = User._expression.fk


def test_fk_on_list_reference_raises(setup_db):
    """TableExpression.fk raises ValueError for list (non-scalar) references."""
    class Author(Table, with_timestamps=True):
        name: str

    class User(Table, with_timestamps=True):
        name: str
        books: list[Author] = []

    books_expr = User.get_column_expression("books")
    with pytest.raises(ValueError, match="fk only valid for scalar refs"):
        _ = books_expr.fk


# --- __eq__ ---


def test_eq_root_with_table_instance(setup_db):
    """Root TableExpression == instance compares by id."""
    class User(Table, with_timestamps=True):
        name: str

    user = User(name="Alice")
    expr = User._expression == user
    assert isinstance(expr, NaryOperatorExpression)
    assert expr.symbol == "="
    assert "user.id" in expr.sql
    assert user.id in expr.values


def test_eq_root_with_int(setup_db):
    """Root TableExpression == int compares id to literal."""
    class User(Table, with_timestamps=True):
        name: str

    expr = User._expression == 42
    assert isinstance(expr, NaryOperatorExpression)
    assert expr.symbol == "="
    assert 42 in expr.values


def test_eq_root_with_none(setup_db):
    """Root TableExpression == None produces IS NULL on id."""
    class User(Table, with_timestamps=True):
        name: str

    expr = User._expression == None
    assert isinstance(expr, UnaryOperatorExpression)
    assert "IS NULL" in expr.sql


def test_eq_invalid_type_raises(setup_db):
    """TableExpression == non-Table/int/None raises ValueError."""
    class User(Table, with_timestamps=True):
        name: str

    with pytest.raises(ValueError, match="table instance, int, or None"):
        User._expression == "invalid"


def test_eq_type_mismatch_raises(setup_db):
    """Scalar ref == wrong Table type raises ValueError."""
    class Author(Table, with_timestamps=True):
        name: str

    class Genre(Table, with_timestamps=True):
        label: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    genre = Genre(label="Fiction")
    with pytest.raises(ValueError, match="same type"):
        Book.author == genre


def test_eq_joined_scalar_ref_with_instance(setup_db):
    """Joined scalar ref == instance compares FK to instance.id."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    author = Author(name="Bob")
    expr = Book.author == author
    assert isinstance(expr, NaryOperatorExpression)
    assert expr.symbol == "="
    assert author.id in expr.values


def test_eq_joined_scalar_ref_with_none(setup_db):
    """Joined scalar ref == None produces IS NULL on FK column."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    expr = Book.author == None
    assert isinstance(expr, UnaryOperatorExpression)
    assert "IS NULL" in expr.sql


def test_eq_joined_scalar_ref_with_int(setup_db):
    """Joined scalar ref == int compares FK to literal."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    expr = Book.author == 7
    assert isinstance(expr, NaryOperatorExpression)
    assert 7 in expr.values


# --- polymorphic __eq__ (table=None) ---


def test_eq_polymorphic_with_instance(setup_db):
    """Polymorphic ref == instance produces json_extract AND expression."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    # Manually build a polymorphic TableExpression (table=None)
    poly_expr = TableExpression(parent=Book._expression, table=None, path=("author",))
    author = Author(name="X")
    expr = poly_expr == author
    assert isinstance(expr, NaryOperatorExpression)
    assert expr.symbol == "AND"
    assert "json_extract" in expr.sql


def test_eq_polymorphic_with_none(setup_db):
    """Polymorphic ref == None produces IS NULL on FK."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    poly_expr = TableExpression(parent=Book._expression, table=None, path=("author",))
    expr = poly_expr == None
    assert isinstance(expr, UnaryOperatorExpression)
    assert "IS NULL" in expr.sql


def test_eq_polymorphic_with_int_raises(setup_db):
    """Polymorphic ref == int raises ValueError."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    poly_expr = TableExpression(parent=Book._expression, table=None, path=("author",))
    with pytest.raises(ValueError, match="Polymorphic"):
        poly_expr == 5


# --- __ne__ ---


def test_ne_returns_expression(setup_db):
    """TableExpression != instance returns a NOT expression, not a bool."""
    class User(Table, with_timestamps=True):
        name: str

    user = User(name="Alice")
    expr = User._expression != user
    assert isinstance(expr, UnaryOperatorExpression)
    assert expr.symbol == "NOT"


# --- is_null / is_not_null overrides ---


def test_table_expression_is_null(setup_db):
    """TableExpression.is_null() delegates through __eq__(None)."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    expr = Book.author.is_null()
    assert isinstance(expr, UnaryOperatorExpression)
    assert "IS NULL" in expr.sql


def test_table_expression_is_not_null(setup_db):
    """TableExpression.is_not_null() delegates through __ne__(None)."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    expr = Book.author.is_not_null()
    assert isinstance(expr, UnaryOperatorExpression)
    assert expr.symbol == "NOT"


# --- _isnull fallthrough for root ---


def test_isnull_on_root_falls_through_to_super(setup_db):
    """TableExpression._isnull on root (no parent) falls through to Expression._isnull."""
    class User(Table, with_timestamps=True):
        name: str

    expr = User._expression._isnull(True)
    assert isinstance(expr, UnaryOperatorExpression)
    assert "IS NULL" in expr.sql


# --- root_table ---


def test_root_table_on_joined_expression(setup_db):
    """root_table walks up parent chain to return the root Table class."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    author_expr = Book.get_column_expression("author")
    assert author_expr.root_table is Book
