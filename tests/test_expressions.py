"""Tests for ormantism.expressions: Expression tree, Table/Column expressions, operators."""

import pytest

from ormantism.table import Table
from ormantism.expressions import (
    ALIAS_SEPARATOR,
    Expression,
    ArgumentedExpression,
    FunctionExpression,
    UnaryOperatorExpression,
    BinaryOperatorExpression,
    TableExpression,
    ColumnExpression,
    OrderExpression,
)


# --- Constants and base Expression ---


def test_alias_separator():
    """ALIAS_SEPARATOR is the string used in SQL aliases (e.g. user____books)."""
    assert ALIAS_SEPARATOR == "____"
    assert "user" + ALIAS_SEPARATOR + "books" == "user____books"


def test_expression_base_sql_raises():
    """Base Expression.sql raises NotImplementedError."""
    expr = Expression.model_construct()
    with pytest.raises(NotImplementedError, match="sql"):
        _ = expr.sql


def test_expression_base_values_empty():
    """Base Expression.values is ()."""
    expr = Expression.model_construct()
    assert expr.values == ()


# --- ArgumentedExpression, FunctionExpression, Unary/Binary operators ---


def test_function_expression_sql():
    """FunctionExpression renders as symbol(args...)."""
    expr = FunctionExpression(symbol="LOWER", arguments=("x",))
    assert expr.sql == "LOWER(?)"
    assert expr.values == ("x",)


def test_function_expression_sql_empty_symbol_raises():
    """FunctionExpression with empty symbol raises ValueError."""
    expr = FunctionExpression(symbol="", arguments=(1,))
    with pytest.raises(ValueError, match="symbol"):
        _ = expr.sql


def test_unary_operator_prefix():
    """UnaryOperatorExpression with postfix=False renders symbol argument."""
    expr = UnaryOperatorExpression(symbol="NOT", arguments=("foo",), postfix=False)
    assert expr.sql == "NOT ?"
    assert expr.values == ("foo",)


def test_unary_operator_postfix():
    """UnaryOperatorExpression with postfix=True renders argument symbol."""
    expr = UnaryOperatorExpression(symbol="IS NULL", arguments=("col",), postfix=True)
    assert expr.sql == "? IS NULL"
    assert expr.values == ("col",)


def test_binary_operator_expression_sql_and_values():
    """BinaryOperatorExpression renders (left symbol right) and collects values."""
    expr = BinaryOperatorExpression(symbol="=", arguments=("a.id", 42))
    assert expr.sql == "(? = ?)"
    assert expr.values == ("a.id", 42)


def test_binary_operator_empty_symbol_raises():
    """BinaryOperatorExpression with empty symbol raises ValueError."""
    expr = BinaryOperatorExpression(symbol="", arguments=(1, 2))
    with pytest.raises(ValueError, match="symbol"):
        _ = expr.sql


def test_argumented_expression_values_recursion():
    """ArgumentedExpression.values recurses into nested expressions."""
    inner = BinaryOperatorExpression(symbol="=", arguments=("x", 1))
    outer = BinaryOperatorExpression(symbol="AND", arguments=(inner, "y"))
    assert outer.values == ("x", 1, "y")


# --- TableExpression and ColumnExpression (need Table with _get_field, _get_table_name) ---


def test_table_expression_root_alias(setup_db):
    """Root TableExpression uses table name as sql_alias."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    assert root.sql_alias == "user"


def test_table_expression_joined_alias(setup_db):
    """Joined TableExpression alias is parent_alias + SEP + path segment."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    root = TableExpression(table=Book, parent=None, path=())
    author_table = TableExpression(table=Author, parent=root, path=("author",))
    assert author_table.sql_alias == "book____author"


def test_table_expression_get_column_expression_scalar(setup_db):
    """get_column_expression for non-reference returns ColumnExpression."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    col = root.get_column_expression("name")
    assert isinstance(col, ColumnExpression)
    assert col.name == "name"
    assert col.table_expression is root


def test_table_expression_get_column_expression_reference(setup_db):
    """get_column_expression for reference returns child TableExpression."""
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
    """Root TableExpression yields FROM table."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    decls = list(root.sql_declarations)
    assert decls == ["FROM user"]


def test_table_expression_sql_declarations_with_join(setup_db):
    """TableExpression with parent yields JOIN then FROM from parent."""
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
    """TableExpression with parent but empty path asserts."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    bad = TableExpression(table=User, parent=root, path=())
    with pytest.raises(AssertionError, match="Path must not be empty"):
        list(bad.sql_declarations)


def test_column_expression_sql_and_values(setup_db):
    """ColumnExpression.sql is alias.name; values is ()."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    col = root.get_column_expression("name")
    assert col.sql == "user.name"
    assert col.values == ()


def test_column_expression_sql_for_select(setup_db):
    """ColumnExpression.sql_for_select adds AS alias____name."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    col = root.get_column_expression("name")
    assert col.sql_for_select == "user.name AS user____name"


def test_column_expression_desc(setup_db):
    """ColumnExpression.desc returns OrderExpression with desc=True."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    col = root.get_column_expression("name")
    order = col.desc
    assert isinstance(order, OrderExpression)
    assert order.desc is True
    assert order.column_expression is col


def test_order_expression_asc(setup_db):
    """OrderExpression with desc=False renders column ASC."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    col = root.get_column_expression("name")
    order = OrderExpression(column_expression=col, desc=False)
    assert order.sql == "user.name ASC"


def test_order_expression_desc(setup_db):
    """OrderExpression with desc=True renders column DESC."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    col = root.get_column_expression("name")
    order = OrderExpression(column_expression=col, desc=True)
    assert order.sql == "user.name DESC"


# --- Operator overloads (using ColumnExpression so .sql works) ---


def test_expression_eq(setup_db):
    """Expression __eq__ builds BinaryOperatorExpression."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    col = root.get_column_expression("id")
    expr = col == 42
    assert isinstance(expr, BinaryOperatorExpression)
    assert expr.symbol == "="
    assert expr.sql == "(user.id = ?)"
    assert expr.values == (42,)


def test_expression_in(setup_db):
    """Expression.in_ builds IN expression."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    col = root.get_column_expression("id")
    expr = col.in_([1, 2, 3])
    assert isinstance(expr, BinaryOperatorExpression)
    assert expr.symbol == "IN"
    assert expr.arguments[0] is col
    assert expr.arguments[1] == [1, 2, 3]
    assert expr.values == ([1, 2, 3],)


def test_expression_is_null(setup_db):
    """Expression.is_null builds postfix IS NULL."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    col = root.get_column_expression("name")
    expr = col.is_null()
    assert isinstance(expr, UnaryOperatorExpression)
    assert expr.symbol == "IS NULL"
    assert expr.postfix is True
    assert expr.sql == "user.name IS NULL"
    assert expr.values == ()


def test_expression_and_or(setup_db):
    """Expression __and__ and __or__ build AND/OR expressions."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    col = root.get_column_expression("id")
    e1 = (col == 1) & (col == 2)
    assert isinstance(e1, BinaryOperatorExpression)
    assert e1.symbol == "AND"
    assert e1.sql == "((user.id = ?) AND (user.id = ?))"
    assert e1.values == (1, 2)

    e2 = (col == 1) | (col == 2)
    assert e2.symbol == "OR"
    assert e2.values == (1, 2)


def test_expression_arithmetic_and_comparison(setup_db):
    """Expression supports +, -, *, /, <, <=, >, >=, __neg__, __pow__."""
    class User(Table, with_timestamps=True):
        x: int = 0
        y: int = 0

    root = TableExpression(table=User, parent=None, path=())
    cx = root.get_column_expression("x")
    cy = root.get_column_expression("y")

    assert (cx + cy).symbol == "+"
    assert (cx - cy).symbol == "-"
    assert (cx * cy).symbol == "*"
    assert (cx / cy).symbol == "/"
    assert (cx % cy).symbol == "%"
    assert (cx < 10).symbol == "<"
    assert (cx <= 10).symbol == "<="
    assert (cx > 0).symbol == ">"
    assert (cx >= 0).symbol == ">="
    assert (-cx).symbol == "-"
    assert (cx ** 2).symbol == "POW"
    assert isinstance((cx ** 2), FunctionExpression)


def test_expression_not_method(setup_db):
    """Expression.__not__ builds NOT prefix (no Python operator for it)."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    col = root.get_column_expression("name")
    not_expr = col.__not__()
    assert isinstance(not_expr, UnaryOperatorExpression)
    assert not_expr.symbol == "NOT"
    assert not_expr.sql == "NOT user.name"


def test_table_class_has_column_attributes(setup_db):
    """Table subclasses get class-level .name, etc. as ColumnExpression; id via root for queries."""
    class User(Table, with_timestamps=True):
        name: str

    root = User._root_expression()
    id_expr = root.get_column_expression("id")
    assert isinstance(id_expr, ColumnExpression)
    assert id_expr.name == "id"
    assert isinstance(User.name, ColumnExpression)
    assert User.name.name == "name"


def test_table_expression_getattr_chains(setup_db):
    """TableExpression.__getattr__ allows User.books.title style chaining."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    # Class-level: Book.author is TableExpression(Author), Book.author.name is ColumnExpression
    assert isinstance(Book.author, TableExpression)
    assert Book.author.table is Author
    name_expr = Book.author.name
    assert isinstance(name_expr, ColumnExpression)
    assert name_expr.name == "name"


def test_expression_is_not_and_is_not_null(setup_db):
    """Expression.is_not and is_not_null build IS NOT / IS NOT NULL."""
    class User(Table, with_timestamps=True):
        name: str

    root = TableExpression(table=User, parent=None, path=())
    col = root.get_column_expression("name")
    not_expr = col.is_not(None)
    assert not_expr.symbol == "IS NOT"
    assert not_expr.sql == "(user.name IS NOT ?)"
    not_null = col.is_not_null()
    assert not_null.symbol == "IS NOT NULL"
    assert not_null.sql == "user.name IS NOT NULL"
