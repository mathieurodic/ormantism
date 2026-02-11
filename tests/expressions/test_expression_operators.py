"""Tests for ormantism.expressions operator overloads (eq, in, is_null, and/or, arithmetic, NOT, collect_join_paths)."""

from ormantism.table import Table
from ormantism.expressions import (
    TableExpression,
    ColumnExpression,
    OrderExpression,
    NaryOperatorExpression,
    UnaryOperatorExpression,
    FunctionExpression,
    collect_join_paths_from_expression,
)


def test_expression_eq(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("id")
    expr = col == 42
    assert isinstance(expr, NaryOperatorExpression)
    assert expr.symbol == "="
    assert expr.sql == "(user.id = ?)"
    assert expr.values == (42,)


def test_expression_in(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("id")
    expr = col.in_([1, 2, 3])
    assert isinstance(expr, NaryOperatorExpression)
    assert expr.symbol == "IN"
    assert expr.arguments[0] is col
    assert expr.arguments[1] == [1, 2, 3]
    assert expr.values == ([1, 2, 3],)


def test_expression_is_null(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("name")
    expr = col.is_null()
    assert isinstance(expr, UnaryOperatorExpression)
    assert expr.symbol == "IS NULL"
    assert expr.postfix is True
    assert expr.sql == "user.name IS NULL"
    assert expr.values == ()


def test_expression_is_and_is_not(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("name")
    expr = col.is_(None)
    assert isinstance(expr, NaryOperatorExpression)
    assert expr.symbol == "IS"
    assert expr.sql == "(user.name IS ?)"
    is_not_null = col.is_not_null()
    assert is_not_null.symbol == "IS NOT NULL"
    assert is_not_null.postfix is True


def test_expression_pos_and_pow(setup_db):
    class User(Table, with_timestamps=True):
        x: int = 0

    col = User.get_column_expression("x")
    pos_expr = +col
    assert isinstance(pos_expr, UnaryOperatorExpression)
    assert pos_expr.symbol == "+"
    pow_expr = col ** 3
    assert isinstance(pow_expr, FunctionExpression)
    assert pow_expr.symbol == "POW"
    assert pow_expr.arguments == (col, 3)


def test_expression_and_or(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("id")
    e1 = (col == 1) & (col == 2)
    assert isinstance(e1, NaryOperatorExpression)
    assert e1.symbol == "AND"
    assert e1.sql == "((user.id = ?) AND (user.id = ?))"
    assert e1.values == (1, 2)
    e2 = (col == 1) | (col == 2)
    assert e2.symbol == "OR"
    assert e2.values == (1, 2)


def test_expression_arithmetic_and_comparison(setup_db):
    class User(Table, with_timestamps=True):
        x: int = 0
        y: int = 0

    cx = User.get_column_expression("x")
    cy = User.get_column_expression("y")
    assert (cx + cy).symbol == "+"
    assert (cx - cy).symbol == "-"
    assert (cx * cy).symbol == "*"
    assert (cx / cy).symbol == "/"
    assert (cx % cy).symbol == "%"
    assert (cx < 10).symbol == "<"
    assert (cx <= 10).symbol == "<="
    assert (cx > 0).symbol == ">"
    ge_expr = cx >= 0
    assert ge_expr.symbol == ">="
    assert ge_expr.values == (0,)
    # Explicit __ge__ call to cover line 102
    assert cx.__ge__(5).symbol == ">=" and cx.__ge__(5).values == (5,)
    assert (-cx).symbol == "-"
    assert (cx ** 2).symbol == "POW"
    assert isinstance((cx ** 2), FunctionExpression)


def test_collect_join_paths_from_expression(setup_db):
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    author_expr = Book.get_column_expression("author")
    name_col = author_expr.get_column_expression("name")
    expr = name_col == "x"
    paths = collect_join_paths_from_expression(expr)
    assert "author" in paths


def test_collect_join_paths_from_order_expression(setup_db):
    """collect_join_paths_from_expression with OrderExpression walks column_expression (lines 303-304)."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    name_col = Book.author.get_column_expression("name")
    order_expr = OrderExpression(column_expression=name_col, desc=True)
    paths = collect_join_paths_from_expression(order_expr)
    assert "author" in paths


def test_expression_with_table_instance_binds_id(setup_db):
    """ArgumentedExpression._argument_to_values binds Table instance to its id (line 142)."""
    class User(Table, with_timestamps=True):
        name: str = ""

    class Post(Table, with_timestamps=True):
        title: str = ""
        author: User | None = None

    user = User(name="u")
    col = Post.get_column_expression("author_id")
    expr = col == user
    assert expr.values == (user.id,)


def test_expression_not_method(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("name")
    not_expr = col.__not__()
    assert isinstance(not_expr, UnaryOperatorExpression)
    assert not_expr.symbol == "NOT"
    assert not_expr.sql == "NOT user.name"


def test_expression_is_not_and_is_not_null(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("name")
    not_expr = col.is_not(None)
    assert not_expr.symbol == "IS NOT"
    assert not_expr.sql == "(user.name IS NOT ?)"
    not_null = col.is_not_null()
    assert not_null.symbol == "IS NOT NULL"
    assert not_null.sql == "user.name IS NOT NULL"
