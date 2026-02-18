"""Tests for ormantism.expressions operator overloads (eq, in, is_null, and/or, arithmetic, NOT, collect_join_paths)."""

from ormantism.table import Table
from ormantism.expressions import (
    TableExpression,
    ColumnExpression,
    OrderExpression,
    NaryOperatorExpression,
    UnaryOperatorExpression,
    FunctionExpression,
    LikeExpression,
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


def test_expression_ne(setup_db):
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("id")
    expr = col != 42
    assert isinstance(expr, NaryOperatorExpression)
    assert expr.symbol == "!="
    assert expr.sql == "(user.id != ?)"
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
    name_col = author_expr["name"]
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

    name_col = Book.author["name"]
    order_expr = OrderExpression(column_expression=name_col, desc=True)
    paths = collect_join_paths_from_expression(order_expr)
    assert "author" in paths


def test_expression_with_table_instance_binds_id(setup_db):
    """ArgumentedExpression._argument_to_values binds Table instance to its id."""
    class User(Table, with_timestamps=True):
        name: str = ""

    class Post(Table, with_timestamps=True):
        title: str = ""
        author: User | None = None

    user = User(name="u")
    expr = Post.author.fk == user
    assert user.id in expr.values


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


def test_expression_isnull_both_branches(setup_db):
    """Expression._isnull(True) -> is_null(), _isnull(False) -> is_not_null()."""
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("name")
    is_null_expr = col._isnull(True)
    assert is_null_expr.symbol == "IS NULL"
    is_not_null_expr = col._isnull(False)
    assert is_not_null_expr.symbol == "IS NOT NULL"


def test_expression_iexact_string_and_non_string(setup_db):
    """Expression._iexact: string uses LOWER(expr)=lower(value), non-string uses expr == value."""
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("name")
    # string: LOWER(col) = value.lower()
    e1 = col._iexact("Hello")
    assert "LOWER" in e1.sql
    assert e1.values == ("hello",)
    # non-string: col == value
    e2 = col._iexact(42)
    assert e2.symbol == "="
    assert e2.values == (42,)


def test_expression_between_tuple_and_two_args(setup_db):
    """Expression.between accepts (low, high) or two arguments."""
    class User(Table, with_timestamps=True):
        x: int = 0

    col = User.get_column_expression("x")
    e1 = col.between((5, 15))
    assert e1.symbol == "AND"
    assert e1.values == (5, 15)
    e2 = col.between(5, 15)
    assert e2.symbol == "AND"
    assert e2.values == (5, 15)


def test_expression_string_methods_lower_upper_trim(setup_db):
    """Expression.lower, upper, trim, ltrim, rtrim return FunctionExpression."""
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("name")
    assert col.lower().symbol == "LOWER"
    assert col.upper().symbol == "UPPER"
    assert col.trim().symbol == "TRIM"
    assert col.ltrim().symbol == "LTRIM"
    assert col.rtrim().symbol == "RTRIM"
    assert "LOWER" in col.lower().sql
    assert "UPPER" in col.upper().sql


def test_expression_like_methods(setup_db):
    """startswith, contains, endswith, like and their i* variants return LikeExpression with correct flags."""
    class User(Table, with_timestamps=True):
        name: str

    col = User.name
    assert isinstance(col.startswith("A"), LikeExpression)
    assert col.startswith("A").fuzzy_start is False and col.startswith("A").fuzzy_end is True
    assert isinstance(col.contains("x"), LikeExpression)
    assert col.contains("x").fuzzy_start is True and col.contains("x").fuzzy_end is True
    assert isinstance(col.endswith("z"), LikeExpression)
    assert col.endswith("z").fuzzy_start is True and col.endswith("z").fuzzy_end is False
    assert isinstance(col.like("exact"), LikeExpression)
    assert col.like("exact").fuzzy_start is False and col.like("exact").fuzzy_end is False
    assert col.istartswith("a").case_insensitive is True
    assert col.icontains("y").case_insensitive is True
    assert col.iendswith("b").case_insensitive is True
    assert col.ilike("x").case_insensitive is True


def test_expression_pow(setup_db):
    """__pow__ returns FunctionExpression with POW."""
    class User(Table, with_timestamps=True):
        name: str

    col = User.get_column_expression("id")
    expr = col ** 2
    assert expr.symbol == "POW"
    assert expr.arguments == (col, 2)
    assert "POW" in expr.sql
    assert expr.values == (2,)


def test_expression_like_sql_and_values(setup_db):
    """LikeExpression produces SQL with LIKE and bound pattern; startswith/contains use dialect concat."""
    class User(Table, with_timestamps=True):
        name: str

    col = User.name
    # startswith: column LIKE pattern% (concat adds literal '%', so values include pattern and '%')
    expr = col.startswith("John")
    assert "LIKE" in expr.sql
    assert "user.name" in expr.sql or "name" in expr.sql
    assert "John" in expr.values
    # contains: %pattern%
    c = col.contains("oh")
    assert "LIKE" in c.sql
    assert "oh" in c.values


def test_like_expression_escape_needle_false(setup_db):
    """LikeExpression with escape_needle=False does not escape and no ESCAPE clause."""
    class User(Table, with_timestamps=True):
        name: str

    expr = User.name.like("x%")
    like_expr = LikeExpression(symbol="LIKE", arguments=(User.name, "x%"), fuzzy_start=False, fuzzy_end=False, escape_needle=False)
    assert "ESCAPE" not in like_expr.sql
    assert like_expr.values == ("x%",)


def test_like_expression_case_insensitive_non_str_needle(setup_db):
    """LikeExpression case_insensitive with non-str needle uses LOWER(needle) expression."""
    from ormantism.expressions import FunctionExpression

    class User(Table, with_timestamps=True):
        name: str

    # Build a LIKE with an expression as pattern (e.g. column) to hit the non-str branch
    like_expr = LikeExpression(
        symbol="LIKE",
        arguments=(User.name, User.name),  # pattern is column, not str
        fuzzy_start=False,
        fuzzy_end=False,
        case_insensitive=True,
        escape_needle=False,
    )
    assert "LOWER" in like_expr.sql
    # values come from the composed expression
    assert like_expr.values is not None


def test_collect_join_paths_from_expression_includes_like_expression(setup_db):
    """collect_join_paths_from_expression walks LikeExpression and collects column paths."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    expr = Book.author.name.contains("x")
    paths = collect_join_paths_from_expression(expr)
    assert "author" in paths


def test_collect_join_paths_from_expression_table_expression_with_path(setup_db):
    """collect_join_paths_from_expression adds path when walking a TableExpression with path."""
    class Author(Table, with_timestamps=True):
        name: str

    class Book(Table, with_timestamps=True):
        title: str
        author: Author | None = None

    paths = collect_join_paths_from_expression(Book.author)
    assert paths == {"author"}
