"""Tests for ormantism.expressions: base Expression, ALIAS_SEPARATOR, ArgumentedExpression, Function/Unary/Binary operators."""

import pytest

from ormantism.expressions import (
    ALIAS_SEPARATOR,
    Expression,
    ArgumentedExpression,
    FunctionExpression,
    UnaryOperatorExpression,
    NaryOperatorExpression,
)


def test_alias_separator():
    assert ALIAS_SEPARATOR == "____"
    assert "user" + ALIAS_SEPARATOR + "books" == "user____books"


def test_expression_base_sql_raises():
    expr = Expression.model_construct()
    with pytest.raises(NotImplementedError, match="sql"):
        _ = expr.sql


def test_expression_base_values_empty():
    expr = Expression.model_construct()
    assert expr.values == ()


def test_expression_base_dialect_raises():
    """Expression._dialect raises NotImplementedError when not overridden."""
    expr = Expression.model_construct()
    with pytest.raises(NotImplementedError, match="_dialect"):
        _ = expr._dialect


def test_function_expression_sql():
    expr = FunctionExpression(symbol="LOWER", arguments=("x",))
    assert expr.sql == "LOWER(?)"
    assert expr.values == ("x",)


def test_function_expression_sql_empty_symbol_raises():
    expr = FunctionExpression(symbol="", arguments=(1,))
    with pytest.raises(ValueError, match="symbol"):
        _ = expr.sql


def test_unary_operator_prefix():
    expr = UnaryOperatorExpression(symbol="NOT", arguments=("foo",), postfix=False)
    assert expr.sql == "NOT ?"
    assert expr.values == ("foo",)


def test_unary_operator_postfix():
    expr = UnaryOperatorExpression(symbol="IS NULL", arguments=("col",), postfix=True)
    assert expr.sql == "? IS NULL"
    assert expr.values == ("col",)


def test_binary_operator_expression_sql_and_values():
    expr = NaryOperatorExpression(symbol="=", arguments=("a.id", 42))
    assert expr.sql == "(? = ?)"
    assert expr.values == ("a.id", 42)


def test_binary_operator_empty_symbol_raises():
    expr = NaryOperatorExpression(symbol="", arguments=(1, 2))
    with pytest.raises(ValueError, match="symbol"):
        _ = expr.sql


def test_nary_operator_empty_arguments_raises():
    expr = NaryOperatorExpression(symbol="=", arguments=())
    with pytest.raises(ValueError, match="at least one argument"):
        _ = expr.sql


def test_argumented_expression_values_recursion():
    inner = NaryOperatorExpression(symbol="=", arguments=("x", 1))
    outer = NaryOperatorExpression(symbol="AND", arguments=(inner, "y"))
    assert outer.values == ("x", 1, "y")


def test_argumented_expression_dialect_raises_when_no_arg_has_dialect():
    """ArgumentedExpression._dialect raises AttributeError when no argument has _dialect."""
    expr = NaryOperatorExpression(symbol="=", arguments=(1, 2))
    with pytest.raises(AttributeError, match="_dialect"):
        _ = expr._dialect


def test_argumented_expression_dialect_continues_when_arg_dialect_raises():
    """ArgumentedExpression._dialect skips an argument whose _dialect raises AttributeError."""
    class FakeExpr(Expression):
        @property
        def sql(self):
            return "?"

        @property
        def _dialect(self):
            raise AttributeError("_dialect")

    fake = FakeExpr.model_construct()
    expr = NaryOperatorExpression(symbol="=", arguments=(fake, 1))
    with pytest.raises(AttributeError, match="_dialect"):
        _ = expr._dialect


def test_argumented_expression_dialect_continue_twice_then_raise():
    """ArgumentedExpression._dialect: two args that raise AttributeError hit except/continue twice then raise."""
    class FakeExpr(Expression):
        @property
        def sql(self):
            return "?"

        @property
        def _dialect(self):
            raise AttributeError("_dialect")

    fake1 = FakeExpr.model_construct()
    fake2 = FakeExpr.model_construct()
    expr = NaryOperatorExpression(symbol="=", arguments=(fake1, fake2))
    with pytest.raises(AttributeError, match="_dialect"):
        _ = expr._dialect
