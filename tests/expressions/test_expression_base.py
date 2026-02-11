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


def test_argumented_expression_values_recursion():
    inner = NaryOperatorExpression(symbol="=", arguments=("x", 1))
    outer = NaryOperatorExpression(symbol="AND", arguments=(inner, "y"))
    assert outer.values == ("x", 1, "y")
