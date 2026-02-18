"""Tests for ormantism.utils.is_type_annotation."""

from typing import Optional, Union, List, Dict

from ormantism.utils.is_type_annotation import is_type_annotation


def test_is_type_annotation_bare_types():
    assert is_type_annotation(int) is True
    assert is_type_annotation(str) is True


def test_is_type_annotation_generic():
    assert is_type_annotation(list[int]) is True
    assert is_type_annotation(dict[str, int]) is True


def test_is_type_annotation_union():
    assert is_type_annotation(Optional[int]) is True
    assert is_type_annotation(Union[int, str]) is True


def test_is_type_annotation_rejects_string_in_union():
    assert is_type_annotation(Union[int, "not_a_type"]) is False


def test_is_type_annotation_optional_and_nested():
    assert is_type_annotation(Union[int, None]) is True
    assert is_type_annotation(Optional[list[str]]) is True
    assert is_type_annotation(Dict[str, list[int]]) is True
    assert is_type_annotation(Union[list[int], None]) is True
