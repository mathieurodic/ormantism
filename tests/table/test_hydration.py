"""Tests for Table: rearrange_data_for_hydration, integrate_data_for_hydration, make_empty_instance."""

import pytest

from ormantism.table import Table
from ormantism.expressions import ALIAS_SEPARATOR

# Docstring example (Author with one book and two kids) - single source of truth
_DOCSTRING_UNPARSED = [
    {"id": 1, "name": "Alice", "book____id": 10, "book____title": "Python 101", "kids____id": 1, "kids____x": 10},
    {"id": 1, "name": "Alice", "book____id": 10, "book____title": "Python 101", "kids____id": 2, "kids____x": 20},
]
_DOCSTRING_REARRANGED = {
    1: {"id": 1, "name": "Alice", "book": {10: {"id": 10, "title": "Python 101"}}, "kids": {1: {"id": 1, "x": 10}, 2: {"id": 2, "x": 20}}},
}


def _normalize(result):
    """Convert defaultdict to dict for assertion comparison."""
    from collections import defaultdict

    if isinstance(result, defaultdict):
        return {k: _normalize(v) for k, v in result.items()}
    return result


class TestRearrangeDataForHydration:
    """Test Table.rearrange_data_for_hydration (static method)."""

    def test_docstring_example(self):
        """Exact docstring example (Author with one book and two kids) - single source of truth."""
        result = Table.rearrange_data_for_hydration(_DOCSTRING_UNPARSED)
        assert _normalize(result) == _DOCSTRING_REARRANGED

    def test_empty_unparsed_data_returns_empty_dict(self):
        """Empty list returns empty dict."""
        result = Table.rearrange_data_for_hydration([])
        assert _normalize(result) == {}

    def test_single_row_root_only(self):
        """Single row with root columns only (no path)."""
        unparsed_data = [{"id": 1, "name": "Alice"}]
        result = Table.rearrange_data_for_hydration(unparsed_data)
        assert _normalize(result) == {1: {"id": 1, "name": "Alice"}}

    def test_single_row_root_and_single_ref(self):
        """Single row with root + single ref (book). Nested under root per docstring."""
        unparsed_data = [{
            "id": 1,
            "name": "Alice",
            f"book{ALIAS_SEPARATOR}id": 10,
            f"book{ALIAS_SEPARATOR}title": "Python 101",
        }]
        result = Table.rearrange_data_for_hydration(unparsed_data)
        assert _normalize(result) == {
            1: {"id": 1, "name": "Alice", "book": {10: {"id": 10, "title": "Python 101"}}},
        }

    def test_multiple_rows_root_and_collection_ref(self):
        """Multiple rows with same root, different collection items. Same pk merges into one row."""
        unparsed_data = [
            {
                "id": 1,
                "name": "Alice",
                f"book{ALIAS_SEPARATOR}id": 10,
                f"book{ALIAS_SEPARATOR}title": "Python 101",
                f"kids{ALIAS_SEPARATOR}id": 1,
                f"kids{ALIAS_SEPARATOR}x": 10,
            },
            {
                "id": 1,
                "name": "Alice",
                f"book{ALIAS_SEPARATOR}id": 10,
                f"book{ALIAS_SEPARATOR}title": "Python 101",
                f"kids{ALIAS_SEPARATOR}id": 2,
                f"kids{ALIAS_SEPARATOR}x": 20,
            },
        ]
        result = Table.rearrange_data_for_hydration(unparsed_data)
        assert _normalize(result) == {
            1: {
                "id": 1,
                "name": "Alice",
                "book": {10: {"id": 10, "title": "Python 101"}},
                "kids": {1: {"id": 1, "x": 10}, 2: {"id": 2, "x": 20}},
            },
        }

    def test_path_with_no_id_skipped(self):
        """Row segment with no 'id' key is skipped (continue)."""
        unparsed_data = [{"id": 1, "name": "Alice", f"book{ALIAS_SEPARATOR}title": "Python 101"}]
        result = Table.rearrange_data_for_hydration(unparsed_data)
        assert _normalize(result) == {1: {"id": 1, "name": "Alice"}}

    def test_deep_path_navigation(self):
        """Deep path (book____author) contains separator, so navigates to nested structure."""
        unparsed_data = [{
            "id": 1,
            "name": "Alice",
            f"book{ALIAS_SEPARATOR}id": 10,
            f"book{ALIAS_SEPARATOR}author{ALIAS_SEPARATOR}id": 99,
            f"book{ALIAS_SEPARATOR}author{ALIAS_SEPARATOR}name": "Bob",
        }]
        result = Table.rearrange_data_for_hydration(unparsed_data)
        assert _normalize(result) == {
            1: {
                "id": 1,
                "name": "Alice",
                "book": {10: {"id": 10, "author": {99: {"id": 99, "name": "Bob"}}}},
            },
        }

    def test_same_pk_deduplicated_last_wins(self):
        """Repeated rows with same pk overwrite; last wins."""
        unparsed_data = [
            {"id": 1, "name": "Alice"},
            {"id": 1, "name": "Alicia"},
        ]
        result = Table.rearrange_data_for_hydration(unparsed_data)
        assert _normalize(result) == {1: {"id": 1, "name": "Alicia"}}

    def test_sorted_paths_iteration_order(self):
        """Paths are processed in sorted order ('' < 'book' < 'kids')."""
        unparsed_data = [{
            "id": 1,
            f"kids{ALIAS_SEPARATOR}id": 1,
            f"kids{ALIAS_SEPARATOR}x": 10,
            f"book{ALIAS_SEPARATOR}id": 10,
            f"book{ALIAS_SEPARATOR}title": "X",
        }]
        result = Table.rearrange_data_for_hydration(unparsed_data)
        assert _normalize(result) == {
            1: {
                "id": 1,
                "book": {10: {"id": 10, "title": "X"}},
                "kids": {1: {"id": 1, "x": 10}},
            },
        }

    def test_row_without_root_id_skipped(self):
        """Row with no root id (path '') is skipped entirely."""
        unparsed_data = [
            {f"book{ALIAS_SEPARATOR}id": 10, f"book{ALIAS_SEPARATOR}title": "X"},
        ]
        result = Table.rearrange_data_for_hydration(unparsed_data)
        assert _normalize(result) == {}

    def test_deep_path_parent_missing_id_skipped(self):
        """Deep path (book____author) is skipped when parent (book) has no id in row."""
        unparsed_data = [{
            "id": 1,
            "name": "Alice",
            f"book{ALIAS_SEPARATOR}author{ALIAS_SEPARATOR}id": 99,
            f"book{ALIAS_SEPARATOR}author{ALIAS_SEPARATOR}name": "Bob",
        }]
        result = Table.rearrange_data_for_hydration(unparsed_data)
        # Author data is not added; navigation may create empty book dict
        assert _normalize(result) == {1: {"id": 1, "name": "Alice", "book": {}}}


class TestIntegrateDataForHydration:
    """Test Table.integrate_data_for_hydration (instance method)."""

    def test_empty_rearranged_data_returns_early(self, setup_db):
        """Empty dict returns immediately without mutating instance."""
        class Author(Table, with_timestamps=False):
            name: str = ""

        instance = Author.make_empty_instance(1)
        instance.integrate_data_for_hydration({})
        assert instance.id == 1
        assert not hasattr(instance, "name") or instance.name == ""

    def test_root_only_scalar_columns(self, setup_db):
        """Integrate scalar columns only (no refs)."""
        class Author(Table, with_timestamps=False):
            name: str = ""

        instance = Author.make_empty_instance(1)
        rearranged = {1: {"id": 1, "name": "Alice"}}
        instance.integrate_data_for_hydration(rearranged)
        assert instance.id == 1
        assert instance.name == "Alice"

    def test_single_reference(self, setup_db):
        """Integrate single ref: one nested instance, recursively integrated."""
        class Book(Table, with_timestamps=False):
            title: str = ""

        class Author(Table, with_timestamps=False):
            name: str = ""
            book: Book | None = None

        instance = Author.make_empty_instance(1)
        rearranged = {
            1: {
                "id": 1,
                "name": "Alice",
                "book": {10: {"id": 10, "title": "Python 101"}},
            },
        }
        instance.integrate_data_for_hydration(rearranged)
        assert instance.id == 1
        assert instance.name == "Alice"
        assert instance.book is not None
        assert instance.book.id == 10
        assert instance.book.title == "Python 101"

    def test_collection_reference(self, setup_db):
        """Integrate collection ref: list of nested instances."""
        class Kid(Table, with_timestamps=False):
            x: int = 0

        class Author(Table, with_timestamps=False):
            name: str = ""
            kids: list[Kid] = []

        instance = Author.make_empty_instance(1)
        rearranged = {
            1: {
                "id": 1,
                "name": "Alice",
                "kids": {1: {"id": 1, "x": 10}, 2: {"id": 2, "x": 20}},
            },
        }
        instance.integrate_data_for_hydration(rearranged)
        assert instance.id == 1
        assert instance.name == "Alice"
        assert len(instance.kids) == 2
        assert instance.kids[0].id == 1 and instance.kids[0].x == 10
        assert instance.kids[1].id == 2 and instance.kids[1].x == 20

    def test_empty_collection_reference(self, setup_db):
        """Integrate empty collection ref yields empty list."""
        class Kid(Table, with_timestamps=False):
            x: int = 0

        class Author(Table, with_timestamps=False):
            name: str = ""
            kids: list[Kid] = []

        instance = Author.make_empty_instance(1)
        rearranged = {1: {"id": 1, "name": "Alice", "kids": {}}}
        instance.integrate_data_for_hydration(rearranged)
        assert instance.id == 1
        assert instance.name == "Alice"
        assert instance.__dict__["kids"] == []

    def test_docstring_example_single_and_collection(self, setup_db):
        """Exact docstring example: Author with one book and two kids."""
        class Kid(Table, with_timestamps=False):
            x: int = 0

        class Book(Table, with_timestamps=False):
            title: str = ""

        class Author(Table, with_timestamps=False):
            name: str = ""
            book: Book | None = None
            kids: list[Kid] = []

        instance = Author.make_empty_instance(1)
        instance.integrate_data_for_hydration(_DOCSTRING_REARRANGED)
        assert instance.id == 1
        assert instance.name == "Alice"
        assert instance.book is not None
        assert instance.book.id == 10
        assert instance.book.title == "Python 101"
        assert len(instance.kids) == 2
        assert instance.kids[0].id == 1 and instance.kids[0].x == 10
        assert instance.kids[1].id == 2 and instance.kids[1].x == 20

    def test_deep_nesting_single_ref_chain(self, setup_db):
        """Integrate deep path: Author -> Book -> Publisher (single ref chain)."""
        class Publisher(Table, with_timestamps=False):
            name: str = ""

        class Book(Table, with_timestamps=False):
            title: str = ""
            publisher: Publisher | None = None

        class Author(Table, with_timestamps=False):
            name: str = ""
            book: Book | None = None

        instance = Author.make_empty_instance(1)
        rearranged = {
            1: {
                "id": 1,
                "name": "Alice",
                "book": {10: {"id": 10, "title": "X", "publisher": {99: {"id": 99, "name": "Acme"}}}},
            },
        }
        instance.integrate_data_for_hydration(rearranged)
        assert instance.book is not None
        assert instance.book.title == "X"
        assert instance.book.publisher is not None
        assert instance.book.publisher.id == 99
        assert instance.book.publisher.name == "Acme"

    def test_assert_fails_when_multiple_root_pks(self, setup_db):
        """Raises AssertionError when rearranged_data has more than one root pk."""
        class Author(Table, with_timestamps=False):
            name: str = ""

        instance = Author.make_empty_instance(1)
        rearranged = {1: {"id": 1, "name": "A"}, 2: {"id": 2, "name": "B"}}
        with pytest.raises(AssertionError):
            instance.integrate_data_for_hydration(rearranged)


class TestMakeEmptyInstance:
    """Test Table.make_empty_instance (class method)."""

    def test_returns_instance_with_given_id(self, setup_db):
        """make_empty_instance returns an instance with the given id, no DB row."""
        class Author(Table, with_timestamps=False):
            name: str = ""

        instance = Author.make_empty_instance(42)
        assert instance.id == 42
        assert instance.__class__ is Author
