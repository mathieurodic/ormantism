"""Tests for ormantism.table_mixins: _transform_query behavior and mixin attributes."""

import pytest
from ormantism.table import Table
from ormantism.query import Query


class TestWithTimestampsTransformQuery:
    """_WithTimestamps._transform_query adds default order by created_at DESC."""

    def test_q_returns_query_with_created_at_order(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

        q = A.q()
        assert len(q.order_by_expressions) == 1
        assert "created_at" in q.order_by_expressions[0].column_expression.path_str
        assert q.order_by_expressions[0].desc is True

    def test_transform_skips_when_order_already_set_by_earlier_mixin(self, setup_db):
        class A(Table, with_timestamps=True):
            name: str = ""

            @classmethod
            def _transform_query(cls, q):
                q = q.order_by(cls._expression["name"])
                return q

        q = A.q()
        # A._transform_query adds name first; _WithTimestamps sees non-empty order and skips
        assert len(q.order_by_expressions) == 1
        assert "name" in q.order_by_expressions[0].column_expression.path_str


class TestWithVersionTransformQuery:
    """_WithVersion._transform_query adds default order by versioning_along + version DESC."""

    def test_q_returns_query_with_version_order(self, setup_db):
        class Doc(Table, versioning_along=("name",)):
            name: str = ""

        q = Doc.q()
        assert len(q.order_by_expressions) == 2  # name (ASC), version (DESC)
        paths = [e.column_expression.path_str for e in q.order_by_expressions]
        assert "name" in paths
        assert "version" in paths
        version_expr = next(e for e in q.order_by_expressions if "version" in e.column_expression.path_str)
        assert version_expr.desc is True

    def test_version_order_includes_multiple_along_columns(self, setup_db):
        class Doc(Table, versioning_along=("name", "key")):
            name: str = ""
            key: str = ""

        q = Doc.q()
        paths = [e.column_expression.path_str for e in q.order_by_expressions]
        assert "name" in paths
        assert "key" in paths
        assert "version" in paths


class TestMixinAttributes:
    """Mixin classes provide expected attributes and TABLE_SQL_CREATIONS."""

    def test_with_primary_key_has_table_sql_creations(self):
        from ormantism.table import _WithPrimaryKey

        assert hasattr(_WithPrimaryKey, "TABLE_SQL_CREATIONS")
        assert "id" in str(_WithPrimaryKey.TABLE_SQL_CREATIONS)

    def test_with_created_at_timestamp_has_table_sql_creations(self):
        from ormantism.table import _WithCreatedAtTimestamp

        assert hasattr(_WithCreatedAtTimestamp, "TABLE_SQL_CREATIONS")
        assert "created_at" in str(_WithCreatedAtTimestamp.TABLE_SQL_CREATIONS)
