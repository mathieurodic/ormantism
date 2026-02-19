"""Tests for Pydantic validation behavior on Table construction.

The reported issue mentions that some ORMs (e.g. SQLModel) don't run Pydantic
field validators on object construction. ORMantism Tables are Pydantic models
(via SuperModel mixins), so validators *should* run on regular `Table(**data)`.

Note: hydration (loading from DB) intentionally bypasses Pydantic validation via
Hydratable._suspend_validation / make_empty_instance, because data is parsed by
Column.parse() instead.
"""

import pytest
from pydantic import ValidationError, field_validator

from ormantism.table import Table


class TestTableValidation:
    def test_field_validator_runs_on_init_and_can_transform(self, setup_db):
        class User(Table, with_timestamps=False):
            name: str

            @field_validator("name")
            @classmethod
            def strip_name(cls, v: str) -> str:
                return v.strip()

        u = User(name="  Alice  ")
        assert u.name == "Alice"

    def test_field_validator_runs_on_init_and_can_error(self, setup_db):
        class User(Table, with_timestamps=False):
            name: str

            @field_validator("name")
            @classmethod
            def non_empty(cls, v: str) -> str:
                v = v.strip()
                if not v:
                    raise ValueError("name must not be empty")
                return v

        with pytest.raises(ValidationError):
            User(name="   ")

    def test_init_rejects_unparseable_values(self, setup_db):
        class User(Table, with_timestamps=False):
            # Defaults avoid "field required" errors so the test focuses on type/value parsing.
            name: str
            age: int = 0
            is_admin: bool = False

        # Pydantic is non-strict by default, so some coercions are expected to work.
        u = User(name="Alice", age="1", is_admin="true")
        assert u.age == 1
        assert u.is_admin is True

        with pytest.raises(ValidationError):
            User(name=123)

        with pytest.raises(ValidationError):
            User(name="Alice", age="abc")

        with pytest.raises(ValidationError):
            User(name="Alice", age=1.2)

        with pytest.raises(ValidationError):
            User(name="Alice", is_admin="nope")


class TestTableValidationOnUpdate:
    def test_update_and_assignment_validate_types(self, setup_db):
        class User(Table, with_timestamps=False):
            age: int = 0

        u = User()

        # update() should validate via Pydantic
        with pytest.raises(ValidationError):
            u.update(age="abc")

        # __setattr__ delegates to update() for non-underscore attrs
        with pytest.raises(ValidationError):
            u.age = "abc"
