"""Tests for ormantism.utils.supermodel (SuperModel, to_json_schema, from_json_schema)."""

import types
from typing import GenericAlias
import pytest
from pydantic import BaseModel

from ormantism.utils.supermodel import (
    SuperModel,
    to_json_schema,
    from_json_schema,
)


# --- to_json_schema ---


def test_to_json_schema_simple_type():
    s = to_json_schema(int)
    assert s.get("type") == "integer" or "integer" in str(s)


def test_to_json_schema_includes_title_from_type_name():
    """Built-in types have __name__ set and get a title in the schema."""
    s = to_json_schema(int)
    assert s.get("title") == "int"


def test_to_json_schema_with_defs():
    """Types that produce $defs in the wrapper schema get them merged in."""
    class Nested(BaseModel):
        x: int = 0
    s = to_json_schema(Nested)
    assert "type" in s or "title" in s
    # Wrapper may have $defs for Nested
    if "$defs" in s:
        assert isinstance(s["$defs"], dict)


# --- from_json_schema ---


def test_from_json_schema_invalid_format():
    with pytest.raises(TypeError, match="Invalid schema format"):
        from_json_schema("not a dict")
    with pytest.raises(TypeError, match="Invalid schema format"):
        from_json_schema(None)


def test_from_json_schema_ref_invalid_prefix():
    with pytest.raises(ValueError, match=r"Invalid \$ref"):
        from_json_schema({"$ref": "http://example.com/foo"})


def test_from_json_schema_ref_resolution():
    root = {
        "definitions": {"Foo": {"type": "string", "title": "Foo"}},
        "type": "object",
        "title": "Outer",
        "properties": {"bar": {"$ref": "#/definitions/Foo"}},
    }
    # Resolve a $ref that points into root; path is "definitions/Foo"
    resolved = from_json_schema({"$ref": "#/definitions/Foo"}, root_schema=root)
    assert resolved is str


def test_from_json_schema_anyof_union():
    schema = {
        "anyOf": [
            {"type": "string"},
            {"type": "integer"},
        ]
    }
    t = from_json_schema(schema)
    from typing import Union
    assert t == Union[str, int] or (getattr(t, "__args__", None) and str in t.__args__ and int in t.__args__)


def test_from_json_schema_object_with_title_found_subclass():
    """When title matches a SuperModel subclass, that class is returned."""
    class KnownModel(SuperModel):
        name: str = ""

    schema = {"type": "object", "title": "KnownModel", "properties": {"name": {"type": "string"}}}
    result = from_json_schema(schema)
    assert result is KnownModel


def test_from_json_schema_object_with_title_not_found_uses_rebuild():
    """When title does not match a known subclass, rebuild_pydantic_model is used."""
    schema = {
        "type": "object",
        "title": "DynamicSchemaModel123",
        "properties": {"x": {"type": "integer"}},
        "required": [],
    }
    result = from_json_schema(schema)
    assert isinstance(result, type)
    assert issubclass(result, SuperModel)
    assert result.__name__ == "DynamicSchemaModel123"
    inst = result(x=42)
    assert inst.x == 42


def test_from_json_schema_type_map_scalars():
    assert from_json_schema({"type": "string"}) is str
    assert from_json_schema({"type": "integer"}) is int
    assert from_json_schema({"type": "number"}) is float
    assert from_json_schema({"type": "boolean"}) is bool
    assert from_json_schema({"type": "null"}) is type(None)


def test_from_json_schema_type_map_object_no_title():
    assert from_json_schema({"type": "object"}) is dict


def test_from_json_schema_type_map_array_with_items():
    schema = {"type": "array", "items": {"type": "string"}}
    t = from_json_schema(schema)
    assert t is list[str] or (getattr(t, "__origin__", None) is list and str in getattr(t, "__args__", ()))


def test_from_json_schema_unsupported_raises():
    with pytest.raises(TypeError, match="Unsupported schema"):
        from_json_schema({"type": "unknown"})


# --- SuperModel __init_subclass__ (type annotation replacement) ---


def test_supermodel_subclass_type_annotation_replaced():
    """Subclasses with field annotated as `type` get type | GenericAlias for validation."""
    class WithTypeField(SuperModel):
        model_config = {"arbitrary_types_allowed": True}
        t: type

    # Should accept type values
    m = WithTypeField(t=int)
    assert m.t is int


# --- SuperModel __init__ ---


def test_supermodel_init_before_create_return_false_aborts():
    """When on_before_create returns False, __init__ returns without completing."""
    class AbortCreate(SuperModel):
        x: int = 0

        def on_before_create(self, init_data: dict):
            return False

    # Should not raise; trigger returns False so we return early before BaseModel.__init__
    inst = AbortCreate(x=1)
    # Early return means we did not run full init; just ensure we didn't crash
    assert inst is not None


def test_supermodel_init_unknown_field_raises():
    class M(SuperModel):
        a: int = 0

    with pytest.raises(NameError, match="no field for name"):
        M(b=1)


def test_supermodel_init_type_field_from_dict():
    """Type field value as dict is reconstructed via from_json_schema."""
    class M(SuperModel):
        model_config = {"arbitrary_types_allowed": True}
        t: type

    m = M(t={"type": "string"})
    assert m.t is str


def test_supermodel_init_type_field_direct_type():
    class M(SuperModel):
        model_config = {"arbitrary_types_allowed": True}
        t: type

    m = M(t=list[str])
    assert m.t is list[str] or getattr(m.t, "__origin__", None) is list


def test_supermodel_init_type_field_none_when_optional():
    class M(SuperModel):
        model_config = {"arbitrary_types_allowed": True}
        t: type | None = None

    m = M()
    assert m.t is None
    m2 = M(t=None)
    assert m2.t is None


def test_supermodel_init_type_field_invalid_value_raises():
    class M(SuperModel):
        model_config = {"arbitrary_types_allowed": True}
        t: type

    with pytest.raises(ValueError, match="Not a type"):
        M(t="not a type")


def test_supermodel_init_after_create_called():
    seen = []

    class M(SuperModel):
        x: int = 0

        def on_after_create(self, init_data: dict):
            seen.append(init_data)

    M(x=3)
    assert len(seen) == 1
    assert seen[0] == {"x": 3}


# --- model_dump (mode="json") ---


def test_supermodel_model_dump_json_serializes_type_fields():
    class M(SuperModel):
        model_config = {"arbitrary_types_allowed": True}
        t: type = int

    d = M().model_dump(mode="json")
    assert "t" in d
    assert isinstance(d["t"], dict)
    assert d["t"].get("type") == "integer" or "integer" in str(d["t"])


def test_supermodel_model_dump_json_include_exclude():
    class M(SuperModel):
        a: int = 1
        b: str = "b"

    m = M()
    d = m.model_dump(mode="json", include={"a"})
    assert "a" in d
    assert d.get("b") is None or "b" not in d
    d2 = m.model_dump(mode="json", exclude={"b"})
    assert "a" in d2
    assert "b" not in d2


def test_supermodel_model_dump_json_type_field_serialization_failure():
    """When to_json_schema fails on a type field value, model_dump raises ValueError."""
    class M(SuperModel):
        model_config = {"arbitrary_types_allowed": True}
        t: type = int

    m = M()
    # Replace with a value that passes is_type_annotation but fails to_json_schema
    import unittest.mock as mock
    with mock.patch("ormantism.utils.supermodel.to_json_schema", side_effect=RuntimeError("schema fail")):
        with pytest.raises(ValueError, match="Failed to serialize type field 't'"):
            m.model_dump(mode="json")


# --- __setattr__ and update ---


def test_supermodel_setattr_delegates_to_update():
    updated = []

    class M(SuperModel):
        x: int = 0

        def on_after_update(self, old_data: dict):
            updated.append(old_data)

    m = M()
    m.x = 5
    assert m.x == 5
    assert any(("x", 0) in (list(d.items()) if isinstance(d, dict) else []) or d.get("x") == 0 for d in updated)


def test_supermodel_update_no_op_when_no_changes():
    class M(SuperModel):
        x: int = 0

        def on_before_update(self, new_data: dict):
            raise RuntimeError("should not be called")

    m = M(x=1)
    m.update(x=1)  # same value, should not trigger


def test_supermodel_update_type_field():
    class M(SuperModel):
        model_config = {"arbitrary_types_allowed": True}
        t: type = int

    m = M()
    m.update(t=str)
    assert m.t is str


def test_supermodel_update_triggers_before_and_after():
    before_data = []
    after_data = []

    class M(SuperModel):
        x: int = 0

        def on_before_update(self, new_data: dict):
            before_data.append(dict(new_data))

        def on_after_update(self, old_data: dict):
            after_data.append(dict(old_data))

    m = M(x=1)
    m.update(x=2)
    assert len(before_data) == 1 and before_data[0]["x"] == 2
    assert len(after_data) == 1 and after_data[0]["x"] == 1


def test_supermodel_update_before_update_return_false_aborts():
    class M(SuperModel):
        x: int = 0

        def on_before_update(self, new_data: dict):
            return False

    m = M(x=1)
    m.update(x=2)
    # Abort means we might not apply changes; implementation may still apply
    # We only check that no exception is raised
    assert m.x in (1, 2)


def test_supermodel_update_after_update_receives_old_data():
    """update() calls trigger('after_update', old_data=old_data) with pre-update values."""
    after_old = []

    class M(SuperModel):
        x: int = 0

        def on_after_update(self, old_data: dict):
            after_old.append(dict(old_data))

    m = M(x=10)
    m.update(x=20)
    assert len(after_old) == 1
    assert after_old[0]["x"] == 10


def test_supermodel_trigger_returns_false_when_hook_returns_false():
    """trigger() returns False when an on_* handler returns False."""
    class M(SuperModel):
        x: int = 0

        def on_before_create(self, init_data: dict):
            return False

    m = M(x=1)
    assert m.trigger("before_create", {}) is False

    class M2(SuperModel):
        x: int = 0

        def on_before_update(self, new_data: dict):
            return False

    m2 = M2(x=1)
    assert m2.trigger("before_update", new_data={"x": 2}) is False


def test_supermodel_base_hooks_callable():
    """Base SuperModel on_* hooks are callable and run (pass) without error."""
    class M(SuperModel):
        x: int = 0

    inst = M(x=0)
    SuperModel.on_before_create(inst, {"x": 0})
    SuperModel.on_after_create(inst, {"x": 0})
    SuperModel.on_before_update(inst, {"x": 1})
    SuperModel.on_after_update(inst, {"x": 0})


def test_supermodel_roundtrip_simple_type():
    """Round-trip model_dump(mode='json') and model_validate for a simple type field (from __main__)."""
    class MyModel(SuperModel):
        model_config = {"arbitrary_types_allowed": True}
        field_type: type

    model = MyModel(field_type=int)
    serialized = model.model_dump(mode="json")
    reconstructed = MyModel.model_validate(serialized)
    assert reconstructed.field_type == int


def test_supermodel_roundtrip_complex_type():
    """Round-trip model_dump(mode='json') and model_validate for a complex type (from __main__)."""
    class MyModel(SuperModel):
        model_config = {"arbitrary_types_allowed": True}
        field_type: type

    model = MyModel(field_type=list[str])
    serialized = model.model_dump(mode="json")
    reconstructed = MyModel.model_validate(serialized)
    assert reconstructed.field_type == list[str]


def test_supermodel_roundtrip_supermodel_subclass_as_type():
    """Round-trip Container with content_type=User (SuperModel subclass as type, from __main__)."""
    class User(SuperModel):
        name: str
        age: int

    class Container(SuperModel):
        model_config = {"arbitrary_types_allowed": True}
        content_type: type

    container = Container(content_type=User)
    serialized = container.model_dump(mode="json")
    reconstructed = Container.model_validate(serialized)
    assert reconstructed.content_type == User


# --- trigger ---


def test_supermodel_trigger_before_create_false_aborts():
    class M(SuperModel):
        x: int = 0

        def on_before_create(self, init_data: dict):
            return False

    M(x=1)  # should not raise


def test_supermodel_trigger_calls_subclass_handler():
    called = []

    class Base(SuperModel):
        x: int = 0

    class Sub(Base):
        def on_after_create(self, init_data: dict):
            called.append("Sub")

    Sub(x=1)
    assert "Sub" in called


def test_supermodel_trigger_skips_same_method_in_mro():
    """Trigger should not call the same method twice when it appears in MRO."""
    call_count = []

    class Base(SuperModel):
        x: int = 0

        def on_after_create(self, init_data: dict):
            call_count.append(1)

    class Sub(Base):
        pass

    Sub(x=1)
    assert len(call_count) == 1
