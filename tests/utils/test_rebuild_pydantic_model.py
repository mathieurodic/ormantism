"""Tests for ormantism.utils.rebuild_pydantic_model (get_field_type, rebuild_pydantic_model)."""

import pytest
from pydantic import BaseModel
from ormantism.utils.schema import (
    get_field_type,
    rebuild_pydantic_model,
)


# --- get_field_type ---


def test_get_field_type_string():
    assert get_field_type({"type": "string"}) is str


def test_get_field_type_integer():
    assert get_field_type({"type": "integer"}) is int


def test_get_field_type_number():
    assert get_field_type({"type": "number"}) is float


def test_get_field_type_boolean():
    assert get_field_type({"type": "boolean"}) is bool


def test_get_field_type_array():
    result = get_field_type({"type": "array", "items": {"type": "string"}})
    assert getattr(result, "__origin__", None) is list
    assert getattr(result, "__args__", ())[0] is str


def test_get_field_type_array_empty_items():
    """Array with no items schema falls back to get_field_type({}) -> str default."""
    result = get_field_type({"type": "array", "items": {}})
    assert getattr(result, "__origin__", None) is list


def test_get_field_type_object_nested_model():
    result = get_field_type({
        "type": "object",
        "title": "Nested",
        "properties": {"x": {"type": "integer"}},
        "required": ["x"],
    })
    assert isinstance(result, type)
    assert issubclass(result, BaseModel)
    assert result.__name__ == "Nested"
    inst = result(x=1)
    assert inst.x == 1


def test_get_field_type_object_optional_nested_field():
    result = get_field_type({
        "type": "object",
        "properties": {"opt": {"type": "string"}},
        "required": [],
    })
    inst = result(opt="hello")
    assert inst.opt == "hello"
    inst2 = result(opt=None)
    assert inst2.opt is None


def test_get_field_type_object_nested_default():
    result = get_field_type({
        "type": "object",
        "title": "WithDefault",
        "properties": {"v": {"type": "integer", "default": 10}},
        "required": [],
    })
    inst = result()
    assert inst.v == 10


def test_get_field_type_unknown_default_str():
    """Unknown or missing type defaults to str."""
    assert get_field_type({"type": "unknown"}) is str
    assert get_field_type({}) is str


# --- rebuild_pydantic_model ---


def test_rebuild_pydantic_model_simple():
    schema = {
        "title": "Simple",
        "type": "object",
        "properties": {"x": {"type": "integer"}},
        "required": ["x"],
    }
    Model = rebuild_pydantic_model(schema)
    assert Model.__name__ == "Simple"
    assert issubclass(Model, BaseModel)
    inst = Model(x=42)
    assert inst.x == 42


def test_rebuild_pydantic_model_no_title_uses_dynamic_model():
    schema = {
        "type": "object",
        "properties": {"a": {"type": "string"}},
        "required": [],
    }
    Model = rebuild_pydantic_model(schema)
    assert Model.__name__ == "DynamicModel"
    inst = Model(a="hi")
    assert inst.a == "hi"


def test_rebuild_pydantic_model_optional_field():
    schema = {
        "title": "Opt",
        "type": "object",
        "properties": {"opt": {"type": "string"}},
        "required": [],
    }
    Model = rebuild_pydantic_model(schema)
    # Optional[str] with default ... still requires the key; value can be None
    inst = Model(opt=None)
    assert inst.opt is None


def test_rebuild_pydantic_model_field_default():
    schema = {
        "title": "WithDefault",
        "type": "object",
        "properties": {"flag": {"type": "boolean", "default": True}},
        "required": [],
    }
    Model = rebuild_pydantic_model(schema)
    inst = Model()
    assert inst.flag is True


def test_rebuild_pydantic_model_ref_invalid_raises():
    with pytest.raises(ValueError, match=r"Invalid \$ref"):
        rebuild_pydantic_model({"$ref": "http://example.com/foo"})
    with pytest.raises(ValueError, match=r"Invalid \$ref"):
        rebuild_pydantic_model({"$ref": "not/a/ref"})


def test_rebuild_pydantic_model_ref_resolution():
    schema = {
        "$ref": "#/definitions/Foo",
        "definitions": {
            "Foo": {
                "title": "Foo",
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
            }
        },
    }
    Model = rebuild_pydantic_model(schema)
    assert Model.__name__ == "Foo"
    inst = Model(name="bar")
    assert inst.name == "bar"


def test_rebuild_pydantic_model_ref_deep_path():
    schema = {
        "$ref": "#/a/b/c",
        "a": {"b": {"c": {"title": "Deep", "type": "object", "properties": {"v": {"type": "integer"}}, "required": ["v"]}}},
    }
    Model = rebuild_pydantic_model(schema)
    assert Model.__name__ == "Deep"
    inst = Model(v=1)
    assert inst.v == 1


def test_rebuild_pydantic_model_custom_base():
    class MyBase(BaseModel):
        pass

    schema = {
        "title": "Sub",
        "type": "object",
        "properties": {"x": {"type": "integer"}},
        "required": ["x"],
    }
    Model = rebuild_pydantic_model(schema, base=MyBase)
    assert issubclass(Model, MyBase)
    assert issubclass(Model, BaseModel)
    inst = Model(x=1)
    assert inst.x == 1


def test_rebuild_pydantic_model_schema_not_mutated():
    """deepcopy ensures input schema is not modified (e.g. $ref popped)."""
    schema = {"$ref": "#/definitions/Foo", "definitions": {"Foo": {"title": "Foo", "type": "object", "properties": {}, "required": []}}}
    rebuild_pydantic_model(schema)
    assert "$ref" in schema
    assert schema["$ref"] == "#/definitions/Foo"


def test_rebuild_pydantic_model_full_schema():
    """Covers string, integer, boolean, array, nested object, required/optional, defaults."""
    schema = {
        "title": "MyModel",
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
            "is_active": {"type": "boolean", "default": True},
            "tags": {"type": "array", "items": {"type": "string"}},
            "address": {
                "type": "object",
                "title": "Address",
                "properties": {"street": {"type": "string"}, "city": {"type": "string"}},
                "required": ["street", "city"],
            },
        },
        "required": ["name", "age", "address"],
    }
    MyModel = rebuild_pydantic_model(schema)
    assert MyModel.__name__ == "MyModel"
    inst = MyModel(name="Alice", age=30, address={"street": "Main", "city": "NYC"}, tags=[])
    assert inst.name == "Alice"
    assert inst.age == 30
    assert inst.is_active is True
    assert inst.tags == []
    assert inst.address.street == "Main"
    assert inst.address.city == "NYC"
