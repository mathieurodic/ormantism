"""Schema and serialization utilities: JSON Schema, Pydantic model building, recursive serialize."""

import logging
import typing
from copy import copy, deepcopy
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Type

from pydantic import BaseModel, create_model

from .get_base_type import get_base_type
from .find_subclass import find_subclass

logger = logging.getLogger(__name__)


def to_json_schema(T: type) -> dict:
    """Return a JSON Schema dict for the given type (e.g. for type fields)."""
    wrapper = create_model("Wrapper", wrapped=T)
    wrapper_schema = wrapper.model_json_schema()
    schema = wrapper_schema["properties"]["wrapped"]
    # include definitions defined at root
    if "$defs" in wrapper_schema:
        schema["$defs"] = schema.get("$defs", {}) | wrapper_schema["$defs"]
    # original class name should be shema titles
    name = getattr(T, "__name__", None)
    if name is not None:
        schema["title"] = name
    return schema


def from_json_schema(schema: dict, root_schema: dict = None) -> type:
    """Reconstruct a Python type from its JSON schema representation."""
    if not isinstance(schema, dict):
        raise TypeError("Invalid schema format")
    schema = copy(schema)
    if root_schema is None:
        root_schema = schema

    # resolve ref (if necessary)
    ref = schema.pop("$ref", None)
    if ref:
        if not ref.startswith("#/"):
            raise ValueError(f"Invalid $ref: {ref}")
        path = ref[2:]
        cursor = root_schema
        if path:
            for key in path.split("/"):
                cursor = cursor[key]
        schema |= cursor

    # Is it a union?
    if "anyOf" in schema:
        annotations = [from_json_schema(subschema) for subschema in schema["anyOf"]]
        return typing.Union[*annotations]

    schema_type = schema.get("type")
    title = schema.get("title")

    # Handle object types that might be SuperModel subclasses
    if schema_type == "object" and title:
        from .supermodel import SuperModel
        model_cls = find_subclass(SuperModel, title)
        if model_cls:
            return model_cls
        logger.warning("Cannot find subclass of SuperModel: %s", title)
        return rebuild_pydantic_model(schema=schema, base=SuperModel)

    # Handle basic scalar and container types
    type_map = {
        "string": str,
        "integer": int,
        "number": float,
        "boolean": bool,
        "array": list,
        "object": dict,
        "null": type(None),
    }

    if schema_type in type_map:
        if schema_type == "array":
            items = schema.get("items")
            if items:
                item_type = from_json_schema(items, root_schema)
                return list[item_type]
        elif schema_type == "object":
            return dict
        return type_map[schema_type]

    raise TypeError(f"Unsupported schema: {schema}")


def get_field_type(field_info: Dict[str, Any]) -> Any:
    """Map a JSON Schema field dict to a Python type or nested model."""
    field_type = field_info.get("type")

    if field_type == "string":
        return str
    if field_type == "integer":
        return int
    if field_type == "number":
        return float
    if field_type == "boolean":
        return bool
    if field_type == "array":
        items = field_info.get("items", {})
        return List[get_field_type(items)]
    if field_type == "object":
        nested_model_name = field_info.get("title", "NestedModel")
        nested_properties = field_info.get("properties", {})
        nested_required = field_info.get("required", [])

        nested_fields = {}
        for name, info in nested_properties.items():
            nested_field_type = get_field_type(info)
            if name not in nested_required:
                nested_field_type = Optional[nested_field_type]
            nested_fields[name] = (nested_field_type, info.get("default", ...))

        return create_model(nested_model_name, **nested_fields)
    return str  # Default type


def rebuild_pydantic_model(schema: Dict[str, Any], base=BaseModel) -> Type[BaseModel]:
    """Create a Pydantic model class from a JSON Schema object (supports $ref)."""
    schema = deepcopy(schema)
    ref = schema.pop("$ref", None)
    if ref:
        if not ref.startswith("#/"):
            raise ValueError(f"Invalid $ref: {ref}")
        path = ref[2:].split("/")
        cursor = schema
        if path:
            for key in path:
                cursor = cursor[key]
        schema |= cursor

    fields = {}
    model_name = schema.get("title", "DynamicModel")
    properties = schema.get("properties", {})
    required_fields = schema.get("required", [])

    for field_name, field_info in properties.items():
        field_type = get_field_type(field_info)
        if field_name not in required_fields:
            field_type = Optional[field_type]
        fields[field_name] = (field_type, field_info.get("default", ...))

    return create_model(model_name, **fields, __base__=base)


def serialize(data: any) -> dict | list | int | float | str | bool | None:
    """
    Convert any list or dict of scalars or pydantic.BaseModel instances (even nested)
    to a JSON serializable format using only dict, list, int, float, str, and bool.
    """
    if isinstance(data, BaseModel):
        return serialize(data.model_dump(mode="json"))
    if isinstance(data, dict):
        return {key: serialize(value) for key, value in data.items()}
    if isinstance(data, (list, tuple, set)):
        return [serialize(item) for item in data]
    if isinstance(data, Enum):
        return data.name
    if isinstance(data, (int, float, str, bool)) or data is None:
        return data
    if isinstance(data, datetime):
        return str(data)
    raise ValueError(data)
