"""Build Pydantic models from JSON Schema (e.g. for type fields)."""

from copy import deepcopy
from typing import Any, Dict, List, Optional, Type

from pydantic import BaseModel, create_model


def get_field_type(field_info: Dict[str, Any]) -> Any:
    """Map a JSON Schema field dict to a Python type or nested model."""
    field_type = field_info.get('type')

    if field_type == 'string':
        return str
    if field_type == 'integer':
        return int
    if field_type == 'number':
        return float
    if field_type == 'boolean':
        return bool
    if field_type == 'array':
        items = field_info.get('items', {})
        return List[get_field_type(items)]
    if field_type == 'object':
        nested_model_name = field_info.get('title', 'NestedModel')
        nested_properties = field_info.get('properties', {})
        nested_required = field_info.get('required', [])

        nested_fields = {}
        for name, info in nested_properties.items():
            nested_field_type = get_field_type(info)
            if name not in nested_required:
                nested_field_type = Optional[nested_field_type]
            nested_fields[name] = (nested_field_type, info.get('default', ...))

        return create_model(nested_model_name, **nested_fields)
    return str  # Default type

def rebuild_pydantic_model(schema: Dict[str, Any], base=BaseModel) -> Type[BaseModel]:
    """Create a Pydantic model class from a JSON Schema object (supports $ref)."""
    # resolve ref (in necessary)
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

    # initialize
    fields = {}
    model_name = schema.get("title", "DynamicModel")
    properties = schema.get('properties', {})
    required_fields = schema.get('required', [])

    for field_name, field_info in properties.items():
        field_type = get_field_type(field_info)
        if field_name not in required_fields:
            field_type = Optional[field_type]
        fields[field_name] = (field_type, field_info.get('default', ...))

    return create_model(model_name, **fields, __base__=base)
