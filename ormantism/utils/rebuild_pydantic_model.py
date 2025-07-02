from pydantic import BaseModel, create_model
from typing import Dict, Type, Any, Optional, List

def get_field_type(field_info: Dict[str, Any]) -> Any:
    field_type = field_info.get('type')

    if field_type == 'string':
        return str
    elif field_type == 'integer':
        return int
    elif field_type == 'number':
        return float
    elif field_type == 'boolean':
        return bool
    elif field_type == 'array':
        items = field_info.get('items', {})
        return List[get_field_type(items)]
    elif field_type == 'object':
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
    else:
        return str  # Default type

def rebuild_pydantic_model(schema: Dict[str, Any]) -> Type[BaseModel]:
    fields = {}
    model_name = schema.get("title", "DynamicModel")
    properties = schema.get('properties', {})
    required_fields = schema.get('required', [])

    for field_name, field_info in properties.items():
        field_type = get_field_type(field_info)
        if field_name not in required_fields:
            field_type = Optional[field_type]
        fields[field_name] = (field_type, field_info.get('default', ...))

    return create_model(model_name, **fields)


if __name__ == "__main__":
    # Example usage
    schema = {
        "title": "MyModel",
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
            "is_active": {"type": "boolean", "default": True},
            "tags": {
                "type": "array",
                "items": {"type": "string"}
            },
            "address": {
                "type": "object",
                "title": "Address",
                "properties": {
                    "street": {"type": "string"},
                    "city": {"type": "string"}
                },
                "required": ["street", "city"]
            }
        },
        "required": ["name", "age", "address"]
    }

    MyModel = rebuild_pydantic_model(schema)

    from pprint import pprint
    pprint(MyModel)
    pprint(MyModel.model_fields)
    pprint(MyModel.model_fields["address"].annotation.model_fields)
