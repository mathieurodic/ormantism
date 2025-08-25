import logging
from copy import copy
from pydantic import BaseModel, create_model, TypeAdapter
from types import GenericAlias
from .is_type_annotation import is_type_annotation


logger = logging.getLogger(__name__)


def to_json_schema(T: type) -> dict:
    wrapper = create_model("Wrapper", wrapped=T)
    wrapper_schema = wrapper.model_json_schema()
    schema = wrapper_schema["properties"]["wrapped"]
    # include definitions defined at root
    if "$defs" in wrapper_schema:
        schema["$defs"] = schema.get("$defs", {}) | wrapper_schema["$defs"]
    # original class name should be shema titles
    schema["title"] = T.__name__
    return schema

def from_json_schema(schema: dict, root_schema: dict=None) -> type:
    """Reconstruct a Python type from its JSON schema representation."""
    if not isinstance(schema, dict):
        raise TypeError("Invalid schema format")
    schema = copy(schema)
    if root_schema is None:
        root_schema = schema

    # resolve ref (in necessary)
    ref = schema.pop("$ref", None)
    if ref:
        if not ref.startswith("#/"):
            raise ValueError(f"Invalid $ref: {ref}")
        path = ref[2:].split("/")
        cursor = root_schema
        if path:
            for key in path:
                cursor = cursor[key]
        schema |= cursor

    schema_type = schema.get("type")
    title = schema.get("title")

    # Handle object types that might be SuperModel subclasses
    if schema_type == "object" and title:
        # Discover all SuperModel subclasses dynamically
        def find_subclass(cls, name):
            if cls.__name__ == name:
                return cls
            for sub in cls.__subclasses__():
                result = find_subclass(sub, name)
                if result:
                    return result
            return None
        
        model_cls = find_subclass(SuperModel, title)
        if model_cls:
            return model_cls
        else:
            raise TypeError(f"Cannot find subclass of SuperModel: {title}")

    # Handle basic scalar and container types
    type_map = {
        "string": str,
        "integer": int,
        "number": float,
        "boolean": bool,
        "array": list,
        "object": dict,
        "null": type(None)
    }

    if schema_type in type_map:
        if schema_type == "array":
            items = schema.get("items")
            if items:
                item_type = from_json_schema(items, root_schema)
                return list[item_type]
        elif schema_type == "object":
            # For generic dict without title, assume dict
            return dict
        return type_map[schema_type]

    raise TypeError(f"Unsupported schema: {schema}")


class SuperModel(BaseModel):

    # instanciation

    def __init__(self, /, **data: any) -> None:
        # for triggers
        init_data = copy(data)
        self.trigger("before_create", data)
        # process type attributes
        type_data = {}
        for key, value in data.items():
            field_info = self.__class__.model_fields.get(key)
            if is_type_annotation(value):
                if isinstance(value, dict):
                    value = from_json_schema(value)
                if isinstance(value, type):
                    data[key] = value
                elif isinstance(value, GenericAlias):
                    type_data[key] = value
                    data[key] = type(None)
                else:
                    raise ValueError(f"Not a type: {value}")
        BaseModel.__init__(self, **data)
        for key, value in type_data.items():
            object.__setattr__(self, key, value)
        # trigger
        self.trigger("after_create", init_data)
    
    # serialization
        
    def model_dump(self, *, mode: str = 'python', include=None, exclude=None, 
                   by_alias: bool = False, exclude_unset: bool = False, 
                   exclude_defaults: bool = False, exclude_none: bool = False,
                   round_trip: bool = False, warnings: bool = True) -> dict[str, any]:
        
        if exclude:
            exclude = copy(exclude)
        else:
            exclude = set()

        if include:
            include = copy(include)
        else:
            include = set(self.__class__.model_fields)


        result = {}
        if mode == "json":
            cls = self.__class__
            for key, field_info in cls.model_fields.items():
                if key not in include:
                    continue
                if key in exclude:
                    continue
                value = getattr(self, key)
                if is_type_annotation(value):
                    print("TYPE")
                    include.remove(key)
                    exclude.add(key)
                    adapter = TypeAdapter(field_info.annotation)
                    adapter.validate_python(value)
                    try:
                        result[key] = to_json_schema(value)
                    except Exception as e:
                        raise ValueError(f"Failed to serialize type field '{key}': {e}")
        
        result |= BaseModel.model_dump(self,
            mode=mode, include=include, exclude=exclude,
            by_alias=by_alias, exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults, exclude_none=exclude_none,
            round_trip=round_trip, warnings=warnings
        )

        return result

    # modification

    def __setattr__(self, name, value):
        if name.startswith("_"):
            return BaseModel.__setattr__(self, name, value)
        self.update(**{name: value})
        return getattr(self, name)

    def update(self, **new_data):
        cls = self.__class__
        # only consider really altered attributes
        old_data = {name: getattr(self, name)
               for name, value in new_data.items()}
        new_data = {name: value
                    for name, value in new_data.items()
                    if not hasattr(self, name)
                    or type(value) != type(getattr(self, name))
                    or value != getattr(self, name)}
        if not new_data:
            return
        # keep track of old data (for last trigger)
        old_data = {name: getattr(self, name, None)
                    for name in new_data.keys()}
        self.trigger("before_update", new_data=new_data)
        for name, value in new_data.items():
            field_info = cls.model_fields.get(name)
            if field_info and is_type_annotation(field_info.annotation):
                # TODO: better validation here
                self.__dict__[name] = value
            else:
                BaseModel.__setattr__(self, name, value)
        self.trigger("after_update", old_data=old_data)

    # triggers

    def trigger(self, event_name: str, *args, **kwargs):
        method_name = f"on_{event_name}"
        called_methods = [getattr(SuperModel, method_name)]
        for cls in type(self).__mro__:
            if cls == SuperModel:
                continue
            method = getattr(cls, method_name, None)
            if not method or method in called_methods:
                continue
            called_methods.append(method)
            logger.info("Calling trigger %s for %s: %s", event_name, self.__class__.__name__, method)
            if method:
                if method(self, *args, **kwargs) is False:
                    break

    def on_before_create(self, init_data: dict):
        pass

    def on_after_create(self, init_data: dict):
        pass
    
    def on_before_update(self, new_data: dict):
        pass

    def on_after_update(self, old_data: dict):
        pass


# Example usage and testing
if __name__ == "__main__":
    
    # Test basic usage
    class MyModel(SuperModel):
        field_type: type
    
    # Test with simple type
    model = MyModel(field_type=int)
    serialized = model.model_dump(mode="json")
    print("Serialized:", serialized)
    
    reconstructed = MyModel.model_validate(serialized)
    print("Reconstructed type:", reconstructed.field_type)
    print("Types match:", reconstructed.field_type == int)
    
    # Test with complex type
    model2 = MyModel(field_type=list[str])
    serialized2 = model2.model_dump(mode="json")
    print("Complex serialized:", serialized2)
    
    reconstructed2 = MyModel.model_validate(serialized2)
    print("Complex reconstructed type:", reconstructed2.field_type)
    
    # Test with SuperModel subclass
    class User(SuperModel):
        name: str
        age: int
    
    class Container(SuperModel):
        content_type: type
    
    container = Container(content_type=User)
    serialized3 = container.model_dump(mode="json")
    print("SuperModel serialized:", serialized3)
    
    reconstructed3 = Container.model_validate(serialized3)
    print("SuperModel reconstructed type:", reconstructed3.content_type)
    print("SuperModel types match:", reconstructed3.content_type == User)
