"""Base model with lifecycle triggers, type-field support, and JSON-safe model_dump."""

import logging
import types
from copy import copy
from types import GenericAlias

from pydantic import BaseModel

from .is_type_annotation import is_type_annotation
from .get_base_type import get_base_type
from .schema import to_json_schema, from_json_schema

# Re-export for backward compatibility
__all__ = ["SuperModel", "to_json_schema", "from_json_schema"]

logger = logging.getLogger(__name__)


class SuperModel(BaseModel):
    """Pydantic BaseModel with before/after create/update triggers and type-field serialization."""

    def __init_subclass__(cls, **kwargs):
        """Replace bare `type` annotations with type | GenericAlias for validation."""
        # Transform type annotations before the class is fully created
        if hasattr(cls, '__annotations__'):
            new_annotations = {}
            for field_name, annotation in cls.__annotations__.items():
                # If the annotation is exactly 'type', replace it with Union
                if annotation is type:
                    new_annotations[field_name] = type | types.GenericAlias
                else:
                    new_annotations[field_name] = annotation
            cls.__annotations__ = new_annotations

        # Call parent's __init_subclass__
        super().__init_subclass__(**kwargs)

    # instanciation

    def __init__(self, /, **data: any) -> None:
        """Initialize from keyword data; run before_create/after_create triggers and validate."""
        # for triggers
        init_data = copy(data)
        if not self.trigger("before_create", data):
            return
        # process type attributes separately
        type_data = {}
        for name, value in data.items():
            field_info = self.__class__.model_fields.get(name)
            if field_info is None:
                raise NameError(
                    f"{self.__class__.__name__} has no field for name: {name}"
                )
            base_type, _secondary_type, is_required = get_base_type(
                field_info.annotation
            )
            if base_type == type:
                if isinstance(value, dict):
                    value = from_json_schema(value)
                if isinstance(value, type):
                    data[name] = value
                elif isinstance(value, (GenericAlias, types.UnionType)):
                    type_data[name] = value
                    data[name] = type(None)
                elif value is None and not is_required:
                    pass
                else:
                    raise ValueError(f"Not a type: {value} ({type(value)})")
        # validate non-types with BaseModel
        BaseModel.__init__(self, **data)
        # set type attributes without BaseModel check
        for name, value in type_data.items():
            object.__setattr__(self, name, value)
        # trigger
        self.trigger("after_create", init_data)

    # serialization

    def model_dump(self, *, mode: str = 'python', include=None, exclude=None,
                   by_alias: bool = False, exclude_unset: bool = False,
                   exclude_defaults: bool = False, exclude_none: bool = False,
                   round_trip: bool = False, warnings: bool = True) -> dict[str, any]:
        """Dump to dict; in \"json\" mode, type fields are serialized as JSON Schema."""
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
            for key, _ in cls.model_fields.items():
                if key not in include:
                    continue
                if key in exclude:
                    continue
                value = getattr(self, key)
                if is_type_annotation(value):
                    include.remove(key)
                    exclude.add(key)
                    # adapter = TypeAdapter(field_info.annotation)
                    # adapter.validate_python(value)
                    try:
                        result[key] = to_json_schema(value)
                    except Exception as e:
                        raise ValueError(
                            f"Failed to serialize type field '{key}': {e}"
                        ) from e

        result |= BaseModel.model_dump(
            self,
            mode=mode,
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            round_trip=round_trip,
            warnings=warnings,
        )

        return result

    # modification

    def __setattr__(self, name, value):
        """Non-underscore attributes are delegated to update() for trigger support."""
        if name.startswith("_"):
            return BaseModel.__setattr__(self, name, value)
        self.update(**{name: value})
        return getattr(self, name)

    def update(self, **new_data):
        """Apply changes; run before_update/after_update triggers, persist via subclass."""
        cls = self.__class__
        # only consider really altered attributes
        new_data = {
            name: value
            for name, value in new_data.items()
            if not hasattr(self, name)
            or type(value) != type(getattr(self, name))
            or value != getattr(self, name)
        }
        if not new_data:
            return
        # keep track of old data (for last trigger)
        old_data = {name: getattr(self, name, None)
                    for name in new_data.keys()}
        self.trigger("before_update", new_data=new_data)
        for name, value in new_data.items():
            field_info = cls.model_fields.get(name)
            if field_info is not None and is_type_annotation(field_info.annotation):
                # TODO: better validation here
                self.__dict__[name] = value
            else:
                BaseModel.__setattr__(self, name, value)
        self.trigger("after_update", old_data=old_data)

    # triggers

    def trigger(self, event_name: str, *args, **kwargs):
        """Call on_<event_name> on self and subclasses in MRO order; return False to abort."""
        method_name = f"on_{event_name}"
        called_methods = [getattr(SuperModel, method_name)]
        for cls in type(self).__mro__:
            if cls == SuperModel:
                continue
            method = getattr(cls, method_name, None)
            if not method or method in called_methods:
                continue
            called_methods.append(method)
            logger.debug(
                "Calling trigger %s for %s: %s",
                event_name,
                self.__class__.__name__,
                method,
            )
            if method:
                if method(self, *args, **kwargs) is False:
                    return False
        return True

    def on_before_create(self, init_data: dict):
        """Override to run logic before a new instance is created; return False to abort."""

    def on_after_create(self, init_data: dict):
        """Override to run logic after a new instance is created."""

    def on_before_update(self, new_data: dict):
        """Override to run logic before an instance is updated; return False to abort."""

    def on_after_update(self, old_data: dict):
        """Override to run logic after an instance is updated."""
