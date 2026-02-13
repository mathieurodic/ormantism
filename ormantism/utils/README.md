# Utils

Utility helpers for type resolution, serialization, schema, and model discovery. Used by `column`, `table`, and `query` modules.

---

## Modules

| available as | description |
|--------------|-------------|
| ormantism.utils.find_subclass | Find a subclass by name in a class hierarchy |
| ormantism.utils.get_base_type | Resolve base and container types from annotations (unions, generics) |
| ormantism.utils.get_table_by_name | Look up a Table subclass by name (e.g. from stored polymorphic reference) |
| ormantism.utils.is_table | Check if a type is Table-like (has _get_table_name and model_fields) |
| ormantism.utils.is_type_annotation | Detect whether a value is a type annotation |
| ormantism.utils.make_hashable | Convert objects (including Pydantic models) to hashable form for sets/caches |
| ormantism.utils.schema | JSON Schema (to/from), rebuild_pydantic_model, serialize |
| ormantism.utils.supermodel | Base model with lifecycle triggers and type-field serialization |

---

## Schema (ormantism.utils.schema)

Consolidated schema and serialization utilities:

- **to_json_schema(T)** — Return a JSON Schema dict for the given type
- **from_json_schema(schema)** — Reconstruct a Python type from JSON schema
- **rebuild_pydantic_model(schema, base)** — Build a Pydantic model dynamically from JSON schema
- **get_field_type(field_info)** — Map a JSON Schema field dict to a Python type
- **serialize(data)** — Recursively serialize nested structures to JSON-serializable types

```python
from ormantism.utils.schema import to_json_schema, from_json_schema, serialize

schema = to_json_schema(int)  # {"type": "integer", "title": "int"}
val = serialize({"x": 1, "nested": {"y": 2}})  # recursive dict serialization
```

---

## SuperModel

`SuperModel` is the base for all Table mixins (`_WithPrimaryKey`, `_WithTimestamps`, etc.). It provides:

- Lifecycle hooks: `on_before_create`, `on_after_create`, `on_before_update`, `on_after_update`
- Type-field support: fields annotated with `type[...]` are serialized as JSON Schema
- JSON-safe `model_dump(mode="json")`

**Note:** Extracting SuperModel into an independent package (e.g. `pydantic-supermodel`) is a future option for reuse outside the ORM.
