"""Hydratable mixin: instance-building from raw row data."""

from collections import defaultdict
from typing import Any

from ..expressions import ALIAS_SEPARATOR


class Hydratable:
    """Mixin that provides instance-building from raw row data (hydration)."""

    @classmethod
    def make_empty_instance(cls, id: int) -> "Table":
        """Generate an empty instance of the table."""
        cls._suspend_validation()
        instance = cls()
        instance.__dict__["id"] = id
        cls._resume_validation()
        return instance

    @staticmethod
    def rearrange_data_for_hydration(unparsed_data: list[dict[str, Any]]) -> dict[str, Any]:
        """Rearrange unparsed row data into a nested structure for hydration."""
        deep_defaultdict = lambda: defaultdict(deep_defaultdict)
        rearranged_data = deep_defaultdict()
        for unparsed_row in unparsed_data:
            keys = set(unparsed_row.keys())

            def should_strip(k: str) -> bool:
                joined_id_key = f"{k}{ALIAS_SEPARATOR}id"
                if joined_id_key not in keys:
                    return False
                return unparsed_row.get(joined_id_key) is not None

            unparsed_row = {
                k: v for k, v in unparsed_row.items()
                if not (ALIAS_SEPARATOR not in k and should_strip(k))
            }
            data_per_table = deep_defaultdict()
            for alias, value in unparsed_row.items():
                if ALIAS_SEPARATOR in alias:
                    table_path, column_name = alias.rsplit(ALIAS_SEPARATOR, 1)
                else:
                    table_path, column_name = "", alias
                data_per_table[table_path][column_name] = value

            root_pk = data_per_table.get("", {}).get("id")
            if root_pk is None:
                continue

            for table_path in sorted(data_per_table.keys()):
                values = data_per_table[table_path]
                pk = values.get("id")
                if pk is None:
                    continue

                if table_path == "":
                    data = rearranged_data
                    target_pk = root_pk
                else:
                    data = rearranged_data[root_pk]
                    path_parts = table_path.split(ALIAS_SEPARATOR)
                    skip_path = False
                    for i, part in enumerate(path_parts):
                        data = data[part]
                        if i < len(path_parts) - 1:
                            parent_path = ALIAS_SEPARATOR.join(path_parts[: i + 1])
                            parent_pk = data_per_table.get(parent_path, {}).get("id")
                            if parent_pk is None:
                                skip_path = True
                                break
                            data = data[parent_pk]
                    if skip_path:
                        continue
                    target_pk = pk

                if target_pk not in data:
                    data[target_pk] = deep_defaultdict()
                for key, value in values.items():
                    data[target_pk][key] = value
        return rearranged_data

    def integrate_data_for_hydration(self, rearranged_data: dict[str, Any]) -> None:
        """Integrate rearranged data into this instance, mutating it in place."""
        if not rearranged_data:
            return
        assert len(rearranged_data) == 1
        root_pk = list(rearranged_data.keys())[0]
        values = rearranged_data[root_pk]
        table = self.__class__
        for key, value in values.items():
            column = table._get_column(key)
            if column.is_reference:
                if column.secondary_type is not None and column.base_type not in (list, tuple, set):
                    raise ValueError("Unexpected reference type in integrate_data_for_hydration")
                if not isinstance(value, (dict, defaultdict)):
                    value = column.parse(value)
                else:
                    if column.is_collection:
                        nested_table = column.reference_type
                        nested_instances = []
                        for nested_key, nested_value in value.items():
                            nested_instance = nested_table.make_empty_instance(nested_key)
                            nested_instance.integrate_data_for_hydration({nested_key: nested_value})
                            nested_instances.append(nested_instance)
                        value = nested_instances
                    else:
                        nested_table = column.reference_type
                        nested_key, nested_value = next(iter(value.items()))
                        nested_instance = nested_table.make_empty_instance(nested_key)
                        nested_instance.integrate_data_for_hydration(value)
                        value = nested_instance
            else:
                value = column.parse(value)
            self.__dict__[key] = value
        for key, column in table._get_columns().items():
            if key not in values and column.is_reference and column.secondary_type is not None:
                self.__dict__[key] = []

    def hydrate_with(self, unparsed_data: list[dict[str, Any]]) -> None:
        """Hydrate a Table instance from a row dict mapping column alias to unparsed value."""
        rearranged_data = self.rearrange_data_for_hydration(unparsed_data)
        self.integrate_data_for_hydration(rearranged_data)

    def make_instance(self, unparsed_data: list[dict[str, Any]]) -> "Table":
        """Make a new instance of the table from unparsed data."""
        instance = self.make_empty_instance(id=None)
        instance.hydrate_with(unparsed_data)
        return instance

    @classmethod
    def _suspend_validation(cls):
        """Replace __init__/__setattr__ so instances can be built without validation."""
        def __init__(self, *_args, **kwargs):
            self.__dict__.update(**kwargs)
            self.__pydantic_fields_set__ = set(cls.model_fields)
            # Pydantic internals used by utilities like `model_copy()`.
            # When we bypass BaseModel.__init__, we must still create these.
            self.__pydantic_extra__ = None
            self.__pydantic_private__ = None
        def __setattr__(self, name, value):
            self.__dict__[name] = value
            return value
        __init__.__pydantic_base_init__ = True
        cls.__setattr_backup__ = cls.__setattr__
        cls.__setattr__ = __setattr__
        cls.__init_backup__ = cls.__init__
        cls.__init__ = __init__

    @classmethod
    def _resume_validation(cls):
        """Restore normal __init__/__setattr__ after _suspend_validation."""
        if hasattr(cls, "__init_backup__"):
            cls.__init__ = cls.__init_backup__
            cls.__setattr__ = cls.__setattr_backup__
            delattr(cls, "__init_backup__")
            delattr(cls, "__setattr_backup__")
