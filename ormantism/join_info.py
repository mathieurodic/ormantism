from collections import defaultdict
from pydantic import BaseModel as PydanticBaseModel
from pydantic.fields import Field as PydanticField

from .base import Base


class JoinInfo(PydanticBaseModel):
    model: type[Base]
    children: dict[str, "JoinInfo"] = PydanticField(default_factory=dict)

    def add_children(self, path: list[str]):
        name = path[0]
        field = self.model._get_field(name)
        if not field.is_reference:
            raise ValueError(f"Field `{name}` is not a reference (in path `{path}`)")
        child = self.children[field.name] = JoinInfo(model=field.base_type)
        if len(path) > 1:
            child.add_children(path[1:])

    def get_tables_statements(self, parent_alias: str=None):
        if not parent_alias:
            parent_alias = self.model._get_table_name()
            yield f"FROM {parent_alias}"
        for name, child in self.children.items():
            alias = f"{parent_alias}__{name}"
            yield f"LEFT JOIN {child.model._get_table_name()} AS {alias} ON {alias}.id = {parent_alias}.{name}_id"
            yield from child.get_tables_statements(alias)
    
    def get_columns(self, parent_alias: str=None):
        if not parent_alias:
            parent_alias = self.model._get_table_name()
        for field in self.model._get_fields().values():
            yield f"{parent_alias}__{field.column_name}", f"{parent_alias}.{field.column_name}"
        for name, child in self.children.items():
            alias = f"{parent_alias}__{name}"
            yield from child.get_columns(alias)
    
    def get_columns_statements(self):
        for key, value in self.get_columns():
            yield f"{value} AS {key}"

    def get_data(self, row: tuple):
        # fill with data
        def infinite_defaultdict():
            return defaultdict(infinite_defaultdict)
        data = infinite_defaultdict()
        for (alias, _), value in zip(self.get_columns(), row):
            path = alias.split("__")[1:]
            item = data
            for p in path[:-1]:
                item = item[p]
            item[path[-1]] = value
        return data
    
    def get_instance(self, row: tuple) -> Base:
        _lazy_identifiers = {}
        def _get_instance_recursive(data: dict, info: JoinInfo):
            for name, field in info.model._get_fields().items():
                if field.is_reference:
                    reference_id = data.pop(name + "_id")
                    if reference_id is None:
                        data[name] = None
                    elif name in info.children:
                        data[name] = _get_instance_recursive(data[name], info.children[name])
                    else:
                        data.pop(name, None)
                        _lazy_identifiers[name] = reference_id
                else:
                    data[name] = field.parse(data[name])
            info.model._ensure_lazy_loaders()
            # print(f"{info.model.links_to}")
            info.model._suspend_validation()
            instance = info.model(**data)
            instance.__dict__.update(data)
            instance._lazy_identifiers = _lazy_identifiers
            info.model._resume_validation()
            return instance
        return _get_instance_recursive(self.get_data(row), self)
