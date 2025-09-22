import json
import logging
from collections import defaultdict
from pydantic import BaseModel as PydanticBaseModel
from pydantic.fields import Field as PydanticField

from .table import Table
from .utils.get_table_by_name import get_table_by_name


JOIN_SEPARATOR = "____"
logger = logging.getLogger(__name__)


class JoinInfo(PydanticBaseModel):
    model: type
    children: dict[str, "JoinInfo"] = PydanticField(default_factory=dict)

    def add_children(self, path: list[str]):
        name = path[0]
        field = self.model._get_field(name)
        if not field.is_reference:
            raise ValueError(f"Field `{name}` is not a reference (in path `{path}`)")
        if field.reference_type == Table:
            raise ValueError(f"Generic reference cannot be preloaded: {".".join(path)}")
        child = self.children[field.name] = JoinInfo(model=field.reference_type)
        if len(path) > 1:
            child.add_children(path[1:])

    def get_tables_statements(self, parent_alias: str=None):
        if not parent_alias:
            parent_alias = self.model._get_table_name()
            yield f"FROM {parent_alias}"
        for name, child in self.children.items():
            alias = f"{parent_alias}{JOIN_SEPARATOR}{name}"
            yield f"LEFT JOIN {child.model._get_table_name()} AS {alias} ON {alias}.id = {parent_alias}.{name}_id"
            yield from child.get_tables_statements(alias)
    
    def get_columns(self, parent_alias: str=None):
        if not parent_alias:
            parent_alias = self.model._get_table_name()
        for field in self.model._get_fields().values():
            if field.is_reference:
                # scalar reference
                if field.secondary_type is None:
                    if field.base_type == Table:
                        yield f"{parent_alias}{JOIN_SEPARATOR}{field.name}_table", f"{parent_alias}.{field.name}_table"
                    yield f"{parent_alias}{JOIN_SEPARATOR}{field.name}_id", f"{parent_alias}.{field.name}_id"
                # list of references
                elif issubclass(field.base_type, (list, tuple, set)):
                    if field.secondary_type == Table:
                        yield f"{parent_alias}{JOIN_SEPARATOR}{field.name}_tables", f"{parent_alias}.{field.name}_tables"
                    yield f"{parent_alias}{JOIN_SEPARATOR}{field.name}_ids", f"{parent_alias}.{field.name}_ids"
                # ?
                else:
                    raise ValueError()
            else:
                yield f"{parent_alias}{JOIN_SEPARATOR}{field.column_name}", f"{parent_alias}.{field.column_name}"
        # preload via join
        for name, child in self.children.items():
            field = self.model._get_field(name)
            if not field.is_reference:
                continue
            # is this a generic reference?
            alias = f"{parent_alias}{JOIN_SEPARATOR}{name}"
            if field.reference_type != Table:
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
            path = alias.split(JOIN_SEPARATOR)[1:]
            item = data
            for p in path[:-1]:
                item = item[p]
            item[path[-1]] = value
        return data
    
    def get_instance(self, row: tuple) -> Table:

        _lazy_joins = {}

        def _get_instance_recursive(data: dict, info: JoinInfo):
            for name, field in info.model._get_fields().items():

                # references...
                if field.is_reference:

                    # scalar reference
                    if field.secondary_type is None:
                        # extract useful data from columns
                        if field.base_type == Table:
                            reference_table = data.pop(f"{name}_table")
                            reference_type = get_table_by_name(reference_table)
                        else:
                            reference_type = field.base_type
                        reference_id = data.pop(f"{name}_id")
                        # format
                        if reference_id is None:
                            data[name] = None
                        elif name in info.children:
                            data[name] = reference_type.load(id=reference_id)
                        else:
                            data.pop(name, None)
                            _lazy_joins[name] = (reference_type, reference_id)

                    # list of references
                    elif issubclass(field.base_type, (list, tuple, set)):
                        # extract useful data from columns
                        references_ids = data.pop(f"{name}_ids")
                        if isinstance(references_ids, str):
                            references_ids = json.loads(references_ids)
                        if field.secondary_type == Table:
                            references_tables = data.pop(f"{name}_tables")
                            if isinstance(references_tables, str):
                                references_tables = json.loads(references_tables)
                            references_types = list(map(get_table_by_name, references_tables))
                        else:
                            references_types = len(references_ids) * [field.secondary_type]
                        # format data
                        if references_ids in (None, []):
                            data[name] = []
                        elif name in info.children:
                            data[name] = [reference_type.load(id=reference_id)
                                          for reference_type, reference_id
                                          in zip(references_types, references_ids)]
                        else:
                            data.pop(name, None)
                            _lazy_joins[name] = (references_types, references_ids)
                    # ?
                    else:
                        raise ValueError()

                # ...other things
                else:
                    data[name] = field.parse(data[name])


            info.model._ensure_lazy_loaders()
            info.model._suspend_validation()
            instance = info.model(**data)
            instance.__dict__.update(data)
            instance._lazy_joins = _lazy_joins
            info.model._resume_validation()
            return instance
        return _get_instance_recursive(self.get_data(row), self)
