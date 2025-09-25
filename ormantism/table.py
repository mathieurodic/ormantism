from copy import deepcopy
from typing import ClassVar
import json
import datetime
import logging
from functools import cache

from pydantic import BaseModel
from pydantic._internal._model_construction import ModelMetaclass

from .utils.supermodel import SuperModel
from .utils.make_hashable import make_hashable
from .transaction import transaction
from .field import Field, JSON


logger = logging.getLogger("ormantism")


class _WithPrimaryKey(SuperModel):
    id: int = None

class _WithSoftDelete(SuperModel):
    deleted_at: datetime.datetime|None = None

class _WithCreatedAtTimestamp(SuperModel):
    created_at: datetime.datetime = None

class _WithTimestamps(_WithCreatedAtTimestamp, _WithSoftDelete):
    updated_at: datetime.datetime|None = None

class _WithVersion(_WithSoftDelete):
    version: int = 0


class TableMeta(ModelMetaclass):

    def __new__(mcs, name, bases, namespace,
                with_primary_key: bool=True,
                with_created_at_timestamp: bool=False,
                with_timestamps: bool=False,
                versioning_along: tuple[str]=None,
                connection_name: str=None,
                **kwargs):
        # inherited behaviors
        default_bases: tuple[type[SuperModel]] = tuple()
        if with_primary_key:
            default_bases += (_WithPrimaryKey,)
        if with_created_at_timestamp:
            default_bases += (_WithCreatedAtTimestamp,)
        if with_timestamps:
            default_bases += (_WithTimestamps,)
        if versioning_along:
            default_bases += (_WithVersion,)
        # start building result
        result = super().__new__(mcs, name, bases + default_bases, namespace, **kwargs)
        # connection name
        if not connection_name:
            for base in bases:
                if base._CONNECTION_NAME:
                    connection_name = base._CONNECTION_NAME
        result._CONNECTION_NAME = connection_name
        # versioning
        if versioning_along is None:
            for base in bases:
                if getattr(base, "_VERSIONING_ALONG", None):
                    versioning_along = base._VERSIONING_ALONG
        result._VERSIONING_ALONG = versioning_along
        # read-only
        result._READ_ONLY_FIELDS = sum((tuple(base.model_fields.keys())
                                        for base in default_bases), start=())
        # here we go :)
        return result


# class Table(SuperModel, metaclass=BaseMeta):
class Table(metaclass=TableMeta):

    model_config = dict(
        arbitrary_types_allowed = True,
    )
    _CHECKED_TABLE_EXISTENCE: ClassVar[bool] = False

    def __eq__(self, other: "Table"):
        if not isinstance(other, self.__class__):
            raise ValueError(f"Comparing instances of different classes: {self.__class__} and {other.__class__}")
        return hash(self) == hash(other)

    def __hash__(self):
        return hash(make_hashable(self))

    def __deepcopy__(self, memo):
        return self

    # INSERT
    def on_after_create(self, init_data: dict):
        # if primary key already set: skip entirely
        if self.id is not None and self.id >= 0:
            return
        self.check_read_only(init_data)
        # special column for versioning
        if isinstance(self, _WithVersion):
            sql = f"UPDATE {self._get_table_name()} SET deleted_at = CURRENT_TIMESTAMP WHERE deleted_at IS NULL"
            values = []
            for name, value in init_data.items():
                if name not in self._VERSIONING_ALONG:
                    continue
                if value is None:
                    sql += f" AND {name} IS NULL"
                else:
                    sql += f" AND {name} = ?"
                    values.append(value)
            sql += " RETURNING version"
            rows = self._execute(sql, values)
            init_data["version"] = (max(version for version, in rows) + 1) if rows else 0
        # format data
        exclude = set(self.__class__.model_fields)
        include = set()
        processed_data = self.process_data(init_data)
        formatted_data = {}
        # return
        for name, value in processed_data.items():
            # object.__setattr__(self, name, value)
            include.add(name)
            if name in exclude:
                exclude.remove(name)
            else:
                formatted_data[name] = value
        formatted_data |= self.model_dump(include=include,
                                          exclude=exclude,
                                          mode="json")
        # perform insertion
        if formatted_data:
            sql = (f"INSERT INTO {self._get_table_name()} ({", ".join(formatted_data.keys())})\n"
                f"VALUES ({", ".join("?" for v in formatted_data.values())})")
        else:
            sql = f"INSERT INTO {self._get_table_name()} DEFAULT VALUES"
        # retrieve automatic columns from inserted row
        self._execute_returning(sql=sql,
                                parameters=list(formatted_data.values()),
                                for_insertion=True)
        # trigger
        if hasattr(self, "__post_init__"):
            self.__post_init__()

    # UPDATE

    def on_before_update(self, new_data):
        """Apply changes to database"""
        # ensure we're not trying to write read-only fields
        self.check_read_only(new_data)
        # fill SET statement
        set_statement = self.process_data(new_data)
        # fill WHERE statement
        where_statement = {}
        if isinstance(self, _WithPrimaryKey):
            where_statement = {"id": self.id}
        else:
            raise NotImplementedError()

        # compute parameters
        parameters = tuple(self._get_field(name).serialize(value) if self._has_field(name) else value
                           for statement in (set_statement, where_statement)
                           for name, value in statement.items())

        # execute query
        self._execute_returning(sql=f"UPDATE {self._get_table_name()}\n"
                                    f"SET {", ".join(f"{k} = ?" for k in set_statement)}\n"
                                    f"WHERE {", ".join(f"{k} = ?" for k in where_statement)}",
                                parameters=parameters)

    # INSERT or SELECT / UPDATE
    @classmethod
    def load_or_create(cls, _search_fields=None, **data):
        # if restriction applies
        if _search_fields is None:
            searched_data = data
        else:
            searched_data = {key: data[key] for key in _search_fields if key in data}
        # return corresponding row if already exists
        loaded = cls.load(**searched_data)
        if loaded:
            logger.warning(data)
            changed_data = {key: value
                            for key, value in data.items()
                            if getattr(loaded, key) != value}
            changed_data = {}
            for name, value in data.items():
                field = cls._get_field(name)
                if field.is_reference:
                    if name not in loaded._lazy_joins:
                        if value is not None:
                            changed_data[name] = value.id
                            raise Exception()
                    elif value is None:
                        changed_data[name] = None
                    else:
                        foreign_key = loaded._lazy_joins[name]
                        if isinstance(foreign_key, int):
                            if foreign_key != value.id:
                                changed_data[name] = value.id
                        elif isinstance(foreign_key, tuple) and len(foreign_key) == 2:
                            if foreign_key[0] != value.__class__ or foreign_key[1] != value.id:
                                changed_data[name] = value
                        else:
                            ValueError("?!")

                elif getattr(loaded, name) != value:
                    if cls._get_field(name):
                        changed_data[name] = value
            logger.warning(changed_data)
            loaded.update(**changed_data)
            return loaded
        # build new item if not found
        return cls(**data)

    ##

    @classmethod
    @cache
    def _has_field(cls, name: str) -> bool:
        return name in cls.model_fields

    @classmethod
    @cache
    def _get_fields(cls) -> dict[str, Field]:
        return {
            name: Field.from_pydantic_info(cls, name, info)
            for name, info in cls.model_fields.items()
        }
    
    @classmethod
    @cache
    def _get_field(cls, name: str):
        fields = cls._get_fields()
        if name in fields:
            return fields[name]
        for field in fields.values():
            if field.column_name == name:
                return field
        raise KeyError(f"No such field for {cls.__name__}: {name}")

    @classmethod
    @cache
    def _get_non_default_fields(cls):
        return {
            name: field
            for name, field in cls._get_fields().items()
            if name not in cls._READ_ONLY_FIELDS
        }

    # execute SQL
    
    @classmethod
    def _execute(cls, sql: str, parameters: list=[], check=True) -> list[tuple]:
        if check and cls != Table and not cls._CHECKED_TABLE_EXISTENCE:
            cls._create_table()
            cls._add_columns()
            cls._CHECKED_TABLE_EXISTENCE = True
        with transaction(connection_name=cls._CONNECTION_NAME) as t:
            cursor = t.execute(sql, parameters)
            result = cursor.fetchall()
            cursor.close()
        return result
    
    def _execute_returning(self, sql: str, parameters: list=[], for_insertion=False):
        returned_fields = set(self._READ_ONLY_FIELDS)
        if for_insertion:
            for name, field in self._get_fields().items():
                if not field.is_reference and field.default is not None:
                    returned_fields.add(name)
        if self._READ_ONLY_FIELDS:
            sql += "\nRETURNING " + ", ".join(returned_fields)
        rows = self._execute(sql, list(parameters))

        # parse returned value & set them
        for name, value in zip(returned_fields, rows[0]):
            field = self._get_field(name)
            parsed_value = field.parse(value)
            object.__setattr__(self, name, parsed_value)
        
    # CREATE TABLE

    @classmethod
    def _create_table(cls, created: set[type["Table"]]=set()):
        # create tables for references first
        created.add(cls)
        for field in cls._get_fields().values():
            if field.is_reference:
                for t in (field.base_type, field.secondary_type):
                    if issubclass(t, Table) and t != Table and t not in created:
                        t._create_table(created)
        # initialize statements for table creation
        statements = []
        # id & created_at are special
        if issubclass(cls, _WithPrimaryKey):
            statements += ["id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT"]
        if issubclass(cls, _WithCreatedAtTimestamp):
            statements += ["created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"]
        # other columns are easy
        statements += sum((
            list(field.sql_creations)
            for field in cls._get_fields().values()
            if field.name not in ("created_at", "id")
        ), start=[])
        # foreign keys
        statements += [
            f"FOREIGN KEY ({name}_id) REFERENCES {field.base_type._get_table_name()}(id)"
            for name, field in cls._get_fields().items()
            if field.is_reference and field.secondary_type is None
            and field.base_type != Table
        ]
        # build & execute SQL
        sql = f"CREATE TABLE IF NOT EXISTS {cls._get_table_name()} (\n  {",\n  ".join(statements)})"
        cls._execute(sql, check=False)

    @classmethod
    def _add_columns(cls):
        rows = cls._execute(f"SELECT name FROM pragma_table_info('{cls._get_table_name()}')", check=False)
        columns_names = {name for name, in rows}
        new_fields = [field for field in cls._get_fields().values()
                      if field.column_name not in columns_names]
        for field in new_fields:
            for sql_creation in field.sql_creations:
                cls._execute(f"ALTER TABLE {cls._get_table_name()} ADD COLUMN {sql_creation}", check=False)
                if field.is_reference:
                    raise Exception("cannot add foreign key constraint on existing table")

    def check_read_only(self, data):
        """Check we are not attempting to alter read-only fields"""
        read_only_fields = list(set(data) & set(self._READ_ONLY_FIELDS))
        read_only_fields_count = len(read_only_fields)
        if read_only_fields_count:
            plural = "s" if read_only_fields_count > 1 else ""
            raise AttributeError(f"Cannot set read-only attribute{plural} of {self.__class__.__name__}: {", ".join(read_only_fields)}")

    @classmethod
    def process_data(cls, data: dict, for_filtering: bool=False) -> dict:
        data = deepcopy(data)
        for name in list(data):
            value = data.pop(name)
            # is there no field for this name?
            try:
                field = cls._get_field(name)
            except KeyError:
                raise ValueError(f"Invalid key found in data for {cls.__name__}: {name}")
            # so, there is.
            if field.is_reference:
                # scalar reference
                if field.secondary_type is None:
                    if field.base_type == Table:
                        data[f"{field.name}_table"] = value._get_table_name() if value else None
                    data[f"{field.name}_id"] = (value if isinstance(value, int) else value.id) if value else None
                # list of references
                elif isinstance(value, (list, tuple, set)) and issubclass(field.base_type, (list, tuple, set)):
                    if field.secondary_type == Table:
                        data[f"{field.name}_tables"] = [referred._get_table_name() for referred in value]
                    data[f"{field.name}_ids"] = [referred.id for referred in value]
                # ?
                else:
                    raise NotImplementedError(field.name, value, field.base_type, field.secondary_type)
            # model
            elif isinstance(value, BaseModel):
                data[name] = value.model_dump(mode="json")
            # just some regular stuff
            else:
                data[name] = field.serialize(value, for_filtering=for_filtering)
        return data

    # DELETE
    def delete(self):
        if isinstance(self, _WithSoftDelete):
            self._execute(f"UPDATE {self._get_table_name()} SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?", [self.id])
        else:
            self._execute(f"DELETE FROM {self._get_table_name()} WHERE id = ?", [self.id])

    # SELECT
    @classmethod
    def load(cls, reversed:bool=True, as_collection:bool=False, with_deleted=False, preload:str|list[str]=None, **criteria) -> "Table":
        original_criteria = deepcopy(criteria)
        criteria = cls.process_data(criteria, for_filtering=True)
        # criteria = {name: cls._get_field(name).serialize(value)
        #             for name, value in criteria.items()}
        if not preload:
            preload = []
        if isinstance(preload, str):
            preload = [preload]
        cls._ensure_lazy_loaders()
        from .join_info import JoinInfo
        join_info = JoinInfo(model=cls)
        for path_str in preload:
            path = path_str.split(".")
            join_info.add_children(path)
            
        # SELECT
        sql = f"SELECT "
        sql += ", ".join(join_info.get_columns_statements()) + "\n"
        # FROM / JOIN
        sql += "\n".join(join_info.get_tables_statements())

        # WHERE
        values = []
        sql += "\nWHERE 1 = 1"
        if issubclass(cls, _WithTimestamps) and not with_deleted:
            criteria |= dict(deleted_at=None)
        if criteria:
            # for name, value in cls.process_data(criteria).items():
            for name, value in criteria.items():
                sql += f"\nAND {cls._get_table_name()}.{name}"
                if value is None:
                    sql += " IS NULL"
                else:
                    if cls._has_field(name) and cls._get_field(name).column_base_type == JSON and not isinstance(original_criteria.get(name), str):
                        sql += " = JSON(?)"
                    else:
                        sql += " = ?"
                    values.append(value)

        # ORDER & LIMIT
        order_columns = []
        if issubclass(cls, _WithTimestamps):
            order_columns += ["created_at"]
        elif issubclass(cls, _WithVersion):
            order_columns += list(cls._VERSIONING_ALONG)
            order_columns += ["version"]
        else:
            order_columns += ["id"]
        sql += f"\nORDER BY {", ".join(f"{cls._get_table_name()}.{column}" + (" DESC" if reversed else "")
                                       for column in order_columns)}"
        if not as_collection:
            sql += "\nLIMIT 1"

        # execute & return result
        if as_collection:
            rows = cls._execute(sql, values)
            return [
                join_info.get_instance(row)
                for row in rows
            ]
        else:
            rows = cls._execute(sql, values)
            if not rows:
                return None
            return join_info.get_instance(rows[0])

    @classmethod
    def load_all(cls, **criteria) -> list["Table"]:
        return cls.load(as_collection=True, **criteria)

    # helper methods

    @classmethod
    def _get_table_name(cls) -> str:
        return cls.__name__.lower()

    @classmethod
    def _suspend_validation(cls):
        def __init__(self, *args, **kwargs):
            self.__dict__.update(**kwargs)
            self.__pydantic_fields_set__ = set(cls.model_fields)
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
        if hasattr(cls, "__init_backup__"):
            cls.__init__ = cls.__init_backup__
            cls.__setattr__ = cls.__setattr_backup__
            delattr(cls, "__init_backup__")
            delattr(cls, "__setattr_backup__")

    @classmethod
    def _add_lazy_loader(cls, name: str):
        def lazy_loader(self):
            if not name in self.__dict__:
                model, identifier = self._lazy_joins[name]
                if issubclass(model, Table):
                    value = None if identifier is None else model.load(id=identifier)
                else:
                    value = [reference_type.load(id=reference_id)
                             for reference_type, reference_id
                             in zip(model, identifier)]
                self.__dict__[name] = value
            return self.__dict__[name]
        setattr(cls, name, property(lazy_loader))
    
    @classmethod
    def _ensure_lazy_loaders(cls):
        if hasattr(cls, "_has_lazy_loaders"):
            return
        for name, field in cls._get_fields().items():
            if field.is_reference:
                cls._add_lazy_loader(name)
        cls._has_lazy_loaders = True
