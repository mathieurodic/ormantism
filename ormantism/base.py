import sqlite3
import datetime
import dataclasses
import logging
from functools import cache

from pydantic import BaseModel as PydanticBaseModel
from pydantic._internal._model_construction import ModelMetaclass

from .database import transaction
from .field import Field


logger = logging.getLogger("ormantism")


class BaseWithoutTimestamps(PydanticBaseModel):
    id: int = None

BaseWithoutTimestamps._DEFAULT_FIELDS = ("id",)


class BaseWithTimestamps(BaseWithoutTimestamps):
    created_at: datetime.datetime = None
    updated_at: datetime.datetime = None
    deleted_at: datetime.datetime = None

BaseWithTimestamps._DEFAULT_FIELDS = ("id", "created_at", "updated_at", "deleted_at")


class TimestampMeta(ModelMetaclass):
    
    def __new__(mcs, name, bases, namespace, with_timestamps=False, **kwargs):
        return super().__new__(mcs, name,
                               bases + (BaseWithTimestamps if with_timestamps else BaseWithoutTimestamps,),
                               namespace, **kwargs)


class Base(metaclass=TimestampMeta):
    id: int = None

    @classmethod
    @cache
    def _get_fields(cls) -> dict[str, Field]:
        return {
            name: Field.from_pydantic_info(name, info)
            for name, info in cls.model_fields.items()
        }
    
    @classmethod
    @cache
    def _get_field(cls, name: str):
        return cls._get_fields()[name]

    @classmethod
    @cache
    def _get_columns(cls):
        columns = {}
        for name, field in cls._get_fields().items():
            column = Field(**dataclasses.asdict(field))
            if field.is_reference:
                name += "_id"
                column.name = name
                column.base_type = int
            columns[name] = column
        return columns

    @classmethod
    @cache
    def _get_non_default_fields(cls):
        return {
            name: field
            for name, field in cls._get_fields().items()
            if name not in cls._DEFAULT_FIELDS
        }

    # execute SQL

    @classmethod
    def _execute(cls, sql: str, parameters: list=[]):
        logger.debug(sql)
        logger.debug(parameters)
        with transaction() as t:
            try:
                return t.execute(sql, parameters)
            except sqlite3.OperationalError as e:
                if not str(e).startswith("no such table: "):
                    raise
                cls._create_table()
                return t.execute(sql, parameters)

    # CREATE TABLE

    @classmethod
    def _create_table(cls):
        for field in cls._get_fields().values():
            if field.is_reference:
                field.base_type._create_table()
        translate_type = {
            int: "INTEGER NOT NULL",
            int|None: "INTEGER",
            str: "TEXT NOT NULL",
            str|None: "TEXT",
            datetime.datetime: "TIMESTAMP NOT NULL",
            datetime.datetime|None: "TIMESTAMP",
            list[str]: "JSON NOT NULL",
        }
        sql = f"CREATE TABLE IF NOT EXISTS {cls._get_table_name()} ({", ".join([
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
            "updated_at TIMESTAMP",
            "deleted_at TIMESTAMP",
        ] + [
            field.sql_declaration
            for name, field in cls._get_columns().items()
            if name not in cls._DEFAULT_FIELDS
        ] + [
            f"FOREIGN KEY ({name}_id) REFERENCES {field.base_type._get_table_name()}(id)"
            for name, field in cls._get_fields().items()
            if field.is_reference
        ])})"
        cls._execute(sql)

    # INSERT
    def model_post_init(self, __context: any) -> None:
        if self.id is not None and self.id >= 0:
            return
        data = self._get_columns_data() | {"id": None}
        sql = f"INSERT INTO {self._get_table_name()} ({", ".join(data.keys())})\nVALUES  ({", ".join("?" for v in data.values())})"
        self._execute(sql, list(data.values()))
        more_columns = ", created_at" if isinstance(self, BaseWithTimestamps) else ""
        cursor = self._execute(f"SELECT id{more_columns} FROM {self._get_table_name()} WHERE id = last_insert_rowid()")
        row = cursor.fetchone()
        self.__dict__["id"] = row[0]
        if isinstance(self, BaseWithTimestamps):
            self.__dict__["created_at"] = datetime.datetime.fromisoformat(row[1])

    # UPDATE
    def __setattr__(self, name, value):
        self.__dict__[name] = value
        if name[0] == "_":
            return
        self.update(**{name: value})

    def update(self, **kwargs):
        self.__dict__.update(kwargs)
        sql = f"UPDATE {self._get_table_name()} SET "
        for i, (name, value) in enumerate(kwargs.items()):
            field = self._get_field(name)
            if value is not None and not isinstance(value, field.base_type):
                raise ValueError(f"Wrong type for `{self.__class__.__name__}.{name}`: {type(value)}")
            if i:
                sql += ", "
            sql += name
            if field.is_reference:
                sql += "_id"
            if value is None:
                sql += " = NULL"
            elif field.is_reference:
                sql += " = ?"
                parameters = [value.id] if field.is_reference else [value]
            sql += ", updated_at = CURRENT_TIMESTAMP WHERE id = ?"
            self._execute(sql, parameters + [self.id])

    # DELETE
    def delete(self):
        if isinstance(self, BaseWithTimestamps):
            self._execute(f"UPDATE {self._get_table_name()} SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?", [self.id])
        else:
            self._execute(f"DELETE FROM {self._get_table_name()} WHERE id = ?", [self.id])

    # SELECT
    @classmethod
    def load(cls, last_created:bool=False, as_collection:bool=False, with_deleted=False, preload:str|list[str]=None, **criteria) -> "Base":
        if not preload:
            preload = []
        if isinstance(preload, str):
            preload = [preload]
        cls._ensure_lazy_loaders()
        from .join_info import JoinInfo
        join_info = JoinInfo(model=cls)
        for path_str in preload:
            path = path_str.split(".")
            print(f"{path=}")
            join_info.add_children(path)
            
        # SELECT
        sql = f"SELECT "
        sql += ", ".join(join_info.get_columns_statements()) + "\n"
        # FROM / JOIN
        sql += "\n".join(join_info.get_tables_statements())

        # WHERE
        values = []
        sql += "\nWHERE 1 = 1"
        if issubclass(cls, BaseWithTimestamps) and not with_deleted:
            criteria = dict(deleted_at=None, **criteria)
        if criteria:
            for key, value in criteria.items():
                sql += f"\nAND {cls._get_table_name()}.{key}"
                if value is None:
                    sql += " IS NULL"
                else:
                    sql += " = ?"
                    values.append(value)
        if last_created:
            sql += f"\nORDER BY {cls._get_table_name()}.created_at DESC\nLIMIT 1"

        if as_collection:
            rows = cls._execute(sql, values).fetchall()
            return [
                join_info.get_instance(row)
                for row in rows
            ]
        else:
            row = cls._execute(sql, values).fetchone()
            if row is None:
                return None
            return join_info.get_instance(row)

    @classmethod
    def load_all(cls, **criteria) -> list["Base"]:
        return cls.load(as_collection=True, **criteria)

    # helper methods

    def _get_columns_data(self) -> dict[str, any]:
        data = {}
        for name, field in self._get_fields().items():
            if name in self._DEFAULT_FIELDS:
                continue
            data[name] = field.serialize(getattr(self, name))
        return data

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
    def _add_lazy_loader(cls, name: str, model: type["Base"]):
        def lazy_loader(self):
            if not name in self.__dict__:
                identifier = self._lazy_identifiers.get(name)
                value = None if identifier is None else model.load(id=identifier)
                self.__dict__[name] = value
            return self.__dict__[name]
        setattr(cls, name, property(lazy_loader))
    
    @classmethod
    def _ensure_lazy_loaders(cls):
        if hasattr(cls, "_has_lazy_loaders"):
            return
        for name, field in cls._get_fields().items():
            if field.is_reference:
                cls._add_lazy_loader(name, field.base_type)
        cls._has_lazy_loaders = True


if __name__ == "__main__":
    
    # company model
    class Company(Base):
        name: str
    # employee model, with a foreign key to company
    class Employee(Base):
        firstname: str
        lastname: str
        company: Company
    # show columns
    c1 = Company.load(id=4)
    c2 = Company.load(name="AutoKod", last_created=True)
    c3 = Company.load(name="AutoKod II", last_created=True)
    c4 = Company(name="AutoKod")
    c5 = Company(name="AutoKod")
    c5.name += " II"
    c5.save()
    print(c1)
    print(c2)
    print(c3)
    print(c4)
    e1 = Employee(firstname="Mathieu", lastname="Rodic", company=c1)
    e2 = Employee.load(company_id=c1.id, last_created=True)
    e_all = Employee.load_all(company_id=c1.id)
    print(e1)
    print(e2)
    print(e_all)
    exit()
    # e = Employee.load(id=23)
    print(Company._get_columns_names())
    print(Employee._get_columns_names())
    print(Company._build_instance({"id": 12, "name": "Hello :)"}))

    #

    class A(Base, with_timestamps=False): pass
    print()
    print(A._get_fields())
    print(A._get_columns())
    print()
    class B(Base, with_timestamps=True):
        value: int = 42
    print()
    print(B._get_fields())
    print(B._get_columns())
    print(B().id)
    b = B()
    b.value = 69
    print(B.load(id=b.id).value)
    print()
    class C(Base, with_timestamps=True):
        links_to: B = None
    print()
    print(C._get_fields())
    print(C._get_columns())
    print()
    print()
    print(C().id)
    print(C().created_at)
    print(C()._get_columns_data())
    # print(C().delete())
    print()
    print("((((((( 2.0 )))))))")
    c = C.load(id=1)
    print("((((((( 2.1 )))))))")
    c.links_to = B()
    print("((((((( 2.2 )))))))")
    print()
    # explicit pre-loading
    c = C.load(id=1, preload="links_to")
    print("((((((( 3.0 )))))))")
    print(c)
    print("((((((( 3.1 )))))))")
    print(c.links_to)
    print("((((((( 3.2 )))))))")
    print(c)
    print("((((((( 3.3 )))))))")
    print()
    # lazy loading
    c = C.load(id=1)
    print("((((((( 4.0 )))))))")
    print(c)
    print("((((((( 4.1 )))))))")
    print(c.links_to)
    print("((((((( 4.2 )))))))")
    print(c.links_to)
    print("((((((( 4.3 )))))))")
    print(c)
    print("((((((( 4.4 )))))))")
    print()
