import sqlite3
import datetime
import dataclasses
import logging
from functools import cache

from pydantic import BaseModel as PydanticBaseModel, ConfigDict as PydanticConfigDict
from pydantic._internal._model_construction import ModelMetaclass

from .utils.make_hashable import make_hashable
from .database import transaction
from .field import Field


logger = logging.getLogger("ormantism")


class _WithPrimaryKey(PydanticBaseModel):
    id: int = None


class _WithTimestamps(PydanticBaseModel):
    created_at: datetime.datetime = None
    updated_at: datetime.datetime = None
    deleted_at: datetime.datetime = None


class _WithVersion(PydanticBaseModel):
    version: int = 0
    is_active: bool = True


class BaseMeta(ModelMetaclass):

    def __new__(mcs, name, bases, namespace,
                with_primary_key: bool=True,
                with_timestamps: bool=False,
                versioning_along: tuple[str]=None,
                **kwargs):
        default_bases: tuple[type[PydanticBaseModel]] = tuple()
        if with_primary_key:
            default_bases += (_WithPrimaryKey,)
        if with_timestamps:
            default_bases += (_WithTimestamps,)
        if versioning_along:
            default_bases += (_WithVersion,)
        bases += default_bases
        result = super().__new__(mcs, name, bases, namespace, **kwargs)
        result._DEFAULT_FIELDS = sum((tuple(base.model_fields.keys())
                                     for base in default_bases), start=())
        if versioning_along is None:
            for base in bases:
                if getattr(base, "_VERSIONING_ALONG", None):
                    versioning_along = base._VERSIONING_ALONG
        result._VERSIONING_ALONG = versioning_along
        return result


class Base(metaclass=BaseMeta):
    id: int = None

    model_config = PydanticConfigDict(
        arbitrary_types_allowed = True,
        json_encoders = {
            type[PydanticBaseModel]: lambda v: v.__name__
        },
    )

    def __hash__(self):
        return hash(make_hashable(self))

    # INSERT
    def model_post_init(self, __context: any) -> None:
        if self.id is not None and self.id >= 0:
            return
        data = self._get_columns_data() | {"id": None}
        # special column for versioning
        if isinstance(self, _WithVersion):
            sql = f"UPDATE {self._get_table_name()} SET is_active = false WHERE is_active "
            values = []
            for name, value in data.items():
                if name not in self._VERSIONING_ALONG:
                    continue
                if value is None:
                    sql += f" AND {name} IS NULL"
                else:
                    sql += f" AND {name} = ?"
                    values.append(value)
            sql += " RETURNING version"
            row = self._execute(sql, values).fetchone()
            data["version"] = self.__dict__["version"] = (row[0] + 1) if row else 0
            data["is_active"] = True
        # perform insertion
        sql = f"INSERT INTO {self._get_table_name()} ({", ".join(data.keys())})\nVALUES  ({", ".join("?" for v in data.values())})"
        self._execute(sql, list(data.values()))
        # retrieve automatic columns from inserted row
        more_columns = ", created_at" if isinstance(self, _WithTimestamps) else ""
        cursor = self._execute(f"SELECT id{more_columns} FROM {self._get_table_name()} WHERE id = last_insert_rowid()")
        row = cursor.fetchone()
        self.__dict__["id"] = row[0]
        if isinstance(self, _WithTimestamps):
            self.__dict__["created_at"] = datetime.datetime.fromisoformat(row[1])
        # trigger
        if hasattr(self, "__post_init__"):
            self.__post_init__()

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
    def _get_non_default_fields(cls):
        return {
            name: field
            for name, field in cls._get_fields().items()
            if name not in cls._DEFAULT_FIELDS
        }

    # execute SQL
    @classmethod
    def _execute_from(cls, t, sql: str, parameters: list=[]):
        logger.debug(sql)
        logger.debug(parameters)
        try:
            return t.execute(sql, parameters)
        except sqlite3.OperationalError as e:
            t.rollback()
            if not str(e).startswith("no such table: "):
                raise
            cls._create_table()
            return t.execute(sql, parameters)

    @classmethod
    def _execute(cls, sql: str, parameters: list=[]):
        try:
            with transaction() as t:
                return cls._execute_from(t, sql, parameters)
        except sqlite3.OperationalError as e:
            t.rollback()
            if not str(e).startswith("cannot commit transaction - SQL statements in progress"):
                raise
            return cls._execute_from(t, sql, parameters)

    # CREATE TABLE

    @classmethod
    def _create_table(cls, created: set[type["Base"]]=set()):
        # create tables for references first
        created.add(cls)
        for field in cls._get_fields().values():
            if field.is_reference and field.base_type not in created:
                field.base_type._create_table(created)
        # translation from Python to SQL type
        translate_type = {
            int: "INTEGER NOT NULL",
            int|None: "INTEGER",
            str: "TEXT NOT NULL",
            str|None: "TEXT",
            datetime.datetime: "TIMESTAMP NOT NULL",
            datetime.datetime|None: "TIMESTAMP",
            list[str]: "JSON NOT NULL",
        }
        # initialize statements for table creation
        statements = []
        # id & created_at are special
        if issubclass(cls, _WithPrimaryKey):
            statements += ["id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT"]
        if issubclass(cls, _WithTimestamps):
            statements += ["created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"]
        # other columns are easy
        statements += [
            field.sql_declaration
            for field in cls._get_fields().values()
            if field.name not in ("created_at", "id")
        ]
        # foreign keys
        statements += [
            f"FOREIGN KEY ({name}_id) REFERENCES {field.base_type._get_table_name()}(id)"
            for name, field in cls._get_fields().items()
            if field.is_reference
        ]
        # build & execute SQL
        sql = f"CREATE TABLE {cls._get_table_name()} (\n  {",\n  ".join(statements)})"
        cls._execute(sql)

    # UPDATE
    def __setattr__(self, name, value):
        self.__dict__[name] = value
        if name[0] == "_":
            return
        self.update(**{name: value})

    def update(self, **kwargs):

        # versioning: insert a new version
        if isinstance(self, _WithVersion):
            new_instance = self.__class__(**{name: value
                                          for name, value in self.__dict__.items()
                                          if name not in self._DEFAULT_FIELDS
                                          and name in self._get_fields()})
            self.__dict__.update(new_instance.__dict__)
            # trigger
            if hasattr(self, "__post_update__"):
                self.__post_update__()
            return

        # other cases: actually update
        self.__dict__.update(kwargs)
        sql = f"UPDATE {self._get_table_name()} SET "
        parameters = []
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
            else:
                sql += " = ?"
                parameters += [value.id if field.is_reference else field.serialize(value)]
        if isinstance(self, _WithTimestamps):
            sql += ", updated_at = CURRENT_TIMESTAMP WHERE id = ? RETURNING updated_at"
        else:
            sql += " WHERE id = ?"
        cursor = self._execute(sql, parameters + [self.id])
        if isinstance(self, _WithTimestamps):
            self.__dict__["updated_at"] = cursor.fetchone()[0]
        # trigger
        if hasattr(self, "__post_update__"):
            self.__post_update__()

    # DELETE
    def delete(self):
        if isinstance(self, _WithTimestamps):
            self._execute(f"UPDATE {self._get_table_name()} SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?", [self.id])
        else:
            self._execute(f"DELETE FROM {self._get_table_name()} WHERE id = ?", [self.id])

    # SELECT
    @classmethod
    def load(cls, reversed:bool=True, as_collection:bool=False, with_deleted=False, preload:str|list[str]=None, **criteria) -> "Base":
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
            criteria = dict(deleted_at=None, **criteria)
        if criteria:
            for name, value in criteria.items():
                field = cls._get_field(name)
                sql += f"\nAND {cls._get_table_name()}.{field.column_name}"
                if value is None:
                    sql += " IS NULL"
                else:
                    sql += " = ?"
                    values.append(field.serialize(value))

        # ORDER & LIMIT
        order_columns = []
        if issubclass(cls, _WithTimestamps):
            order_columns += ["created_at"]
        elif issubclass(cls, _WithVersion):
            order_columns += list(cls._VERSIONING_ALONG)
            order_columns += ["version"]
        else:
            sql += ["id"]
        sql += f"\nORDER BY {", ".join(f"{cls._get_table_name()}.{column}" + (" DESC" if reversed else "")
                                       for column in order_columns)}"
        if not as_collection:
            sql += "\nLIMIT 1"

        # execute & return result
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
        for field in self._get_fields().values():
            if field.name in self._DEFAULT_FIELDS:
                continue
            value = field.serialize(getattr(self, field.name))
            data[field.column_name] = value
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
    from .database import connect
    connect("sqlite:///:memory:")

    class Document(Base, versioning_along="name"):
        name: str
        content: str
    d1 = Document(name="foo", content="azertyuiop")
    d2 = Document(name="foo", content="azertyuiopqsdfghjlm")
    print(d1)
    print(d2)
    d2.content += " :)"
    print(d2)
    exit()
    
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
