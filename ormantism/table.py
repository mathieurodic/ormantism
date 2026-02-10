"""Table model base for ORM CRUD and schema creation."""

from typing import ClassVar
import inspect
import logging
import sqlite3
from functools import cache

from pydantic import BaseModel

from .utils.make_hashable import make_hashable
from .transaction import transaction
from .field import Field
from .table_meta import TableMeta
from .table_mixins import (
    _WithPrimaryKey,
    _WithSoftDelete,
    _WithCreatedAtTimestamp,
    _WithUpdatedAtTimestamp,
    _WithTimestamps,
    _WithVersion,
)


logger = logging.getLogger("ormantism")


class Table(metaclass=TableMeta):
    """Base class for ORM table models; provides DB operations and identity semantics."""

    model_config = {"arbitrary_types_allowed": True}
    _CHECKED_TABLE_EXISTENCE: ClassVar[bool] = False

    def __eq__(self, other: "Table"):
        """Compare by identity (hash); both must be the same class."""
        if not isinstance(other, self.__class__):
            raise ValueError(
                f"Comparing instances of different classes: {self.__class__} "
                f"and {other.__class__}"
            )
        return hash(self) == hash(other)

    def __hash__(self):
        """Hash for equality and use in sets/dicts."""
        return hash(make_hashable(self))

    def __deepcopy__(self, memo):
        """Return self; table instances are treated as immutable for copy purposes."""
        return self

    # INSERT
    def on_after_create(self, init_data: dict):
        """Persist the instance to the database (INSERT) and set generated columns."""
        # if primary key already set: skip entirely
        if self.id is not None and self.id >= 0:
            return
        self.check_read_only(init_data)
        # special column for versioning
        if isinstance(self, _WithVersion):
            tbl = self._get_table_name()
            sql = f"UPDATE {tbl} SET deleted_at = CURRENT_TIMESTAMP WHERE deleted_at IS NULL"
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
        # Include fields with defaults so they are inserted (and RETURNING matches instance)
        for name, field in self._get_fields().items():
            if (not field.is_reference and name not in self._READ_ONLY_FIELDS
                    and field.default is not None):
                include.add(name)
                # Set default on instance so it matches what we insert (in case model init did not)
                if name not in processed_data:
                    object.__setattr__(self, name, field.default)
        # return
        for name, value in processed_data.items():
            # object.__setattr__(self, name, value)
            include.add(name)
            if name in exclude:
                exclude.remove(name)
            else:
                formatted_data[name] = value

        for name, field in self._get_fields().items():
            if name not in include or name in exclude:
                continue
            if field.is_reference:
                continue
            # Use field default when user did not pass this field (so INSERT and RETURNING match)
            if name not in processed_data and field.default is not None:
                instance_value = field.default
            else:
                instance_value = getattr(self, name)
            formatted_data[name] = field.serialize(instance_value)

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
        # Re-apply defaults for fields we included from default (in case RETURNING order was wrong)
        for name, field in self._get_fields().items():
            if (not field.is_reference and name not in self._READ_ONLY_FIELDS
                    and field.default is not None and name not in processed_data):
                object.__setattr__(self, name, field.default)
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
        if not set_statement:
            return
        # fill WHERE statement
        where_statement = {}
        if isinstance(self, _WithPrimaryKey):
            where_statement = {"id": self.id}
        else:
            raise NotImplementedError()

        # compute SQL
        sql = f"UPDATE {self._get_table_name()}\n"
        sql += "\nSET " + ", ".join(f"{k} = ?" for k in set_statement)
        if isinstance(self, _WithUpdatedAtTimestamp):
            sql += ", updated_at = CURRENT_TIMESTAMP"
        sql += "\nWHERE " + " AND ".join(f"{k} = ?" for k in where_statement)

        # compute parameters
        parameters = tuple(value
                           for statement in (set_statement, where_statement)
                           for value in statement.values())

        # execute query
        self._execute_returning(sql=sql,
                                parameters=parameters)

    # INSERT or SELECT / UPDATE
    @classmethod
    def load_or_create(cls, _search_fields=None, **data):
        """Load a row matching the data, or create one; optional _search_fields."""
        # if restriction applies
        if _search_fields is None:
            searched_data = data
        else:
            searched_data = {key: data[key] for key in _search_fields if key in data}
        # return corresponding row if already exists
        loaded = cls.load(**searched_data)
        if loaded:
            logger.warning(data)
            def _changed(key, value):
                loaded_val = getattr(loaded, key)
                if value is None or loaded_val is None:
                    return value is not loaded_val
                return loaded_val != value
            changed_data = {key: value for key, value in data.items() if _changed(key, value)}
            changed_data = {}
            for name, value in data.items():
                field = cls._get_field(name)
                if field.is_reference:
                    if name not in loaded._lazy_joins:
                        if value is not None:
                            changed_data[name] = value.id
                            raise NotImplementedError(
                                "Unexpected: reference not in _lazy_joins"
                            )
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
                            raise ValueError("?!")

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
        """Return True if the table has a field (or column) with the given name."""
        return name in cls.model_fields

    @classmethod
    @cache
    def _get_fields(cls) -> dict[str, Field]:
        """Return a mapping of field name to Field for all model fields."""
        return {
            name: Field.from_pydantic_info(cls, name, info)
            for name, info in cls.model_fields.items()
        }

    @classmethod
    @cache
    def _get_field(cls, name: str):
        """Return the Field for the given name or column name; raises KeyError if missing."""
        fields = cls._get_fields()
        if name in fields:
            return fields[name]
        for field in fields.values():
            if field.column_name == name:
                return field
        # Generic reference (Table) uses name_table and name_id columns
        if name.endswith("_table") and name[:-6] in fields:
            return fields[name[:-6]]
        raise KeyError(f"No such field for {cls.__name__}: {name}")

    @classmethod
    @cache
    def _get_non_default_fields(cls):
        """Return fields that are not read-only (e.g. not id, created_at from mixins)."""
        return {
            name: field
            for name, field in cls._get_fields().items()
            if name not in cls._READ_ONLY_FIELDS
        }

    # execute SQL

    @classmethod
    def _execute(cls, sql: str, parameters=None, check=True) -> list[tuple]:
        """Run SQL and return fetched rows; optionally ensure table/columns exist first."""
        if parameters is None:
            parameters = []
        if check and cls != Table and not cls._CHECKED_TABLE_EXISTENCE:
            cls._create_table()
            cls._add_columns()
            cls._CHECKED_TABLE_EXISTENCE = True
        with transaction(connection_name=cls._CONNECTION_NAME) as t:
            cursor = t.execute(sql, parameters)
            result = cursor.fetchall()
            cursor.close()
        return result

    def _execute_returning(self, sql: str, parameters=None, for_insertion=False):
        """Execute SQL with RETURNING and set parsed values on self."""
        if parameters is None:
            parameters = []
        # Use a list so RETURNING column order matches parsed row order
        returned_fields = list(self._READ_ONLY_FIELDS)
        if for_insertion:
            for name, field in self._get_fields().items():
                if (not field.is_reference and field.default is not None
                        and name not in returned_fields):
                    returned_fields.append(name)
        if self._READ_ONLY_FIELDS:
            sql += "\nRETURNING " + ", ".join(returned_fields)
        rows = self._execute(sql, list(parameters))

        # parse returned value & set them
        for name, value in zip(returned_fields, rows[0]):
            field = self._get_field(name)
            parsed_value = field.parse(value)
            if parsed_value is None and field.default is not None:
                parsed_value = field.default
            object.__setattr__(self, name, parsed_value)

    # CREATE TABLE

    @classmethod
    def _create_table(cls, created=None):
        """Create the table and referenced tables if they do not exist."""
        if created is None:
            created = set()
        # create tables for references first
        created.add(cls)
        for field in cls._get_fields().values():
            if field.is_reference:
                for t in (field.base_type, field.secondary_type):
                    if (inspect.isclass(t) and issubclass(t, Table)
                            and t != Table and t not in created):
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
        stmt_join = ",\n  ".join(statements)
        sql = f"CREATE TABLE IF NOT EXISTS {cls._get_table_name()} (\n  {stmt_join})"
        cls._execute(sql, check=False)

    @classmethod
    def _add_columns(cls):
        """Add any missing columns to the existing table (SQLite ALTER TABLE)."""
        tbl = cls._get_table_name()
        rows = cls._execute(
            f"SELECT name FROM pragma_table_info('{tbl}')", check=False
        )
        columns_names = {name for name, in rows}
        new_fields = [field for field in cls._get_fields().values()
                      if field.column_name not in columns_names]
        for field in new_fields:
            logger.info("ADD COLUMN %s.%s", field.table.__name__, field.name)
            for sql_creation in field.sql_creations:
                # if sql.
                # if field.is_reference:
                #     raise Exception("cannot add foreign key constraint on existing table")
                try:
                    cls._execute(
                        f"ALTER TABLE {cls._get_table_name()} ADD COLUMN "
                        f"{sql_creation}",
                        check=False,
                    )
                except sqlite3.OperationalError as error:
                    if "duplicate column name" not in error.args[0]:
                        raise

    def check_read_only(self, data):
        """Check we are not attempting to alter read-only fields"""
        read_only_fields = list(set(data) & set(self._READ_ONLY_FIELDS))
        read_only_fields_count = len(read_only_fields)
        if read_only_fields_count:
            plural = "s" if read_only_fields_count > 1 else ""
            fields_str = ", ".join(read_only_fields)
            raise AttributeError(
                f"Cannot set read-only attribute{plural} of "
                f"{self.__class__.__name__}: {fields_str}"
            )

    @classmethod
    def process_data(cls, data: dict, for_filtering: bool = False) -> dict:
        """Convert dict to DB-ready form (serialize refs, expand to _id/_table)."""
        data = {**data}
        for name in list(data):
            value = data.pop(name)
            # is there no field for this name?
            try:
                field = cls._get_field(name)
            except KeyError as exc:
                raise ValueError(
                    f"Invalid key found in data for {cls.__name__}: {name}"
                ) from exc
            # so, there is.
            if field.is_reference:
                # scalar reference
                if field.secondary_type is None:
                    if field.base_type is Table:
                        data[f"{field.name}_table"] = (
                            value._get_table_name() if value else None
                        )
                    data[f"{field.name}_id"] = (
                        (value if isinstance(value, int) else value.id)
                        if value else None
                    )
                # list of references
                elif (isinstance(value, (list, tuple, set))
                        and issubclass(field.base_type, (list, tuple, set))):
                    if field.secondary_type == Table:
                        data[f"{field.name}_tables"] = [
                            referred._get_table_name() for referred in value
                        ]
                    data[f"{field.name}_ids"] = [referred.id for referred in value]
                # ?
                else:
                    raise NotImplementedError(
                        field.name, value, field.base_type, field.secondary_type
                    )
            # model
            elif isinstance(value, BaseModel):
                data[name] = value.model_dump(mode="json")
            # just some regular stuff
            else:
                data[name] = field.serialize(value, for_filtering=for_filtering)
        return data

    # DELETE
    def delete(self):
        """Delete the row (soft delete if _WithSoftDelete, else hard delete)."""
        if isinstance(self, _WithSoftDelete):
            tbl = self._get_table_name()
            self._execute(
                f"UPDATE {tbl} SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?",
                [self.id],
            )
        else:
            self._execute(
                f"DELETE FROM {self._get_table_name()} WHERE id = ?", [self.id]
            )

    # SELECT
    @classmethod
    def load(cls, reverse_order: bool = True, as_collection: bool = False,
             with_deleted=False, preload: str | list[str] = None,
             **criteria) -> "Table":
        """Load by criteria; supports preload paths and optional soft-delete."""
        original_criteria = criteria
        processed_criteria = cls.process_data(criteria, for_filtering=True)
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
        sql = "SELECT "
        sql += ", ".join(join_info.get_columns_statements())
        sql += "\n"
        # FROM / JOIN
        sql += "\n".join(join_info.get_tables_statements())

        # WHERE
        values = []
        sql += "\nWHERE 1 = 1"
        if issubclass(cls, _WithTimestamps) and not with_deleted:
            processed_criteria |= {"deleted_at": None}
        if processed_criteria:
            # for name, value in cls.process_data(processed_criteria).items():
            for name, value in processed_criteria.items():
                field = cls._get_field(name)
                json_wrap = (
                    cls._has_field(name) and field.sql_is_json
                    and not isinstance(original_criteria.get(name), str)
                )
                # column
                if json_wrap:
                    sql += f"\nAND JSON({cls._get_table_name()}.{name})"
                else:
                    sql += f"\nAND {cls._get_table_name()}.{name}"
                # comparison
                if value is None:
                    sql += " IS NULL"
                else:
                    if json_wrap:
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
        order_dir = " DESC" if reverse_order else ""
        sql += "\nORDER BY " + ", ".join(
            f"{cls._get_table_name()}.{column}{order_dir}"
            for column in order_columns
        )
        if not as_collection:
            sql += "\nLIMIT 1"

        # execute & return result
        if as_collection:
            rows = cls._execute(sql, values)
            return [
                join_info.get_instance(row)
                for row in rows
            ]
        rows = cls._execute(sql, values)
        if not rows:
            return None
        return join_info.get_instance(rows[0])

    @classmethod
    def load_all(cls, **criteria) -> list["Table"]:
        """Load all rows matching the given criteria."""
        return cls.load(as_collection=True, **criteria)

    # helper methods

    @classmethod
    def _get_table_name(cls) -> str:
        """Return the database table name (default: lowercased class name)."""
        return cls.__name__.lower()

    @classmethod
    def _suspend_validation(cls):
        """Replace __init__/__setattr__ so instances can be built without validation."""
        def __init__(self, *_args, **kwargs):
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
        """Restore normal __init__/__setattr__ after _suspend_validation."""
        if hasattr(cls, "__init_backup__"):
            cls.__init__ = cls.__init_backup__
            cls.__setattr__ = cls.__setattr_backup__
            delattr(cls, "__init_backup__")
            delattr(cls, "__setattr_backup__")

    @classmethod
    def _add_lazy_loader(cls, name: str):
        """Attach a property on the class that loads the reference for `name` on first access."""
        def lazy_loader(self):
            if not name in self.__dict__:
                model, identifier = self._lazy_joins[name]
                if inspect.isclass(model) and issubclass(model, Table):
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
        """Ensure every reference field has a lazy-loading property."""
        if hasattr(cls, "_has_lazy_loaders"):
            return
        for name, field in cls._get_fields().items():
            if field.is_reference:
                cls._add_lazy_loader(name)
        cls._has_lazy_loaders = True


# Re-export so existing imports from ormantism.table still work
__all__ = [
    "Table",
    "TableMeta",
    "_WithPrimaryKey",
    "_WithSoftDelete",
    "_WithCreatedAtTimestamp",
    "_WithUpdatedAtTimestamp",
    "_WithTimestamps",
    "_WithVersion",
]
