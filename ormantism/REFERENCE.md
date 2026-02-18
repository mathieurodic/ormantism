# Reference

## Code

### Modules

| available as | defined in | description |
|--------------|------------|-------------|
| ormantism | ormantism/__init__.py | ORMantism: a lightweight ORM built on Pydantic and SQL |
| ormantism.column | ormantism/column.py | Column metadata for Table models: types, defaults, SQL, serialize/parse |
| ormantism.connection | ormantism/connection.py | Database connection configuration and factory for MySQL, SQLite, PostgreSQL |
| ormantism.query | ormantism/query.py | Query builder and execution: fluent API, JOINs, WHERE, ORDER BY, hydration |
| ormantism.table | ormantism/table/__init__.py | Table model base, mixins, create_table, add_columns |
| ormantism.transaction | ormantism/transaction.py | Transaction context and manager with savepoint support for nested transactions |
| ormantism.dialects | ormantism/dialects/__init__.py | Database dialects: one class per engine (SQLite, MySQL, PostgreSQL, SQL Server) |
| ormantism.expressions | ormantism/expressions/__init__.py | SQL expression types for query building (column, function, operators) |
| ormantism.utils | ormantism/utils/__init__.py | Utility helpers for type resolution, serialization, schema, and model discovery |
| ormantism.utils.schema | ormantism/utils/schema.py | JSON Schema (to/from), rebuild_pydantic_model, serialize |

### Classes

| available as | description | inherits from | defined at | used at |
|--------------|-------------|---------------|------------|---------|
| ormantism.table.Table | Base class for ORM table models; provides DB operations and identity semantics | | ormantism/table/base.py:28 | ormantism/utils/get_table_by_name.py:10<br>ormantism/table/base.py:179<br>ormantism/table/base.py:183 |
| ormantism.table.TableMeta | Metaclass for Table; builds columns from model fields | pydantic._internal._model_construction.ModelMetaclass | ormantism/table/meta.py:19 | ormantism/table/base.py:28 |
| ormantism.table.Hydratable | Mixin for instance-building from raw row data (hydration) | | ormantism/table/hydratable.py:12 | ormantism/table/base.py:28 |
| ormantism.table._WithCreatedAtTimestamp | Mixin that adds a created_at timestamp set on insert | ormantism.utils.supermodel.SuperModel | ormantism/table/mixins.py:25 | ormantism/table/mixins.py:40<br>ormantism/table/meta.py:32<br>ormantism/table/base.py:28 |
| ormantism.table._WithPrimaryKey | Mixin that adds an auto-increment integer primary key id | ormantism.utils.supermodel.SuperModel | ormantism/table/mixins.py:11 | ormantism/table/meta.py:27<br>ormantism/table/base.py:28<br>ormantism/table/schema.py |
| ormantism.table._WithSoftDelete | Mixin that adds soft delete via deleted_at timestamp | ormantism.utils.supermodel.SuperModel | ormantism/table/mixins.py:17 | ormantism/table/mixins.py:44<br>ormantism/table/mixins.py:49<br>ormantism/table/base.py:28<br>ormantism/query.py:523 |
| ormantism.table._WithTimestamps | Mixin that adds timestamps and soft delete; default order by created_at DESC | ormantism.table._WithCreatedAtTimestamp<br>ormantism.table._WithSoftDelete<br>ormantism.table._WithUpdatedAtTimestamp | ormantism/table/mixins.py:40 | ormantism/table/meta.py:30<br>ormantism/table/base.py:28 |
| ormantism.table._WithUpdatedAtTimestamp | Mixin that adds an updated_at timestamp updated on save | ormantism.utils.supermodel.SuperModel | ormantism/table/mixins.py:31 | ormantism/table/mixins.py:40<br>ormantism/table/meta.py:28<br>ormantism/table/base.py:28 |
| ormantism.table._WithVersion | Mixin that adds a version counter for optimistic locking | ormantism.table._WithSoftDelete | ormantism/table/mixins.py:52 | ormantism/table/meta.py:31<br>ormantism/table/base.py:28 |
| ormantism.transaction.Transaction | Handle for executing SQL within a single transaction (or savepoint) | builtins.object | ormantism/transaction.py:110 | ormantism/transaction.py:79 |
| ormantism.transaction.TransactionError | Custom exception for transaction-related errors | builtins.Exception | ormantism/transaction.py:13 | ormantism/transaction.py:137<br>ormantism/transaction.py:154 |
| ormantism.transaction.TransactionManager | Manages per-thread connections and nested transaction levels (savepoints) | builtins.object | ormantism/transaction.py:16 | ormantism/transaction.py:174 |
| ormantism.utils.supermodel.SuperModel | Pydantic BaseModel with before/after create/update triggers and type-field serialization | pydantic.BaseModel | ormantism/utils/supermodel.py:97 | ormantism/table/mixins.py:11<br>ormantism/table/mixins.py:17<br>ormantism/table/mixins.py:25<br>ormantism/table/mixins.py:31<br>ormantism/table/mixins.py:40<br>ormantism/table/mixins.py:44 |

### Methods

| method path | description | defined at | used at |
|-------------|-------------|------------|---------|
| ormantism.connection.connect | Set default or named database connection from URL | ormantism/connection.py:15 | |
| ormantism.connection._get_connection | Return the Connection for the given name | ormantism/connection.py:45 | ormantism/transaction.py:172 |
| ormantism.transaction.transaction | Context manager for database transactions with savepoint support | ormantism/transaction.py:168 | ormantism/table/base.py:179 |
| ormantism.utils.find_subclass.find_subclass | Find a subclass by name in a class hierarchy | ormantism/utils/find_subclass.py:13 | ormantism/utils/supermodel.py:65 |
| ormantism.utils.get_base_type.get_base_type | Resolve the base type from annotations and containers | ormantism/utils/get_base_type.py:7 | ormantism/column.py:104<br>ormantism/utils/supermodel.py:132 |
| ormantism.utils.get_base_type.get_container_base_type | Get base type from a container (list, dict, etc.) | ormantism/utils/get_base_type.py:51 | ormantism/utils/get_base_type.py:47<br>ormantism/utils/get_base_type.py:48 |
| ormantism.utils.get_table_by_name.get_all_tables | Return all registered Table subclasses | ormantism/utils/get_table_by_name.py:7 | ormantism/utils/get_table_by_name.py:15 |
| ormantism.utils.get_table_by_name.get_table_by_name | Look up a Table subclass by name | ormantism/utils/get_table_by_name.py:13 | ormantism/column.py:277<br>ormantism/column.py:283<br>ormantism/utils/resolve_type.py:13 |
| ormantism.utils.is_type_annotation.is_type_annotation | Detect whether a value is a type annotation | ormantism/utils/is_type_annotation.py:6 | ormantism/utils/is_type_annotation.py:21<br>ormantism/utils/supermodel.py:182<br>ormantism/utils/supermodel.py:237 |
| ormantism.utils.make_hashable.make_hashable | Convert nested structures to hashable form for sets/caches | ormantism/utils/make_hashable.py:10 | ormantism/utils/make_hashable.py:21<br>ormantism/utils/make_hashable.py:27<br>ormantism/column.py:201 |
| ormantism.utils.schema.get_field_type | Extract field type from JSON schema | ormantism/utils/schema.py:94 | ormantism/utils/schema.py:97<br>ormantism/utils/schema.py:105<br>ormantism/utils/schema.py:117 |
| ormantism.utils.schema.rebuild_pydantic_model | Build a Pydantic model dynamically from JSON schema | ormantism/utils/schema.py:125 | ormantism/column.py:289<br>ormantism/utils/schema.py:67<br>ormantism/utils/supermodel.py:70 |
| ormantism.utils.resolve_type.resolve_type | Resolve forward references to Table subclasses | ormantism/utils/resolve_type.py:7 | ormantism/column.py:103 |
| ormantism.utils.schema.serialize | Recursively serialize nested structures to JSON-serializable types | ormantism/utils/schema.py:153 | ormantism/utils/schema.py:159<br>ormantism/utils/schema.py:161<br>ormantism/utils/schema.py:163<br>ormantism/column.py:250 |
| ormantism.utils.schema.to_json_schema | Return a JSON Schema dict for the given type | ormantism/utils/schema.py:20 | ormantism/column.py:248<br>ormantism/utils/supermodel.py:188 |
| ormantism.utils.schema.from_json_schema | Reconstruct a Python type from its JSON schema representation | ormantism/utils/schema.py:35 | ormantism/utils/supermodel.py:139<br>ormantism/utils/schema.py:57<br>ormantism/utils/schema.py:85<br>ormantism/column.py:289 |
| ormantism.utils.supermodel.SuperModel.model_dump | Dump model to dict with JSON-safe values for type fields | ormantism/utils/supermodel.py:157 | ormantism/table/base.py:141<br>ormantism/utils/make_hashable.py:16<br>ormantism/utils/schema.py:159<br>ormantism/utils/supermodel.py:193<br>ormantism/utils/supermodel.py:204 |
| ormantism.utils.supermodel.SuperModel.trigger | Internal method that invokes before/after create/update hooks | ormantism/utils/supermodel.py:246 | ormantism/utils/supermodel.py:120<br>ormantism/utils/supermodel.py:151<br>ormantism/utils/supermodel.py:232<br>ormantism/utils/supermodel.py:241 |
| ormantism.utils.supermodel.SuperModel.on_before_create | Hook called before instance is persisted (override in subclasses) | ormantism/utils/supermodel.py:268 | ormantism/utils/supermodel.py:246 |
| ormantism.utils.supermodel.SuperModel.on_after_create | Hook called after instance is persisted (override in subclasses) | ormantism/utils/supermodel.py:271 | ormantism/utils/supermodel.py:246 |
| ormantism.utils.supermodel.SuperModel.on_before_update | Hook called before update is applied (override in subclasses) | ormantism/utils/supermodel.py:274 | ormantism/utils/supermodel.py:246 |
| ormantism.utils.supermodel.SuperModel.on_after_update | Hook called after update is applied (override in subclasses) | ormantism/utils/supermodel.py:277 | ormantism/utils/supermodel.py:246 |
| ormantism.utils.supermodel.SuperModel.update | Update instance fields and persist to database | ormantism/utils/supermodel.py:218 | ormantism/utils/supermodel.py:214<br>ormantism/table/base.py:94 |
| ormantism.table.create_table | Create the table and referenced tables if they do not exist | ormantism/table/schema.py:14 | ormantism/table/schema.py:32<br>ormantism/query.py:271 |
| ormantism.table.add_columns | Add any missing columns to the existing table | ormantism/table/schema.py:47 | ormantism/query.py:272 |
| ormantism.table.Table.on_after_create | Persist the instance to the database (INSERT) and set generated columns | ormantism/table/base.py:83 | ormantism/utils/supermodel.py:246 |
| ormantism.table.Table.on_before_update | Apply changes to database (called from SuperModel trigger) | ormantism/table/base.py:87 | ormantism/utils/supermodel.py:246 |
| ormantism.table.Table._has_column | Return True if the table has a column with the given name | ormantism/table/base.py:119 | ormantism/query.py:408 |
| ormantism.table.Table._get_columns | Return the mapping of column name to Column | ormantism/table/base.py:124 | ormantism/query.py (multiple) |
| ormantism.table.Table._get_column | Return the Column for the given name; raises KeyError if missing | ormantism/table/base.py:131 | ormantism/table/hydratable.py:98<br>ormantism/table/hydratable.py:110<br>ormantism/query.py (multiple) |
| ormantism.table.Table.check_read_only | Validate that read-only fields are not being updated | ormantism/table/base.py:144 | ormantism/table/base.py:88<br>ormantism/table/base.py:136 |
| ormantism.table.Table.process_data | Serialize refs and prepare data for persistence | ormantism/table/base.py:150 | ormantism/query.py:621<br>ormantism/query.py:802 |
| ormantism.table.Table.delete | Delete the row (soft delete when timestamps enabled) | ormantism/table/base.py:169 | |
| ormantism.table.Table.load | Load by criteria; supports preload paths (deprecated) | ormantism/table/base.py:194 | ormantism/table/base.py:211 |
| ormantism.table.Table.load_all | Load all rows matching criteria (deprecated) | ormantism/table/base.py:207 | ormantism/table/base.py:211<br>tests (multiple) |
| ormantism.table.Table.load_or_create | Load a row matching the data, or create one; optional _search_fields | ormantism/table/base.py:96 | |
| ormantism.table.Table._get_table_name | Return the database table name (default: lowercased class name) | ormantism/table/base.py:215 | ormantism/utils/get_table_by_name.py:16<br>ormantism/table/base.py<br>ormantism/query.py (multiple)<br>ormantism/column.py:250<br>ormantism/column.py:253<br>ormantism/expressions/table.py |
| ormantism.table.Hydratable._suspend_validation | Replace __init__/__setattr__ so instances can be built without validation | ormantism/table/hydratable.py:118 | ormantism/table/hydratable.py:22 |
| ormantism.table.Hydratable._resume_validation | Restore normal __init__/__setattr__ after _suspend_validation | ormantism/table/hydratable.py:131 | ormantism/table/hydratable.py:24 |
| ormantism.table.Hydratable.make_empty_instance | Build an empty instance with given id (for hydration) | ormantism/table/hydratable.py:18 | ormantism/table/hydratable.py:100<br>ormantism/table/hydratable.py:106<br>ormantism/query.py:585<br>ormantism/column.py:278<br>ormantism/column.py:283<br>ormantism/column.py:295<br>ormantism/column.py:296 |
| ormantism.table.Hydratable.rearrange_data_for_hydration | Rearrange joined rows into nested structure for hydration | ormantism/table/hydratable.py:27 | ormantism/table/hydratable.py:112<br>ormantism/query.py:583 |
| ormantism.table.Hydratable.integrate_data_for_hydration | Integrate rearranged data into this instance, mutating in place | ormantism/table/hydratable.py:76 | ormantism/table/hydratable.py:101<br>ormantism/table/hydratable.py:107<br>ormantism/table/hydratable.py:113<br>ormantism/query.py:586 |
| ormantism.table.Hydratable.hydrate_with | Hydrate this instance from unparsed row data | ormantism/table/hydratable.py:110 | ormantism/table/hydratable.py:116 |
| ormantism.table.Hydratable.make_instance | Build a new instance from unparsed row data | ormantism/table/hydratable.py:116 | |
| ormantism.table.TableMeta.__new__ | Metaclass __new__; builds _columns from model fields | ormantism/table/meta.py:26 | ormantism/table/base.py:28 |
| ormantism.transaction.Transaction.execute | Execute SQL within this transaction | ormantism/transaction.py:121 | ormantism/table/base.py:179 |
| ormantism.transaction.TransactionManager._get_connection | Get or create a connection for the current thread | ormantism/transaction.py:32 | ormantism/transaction.py:69 |
| ormantism.transaction.TransactionManager._get_transaction_level | Get current transaction nesting level | ormantism/transaction.py:40 | ormantism/transaction.py:48<br>ormantism/transaction.py:54<br>ormantism/transaction.py:144 |
| ormantism.transaction.TransactionManager._set_transaction_level | Set transaction nesting level | ormantism/transaction.py:44 | ormantism/transaction.py:56<br>ormantism/transaction.py:57 |
| ormantism.transaction.TransactionManager._increment_transaction_level | Increment transaction nesting level (savepoint) | ormantism/transaction.py:48 | ormantism/transaction.py:72 |
| ormantism.transaction.TransactionManager._decrement_transaction_level | Decrement transaction nesting level | ormantism/transaction.py:54 | ormantism/transaction.py:106 |
| ormantism.transaction.TransactionManager.transaction | Context manager for transactions with savepoint support | ormantism/transaction.py:64 | ormantism/transaction.py:175 |
