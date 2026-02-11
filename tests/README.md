# Tests

This directory contains the test suite for ormantism. Tests are grouped by subdirectory according to the area of the codebase they cover.

**Please update this README whenever you add, remove, or significantly change test files or their scope.**

---

## column

Tests for `ormantism.column`: `Column` metadata, SQL generation, serialization, and parsing.

### column/test_field_parse.py

Tests for `Column.parse()`: converting database values back to Python types. Covers JSON, set, tuple, dict, list, enum, BaseModel, scalars (int, float, str, bool), type-from-schema, and error cases.

### column/test_field_reference_and_from_pydantic.py

Tests for `Column.reference_type`, `column_base_type`, and `Column.from_pydantic_info()`: building `Column` instances from Pydantic field info and resolving reference vs non-reference types.

### column/test_field_serialize.py

Tests for `Column.serialize()` and column hashability: converting Python values to database-ready form (IDs for refs, JSON, type schemas, scalars), and using `Column` instances in sets/dicts.

### column/test_field_sql_creations.py

Tests for `Column.sql_creations`: SQL column definitions for references (scalar and list), enums, JSON, BaseModel, and unsupported-type errors.

### column/test_json.py

Tests for the ormantism `JSON` column type and persistence: tables with `JSON`-annotated fields and updating/storing JSON values.

### column/test_type_fields.py

Tests for Table columns with `type` (e.g. `type[BaseModel]`) annotations and their persistence.

---

## connection

Tests for `ormantism.connection`: database connection and URL handling.

### connection/test_connection.py

Tests for `connect()`, `_get_connection()`, and database URL handling: validation of URL type (str or callable), connection lifecycle, and name/default behaviour.

---

## expressions

Tests for `ormantism.expressions`: expression types, operators, and integration with tables.

### expressions/test_column_and_order_expression.py

Tests for `ColumnExpression` and `OrderExpression`: SQL and values for column expressions, and order-by (asc/desc) behaviour.

### expressions/test_expression_base.py

Tests for the base expression layer: `Expression`, `ALIAS_SEPARATOR`, `ArgumentedExpression`, and the SQL/values of `FunctionExpression`, `UnaryOperatorExpression`, and `NaryOperatorExpression`.

### expressions/test_expression_operators.py

Tests for expression operator overloads: equality, `in_`, `is_null`, and/or, arithmetic, `NOT`, `collect_join_paths_from_expression`, and binding of `Table` instances in expressions.

### expressions/test_expression_table_integration.py

Tests for expression integration with Table: class-level column attributes and `__getattr__` chaining from the root table expression.

### expressions/test_table_expression.py

Tests for `TableExpression`: root and joined aliases, `get_column_expression` (scalar vs reference), `sql_declarations` (FROM/JOIN), and error cases (empty path, base `Table` alias).

---

## query

Tests for `ormantism.query`: Query API, SQL building, and instance hydration.

### query/test_query.py

Tests for the main query API: `Query`, `Query.ensure_table_structure`, select/where/order/limit, update/delete, `instance_from_row`, versioning, polymorphic refs, and coverage-oriented helpers.

### query/test_query_join.py

Tests for Query JOIN building: join tree construction, SQL FROM/JOIN clauses, column aliases, and list-reference lazy paths.

---

## table

Tests for `ormantism.table`: Table CRUD, schema, lifecycle, and relationships.

### table/test_foreign_key.py

Tests for Table foreign key fields: specific refs, generic (polymorphic) refs, list refs, and preload vs lazy loading behaviour.

### table/test_table_crud_and_relationships.py

Tests for Table CRUD, timestamps, relationships, lazy loading, and versioning: basic create/load, refs, and versioned tables.

### table/test_table_lifecycle.py

Tests for Table lifecycle hooks and `load_or_create`: `on_after_create`, `on_before_update`, and `load_or_create` (search fields, updates, reference handling).

### table/test_table_metadata_and_equality.py

Tests for Table metadata and equality: `_get_fields`, `_get_table_name`, `_get_field`, options inheritance (e.g. versioning_along), `__eq__`, `__hash__`, and `__deepcopy__`.

### table/test_table_schema_and_delete.py

Tests for Table schema and delete: `_create_table`, `_add_columns`, `process_data` (refs, list refs, BaseModel), `delete()` (soft vs hard), and `load()` ordering (versioned and non-versioned).

---

## transaction

Tests for `ormantism.transaction`: transaction context and errors.

### transaction/test_transaction.py

Tests for `TransactionManager`, the transaction context manager, and `TransactionError`: execution after exit, rollback on exception, and commit behaviour.

---

## utils

Tests for `ormantism.utils`: helpers used by column, table, and query code.

### utils/test_find_subclass.py

Tests for `ormantism.utils.find_subclass` and `_get_subclasses`: finding a subclass by name in a hierarchy.

### utils/test_get_base_type.py

Tests for `ormantism.utils.get_base_type` and `get_container_base_type`: resolving the base type from annotations and containers.

### utils/test_is_type_annotation.py

Tests for `ormantism.utils.is_type_annotation`: detecting type annotations (bare types, Optional, Union, etc.).

### utils/test_make_hashable.py

Tests for `ormantism.utils.make_hashable`: converting nested structures to hashable form for use in sets/caches.

### utils/test_rebuild_pydantic_model.py

Tests for `ormantism.utils.rebuild_pydantic_model`: `get_field_type` and `rebuild_pydantic_model` for dynamic Pydantic model building from JSON schema.

### utils/test_resolve_type.py

Tests for `ormantism.utils.resolve_type`: resolving forward references to Table subclasses.

### utils/test_serialize.py

Tests for `ormantism.utils.serialize`: recursive serialization of nested structures to JSON-serializable types.

### utils/test_supermodel.py

Tests for `ormantism.utils.supermodel`: `SuperModel`, `to_json_schema`, and `from_json_schema` for dynamic schema and model handling.
