# Reference

## Code

### Classes

| available as | inherits from | defined at | used at |
|--------------|---------------|------------|---------|
| ormantism.field.Field | builtins.object | ormantism/field.py:29 | ormantism/field.py:90<br>ormantism/table.py:246 |
| ormantism.join_info.JoinInfo | pydantic.BaseModel | ormantism/join_info.py:15 | ormantism/join_info.py:26<br>ormantism/table.py:412 |
| ormantism.table.Table | | ormantism/table.py:83 | ormantism/join_info.py:148<br>ormantism/table.py:233 |
| ormantism.table.TableMeta | pydantic._internal._model_construction.ModelMetaclass | ormantism/table.py:39 | ormantism/table.py:83 |
| ormantism.table._WithCreatedAtTimestamp | ormantism.utils.supermodel.SuperModel | ormantism/table.py:26 | ormantism/table.py:32<br>ormantism/table.py:56<br>ormantism/table.py:83 |
| ormantism.table._WithPrimaryKey | ormantism.utils.supermodel.SuperModel | ormantism/table.py:20 | ormantism/table.py:52<br>ormantism/table.py:83 |
| ormantism.table._WithSoftDelete | ormantism.utils.supermodel.SuperModel | ormantism/table.py:23 | ormantism/table.py:32<br>ormantism/table.py:35<br>ormantism/table.py:58<br>ormantism/table.py:83 |
| ormantism.table._WithTimestamps | ormantism.table._WithCreatedAtTimestamp<br>ormantism.table._WithSoftDelete<br>ormantism.table._WithUpdatedAtTimestamp | ormantism/table.py:32 | ormantism/table.py:58<br>ormantism/table.py:83 |
| ormantism.table._WithUpdatedAtTimestamp | ormantism.utils.supermodel.SuperModel | ormantism/table.py:29 | ormantism/table.py:32<br>ormantism/table.py:54<br>ormantism/table.py:83 |
| ormantism.table._WithVersion | ormantism.table._WithSoftDelete | ormantism/table.py:35 | ormantism/table.py:60<br>ormantism/table.py:83 |
| ormantism.transaction.Transaction | builtins.object | ormantism/transaction.py:110 | ormantism/transaction.py:77 |
| ormantism.transaction.TransactionError | builtins.Exception | ormantism/transaction.py:11 | ormantism/transaction.py:134<br>ormantism/transaction.py:146 |
| ormantism.transaction.TransactionManager | builtins.object | ormantism/transaction.py:16 | ormantism/transaction.py:166 |
| ormantism.utils.supermodel.SuperModel | pydantic.BaseModel | ormantism/utils/supermodel.py:97 | ormantism/table.py:20<br>ormantism/table.py:23<br>ormantism/table.py:26<br>ormantism/table.py:29<br>ormantism/table.py:32<br>ormantism/table.py:35 |

### Methods

| method path | defined at | used at |
|-------------|------------|---------|
| ormantism.field.Field.sql_is_json | ormantism/field.py:41 | ormantism/table.py:431 |
| ormantism.field.Field.reference_type | ormantism/field.py:47 | ormantism/join_info.py:24<br>ormantism/join_info.py:26<br>ormantism/join_info.py:66 |
| ormantism.field.Field.column_name | ormantism/field.py:56 | ormantism/field.py:149<br>ormantism/field.py:151<br>ormantism/field.py:153<br>ormantism/field.py:155<br>ormantism/field.py:157<br>ormantism/join_info.py:58<br>ormantism/table.py:257<br>ormantism/table.py:340 |
| ormantism.field.Field.column_base_type | ormantism/field.py:63 | ormantism/field.py:148<br>ormantism/field.py:149<br>ormantism/field.py:150<br>ormantism/field.py:152<br>ormantism/field.py:154<br>ormantism/field.py:155<br>ormantism/field.py:157 |
| ormantism.field.Field.from_pydantic_info | ormantism/field.py:69 | ormantism/table.py:246 |
| ormantism.field.Field.sql_creations | ormantism/field.py:99 | ormantism/table.py:320<br>ormantism/table.py:343 |
| ormantism.field.Field.serialize | ormantism/field.py:168 | ormantism/field.py:106<br>ormantism/table.py:141<br>ormantism/table.py:391 |
| ormantism.field.Field.parse | ormantism/field.py:183 | ormantism/join_info.py:143<br>ormantism/table.py:297 |
| ormantism.join_info.JoinInfo.add_children | ormantism/join_info.py:19 | ormantism/join_info.py:28<br>ormantism/table.py:415 |
| ormantism.join_info.JoinInfo.get_tables_statements | ormantism/join_info.py:30 | ormantism/join_info.py:37<br>ormantism/table.py:421 |
| ormantism.join_info.JoinInfo.get_columns | ormantism/join_info.py:39 | ormantism/join_info.py:67<br>ormantism/join_info.py:70<br>ormantism/join_info.py:78 |
| ormantism.join_info.JoinInfo.get_columns_statements | ormantism/join_info.py:69 | ormantism/table.py:419 |
| ormantism.join_info.JoinInfo.get_data | ormantism/join_info.py:73 | ormantism/join_info.py:153 |
| ormantism.join_info.JoinInfo.get_instance | ormantism/join_info.py:86 | ormantism/table.py:465<br>ormantism/table.py:472 |
| ormantism.utils.supermodel.SuperModel.model_dump | ormantism/utils/supermodel.py:150 | ormantism/table.py:388<br>ormantism/utils/make_hashable.py:14<br>ormantism/utils/serialize.py:19<br>ormantism/utils/supermodel.py:267<br>ormantism/utils/supermodel.py:276<br>ormantism/utils/supermodel.py:291 |
| ormantism.utils.supermodel.SuperModel.trigger | ormantism/utils/supermodel.py:229 | ormantism/utils/supermodel.py:119<br>ormantism/utils/supermodel.py:146<br>ormantism/utils/supermodel.py:217<br>ormantism/utils/supermodel.py:225 |
| ormantism.utils.supermodel.SuperModel.on_before_create | ormantism/utils/supermodel.py:245 | ormantism/utils/supermodel.py:240 |
| ormantism.utils.supermodel.SuperModel.on_after_create | ormantism/utils/supermodel.py:248 | ormantism/utils/supermodel.py:240 |
| ormantism.utils.supermodel.SuperModel.on_before_update | ormantism/utils/supermodel.py:251 | ormantism/utils/supermodel.py:240 |
| ormantism.utils.supermodel.SuperModel.on_after_update | ormantism/utils/supermodel.py:254 | ormantism/utils/supermodel.py:240 |
| ormantism.utils.supermodel.SuperModel.update | ormantism/utils/supermodel.py:202 | ormantism/utils/supermodel.py:199<br>ormantism/table.py:230 |
| ormantism.table.Table.on_after_create | ormantism/table.py:100 | ormantism/utils/supermodel.py:240 |
| ormantism.table.Table.on_before_update | ormantism/table.py:157 | ormantism/utils/supermodel.py:240 |
| ormantism.table.Table._has_field | ormantism/table.py:239 | ormantism/table.py:431 |
| ormantism.table.Table._get_fields | ormantism/table.py:244 | ormantism/table.py:136<br>ormantism/table.py:253<br>ormantism/table.py:266<br>ormantism/table.py:287<br>ormantism/table.py:306<br>ormantism/table.py:321<br>ormantism/table.py:327<br>ormantism/table.py:339<br>ormantism/table.py:525<br>ormantism/join_info.py:42<br>ormantism/join_info.py:91 |
| ormantism.table.Table._get_field | ormantism/table.py:252 | ormantism/table.py:207<br>ormantism/table.py:227<br>ormantism/table.py:296<br>ormantism/table.py:368<br>ormantism/table.py:431<br>ormantism/join_info.py:21<br>ormantism/join_info.py:61 |
| ormantism.table.Table._get_non_default_fields | ormantism/table.py:263 | |
| ormantism.table.Table._execute | ormantism/table.py:273 | ormantism/table.py:292<br>ormantism/table.py:333<br>ormantism/table.py:337<br>ormantism/table.py:348<br>ormantism/table.py:397<br>ormantism/table.py:399<br>ormantism/table.py:463<br>ormantism/table.py:469 |
| ormantism.table.Table._execute_returning | ormantism/table.py:284 | ormantism/table.py:150<br>ormantism/table.py:187 |
| ormantism.table.Table._create_table | ormantism/table.py:303 | ormantism/table.py:275<br>ormantism/table.py:310 |
| ormantism.table.Table._add_columns | ormantism/table.py:336 | ormantism/table.py:276 |
| ormantism.table.Table.check_read_only | ormantism/table.py:353 | ormantism/table.py:106<br>ormantism/table.py:162 |
| ormantism.table.Table.process_data | ormantism/table.py:362 | ormantism/table.py:125<br>ormantism/table.py:164<br>ormantism/table.py:405 |
| ormantism.table.Table.delete | ormantism/table.py:395 | |
| ormantism.table.Table.load | ormantism/table.py:403 | ormantism/table.py:199<br>ormantism/table.py:476<br>ormantism/table.py:512<br>ormantism/table.py:514<br>ormantism/join_info.py:109<br>ormantism/join_info.py:131 |
| ormantism.table.Table.load_all | ormantism/table.py:475 | |
| ormantism.table.Table.load_or_create | ormantism/table.py:192 | |
| ormantism.table.Table._get_table_name | ormantism/table.py:481 | ormantism/utils/get_table_by_name.py:13<br>ormantism/table.py:109<br>ormantism/table.py:145<br>ormantism/table.py:148<br>ormantism/table.py:175<br>ormantism/table.py:326<br>ormantism/table.py:332<br>ormantism/table.py:337<br>ormantism/table.py:348<br>ormantism/table.py:376<br>ormantism/table.py:381<br>ormantism/table.py:397<br>ormantism/table.py:399<br>ormantism/table.py:434<br>ormantism/table.py:436<br>ormantism/table.py:456<br>ormantism/join_info.py:32<br>ormantism/join_info.py:36<br>ormantism/join_info.py:41 |
| ormantism.table.Table._suspend_validation | ormantism/table.py:485 | ormantism/join_info.py:147 |
| ormantism.table.Table._resume_validation | ormantism/table.py:499 | ormantism/join_info.py:151 |
| ormantism.table.Table._add_lazy_loader | ormantism/table.py:507 | ormantism/table.py:527 |
| ormantism.table.Table._ensure_lazy_loaders | ormantism/table.py:522 | ormantism/table.py:410<br>ormantism/join_info.py:146 |
| ormantism.table.TableMeta.__new__ | ormantism/table.py:41 | ormantism/table.py:83 |
| ormantism.transaction.Transaction.execute | ormantism/transaction.py:118 | ormantism/table.py:279 |
| ormantism.transaction.TransactionManager._get_connection | ormantism/transaction.py:30 | ormantism/transaction.py:69 |
| ormantism.transaction.TransactionManager._get_transaction_level | ormantism/transaction.py:38 | ormantism/transaction.py:48<br>ormantism/transaction.py:54<br>ormantism/transaction.py:144 |
| ormantism.transaction.TransactionManager._set_transaction_level | ormantism/transaction.py:42 | ormantism/transaction.py:56<br>ormantism/transaction.py:57 |
| ormantism.transaction.TransactionManager._increment_transaction_level | ormantism/transaction.py:46 | ormantism/transaction.py:72 |
| ormantism.transaction.TransactionManager._decrement_transaction_level | ormantism/transaction.py:52 | ormantism/transaction.py:107 |
| ormantism.transaction.TransactionManager.transaction | ormantism/transaction.py:62 | ormantism/transaction.py:167 |
