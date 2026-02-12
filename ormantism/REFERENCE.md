# Reference

## Code

### Classes

| available as | inherits from | defined at | used at |
|--------------|---------------|------------|---------|
| ormantism.field.Field | builtins.object | ormantism/field.py:29 | ormantism/field.py:90<br>ormantism/table.py:297 |
| ormantism.join_info.JoinInfo | pydantic.BaseModel | ormantism/join_info.py:17 | ormantism/join_info.py:34<br>ormantism/table.py:519 |
| ormantism.table.Table | | ormantism/table.py:38 | ormantism/utils/get_table_by_name.py:10<br>ormantism/field.py:107<br>ormantism/table.py:379<br>ormantism/table.py:630 |
| ormantism.table.TableMeta | pydantic._internal._model_construction.ModelMetaclass | ormantism/table_meta.py:19 | ormantism/table.py:35 |
| ormantism.table._WithCreatedAtTimestamp | ormantism.utils.supermodel.SuperModel | ormantism/table_mixins.py:25 | ormantism/table_mixins.py:40<br>ormantism/table_meta.py:32<br>ormantism/table.py:32<br>ormantism/table.py:46<br>ormantism/table.py:73<br>ormantism/table.py:83 |
| ormantism.table._WithPrimaryKey | ormantism.utils.supermodel.SuperModel | ormantism/table_mixins.py:11 | ormantism/table_meta.py:28<br>ormantism/table.py:69<br>ormantism/table.py:83<br>ormantism/table.py:213<br>ormantism/table.py:385 |
| ormantism.table._WithSoftDelete | ormantism.utils.supermodel.SuperModel | ormantism/table_mixins.py:17 | ormantism/table_mixins.py:44<br>ormantism/table_mixins.py:49<br>ormantism/table.py:32<br>ormantism/table.py:35<br>ormantism/table.py:46<br>ormantism/table.py:77<br>ormantism/table.py:83<br>ormantism/table.py:494 |
| ormantism.table._WithTimestamps | ormantism.table._WithCreatedAtTimestamp<br>ormantism.table._WithSoftDelete<br>ormantism.table._WithUpdatedAtTimestamp | ormantism/table_mixins.py:40 | ormantism/table_meta.py:34<br>ormantism/table.py:75<br>ormantism/table.py:83<br>ormantism/table.py:534<br>ormantism/table.py:561 |
| ormantism.table._WithUpdatedAtTimestamp | ormantism.utils.supermodel.SuperModel | ormantism/table_mixins.py:31 | ormantism/table_mixins.py:40<br>ormantism/table_meta.py:30<br>ormantism/table.py:32<br>ormantism/table.py:46<br>ormantism/table.py:71<br>ormantism/table.py:83<br>ormantism/table.py:221 |
| ormantism.table._WithVersion | ormantism.table._WithSoftDelete | ormantism/table_mixins.py:44 | ormantism/table_meta.py:36<br>ormantism/table.py:76<br>ormantism/table.py:83<br>ormantism/table.py:133<br>ormantism/table.py:385<br>ormantism/table.py:494<br>ormantism/table.py:561<br>ormantism/table.py:563 |
| ormantism.transaction.Transaction | builtins.object | ormantism/transaction.py:110 | ormantism/transaction.py:79 |
| ormantism.transaction.TransactionError | builtins.Exception | ormantism/transaction.py:13 | ormantism/transaction.py:137<br>ormantism/transaction.py:154 |
| ormantism.transaction.TransactionManager | builtins.object | ormantism/transaction.py:16 | ormantism/transaction.py:174 |
| ormantism.utils.supermodel.SuperModel | pydantic.BaseModel | ormantism/utils/supermodel.py:97 | ormantism/table_mixins.py:11<br>ormantism/table_mixins.py:17<br>ormantism/table_mixins.py:25<br>ormantism/table_mixins.py:31<br>ormantism/table_mixins.py:40<br>ormantism/table_mixins.py:44<br>ormantism/field.py:90 |

### Methods

| method path | defined at | used at |
|-------------|------------|---------|
| ormantism.connection.connect | ormantism/connection.py:15 | |
| ormantism.connection._get_connection | ormantism/connection.py:45 | ormantism/transaction.py:172 |
| ormantism.transaction.transaction | ormantism/transaction.py:168 | ormantism/table.py:338 |
| ormantism.utils.find_subclass.find_subclass | ormantism/utils/find_subclass.py:13 | ormantism/utils/supermodel.py:65 |
| ormantism.utils.get_base_type.get_base_type | ormantism/utils/get_base_type.py:7 | ormantism/field.py:85<br>ormantism/utils/supermodel.py:132 |
| ormantism.utils.get_base_type.get_container_base_type | ormantism/utils/get_base_type.py:51 | ormantism/utils/get_base_type.py:47<br>ormantism/utils/get_base_type.py:48 |
| ormantism.utils.get_table_by_name.get_all_tables | ormantism/utils/get_table_by_name.py:7 | ormantism/utils/get_table_by_name.py:15 |
| ormantism.utils.get_table_by_name.get_table_by_name | ormantism/utils/get_table_by_name.py:13 | ormantism/join_info.py:120<br>ormantism/utils/resolve_type.py:13 |
| ormantism.utils.is_type_annotation.is_type_annotation | ormantism/utils/is_type_annotation.py:6 | ormantism/utils/is_type_annotation.py:21<br>ormantism/utils/supermodel.py:182<br>ormantism/utils/supermodel.py:237 |
| ormantism.utils.make_hashable.make_hashable | ormantism/utils/make_hashable.py:10 | ormantism/utils/make_hashable.py:21<br>ormantism/utils/make_hashable.py:27<br>ormantism/table.py:119<br>ormantism/field.py:194 |
| ormantism.utils.rebuild_pydantic_model.get_field_type | ormantism/utils/rebuild_pydantic_model.py:9 | ormantism/utils/rebuild_pydantic_model.py:23<br>ormantism/utils/rebuild_pydantic_model.py:31<br>ormantism/utils/rebuild_pydantic_model.py:61 |
| ormantism.utils.rebuild_pydantic_model.rebuild_pydantic_model | ormantism/utils/rebuild_pydantic_model.py:39 | ormantism/field.py:239<br>ormantism/utils/supermodel.py:70 |
| ormantism.utils.resolve_type.resolve_type | ormantism/utils/resolve_type.py:7 | ormantism/field.py:84 |
| ormantism.utils.serialize.serialize | ormantism/utils/serialize.py:8 | ormantism/utils/serialize.py:21<br>ormantism/utils/serialize.py:24<br>ormantism/utils/serialize.py:27<br>ormantism/field.py:208 |
| ormantism.utils.supermodel.to_json_schema | ormantism/utils/supermodel.py:19 | ormantism/field.py:207<br>ormantism/utils/supermodel.py:188 |
| ormantism.utils.supermodel.from_json_schema | ormantism/utils/supermodel.py:33 | ormantism/utils/supermodel.py:55<br>ormantism/utils/supermodel.py:87<br>ormantism/utils/supermodel.py:137<br>ormantism/field.py:240 |
| ormantism.field.Field.sql_is_json | ormantism/field.py:45 | ormantism/table.py:411 |
| ormantism.field.Field.reference_type | ormantism/field.py:55 | ormantism/join_info.py:28<br>ormantism/join_info.py:31<br>ormantism/join_info.py:82 |
| ormantism.field.Field.column_name | ormantism/field.py:65 | ormantism/field.py:143<br>ormantism/field.py:145<br>ormantism/field.py:147<br>ormantism/field.py:149<br>ormantism/field.py:151<br>ormantism/field.py:153<br>ormantism/field.py:155<br>ormantism/field.py:157<br>ormantism/join_info.py:73<br>ormantism/table.py:257<br>ormantism/table.py:340 |
| ormantism.field.Field.column_base_type | ormantism/field.py:73 | ormantism/field.py:166<br>ormantism/field.py:167<br>ormantism/field.py:168<br>ormantism/field.py:169<br>ormantism/field.py:170<br>ormantism/field.py:171<br>ormantism/field.py:172<br>ormantism/field.py:179 |
| ormantism.field.Field.from_pydantic_info | ormantism/field.py:80 | ormantism/table.py:296 |
| ormantism.field.Field.sql_creations | ormantism/field.py:121 | ormantism/table.py:386<br>ormantism/table.py:409 |
| ormantism.field.Field.serialize | ormantism/field.py:198 | ormantism/field.py:125<br>ormantism/table.py:179<br>ormantism/table.py:368 |
| ormantism.field.Field.parse | ormantism/field.py:210 | ormantism/join_info.py:162<br>ormantism/table.py:356 |
| ormantism.join_info.JoinInfo.add_children | ormantism/join_info.py:23 | ormantism/join_info.py:35<br>ormantism/table.py:404 |
| ormantism.join_info.JoinInfo.get_tables_statements | ormantism/join_info.py:38 | ormantism/join_info.py:46<br>ormantism/table.py:410 |
| ormantism.join_info.JoinInfo.get_columns | ormantism/join_info.py:50 | ormantism/join_info.py:87<br>ormantism/join_info.py:88<br>ormantism/join_info.py:96 |
| ormantism.join_info.JoinInfo.get_columns_statements | ormantism/join_info.py:86 | ormantism/table.py:408 |
| ormantism.join_info.JoinInfo.get_data | ormantism/join_info.py:91 | ormantism/join_info.py:172 |
| ormantism.join_info.JoinInfo.get_instance | ormantism/join_info.py:105 | ormantism/table.py:434<br>ormantism/table.py:441 |
| ormantism.utils.supermodel.SuperModel.model_dump | ormantism/utils/supermodel.py:157 | ormantism/table.py:365<br>ormantism/utils/make_hashable.py:16<br>ormantism/utils/serialize.py:20<br>ormantism/utils/supermodel.py:193<br>ormantism/utils/supermodel.py:204 |
| ormantism.utils.supermodel.SuperModel.trigger | ormantism/utils/supermodel.py:246 | ormantism/utils/supermodel.py:120<br>ormantism/utils/supermodel.py:151<br>ormantism/utils/supermodel.py:232<br>ormantism/utils/supermodel.py:241 |
| ormantism.utils.supermodel.SuperModel.on_before_create | ormantism/utils/supermodel.py:268 | ormantism/utils/supermodel.py:246 |
| ormantism.utils.supermodel.SuperModel.on_after_create | ormantism/utils/supermodel.py:271 | ormantism/utils/supermodel.py:246 |
| ormantism.utils.supermodel.SuperModel.on_before_update | ormantism/utils/supermodel.py:274 | ormantism/utils/supermodel.py:246 |
| ormantism.utils.supermodel.SuperModel.on_after_update | ormantism/utils/supermodel.py:277 | ormantism/utils/supermodel.py:246 |
| ormantism.utils.supermodel.SuperModel.update | ormantism/utils/supermodel.py:218 | ormantism/utils/supermodel.py:214<br>ormantism/table.py:279 |
| ormantism.table.Table.on_after_create | ormantism/table.py:52 | ormantism/utils/supermodel.py:246 |
| ormantism.table.Table.on_before_update | ormantism/table.py:129 | ormantism/utils/supermodel.py:246 |
| ormantism.table.Table._has_field | ormantism/table.py:215 | ormantism/table.py:408 |
| ormantism.table.Table._get_fields | ormantism/table.py:221 | ormantism/table.py:153<br>ormantism/table.py:300<br>ormantism/table.py:313<br>ormantism/table.py:332<br>ormantism/table.py:354<br>ormantism/table.py:366<br>ormantism/table.py:372<br>ormantism/table.py:384<br>ormantism/table.py:460<br>ormantism/join_info.py:53<br>ormantism/join_info.py:110 |
| ormantism.table.Table._get_field | ormantism/table.py:230 | ormantism/table.py:255<br>ormantism/table.py:274<br>ormantism/table.py:341<br>ormantism/table.py:403<br>ormantism/table.py:435<br>ormantism/join_info.py:25<br>ormantism/join_info.py:77 |
| ormantism.table.Table._get_non_default_fields | ormantism/table.py:245 | |
| ormantism.table.Table.check_read_only | ormantism/table.py:433 | ormantism/table.py:129<br>ormantism/table.py:205 |
| ormantism.table.Table.process_data | ormantism/table.py:372 | ormantism/table.py:152<br>ormantism/table.py:206<br>ormantism/table.py:406 |
| ormantism.table.Table.delete | ormantism/table.py:418 | |
| ormantism.table.Table.load | ormantism/table.py:507 | ormantism/table.py:246<br>ormantism/table.py:433<br>ormantism/table.py:469<br>ormantism/table.py:471<br>ormantism/join_info.py:127<br>ormantism/join_info.py:149 |
| ormantism.table.Table.load_all | ormantism/table.py:515 | |
| ormantism.table.Table.load_or_create | ormantism/table.py:236 | |
| ormantism.table.Table._get_table_name | ormantism/table.py:522 | ormantism/utils/get_table_by_name.py:16<br>ormantism/table.py:132<br>ormantism/table.py:183<br>ormantism/table.py:185<br>ormantism/table.py:186<br>ormantism/table.py:217<br>ormantism/table.py:344<br>ormantism/table.py:350<br>ormantism/table.py:355<br>ormantism/table.py:383<br>ormantism/table.py:388<br>ormantism/table.py:395<br>ormantism/table.py:413<br>ormantism/table.py:415<br>ormantism/table.py:443<br>ormantism/table.py:453<br>ormantism/table.py:462<br>ormantism/table.py:493<br>ormantism/table.py:500<br>ormantism/table.py:513<br>ormantism/table.py:519<br>ormantism/join_info.py:40<br>ormantism/join_info.py:44<br>ormantism/join_info.py:52 |
| ormantism.table.Table._suspend_validation | ormantism/table.py:601 | ormantism/join_info.py:165 |
| ormantism.table.Table._resume_validation | ormantism/table.py:542 | ormantism/join_info.py:169 |
| ormantism.table.Table._add_lazy_loader | ormantism/table.py:551 | ormantism/table.py:465 |
| ormantism.table.Table._ensure_lazy_loaders | ormantism/table.py:567 | ormantism/table.py:448<br>ormantism/join_info.py:164 |
| ormantism.table.TableMeta.__new__ | ormantism/table_meta.py:22 | ormantism/table.py:38 |
| ormantism.transaction.Transaction.execute | ormantism/transaction.py:121 | ormantism/table.py:339 |
| ormantism.transaction.TransactionManager._get_connection | ormantism/transaction.py:32 | ormantism/transaction.py:69 |
| ormantism.transaction.TransactionManager._get_transaction_level | ormantism/transaction.py:40 | ormantism/transaction.py:48<br>ormantism/transaction.py:54<br>ormantism/transaction.py:144 |
| ormantism.transaction.TransactionManager._set_transaction_level | ormantism/transaction.py:44 | ormantism/transaction.py:56<br>ormantism/transaction.py:57 |
| ormantism.transaction.TransactionManager._increment_transaction_level | ormantism/transaction.py:48 | ormantism/transaction.py:72 |
| ormantism.transaction.TransactionManager._decrement_transaction_level | ormantism/transaction.py:54 | ormantism/transaction.py:106 |
| ormantism.transaction.TransactionManager.transaction | ormantism/transaction.py:64 | ormantism/transaction.py:175 |
