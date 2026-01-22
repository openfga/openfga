# DSQL Migrations for OpenFGA

These migrations are adapted from the PostgreSQL migrations for Aurora DSQL compatibility.

## Key Differences from PostgreSQL Migrations

1. **ASYNC Indexes**: All `CREATE INDEX` statements use `CREATE INDEX ASYNC` (DSQL requirement)
2. **NO TRANSACTION**: All migrations use `-- +goose NO TRANSACTION` since DSQL DDL is always non-transactional
3. **No SERIAL/IDENTITY**: The goose version table uses epoch microseconds instead of auto-increment
4. **No Partial Indexes**: DSQL doesn't support partial indexes, so full indexes are used

## Migration Files

| File | Description |
|------|-------------|
| 001_initialize_schema.sql | Creates core tables (tuple, authorization_model, store, assertion, changelog) |
| 002_add_authorization_model_version.sql | Adds schema_version column |
| 003_add_reverse_lookup_index.sql | Adds reverse lookup index |
| 004_add_authorization_model_serialized_protobuf.sql | Adds serialized_protobuf column |
| 005_add_conditions_to_tuples.sql | Adds condition columns to tuple and changelog |
| 006_add_collate_index.sql | Adds user lookup index with C collation |

## Future Consideration: Splitting Migrations

Per DSQL best practices, each migration should ideally contain only one DDL statement. Currently, migrations 002 and 005 contain multiple statements:

- **002**: ALTER TABLE + UPDATE (mixed DDL and DML)
- **005**: Four ALTER TABLE statements

While these work correctly with `-- +goose NO TRANSACTION` (goose executes each statement separately), consider splitting them into individual migration files if you encounter OCC errors during migration. For example:

```
005a_add_condition_name_to_tuple.sql
005b_add_condition_context_to_tuple.sql
005c_add_condition_name_to_changelog.sql
005d_add_condition_context_to_changelog.sql
```

See [DSQL Development Guide](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/) for more information on DDL best practices.
