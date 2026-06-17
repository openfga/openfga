## Operator runbook

The v1.18.0 release ships the 008 goose migration that converts identifier columns to the `utf8mb4_bin` collation so that case-distinct OpenFGA identities (e.g. `user:Alice` vs `user:alice`) are stored and compared as distinct values:

- `008_collate_identifiers.sql` 
   — `tuple` (`object_type`, `relation`, `_user`, `condition_name`)
   — `changelog` (`object_type`, `object_id`, `relation`, `_user`, `condition_name`)
   — `authorization_model` (`type`)

MySQL does not support `LOCK = NONE` for collation changes, so the migration runs under `LOCK = SHARED`. At the MySQL layer this allows reads while blocking writes to the affected table, but in practice the OpenFGA server may become unavailable for both reads and writes under non-trivial write load: blocked write requests hold database connections until they time out (`--request-timeout`, default `3s`), and once those connections fill the pool (`--datastore-max-open-conns`, default `30`) every subsequent request — including reads — fails to acquire a connection. **Stop traffic at the load balancer (or otherwise drain writes) before applying the migration on a deployment with active write traffic.**

Runtime scales with table size. Benchmark on a staging copy before scheduling the production maintenance window.

A collation change forces MySQL to rebuild each table (`ALGORITHM=COPY`), so the server temporarily holds the original table plus the rebuilt copy on disk. Make sure each affected table has roughly its own size again in free space on the data volume before starting.

If draining traffic is not viable for your deployment, an online schema change tool such as [`gh-ost`](https://github.com/github/gh-ost) or [`pt-online-schema-change`](https://docs.percona.com/percona-toolkit/pt-online-schema-change.html) can apply the same `ALTER`s without taking write downtime by replicating into a shadow table and cutting over.

### Procedure

1. **Pre-flight on a staging copy.** Restore a recent production backup to a staging instance and run the migration there first. This verifies the migration applies cleanly and gives you an approximate runtime to size the production maintenance window around.
2. **Schedule a maintenance window** longer than the staging runtime to account for production variability.
3. **Drain traffic** at the load balancer.
4. **Take a fresh backup** of production immediately before applying the migration.
5. **Run the migration** 
   #### Binary
   ```bash
   ./openfga migrate --datastore-engine mysql --datastore-uri "$URI"
   ```

   #### Docker
   ```bash
   docker run --rm openfga/openfga:<patched-tag> migrate --datastore-engine mysql --datastore-uri "$URI"
   ```
   The migration runs to completion and exits; it is not a long-running server process.
6. **Verify:**
   ```sql
   SELECT version_id FROM goose_db_version ORDER BY id DESC LIMIT 1;  -- expect 10
   SHOW CREATE TABLE tuple\G                                          -- expect utf8mb4_bin on identifier cols
   SHOW CREATE TABLE changelog\G
   SHOW CREATE TABLE authorization_model\G
   ```
7. **Roll OpenFGA** to the patched release (deploy the new binary, or pull `openfga/openfga:<patched-tag>` and restart your containers), then restore traffic at the load balancer.

### Rollback

The Down migration restore case-insensitive collation and **re-introduce the vulnerability**. Do not roll back on production unless you fully understand the consequences.
