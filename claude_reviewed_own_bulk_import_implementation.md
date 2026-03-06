# Code Review: Bulk Import Feature

## Critical (Must Fix)

### C1. Arbitrary file read via path traversal
**`pkg/server/commands/importdata_reader.go:62-68`**

The `source` parameter from the user is passed directly to `os.Open()` with zero path validation. An attacker can read any file on the filesystem that the process has access to:

```go
func (localFileOpener) Open(u *url.URL) (*os.File, error) {
    path := u.Path
    return os.Open(path) // No validation!
}
```

Payload: `source: "file:///etc/shadow"` or `source: "/proc/self/environ"`. The file contents would be parsed as tuples and errors logged, potentially leaking content through error messages or side channels. Even if parsing fails, the file is opened and read.

**Fix**: Either (a) require an explicit allowlist/base directory and validate the resolved path stays within it, or (b) remove local file support entirely and only support object store URIs (S3, GCS) or inline data.

### C2. Missing authorization check on CreateImport
**`pkg/server/importdata.go:108-174`**

`CreateImport` has no `checkWriteAuthz` call, unlike every other write endpoint (`Write`, `WriteAuthorizationModel`, `WriteAssertions`). If access control is enabled, any authenticated caller can bulk-import tuples into any store, bypassing authorization entirely.

**Fix**: Add authorization check before creating the import record, mirroring the pattern in `pkg/server/write.go:49`. Note that `checkWriteAuthz` currently takes an `*openfgav1.WriteRequest` -- you'll need to either adapt it or call `checkAuthz` directly with `apimethod.Write`.

### C3. Postgres `GetImport` returns wrong error for not-found
**`pkg/storage/postgres/postgres.go:1533-1539`**

`GetImport` uses `s.primaryDB.QueryRow()` (`pgxpool.Pool`), which returns `pgx.ErrNoRows` on no results. But `HandleSQLError` (line 1382) checks `sql.ErrNoRows`:

```go
func HandleSQLError(err error, args ...interface{}) error {
    if errors.Is(err, sql.ErrNoRows) {  // Won't match pgx.ErrNoRows!
        return storage.ErrNotFound
    }
    ...
}
```

This means `GetImport` for a non-existent import returns a generic `"sql error: ..."` instead of `storage.ErrNotFound`, which the server layer at `importdata.go:194` relies on to return a `codes.NotFound` gRPC status. Callers will get a 500 instead of 404.

Note: The existing codebase has this same pattern inconsistency (line 1209 uses `pgx.ErrNoRows` directly for a different query). The `GetImport` method should check `pgx.ErrNoRows` explicitly, like the existing code at line 1209.

### C4. Phantom changelog entries for duplicate tuples
**`pkg/storage/sqlcommon/sqlcommon.go:1001-1082`, `pkg/storage/postgres/postgres.go:1413-1489`**

The tuple INSERT has `ON CONFLICT DO NOTHING`, but the changelog INSERT has no conflict handling. When a tuple already exists:
- Tuple insert: silently skipped
- Changelog entry: **written anyway**

This creates ghost WRITE changelog entries for tuples that weren't actually written. Downstream consumers of the changelog (cache invalidation, change feeds, replication) will process phantom writes, potentially causing incorrect behavior. The memory backend (`memory.go:918-933`) correctly skips duplicates for both, making it inconsistent with the SQL backends.

**Fix**: Either (a) use a CTE / `INSERT ... RETURNING` to track which tuples were actually inserted and only write changelog for those, or (b) accept the duplicates and document it as intentional (but this seems wrong for an import that may be retried).

---

## High (Should Fix)

### H1. Context lifetime allows import goroutine to outlive server shutdown intent
**`pkg/server/importdata.go:152`**

```go
importCtx, err := s.importManager.StartImport(context.Background(), importID)
```

Using `context.Background()` means if `Server.Close()` is called but `Shutdown()` is slow (e.g., a large batch is mid-write to the DB), the import can block server shutdown indefinitely. `Shutdown()` calls `m.wg.Wait()` with no timeout (line 72). If the datastore connection is closed before the import goroutine finishes (since `s.datastore.Close()` is called after `s.importManager.Shutdown()` on line 1029), this is technically safe, but there's no timeout on the wait.

**Fix**: Add a timeout to the shutdown wait, or use `context.WithTimeout` in `Shutdown()`.

### H2. `failImport` uses potentially-cancelled context
**`pkg/server/commands/importdata.go:166-170`**

```go
func (c *ImportDataCommand) failImport(ctx context.Context, importID, errMsg string) {
    c.datastore.UpdateImportStatus(ctx, c.storeID, importID, storage.ImportStatusFailed, errMsg)
}
```

If the context was cancelled (which triggers the `BulkWrite` error at line 94-97), `failImport` is called with that same cancelled context. The `UpdateImportStatus` call will fail, leaving the import stuck in "processing" status forever. `interruptImport` correctly handles this by using `context.Background()`, but `failImport` does not.

**Fix**: Use `context.Background()` (with a reasonable timeout) in `failImport`, same as `interruptImport`.

### H3. `imported` count includes duplicates
**`pkg/server/commands/importdata.go:102`**

```go
imported += int64(len(validTuples))
```

This counts all tuples sent to `BulkWrite`, not the ones actually inserted. With `ON CONFLICT DO NOTHING`, the real number of new tuples could be lower. The `TuplesImported` field in the status response is misleading -- it's really "tuples attempted" not "tuples imported".

**Fix**: Either rename to make the semantics clear, or have `BulkWrite` return the actual number of rows inserted (Postgres supports `INSERT ... RETURNING`, MySQL has `ROW_COUNT()`).

### H4. No foreign key on `imports.store` column
**All 3 migration files**

The `imports` table has no FK to `store`. If a store is deleted, orphaned import records remain. More importantly, there's no validation that the store exists when creating an import (beyond `resolveTypesystem` which checks the model, not the store directly).

### H5. No limit on import file size or tuple count
**`pkg/server/commands/importdata.go:64-129`**

There's no maximum on the file size or number of tuples. A user could point to a multi-GB file and the import would process indefinitely, consuming resources. Combined with only 3 concurrent import slots (max), this is a potential resource exhaustion vector.

**Fix**: Add a configurable max tuple count and/or max file size, and fail the import when exceeded.

### H6. Postgres `BulkWrite` duplicates sqlcommon logic instead of delegating
**`pkg/storage/postgres/postgres.go:1399-1492`**

The Postgres `BulkWrite` is a full reimplementation (~90 lines) of the same logic in `sqlcommon.BulkWrite`, instead of delegating to it like MySQL does. This creates a maintenance burden -- any fix to one must be mirrored in the other. The same applies to `CreateImport`, `GetImport`, `UpdateImportProgress`, and `UpdateImportStatus` (all reimplemented instead of delegating).

The reason is that Postgres uses `pgxpool.Pool` directly instead of `*sql.DB`, but this duplication means bugs (like C3 and C4) need to be fixed in two places.

---

## Lower (Fix Later)

### L1. Memory backend BulkWrite is O(n*m) for duplicate detection
**`pkg/storage/memory/memory.go:918-933`**

```go
for _, t := range writes {
    for _, et := range records {
        if match(et, t) { ... }
    }
}
```

Linear scan over all existing tuples for each new tuple. With large stores, this becomes very slow. Fine for tests/dev but worth a comment noting the limitation.

### L2. ULID entropy source not safe for concurrent goroutines
**`pkg/storage/sqlcommon/sqlcommon.go:983`, `pkg/storage/postgres/postgres.go:1407`**

```go
entropy := ulid.DefaultEntropy()
```

`ulid.DefaultEntropy()` returns a `math/rand`-based source that is not safe for concurrent use (though since it's created per-call, not shared, this is fine here -- just noting it in case this is refactored to be shared later).

### L3. Migration column type inconsistency
- Postgres: `store TEXT`, `model_id TEXT`
- MySQL: `store VARCHAR(26)`, `model_id VARCHAR(26)`
- SQLite: `store CHAR(26)`, `model_id CHAR(26)`

ULIDs are always 26 chars. Postgres should use `CHAR(26)` to match the existing table conventions and enable fixed-width storage optimization.

### L4. Reader test coverage gaps
**`pkg/server/commands/importdata_reader_test.go`**

Missing tests for:
- JSON format with malformed data
- JSON format with batching (large enough to trigger multiple batches)
- Conditions in JSON format (only tested in JSONL)
- File with only whitespace/blank lines

### L5. No `ImportDataCommand` unit tests
**`pkg/server/commands/importdata.go`**

The core import processing logic (`Run`, `validateBatch`, `failImport`, `interruptImport`) has no tests. The reader has tests, but the orchestration logic doesn't. This is the most complex part of the feature.

### L6. `errCh` race in `Run`
**`pkg/server/commands/importdata.go:112-120`**

```go
select {
case err := <-errCh:
    if err != nil { ... }
default:
}
```

After `batchCh` is drained, the `default` case means the error check is non-blocking. If the reader goroutine is still closing and hasn't sent the error yet, this races. The reader closes `errCh` after `batchCh`, but there's a small window. A safer pattern would be a blocking `err := <-errCh` since the channel is guaranteed to close.

### L7. Hardcoded `"server shutdown"` in `interruptImport`
**`pkg/server/commands/importdata.go:177`**

The interrupt message is hardcoded to `"server shutdown"` but the context could be cancelled for other reasons. The message should reflect the actual reason if possible.

### L8. `IsExperimentalEnabled` is O(n) linear scan
**`pkg/server/importdata.go:213-219`**

This iterates over the experimentals slice on every call. With the current small number of flags this is negligible, but a `map[string]bool` would be more idiomatic. However, this is an existing pattern in the codebase so it's consistent.
