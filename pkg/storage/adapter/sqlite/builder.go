// Package sqlite adapts the ANSI query builder (pkg/storage/adapter/internal/ansi) to
// SQLite, backed by an in-memory database. It supplies two things to the ansi builder: a
// dialect that overrides the one construct where SQLite's physical schema diverges from
// the packed-`_user` layout the ANSI core assumes (the subject view, which maps to the
// split user_object_type / user_object_id / user_relation columns), and an executor that
// runs the rendered statement through the modernc.org/sqlite driver. SQLite's bind
// placeholder is "?", the ANSI default, so the placeholders the ansi builder emits pass
// straight through. The node algebra and the rest of the rendering live entirely in the
// ansi package.
//
// Construct a Builder with New (wrapping an existing *sql.DB) or Open, which opens a fresh
// database at the given DSN (e.g. ":memory:"). An in-memory SQLite database is scoped to
// its connection, so Open pins the pool to a single connection: every query in the
// returned Builder then sees the same database rather than a per-connection empty one.
package sqlite

import (
	"context"
	"database/sql"

	// Register the modernc.org/sqlite driver under the "sqlite" name for database/sql.
	_ "modernc.org/sqlite"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/internal/ansi"
)

// executor runs rendered statements against a *sql.DB using the standard database/sql
// SQLite driver. It is the seam the ansi builder calls on Query.Execute; rendering is
// entirely the ansi package's concern.
type executor struct {
	db *sql.DB
}

// Query runs the rendered SQL against the database. The "?" placeholders the ansi builder
// emits are SQLite's native positional form, so args bind in order. *sql.Rows already
// satisfies adapter.Rows, so the cursor is returned directly.
func (e *executor) Query(ctx context.Context, query string, args []any) (adapter.Rows, error) {
	return e.db.QueryContext(ctx, query, args...)
}

// New returns a Builder that renders SQLite and executes through the supplied database
// handle. The caller owns the handle's lifecycle (e.g. via Open).
func New(db *sql.DB) adapter.Builder {
	return ansi.New(&executor{db: db}, ansi.WithDialect(dialect{}))
}

// Open opens a fresh SQLite database at dsn (e.g. ":memory:") and returns a Builder over it
// together with the handle, which the caller is responsible for closing.
//
// Because a ":memory:" database belongs to the single connection that created it, the pool
// is pinned to one connection (SetMaxOpenConns(1)); otherwise database/sql could hand out a
// second connection backed by a different, empty in-memory database.
func Open(dsn string) (adapter.Builder, *sql.DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, nil, err
	}
	// An in-memory database lives only for the life of its connection; pin the pool to one
	// so every query shares the same database.
	db.SetMaxOpenConns(1)
	return New(db), db, nil
}
