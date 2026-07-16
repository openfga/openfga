// Package pg adapts the ANSI query builder (pkg/storage/adapter/ansi) to PostgreSQL. It supplies
// two things to the ansi builder: a dialect that overrides the constructs where pg
// diverges from ANSI ($N placeholders and the `_user` subject view), and an executor
// that runs the rendered statement through the native pgx/v5 driver. The node algebra
// and the rest of the rendering live entirely in the ansi package.
//
// Construct a Builder with New (wrapping an existing *pgxpool.Pool) or Open (which opens
// one via pgx). The ordinal $N placeholders the ansi builder emits are PostgreSQL's
// native bind form, so they pass straight through pgx.
package pg

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/ansi"
)

// executor runs rendered statements against a *pgxpool.Pool using the native pgx driver.
// It is the seam the ansi builder calls on Query.Execute; rendering is entirely the ansi
// package's concern.
type executor struct {
	pool *pgxpool.Pool
}

// Query runs the rendered SQL against the pool. The $N placeholders the ansi builder
// emits are PostgreSQL's native ordinal form, so args bind in order.
//
// It runs under pgx's QueryExecModeExec, which still uses the extended protocol but infers
// each parameter's PostgreSQL type from the Go argument's type rather than from a server
// describe. The builder emits untyped literals — most notably the bare "SELECT $1" the
// check planner uses as an existence marker — for which the server has no column context to
// infer a type and so describes the parameter as text; pgx then cannot encode a Go int into
// text. Inferring the type from the Go value side-steps that: an int is sent as an integer,
// a string as text, []byte as bytea. The mode is passed per query so it holds regardless of
// how the caller configured the pool.
func (e *executor) Query(ctx context.Context, query string, args []any) (adapter.Rows, error) {
	queryArgs := append([]any{pgx.QueryExecModeExec}, args...)
	rows, err := e.pool.Query(ctx, query, queryArgs...)
	if err != nil {
		return nil, err
	}
	return rowCursor{rows}, nil
}

// rowCursor adapts pgx.Rows to adapter.Rows. The shapes match except for Close: pgx
// returns nothing (and is safe to call repeatedly), so the adapter reports a nil error.
type rowCursor struct {
	pgx.Rows
}

func (r rowCursor) Close() error {
	r.Rows.Close()
	return nil
}

// New returns a Builder that renders PostgreSQL and executes through the supplied pool.
// The caller owns the pool's lifecycle (e.g. via Open).
func New(pool *pgxpool.Pool) adapter.Builder {
	return ansi.New(&executor{pool: pool}, ansi.WithDialect(dialect{}))
}

// Open opens a pgx connection pool against connString and returns a Builder over it
// together with the pool, which the caller is responsible for closing.
func Open(ctx context.Context, connString string) (adapter.Builder, *pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, nil, err
	}
	return New(pool), pool, nil
}
