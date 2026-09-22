// Package pg is a self-contained adapter.Querier for PostgreSQL. It renders a pre-built
// *query.Statement to PostgreSQL SQL by walking the shared ast tree, and runs it through the
// native pgx/v5 driver.
//
// PostgreSQL-specific rendering choices:
//   - placeholders are ordinal "$N";
//   - a bound set binds as one array parameter and compares with "= ANY ($N)" rather than
//     expanding to "IN (?, ?, ...)"; pgx encodes a Go slice as a PostgreSQL array;
//   - an aggregate FILTER (WHERE ...) is emitted natively — no CASE emulation;
//   - the packed `_user` subject column is decoded with split_part, and casts and JSON
//     constructors carry PostgreSQL's spelling (text/bytea, jsonb_build_object,
//     jsonb_build_array) (see mapping.go).
//
// The AST carries only constructs every supported backend can express, so PostgreSQL renders
// the entire surface and Render has no error return; the node-walk panics are reserved for
// tree corruption (an unknown node kind).
package pg

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/query"
)

// New returns an adapter.Querier that renders PostgreSQL SQL and runs it through the supplied
// pool. The caller owns the pool's lifecycle.
func New(pool *pgxpool.Pool) adapter.Querier {
	return &querier{pool: pool}
}

// querier renders statements to PostgreSQL SQL and executes them against a connection pool.
type querier struct {
	pool *pgxpool.Pool
}

// Execute renders stmt to PostgreSQL SQL and runs it against the pool, binding args in order
// via the native "$N" placeholders.
//
// It runs under pgx's QueryExecModeExec, which infers each parameter's type from the Go
// argument rather than from a server describe. Render emits untyped literals (notably the bare
// "SELECT $1" existence marker) that the server would otherwise describe as text, which fails
// to encode a Go int; inferring from the Go value side-steps that. The mode is passed per query
// so it holds regardless of pool configuration.
func (q *querier) Execute(ctx context.Context, stmt *query.Statement) (adapter.Rows, error) {
	sqlText, args := Render(stmt)
	queryArgs := append([]any{pgx.QueryExecModeExec}, args...)
	rows, err := q.pool.Query(ctx, sqlText, queryArgs...)
	if err != nil {
		return nil, err
	}
	return rowCursor{rows}, nil
}

// rowCursor adapts pgx.Rows to adapter.Rows. The shapes match except for Close: pgx returns
// nothing (and is safe to call repeatedly), so the adapter reports a nil error.
type rowCursor struct {
	pgx.Rows
}

func (r rowCursor) Close() error {
	r.Rows.Close()
	return nil
}

// Render renders a statement to PostgreSQL SQL text and positional bind arguments by walking
// the embedded ast.Select. It is exported so callers can render without a pool — to log or
// inspect the rendered statement, and for tests that assert on SQL text rather than run it.
func Render(stmt *query.Statement) (sql string, args []any) {
	r := &renderer{}
	r.selectStmt(&stmt.Select)
	return r.sb.String(), r.args
}
