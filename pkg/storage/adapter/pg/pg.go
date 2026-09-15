// Package pg is a self-contained adapter.Querier for PostgreSQL. It renders a pre-built
// *query.Statement to PostgreSQL SQL by walking the shared ast tree, and runs it through the
// native pgx/v5 driver.
//
// It owns its rendering end to end: it consumes the SHARED ast tree (walked via the
// *query.Statement handed to Render) but shares none of the node algebra with the other
// adapters. Owning the walk lets it lean on PostgreSQL's own dialect rather than a
// lowest-common-denominator one:
//   - placeholders are ordinal "$N";
//   - a bound set binds as ONE array parameter and compares with "= ANY ($N)", instead of
//     being expanded to "IN (?, ?, ...)"; pgx encodes a Go slice as a PostgreSQL array;
//   - an aggregate FILTER (WHERE ...) is emitted natively — no CASE emulation;
//   - the packed `_user` subject column is decoded with PostgreSQL's split_part, and casts and
//     JSON constructors carry PostgreSQL's spelling (text/bytea, jsonb_build_object,
//     jsonb_build_array) (see mapping.go).
//
// Per the all-or-nothing capability rule, the tightened AST carries only constructs every
// supported backend can express, so PostgreSQL renders the ENTIRE surface — there is no
// construct it rejects. Render therefore has no error return; the panics in the node walk are
// reserved for tree corruption (an unknown node kind), a programming error that must crash
// rather than be reported as a capability gap.
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

// Execute renders stmt to PostgreSQL SQL and runs it against the pool. The "$N" placeholders
// Render emits are PostgreSQL's native ordinal form, so args bind in order.
//
// It runs under pgx's QueryExecModeExec, which still uses the extended protocol but infers each
// parameter's PostgreSQL type from the Go argument's type rather than from a server describe.
// Render emits untyped literals — most notably the bare "SELECT $1" the check planner uses as
// an existence marker — for which the server has no column context to infer a type and so
// describes the parameter as text; pgx then cannot encode a Go int into text. Inferring the
// type from the Go value side-steps that: an int is sent as an integer, a string as text,
// []byte as bytea. The mode is passed per query so it holds regardless of how the pool was
// configured.
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
