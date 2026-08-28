// Package sqlite is a self-contained adapter.Querier for SQLite. It renders a pre-built
// *query.Statement to SQLite SQL by walking the shared ast tree, and runs it against a
// *sql.DB backed by the modernc.org/sqlite driver.
//
// It owns its rendering end to end: it consumes the SHARED ast tree (walked via the
// *query.Statement handed to Render) but shares none of the node algebra or schema
// assumptions of the other SQL adapters. That independence lets it lean on SQLite's own
// dialect — and, crucially, on SQLite's physical schema, which is where it diverges most:
//   - the subject is stored as THREE DISCRETE columns (user_object_type / user_object_id /
//     user_relation), not a packed `_user` column, so the subject view is a plain column
//     reference with none of the string surgery MySQL and PostgreSQL need (see mapping.go);
//   - an aggregate FILTER (WHERE ...) is emitted NATIVELY — SQLite has supported it since
//     3.30, so a filtered aggregate needs no CASE emulation (the rewrite MySQL is forced into);
//   - a bare OFFSET is written as "LIMIT -1 OFFSET n", SQLite's own idiom for an unbounded
//     limit, rather than a magic sentinel row count;
//   - placeholders are always "?", and casts and JSON constructors carry SQLite's spelling
//     (TEXT/INTEGER/REAL/BLOB, json_object, json_array).
//
// A bound set is still expanded to IN(...): SQLite, like MySQL, has no array operand.
//
// Per the all-or-nothing capability rule, the tightened AST carries only constructs every
// supported backend can express, so SQLite renders the ENTIRE surface — there is no construct
// it rejects. Render therefore has no error return; the panics in the node walk are reserved
// for tree corruption (an unknown node kind), a programming error that must crash rather than
// be reported as a capability gap.
package sqlite

import (
	"context"
	"database/sql"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/query"
)

// New returns an adapter.Querier that renders typed query.Statements to SQLite SQL and runs
// them against db. The caller owns db's lifecycle.
func New(db *sql.DB) adapter.Querier {
	return &querier{db: db}
}

// querier renders statements to SQLite SQL and executes them against a database handle.
type querier struct {
	db *sql.DB
}

// Execute renders stmt to SQLite SQL and runs it. The "?" placeholders Render emits are
// SQLite's native positional form, so args bind in order, and *sql.Rows already satisfies
// adapter.Rows, so the cursor is returned directly.
func (q *querier) Execute(ctx context.Context, stmt *query.Statement) (adapter.Rows, error) {
	sqlText, args := Render(stmt)
	rows, err := q.db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		// Return a nil interface, not a non-nil adapter.Rows wrapping a nil *sql.Rows.
		return nil, err
	}
	return rows, nil
}

// Render renders a statement to SQLite SQL text and positional bind arguments by walking the
// embedded ast.Select. It is exported so callers can render without a DB — to log or inspect
// the rendered statement, and for tests that assert on SQL text rather than run it.
func Render(stmt *query.Statement) (sql string, args []any) {
	r := &renderer{}
	r.selectStmt(&stmt.Select)
	return r.sb.String(), r.args
}
