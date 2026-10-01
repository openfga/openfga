// Package sqlite is a self-contained adapter.Querier for SQLite. It renders a pre-built
// *query.Statement to SQLite SQL by walking the shared ast tree, and runs it against a
// *sql.DB backed by the modernc.org/sqlite driver.
//
// SQLite-specific rendering choices:
//   - the subject is stored as three discrete columns (user_object_type / user_object_id /
//     user_relation), not a packed `_user` column, so the subject view is a plain column
//     reference with none of the string surgery MySQL and PostgreSQL need (see mapping.go);
//   - an aggregate FILTER (WHERE ...) is emitted natively (supported since 3.30), so a filtered
//     aggregate needs no CASE emulation;
//   - a bare OFFSET is written as "LIMIT -1 OFFSET n", SQLite's own idiom for an unbounded
//     limit, rather than a magic sentinel row count;
//   - a bound set is expanded to IN(...); like MySQL, SQLite has no array operand;
//   - placeholders are always "?", and casts and JSON constructors carry SQLite's spelling
//     (TEXT/INTEGER/REAL/BLOB, json_object, json_array).
//
// The AST carries only constructs every supported backend can express, so SQLite renders the
// entire surface and Render has no error return; the node-walk panics are reserved for tree
// corruption (an unknown node kind).
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
