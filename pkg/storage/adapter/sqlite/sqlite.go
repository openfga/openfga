// Package sqlite is a self-contained adapter.Querier for SQLite. It renders a pre-built
// *query.Statement to SQLite SQL by walking the shared ast tree, and runs it against a
// *sql.DB backed by the modernc.org/sqlite driver.
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
