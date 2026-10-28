// Package sqlite implements adapter.Querier for SQLite.
package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/query"
)

// ErrUnsupported reports a valid query construct that SQLite cannot express.
var ErrUnsupported = errors.New("sqlite: unsupported construct")

// New returns an adapter.Querier backed by db. The caller owns db's lifecycle.
func New(db *sql.DB) adapter.Querier {
	return &querier{db: db}
}

type querier struct {
	db *sql.DB
}

func (q *querier) Execute(ctx context.Context, stmt *query.Statement) (adapter.Rows, error) {
	sqlText, args, err := Render(stmt)
	if err != nil {
		return nil, err
	}
	rows, err := q.db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		// Avoid a non-nil adapter.Rows wrapping a nil *sql.Rows.
		return nil, err
	}
	return rows, nil
}

// Render returns stmt as SQLite SQL and its positional bind args, without executing it.
// Constructs SQLite cannot express return an error wrapping ErrUnsupported.
func Render(stmt *query.Statement) (sql string, args []any, err error) {
	r := &renderer{}
	r.selectStmt(&stmt.Select)
	if r.err != nil {
		return "", nil, r.err
	}
	return r.sb.String(), r.args, nil
}
