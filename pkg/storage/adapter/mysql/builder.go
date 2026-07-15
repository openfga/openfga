// Package mysql adapts the ANSI query builder (pkg/storage/adapter/internal/ansi) to
// MySQL. It supplies two things to the ansi builder: a dialect that overrides the one
// construct where MySQL diverges from ANSI (the `_user` subject view, decoded with
// SUBSTRING_INDEX / SUBSTRING / LOCATE), and an executor that runs the rendered statement
// through the standard database/sql driver. MySQL's bind placeholder is "?", the ANSI
// default, so the placeholders the ansi builder emits pass straight through. The node
// algebra and the rest of the rendering live entirely in the ansi package.
//
// Construct a Builder with New (wrapping an existing *sql.DB) or Open (which opens one via
// the go-sql-driver/mysql driver).
package mysql

import (
	"context"
	"database/sql"

	// Register the go-sql-driver/mysql driver under the "mysql" name for database/sql.
	_ "github.com/go-sql-driver/mysql"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/internal/ansi"
)

// executor runs rendered statements against a *sql.DB using the standard database/sql
// MySQL driver. It is the seam the ansi builder calls on Query.Execute; rendering is
// entirely the ansi package's concern.
type executor struct {
	db *sql.DB
}

// Query runs the rendered SQL against the database. The "?" placeholders the ansi builder
// emits are MySQL's native positional form, so args bind in order. *sql.Rows already
// satisfies adapter.Rows, so the cursor is returned directly.
func (e *executor) Query(ctx context.Context, query string, args []any) (adapter.Rows, error) {
	return e.db.QueryContext(ctx, query, args...)
}

// New returns a Builder that renders MySQL and executes through the supplied database
// handle. The caller owns the handle's lifecycle (e.g. via Open).
func New(db *sql.DB) adapter.Builder {
	return ansi.New(&executor{db: db}, ansi.WithDialect(dialect{}))
}

// Open opens a database handle against dataSourceName using the go-sql-driver/mysql driver
// and returns a Builder over it together with the handle, which the caller is responsible
// for closing.
func Open(dataSourceName string) (adapter.Builder, *sql.DB, error) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, nil, err
	}
	return New(db), db, nil
}
