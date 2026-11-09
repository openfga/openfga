// Package adapter defines how datastores execute query.Statement values.
package adapter

import (
	"context"

	"github.com/openfga/openfga/pkg/storage/adapter/query"
)

// Querier executes a query.Statement. Implementations must support every statement, so Execute
// never returns an "unsupported" error.
type Querier interface {
	Execute(ctx context.Context, stmt *query.Statement) (Rows, error)
}

// Rows is a driver-agnostic result cursor mirroring *sql.Rows.
type Rows interface {
	// Next returns false when rows are exhausted or on error; check Err.
	Next() bool
	// Scan copies the current row's columns into dest.
	Scan(dest ...any) error
	// Close releases the cursor's resources.
	Close() error
	// Err returns the error, if any, that stopped iteration.
	Err() error
}
