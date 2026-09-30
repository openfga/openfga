// Package adapter declares the dialect-agnostic contract a datastore exposes for
// executing typed query.Statement values against its backend. A concrete implementation
// owns both rendering SQL for its dialect and executing it.
package adapter

import (
	"context"

	"github.com/openfga/openfga/pkg/storage/adapter/query"
)

// Querier executes a typed query.Statement and returns its result cursor.
//
// A backend returns a non-nil Querier only if it can render and run the entire query
// surface, so Execute returns only operational errors, never a capability/unsupported
// sentinel. A backend that cannot support the surface returns nil from
// RelationshipTupleReader.Querier.
type Querier interface {
	// Execute renders the statement for the backend's dialect, runs it, and returns the
	// result cursor. If that cursor frees its backing connection before Close, the Rows
	// should implement ConnReleaseNotifier.
	Execute(ctx context.Context, stmt *query.Statement) (Rows, error)
}

// Rows is the forward-only result cursor returned by Querier.Execute. It mirrors the
// standard database/sql cursor shape without binding to a particular driver, so the
// concrete implementation is free to back it with any datastore.
type Rows interface {
	// Next advances to the next row, reporting false when the result is exhausted or
	// an error occurred (check Err).
	Next() bool

	// Scan copies the current row's columns into the destinations.
	Scan(dest ...any) error

	// Close releases the cursor's resources. If the backing connection is instead freed
	// earlier, the Rows should implement ConnReleaseNotifier.
	Close() error

	// Err reports the error, if any, that terminated iteration.
	Err() error
}

// ConnReleaseNotifier is optionally implemented by a Rows that frees its backing
// connection before Close, e.g. it drains the result into memory and releases on
// exhaustion. Absent this, callers assume the connection is held until Close (or drain).
type ConnReleaseNotifier interface {
	// OnConnRelease registers f to run once, when the connection is released (or now,
	// if already released).
	OnConnRelease(f func())
}
