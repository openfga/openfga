package adapter

import (
	"context"

	"github.com/openfga/openfga/pkg/storage/adapter/query"
)

// Querier executes a typed query.Statement (built via pkg/storage/adapter/query) and
// returns its result cursor. It is the typed-AST analog of Builder: where Builder
// composes and runs the fluent interface algebra, Querier runs an already-built
// statement.
//
// Per the all-or-nothing capability rule, a backend returns a non-nil Querier only if it
// can render and run the ENTIRE query surface. Because non-nil therefore promises full
// support, Execute returns only operational errors — never a capability/unsupported
// sentinel. A backend that cannot support the surface returns nil from
// RelationshipTupleReader.Querier.
type Querier interface {
	// Execute renders the statement for the backend's dialect, runs it, and returns the
	// result cursor.
	Execute(ctx context.Context, stmt *query.Statement) (Rows, error)
}
