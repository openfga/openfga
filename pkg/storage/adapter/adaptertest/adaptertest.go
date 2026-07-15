// Package adaptertest provides test-only helpers for exercising the adapter query
// builder without a live database. It exposes the otherwise-internal ANSI builder as a
// public seam: a render-only builder whose queries can be rendered (not executed) for
// golden-SQL assertions, and an Executor a test can implement to drive queries against
// canned rows.
//
// It lives under pkg/storage/adapter so it may import the internal ansi package, while
// remaining importable by any consumer's tests (e.g. internal/check/engine).
package adaptertest

import (
	"context"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/internal/ansi"
)

// Executor is the seam the builder calls to run a rendered statement: it receives the
// SQL text and ordinal bind arguments and returns a result cursor. Implement it in a
// test to feed canned rows back to the query under test.
type Executor = ansi.Executor

type emptyRows struct{}

func (r emptyRows) Next() bool {
	return false
}

func (r emptyRows) Scan(dest ...any) error {
	return nil
}

func (r emptyRows) Close() error {
	return nil
}

func (r emptyRows) Err() error {
	return nil
}

type Recorder struct {
	SQL        string
	Parameters []any
}

func (r *Recorder) Query(_ context.Context, sql string, args []any) (adapter.Rows, error) {
	r.SQL = sql
	r.Parameters = args
	return emptyRows{}, nil
}

// New returns a Builder that renders standard ANSI SQL and runs statements through exec.
// Pass a nil exec for a render-only builder: its queries never execute, but each can be
// rendered to SQL and bind args via its Build method (assert the returned adapter.Query
// to interface{ Build() (sql string, args []any) }).
func New(exec Executor) adapter.Builder {
	return ansi.New(exec)
}
