package ansi

import (
	"context"
	"errors"
	"testing"

	"github.com/openfga/openfga/pkg/storage/adapter"
)

// TestBuildExported verifies that a query built by this package exposes its rendered SQL
// and bind arguments through the exported Query.Build method, matching what an Executor
// would receive on Execute.
func TestBuildExported(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(a.ObjectID()).From(a).Where(a.ObjectType().Eq(b.Lit("doc")))

	bq, ok := q.(Query)
	if !ok {
		t.Fatalf("query %T does not implement ansi.Query", q)
	}
	sql, args := bq.Build()
	assertSQL(t, sql, "SELECT a.object_id FROM tuple a WHERE a.object_type = ?", args, "doc")
}

// recordingExecutor captures the SQL and args the builder renders, standing in for an
// engine-specific driver.
type recordingExecutor struct {
	sql  string
	args []any
	err  error
}

func (e *recordingExecutor) Query(_ context.Context, sql string, args []any) (adapter.Rows, error) {
	e.sql, e.args = sql, args
	return nil, e.err
}

// TestExecuteFeedsExecutor verifies Execute renders the query and hands the SQL and bind
// arguments to the Executor seam verbatim, returning its error.
func TestExecuteFeedsExecutor(t *testing.T) {
	want := errors.New("boom")
	exec := &recordingExecutor{err: want}
	b := New(exec)
	a := b.Tuple("a")

	_, err := b.Select(a.ObjectID()).From(a).Where(a.Store().Eq(b.Lit("s1"))).Execute(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("Execute error: got %v, want %v", err, want)
	}
	assertSQL(t, exec.sql, "SELECT a.object_id FROM tuple a WHERE a.store = ?", exec.args, "s1")
}
