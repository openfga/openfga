package pg

import (
	"testing"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/internal/ansi"
)

// build renders a query through the pg dialect, returning its SQL and bind arguments. It
// uses a render-only builder (nil pool), exercising the same dialect New installs.
func build(t *testing.T, q adapter.Query) (string, []any) {
	t.Helper()
	bq, ok := q.(ansi.Query)
	if !ok {
		t.Fatalf("query %T does not implement ansi.Query", q)
	}
	return bq.Build()
}

func assertSQL(t *testing.T, gotSQL, wantSQL string, gotArgs []any, wantArgs ...any) {
	t.Helper()
	if gotSQL != wantSQL {
		t.Errorf("SQL mismatch:\n got: %s\nwant: %s", gotSQL, wantSQL)
	}
	if len(gotArgs) != len(wantArgs) {
		t.Fatalf("arg count mismatch: got %d %v, want %d %v", len(gotArgs), gotArgs, len(wantArgs), wantArgs)
	}
	for i := range gotArgs {
		if gotArgs[i] != wantArgs[i] {
			t.Errorf("arg %d mismatch: got %v, want %v", i, gotArgs[i], wantArgs[i])
		}
	}
}

func newBuilder() adapter.Builder { return New(nil) }

// TestOrdinalPlaceholders verifies the pg dialect renders $N placeholders, numbered in
// bind order, rather than the ANSI "?".
func TestOrdinalPlaceholders(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(a.ObjectID()).From(a).Where(
		a.ObjectType().Eq(b.Lit("doc")).And(a.ObjectRelation().Eq(b.Lit("viewer"))),
	)
	sql, args := build(t, q)
	want := "SELECT a.object_id FROM tuple a WHERE (a.object_type = $1 AND a.relation = $2)"
	assertSQL(t, sql, want, args, "doc", "viewer")
}

// TestSubjectView verifies the subject view uses pg's split_part / substring functions.
func TestSubjectView(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(
		a.SubjectType().As("st"),
		a.SubjectID().As("sid"),
		a.SubjectRelation().As("sr"),
	).From(a)
	sql, _ := build(t, q)
	want := "SELECT split_part(a._user, ':', 1) AS st, " +
		"split_part(substring(a._user FROM position(':' IN a._user) + 1), '#', 1) AS sid, " +
		"split_part(a._user, '#', 2) AS sr FROM tuple a"
	assertSQL(t, sql, want, nil)
}

// TestLike verifies LIKE renders unchanged under the pg dialect, with $N placeholders.
func TestLike(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(a.ObjectID()).From(a).Where(a.ObjectID().Like(b.Lit("doc%")))
	sql, args := build(t, q)
	want := "SELECT a.object_id FROM tuple a WHERE a.object_id LIKE $1"
	assertSQL(t, sql, want, args, "doc%")
}
