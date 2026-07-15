package mysql

import (
	"testing"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/internal/ansi"
)

// build renders a query through the mysql dialect, returning its SQL and bind arguments.
// It uses a render-only builder (nil db), exercising the same dialect New installs.
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

// TestPlaceholders verifies the mysql dialect renders MySQL's native "?" placeholders,
// the ANSI default it inherits unchanged.
func TestPlaceholders(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(a.ObjectID()).From(a).Where(
		a.ObjectType().Eq(b.Lit("doc")).And(a.ObjectRelation().Eq(b.Lit("viewer"))),
	)
	sql, args := build(t, q)
	want := "SELECT a.object_id FROM tuple a WHERE (a.object_type = ? AND a.relation = ?)"
	assertSQL(t, sql, want, args, "doc", "viewer")
}

// TestSubjectView verifies the subject view uses MySQL's SUBSTRING_INDEX / SUBSTRING /
// LOCATE functions.
func TestSubjectView(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(
		a.SubjectType().As("st"),
		a.SubjectID().As("sid"),
		a.SubjectRelation().As("sr"),
	).From(a)
	sql, _ := build(t, q)
	want := "SELECT SUBSTRING_INDEX(a._user, ':', 1) AS st, " +
		"SUBSTRING_INDEX(SUBSTRING(a._user, LOCATE(':', a._user) + 1), '#', 1) AS sid, " +
		"IF(LOCATE('#', a._user) = 0, '', SUBSTRING(a._user, LOCATE('#', a._user) + 1)) AS sr FROM tuple a"
	assertSQL(t, sql, want, nil)
}

// TestLike verifies LIKE renders unchanged under the mysql dialect, with "?" placeholders.
func TestLike(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(a.ObjectID()).From(a).Where(a.ObjectID().Like(b.Lit("doc%")))
	sql, args := build(t, q)
	want := "SELECT a.object_id FROM tuple a WHERE a.object_id LIKE ?"
	assertSQL(t, sql, want, args, "doc%")
}
