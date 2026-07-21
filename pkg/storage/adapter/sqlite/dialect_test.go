package sqlite

import (
	"testing"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/ansi"
)

// build renders a statement or query through the sqlite dialect, returning its SQL and bind
// arguments. It uses a render-only builder (nil db), exercising the same dialect New
// installs. It accepts a SelectBuilder or a lowered Query.
func build(t *testing.T, q any) (string, []any) {
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

// newBuilder returns a render-only sqlite builder (nil db), which installs the same dialect
// New would; queries render but never execute.
func newBuilder() adapter.Builder { return New(nil) }

// TestPlaceholders verifies the sqlite dialect renders SQLite's native "?" placeholders,
// the ANSI default it inherits unchanged.
func TestPlaceholders(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(a.ObjectID()).From(a).Where(
		a.ObjectType().Eq(b.Bind("doc")).And(a.ObjectRelation().Eq(b.Bind("viewer"))),
	)
	sql, args := build(t, q)
	want := "SELECT a.object_id FROM tuple a WHERE (a.object_type = ? AND a.relation = ?)"
	assertSQL(t, sql, want, args, "doc", "viewer")
}

// TestSubjectView verifies the subject view maps to SQLite's split subject columns
// (user_object_type / user_object_id / user_relation) rather than decoding a packed
// `_user` column with string functions.
func TestSubjectView(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(
		a.SubjectType().As("st"),
		a.SubjectID().As("sid"),
		a.SubjectRelation().As("sr"),
	).From(a)
	sql, _ := build(t, q)
	want := "SELECT a.user_object_type AS st, " +
		"a.user_object_id AS sid, " +
		"a.user_relation AS sr FROM tuple a"
	assertSQL(t, sql, want, nil)
}

// TestStandardColumns verifies the non-subject columns defer to the standard mapping,
// rendered as plain "<alias>.<column>" references.
func TestStandardColumns(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(
		a.Store(),
		a.ObjectType(),
		a.ObjectID(),
		a.ObjectRelation(),
		a.Condition(),
	).From(a)
	sql, _ := build(t, q)
	want := "SELECT a.store, a.object_type, a.object_id, a.relation, a.condition_name FROM tuple a"
	assertSQL(t, sql, want, nil)
}

// TestLike verifies LIKE renders unchanged under the sqlite dialect, with "?" placeholders.
func TestLike(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(a.ObjectID()).From(a).Where(a.ObjectID().Like(b.Bind("doc%")))
	sql, args := build(t, q)
	want := "SELECT a.object_id FROM tuple a WHERE a.object_id LIKE ?"
	assertSQL(t, sql, want, args, "doc%")
}
