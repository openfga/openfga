package pg_test

import (
	"testing"

	"github.com/openfga/openfga/pkg/storage/adapter/pg"
	"github.com/openfga/openfga/pkg/storage/adapter/query"
)

func assertSQL(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("SQL mismatch:\n got: %s\nwant: %s", got, want)
	}
}

// TestPGOrdinalPlaceholders verifies binds render as "$N", numbered in bind order.
func TestPGOrdinalPlaceholders(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.ObjectID()).
		From(a).
		Where(query.And(
			query.Eq(a.ObjectType(), query.Bind("doc")),
			query.Eq(a.ObjectRelation(), query.Bind("viewer")),
		))
	sql, args := pg.Render(stmt)
	want := "SELECT a.object_id FROM tuple a WHERE (a.object_type = $1 AND a.relation = $2)"
	assertSQL(t, sql, want)
	if len(args) != 2 || args[0] != "doc" || args[1] != "viewer" {
		t.Errorf("args: got %v", args)
	}
}

// TestPGSubjectView verifies the subject columns decode the packed _user with PostgreSQL's
// split_part / substring functions.
func TestPGSubjectView(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(
		query.As(a.SubjectType(), "st"),
		query.As(a.SubjectID(), "sid"),
		query.As(a.SubjectRelation(), "sr"),
	).From(a)
	sql, _ := pg.Render(stmt)
	want := "SELECT split_part(a._user, ':', 1) AS st, " +
		"split_part(substring(a._user FROM position(':' IN a._user) + 1), '#', 1) AS sid, " +
		"split_part(a._user, '#', 2) AS sr FROM tuple a"
	assertSQL(t, sql, want)
}

// TestPGMapsEveryLogicalColumn walks the whole logical schema, guarding the exhaustive column
// switch — an unmapped column would panic here.
func TestPGMapsEveryLogicalColumn(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(
		a.ObjectType(), a.ObjectID(), a.ObjectRelation(),
		a.SubjectType(), a.SubjectID(), a.SubjectRelation(),
		a.Store(), a.Condition(), a.ConditionContext(),
	).From(a)
	sql, _ := pg.Render(stmt)
	want := "SELECT a.object_type, a.object_id, a.relation, " +
		"split_part(a._user, ':', 1), " +
		"split_part(substring(a._user FROM position(':' IN a._user) + 1), '#', 1), " +
		"split_part(a._user, '#', 2), " +
		"a.store, a.condition_name, a.condition_context FROM tuple a"
	assertSQL(t, sql, want)
}

// TestPGBindsSetAsArray is PostgreSQL's headline optimization: a bound set binds as ONE array
// parameter compared with "= ANY ($N)", rather than being expanded to IN(?, ?, ...).
func TestPGBindsSetAsArray(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.ObjectID()).
		From(a).
		Where(query.And(
			query.Eq(a.ObjectType(), query.Bind("document")),
			query.Quantified(a.ObjectRelation(), query.OpEq, query.Any,
				query.BindAll([]string{"viewer", "editor"})),
		))
	sql, args := pg.Render(stmt)
	assertSQL(t, sql,
		"SELECT a.object_id FROM tuple a WHERE (a.object_type = $1 AND a.relation = ANY ($2))")
	if len(args) != 2 { // the set is ONE array param
		t.Errorf("args: got %d %v, want 2", len(args), args)
	}
}

// TestPGNativeFilter verifies a filtered aggregate emits a real FILTER (WHERE ...) clause,
// with no CASE emulation. Relation literals are inlined, so no binds appear.
func TestPGNativeFilter(t *testing.T) {
	a := query.NewTuple("a")
	match := query.Eq(a.ObjectRelation(), query.Lit("viewer"))
	stmt := query.Select(query.Count(a.ObjectID(), query.AggFilter(match))).From(a)

	sql, _ := pg.Render(stmt)
	assertSQL(t, sql,
		"SELECT COUNT(a.object_id) FILTER (WHERE a.relation = 'viewer') FROM tuple a")
}

// TestPGFilteredCountStar covers the no-argument aggregate: COUNT(*) with a native FILTER.
func TestPGFilteredCountStar(t *testing.T) {
	a := query.NewTuple("a")
	match := query.Eq(a.ObjectRelation(), query.Lit("viewer"))
	stmt := query.Select(query.Count(query.Star, query.AggFilter(match))).From(a)

	sql, _ := pg.Render(stmt)
	assertSQL(t, sql, "SELECT COUNT(*) FILTER (WHERE a.relation = 'viewer') FROM tuple a")
}

// TestPGCast verifies the CastType enum spells TypeVarchar as PostgreSQL's text.
func TestPGCast(t *testing.T) {
	a := query.NewTuple("a")
	ctxText := query.Cast[string](a.ConditionContext(), query.TypeVarchar)
	stmt := query.Select(a.ObjectID(), ctxText).
		From(a).
		Where(query.Eq(ctxText, query.Bind("{}")))
	sql, _ := pg.Render(stmt)
	want := "SELECT a.object_id, CAST(a.condition_context AS text) " +
		"FROM tuple a WHERE CAST(a.condition_context AS text) = $1"
	assertSQL(t, sql, want)
}

// TestPGJSONObject verifies the object constructor is jsonb_build_object with the flat "k, v"
// argument form.
func TestPGJSONObject(t *testing.T) {
	a := query.NewTuple("a")
	obj := query.JSONObject(query.Pair(query.Lit("rel"), a.ObjectRelation()))
	stmt := query.Select(obj).From(a)
	sql, _ := pg.Render(stmt)
	assertSQL(t, sql, "SELECT jsonb_build_object('rel', a.relation) FROM tuple a")
}

// TestPGLike verifies LIKE renders with an ordinal placeholder.
func TestPGLike(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.ObjectID()).From(a).Where(query.Like(a.ObjectID(), query.Bind("doc%")))
	sql, args := pg.Render(stmt)
	assertSQL(t, sql, "SELECT a.object_id FROM tuple a WHERE a.object_id LIKE $1")
	if len(args) != 1 || args[0] != "doc%" {
		t.Errorf("args: got %v", args)
	}
}

// TestPGOffsetWithoutLimit verifies PostgreSQL accepts a bare OFFSET, with no synthetic LIMIT
// (the MySQL quirk PostgreSQL does not share).
func TestPGOffsetWithoutLimit(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.ObjectID()).From(a).Offset(20)
	sql, _ := pg.Render(stmt)
	assertSQL(t, sql, "SELECT a.object_id FROM tuple a OFFSET 20")
}
