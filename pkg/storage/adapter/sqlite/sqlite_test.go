package sqlite_test

import (
	"testing"

	"github.com/openfga/openfga/pkg/storage/adapter/query"
	"github.com/openfga/openfga/pkg/storage/adapter/sqlite"
)

func assertSQL(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("SQL mismatch:\n got: %s\nwant: %s", got, want)
	}
}

// TestSQLiteSubjectColumnsAreDiscrete is SQLite's defining divergence: the subject is stored
// across three physical columns, so a subject field is a plain column reference — none of the
// packed-_user string surgery MySQL and PostgreSQL emit.
func TestSQLiteSubjectColumnsAreDiscrete(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.SubjectID()).
		From(a).
		Where(query.Eq(a.SubjectID(), query.Bind("bob")))
	sql, args := sqlite.Render(stmt)
	want := "SELECT a.user_object_id FROM tuple a WHERE a.user_object_id = ?"
	assertSQL(t, sql, want)
	if len(args) != 1 || args[0] != "bob" {
		t.Errorf("args: got %v", args)
	}
}

// TestSQLiteMapsEveryLogicalColumn walks the whole logical schema: the three subject columns
// map to their discrete physical names, the other six pass through. It also guards the
// exhaustive switch — an unmapped column would panic here.
func TestSQLiteMapsEveryLogicalColumn(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(
		a.ObjectType(), a.ObjectID(), a.ObjectRelation(),
		a.SubjectType(), a.SubjectID(), a.SubjectRelation(),
		a.Store(), a.Condition(), a.ConditionContext(),
	).From(a)
	sql, _ := sqlite.Render(stmt)
	want := "SELECT a.object_type, a.object_id, a.relation, " +
		"a.user_object_type, a.user_object_id, a.user_relation, " +
		"a.store, a.condition_name, a.condition_context FROM tuple a"
	assertSQL(t, sql, want)
}

// TestSQLiteSelfJoinAndCast exercises the join clause and an erasure-hatch node (CAST). The
// cast target is the CastType ENUM, and SQLite spells TypeVarchar as TEXT.
func TestSQLiteSelfJoinAndCast(t *testing.T) {
	a := query.NewTuple("a")
	g := query.NewTuple("g")
	ctxText := query.Cast[string](a.ConditionContext(), query.TypeVarchar)
	stmt := query.Select(a.ObjectID(), ctxText).
		From(a).
		Join(g, query.Eq(a.SubjectID(), g.ObjectID())).
		Where(query.Eq(ctxText, query.Bind("{}")))

	sql, _ := sqlite.Render(stmt)
	want := "SELECT a.object_id, CAST(a.condition_context AS TEXT) " +
		"FROM tuple a INNER JOIN tuple g ON a.user_object_id = g.object_id " +
		"WHERE CAST(a.condition_context AS TEXT) = ?"
	assertSQL(t, sql, want)
}

// TestSQLiteNativeFilter is the divergence that separates SQLite from MySQL: SQLite has a real
// FILTER (WHERE ...) clause, so a filtered aggregate emits it directly with no CASE emulation.
// Relation literals are inlined, so no binds appear.
func TestSQLiteNativeFilter(t *testing.T) {
	a := query.NewTuple("a")
	match := query.Eq(a.ObjectRelation(), query.Lit("viewer"))
	stmt := query.Select(query.Count(a.ObjectID(), query.AggFilter(match))).From(a)

	sql, _ := sqlite.Render(stmt)
	assertSQL(t, sql,
		"SELECT COUNT(a.object_id) FILTER (WHERE a.relation = 'viewer') FROM tuple a")
}

// TestSQLiteFilteredCountStar covers the no-argument aggregate: COUNT(*) with a native FILTER.
func TestSQLiteFilteredCountStar(t *testing.T) {
	a := query.NewTuple("a")
	match := query.Eq(a.ObjectRelation(), query.Lit("viewer"))
	stmt := query.Select(query.Count(query.Star, query.AggFilter(match))).From(a)

	sql, _ := sqlite.Render(stmt)
	assertSQL(t, sql, "SELECT COUNT(*) FILTER (WHERE a.relation = 'viewer') FROM tuple a")
}

// TestSQLitePlainDistinct covers the one DISTINCT flavour the tightened surface keeps.
func TestSQLitePlainDistinct(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.ObjectID()).From(a).Distinct()

	sql, _ := sqlite.Render(stmt)
	assertSQL(t, sql, "SELECT DISTINCT a.object_id FROM tuple a")
}

// TestSQLiteOffsetUsesNegativeLimit covers SQLite's idiom: OFFSET is legal only after a LIMIT,
// and SQLite's own "no upper bound" spelling is a negative limit.
func TestSQLiteOffsetUsesNegativeLimit(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.ObjectID()).From(a).Offset(20)

	sql, _ := sqlite.Render(stmt)
	assertSQL(t, sql, "SELECT a.object_id FROM tuple a LIMIT -1 OFFSET 20")
}

// TestSQLiteJSONObject shows the object constructor is json_object with the flat "k, v"
// argument form.
func TestSQLiteJSONObject(t *testing.T) {
	a := query.NewTuple("a")
	obj := query.JSONObject(query.Pair(query.Lit("rel"), a.ObjectRelation()))
	stmt := query.Select(obj).From(a)

	sql, _ := sqlite.Render(stmt)
	assertSQL(t, sql, "SELECT json_object('rel', a.relation) FROM tuple a")
}

// TestSQLiteExpandsBoundSet shows a bound set expanding to IN(...), since SQLite has no array
// operand.
func TestSQLiteExpandsBoundSet(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.ObjectID()).
		From(a).
		Where(query.And(
			query.Eq(a.ObjectType(), query.Bind("document")),
			query.Quantified(a.ObjectRelation(), query.OpEq, query.Any,
				query.BindAll([]string{"viewer", "editor"})),
		))

	sql, args := sqlite.Render(stmt)
	assertSQL(t, sql,
		"SELECT a.object_id FROM tuple a WHERE (a.object_type = ? AND a.relation IN (?, ?))")
	if len(args) != 3 {
		t.Errorf("args: got %v", args)
	}
}
