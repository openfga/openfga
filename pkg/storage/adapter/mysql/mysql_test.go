package mysql_test

import (
	"testing"

	"github.com/openfga/openfga/pkg/storage/adapter/mysql"
	"github.com/openfga/openfga/pkg/storage/adapter/query"
)

func assertSQL(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("SQL mismatch:\n got: %s\nwant: %s", got, want)
	}
}

// TestMySQLOwnsColumnMapping shows the renderer applying its OWN packed-_user decode for
// subject columns — divergent SQL no other adapter produces.
func TestMySQLOwnsColumnMapping(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.SubjectID()).
		From(a).
		Where(query.Eq(a.SubjectID(), query.Bind("bob")))
	sql, args := mysql.Render(stmt)
	want := "SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(a._user, ':', -1), '#', 1) " +
		"FROM tuple a WHERE SUBSTRING_INDEX(SUBSTRING_INDEX(a._user, ':', -1), '#', 1) = ?"
	assertSQL(t, sql, want)
	if len(args) != 1 || args[0] != "bob" {
		t.Errorf("args: got %v", args)
	}
}

// TestMySQLMapsEveryLogicalColumn walks the whole logical schema through the renderer: the
// three subject columns get MySQL's packed-_user decode, the other six pass through. It also
// guards the exhaustive switch — an unmapped column would panic here.
func TestMySQLMapsEveryLogicalColumn(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(
		a.ObjectType(), a.ObjectID(), a.ObjectRelation(),
		a.SubjectType(), a.SubjectID(), a.SubjectRelation(),
		a.Store(), a.Condition(), a.ConditionContext(),
	).From(a)
	sql, _ := mysql.Render(stmt)
	want := "SELECT a.object_type, a.object_id, a.relation, " +
		"SUBSTRING_INDEX(a._user, ':', 1), " +
		"SUBSTRING_INDEX(SUBSTRING_INDEX(a._user, ':', -1), '#', 1), " +
		"IF(LOCATE('#', a._user) = 0, '', SUBSTRING_INDEX(a._user, '#', -1)), " +
		"a.store, a.condition_name, a.condition_context FROM tuple a"
	assertSQL(t, sql, want)
}

// TestMySQLSelfJoinAndCast exercises the join clause and an erasure-hatch node (CAST). The cast
// target is the CastType ENUM, and MySQL spells TypeVarchar as CHAR.
func TestMySQLSelfJoinAndCast(t *testing.T) {
	a := query.NewTuple("a")
	g := query.NewTuple("g")
	ctxText := query.Cast[string](a.ConditionContext(), query.TypeVarchar)
	stmt := query.Select(a.ObjectID(), ctxText).
		From(a).
		Join(g, query.Eq(a.SubjectID(), g.ObjectID())).
		Where(query.Eq(ctxText, query.Bind("{}")))

	sql, _ := mysql.Render(stmt)
	want := "SELECT a.object_id, CAST(a.condition_context AS CHAR) " +
		"FROM tuple a INNER JOIN tuple g ON SUBSTRING_INDEX(SUBSTRING_INDEX(a._user, ':', -1), '#', 1) = g.object_id " +
		"WHERE CAST(a.condition_context AS CHAR) = ?"
	assertSQL(t, sql, want)
}

// --- the divergences that justify MySQL owning its renderer -----------------------------

// TestMySQLEmulatesAggregateFilter is the most important test in this package: a filtered
// aggregate renders as a MySQL CASE-wrapped argument, not a FILTER clause. The node is
// restructured rather than renamed, which no dialect table of names could produce.
func TestMySQLEmulatesAggregateFilter(t *testing.T) {
	a := query.NewTuple("a")
	match := query.Eq(a.ObjectRelation(), query.Lit("viewer"))
	count := query.Count(a.ObjectID(), query.AggFilter(match))
	stmt := query.Select(count).From(a)

	sql, _ := mysql.Render(stmt)
	assertSQL(t, sql,
		"SELECT COUNT(CASE WHEN a.relation = 'viewer' THEN a.object_id END) FROM tuple a")
}

// TestMySQLEmulatesFilteredCountStar covers the no-argument case: COUNT(*) has no argument to
// wrap, so the CASE yields 1 for matching rows and NULL otherwise.
func TestMySQLEmulatesFilteredCountStar(t *testing.T) {
	a := query.NewTuple("a")
	match := query.Eq(a.ObjectRelation(), query.Lit("viewer"))
	stmt := query.Select(query.Count(query.Star, query.AggFilter(match))).From(a)

	sql, _ := mysql.Render(stmt)
	assertSQL(t, sql, "SELECT COUNT(CASE WHEN a.relation = 'viewer' THEN 1 END) FROM tuple a")
}

// TestMySQLPlainDistinct covers the one DISTINCT flavour the tightened surface keeps: a plain
// SELECT DISTINCT.
func TestMySQLPlainDistinct(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.ObjectID()).From(a).Distinct()

	sql, _ := mysql.Render(stmt)
	assertSQL(t, sql, "SELECT DISTINCT a.object_id FROM tuple a")
}

// TestMySQLOffsetImpliesLimit covers a MySQL quirk: OFFSET is only legal after a LIMIT, so a
// bare Offset gets the conventional max-value stand-in.
func TestMySQLOffsetImpliesLimit(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.ObjectID()).From(a).Offset(20)

	sql, _ := mysql.Render(stmt)
	assertSQL(t, sql, "SELECT a.object_id FROM tuple a LIMIT 18446744073709551615 OFFSET 20")
}

// TestMySQLJSONPairArgumentForm shows a divergence in argument STRUCTURE rather than name:
// MySQL's JSON_OBJECT takes the flat "k, v" argument form.
func TestMySQLJSONPairArgumentForm(t *testing.T) {
	a := query.NewTuple("a")
	obj := query.JSONObject(query.Pair(query.Lit("rel"), a.ObjectRelation()))
	stmt := query.Select(obj).From(a)

	sql, _ := mysql.Render(stmt)
	assertSQL(t, sql, "SELECT JSON_OBJECT('rel', a.relation) FROM tuple a")
}

// TestMySQLExpandsBoundSet shows a bound set always expanding to IN(...), since MySQL has no
// array operand.
func TestMySQLExpandsBoundSet(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.ObjectID()).
		From(a).
		Where(query.And(
			query.Eq(a.ObjectType(), query.Bind("document")),
			query.Quantified(a.ObjectRelation(), query.OpEq, query.Any,
				query.BindAll([]string{"viewer", "editor"})),
		))

	sql, args := mysql.Render(stmt)
	assertSQL(t, sql,
		"SELECT a.object_id FROM tuple a WHERE (a.object_type = ? AND a.relation IN (?, ?))")
	if len(args) != 3 {
		t.Errorf("args: got %v", args)
	}
}
