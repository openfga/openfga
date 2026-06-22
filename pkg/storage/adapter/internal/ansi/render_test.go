package ansi

import (
	"testing"

	"github.com/openfga/openfga/pkg/storage/adapter"
)

// renderQuery renders any query produced by this package back to SQL text and bind
// args. It mirrors what the builder hands its Executor, without a live database.
func renderQuery(t *testing.T, q adapter.Query) (string, []any) {
	t.Helper()
	w, ok := q.(sqlWriter)
	if !ok {
		t.Fatalf("query %T does not implement sqlWriter", q)
	}
	return render(w, standardDialect{})
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

func TestSelectBasic(t *testing.T) {
	b := newBuilder()
	t1 := b.Tuple("t")
	q := b.Select(t1.ObjectID(), t1.SubjectID()).
		From(t1).
		Where(t1.ObjectType().Eq(b.Lit("document")).
			And(t1.ObjectRelation().Eq(b.Lit("viewer")))).
		OrderBy(t1.ObjectID().Desc()).
		Limit(10)

	sql, args := renderQuery(t, q)
	assertSQL(t, sql,
		"SELECT t.object_id, "+ansiSubjectIDView("t")+" "+
			"FROM tuple t WHERE (t.object_type = ? AND t.relation = ?) ORDER BY t.object_id DESC LIMIT 10",
		args, "document", "viewer")
}

// ansiSubjectIDView mirrors ANSIDialect's subject-id view so the long synthesized
// expression is written once where the subject view is only incidental to a test. The
// view itself is pinned to literal expectations in TestSubjectLogicalView.
func ansiSubjectIDView(alias string) string { return ANSIDialect{}.StandardColumn(ColumnSubjectID, alias) }

func TestSelectStarNoColumns(t *testing.T) {
	b := newBuilder()
	t1 := b.Tuple("t")
	sql, args := renderQuery(t, b.Select().From(t1))
	assertSQL(t, sql, "SELECT * FROM tuple t", args)
}

func TestSubjectLogicalView(t *testing.T) {
	b := newBuilder()
	t1 := b.Tuple("a")
	sql, _ := renderQuery(t, b.Select(
		t1.SubjectType().As("st"),
		t1.SubjectRelation().As("sr"),
	).From(t1))
	// Literal expectations (not the dialect's own helpers) so a regression in the ANSI
	// subject view is actually caught here.
	want := "SELECT SUBSTRING(a._user FROM 1 FOR POSITION(':' IN a._user) - 1) AS st, " +
		"CASE WHEN POSITION('#' IN a._user) = 0 THEN '' " +
		"ELSE SUBSTRING(a._user FROM POSITION('#' IN a._user) + 1) END AS sr FROM tuple a"
	assertSQL(t, sql, want, nil)
}

func TestSubjectIDView(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	sql, _ := renderQuery(t, b.Select(a.SubjectID().As("sid")).From(a))
	// The id is everything after the first ':' with any '#relation' suffix stripped.
	want := "SELECT CASE WHEN POSITION('#' IN SUBSTRING(a._user FROM POSITION(':' IN a._user) + 1)) = 0 " +
		"THEN SUBSTRING(a._user FROM POSITION(':' IN a._user) + 1) " +
		"ELSE SUBSTRING(SUBSTRING(a._user FROM POSITION(':' IN a._user) + 1) FROM 1 FOR " +
		"POSITION('#' IN SUBSTRING(a._user FROM POSITION(':' IN a._user) + 1)) - 1) END AS sid FROM tuple a"
	assertSQL(t, sql, want, nil)
}

func TestJoinSelfJoin(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	g := b.Tuple("b")
	join := b.Join(adapter.InnerJoin, g).On(
		g.Store().Eq(a.Store()).And(g.SubjectID().Eq(a.ObjectID())),
	)
	q := b.Select(a.ObjectID()).From(a).JoinClause(join)
	sql, args := renderQuery(t, q)
	want := "SELECT a.object_id FROM tuple a INNER JOIN tuple b ON " +
		"(b.store = a.store AND " + ansiSubjectIDView("b") + " = a.object_id)"
	assertSQL(t, sql, want, args)
}

func TestFuncAndAggregateJSON(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	permission := b.Func(adapter.FuncJSONObject,
		b.Lit("relation"), a.ObjectRelation(),
		b.Lit("condition"), a.Condition(),
	)
	permissions := b.Aggregate(adapter.AggJSONArrayAgg, permission).As("perms")
	q := b.Select(a.ObjectID(), permissions).From(a).GroupBy(a.ObjectID())
	sql, args := renderQuery(t, q)
	want := "SELECT a.object_id, JSON_ARRAYAGG(JSON_OBJECT(? VALUE a.relation, ? VALUE a.condition_name)) AS perms " +
		"FROM tuple a GROUP BY a.object_id"
	assertSQL(t, sql, want, args, "relation", "condition")
}

func TestJSONObjectAggPair(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	agg := b.Aggregate(adapter.AggJSONObjectAgg, a.ObjectRelation(), a.Condition()).As("m")
	sql, args := renderQuery(t, b.Select(agg).From(a))
	want := "SELECT JSON_OBJECTAGG(a.relation VALUE a.condition_name) AS m FROM tuple a"
	assertSQL(t, sql, want, args)
}

func TestAggregateModifiers(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	agg := b.Aggregate(adapter.AggCount, a.ObjectID()).
		Distinct().
		Filter(a.ObjectType().Eq(b.Lit("doc"))).
		As("n")
	sql, args := renderQuery(t, b.Select(agg).From(a))
	want := "SELECT COUNT(DISTINCT a.object_id) FILTER (WHERE a.object_type = ?) AS n FROM tuple a"
	assertSQL(t, sql, want, args, "doc")
}

func TestArrayAggOrderBy(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	agg := b.Aggregate(adapter.AggArrayAgg, a.ObjectID()).
		OrderBy(a.ObjectID().Desc()).
		As("ids")
	sql, args := renderQuery(t, b.Select(agg).From(a))
	want := "SELECT ARRAY_AGG(a.object_id ORDER BY a.object_id DESC) AS ids FROM tuple a"
	assertSQL(t, sql, want, args)
}

func TestSimpleCase(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	expr := b.Case().
		Value(a.ObjectType()).
		When(b.Lit("document"), b.Lit(1)).
		When(b.Lit("folder"), b.Lit(2)).
		Else(b.Lit(0)).
		As("kind")
	sql, args := renderQuery(t, b.Select(expr).From(a))
	want := "SELECT CASE a.object_type WHEN ? THEN ? WHEN ? THEN ? ELSE ? END AS kind FROM tuple a"
	assertSQL(t, sql, want, args, "document", 1, "folder", 2, 0)
}

func TestSearchedCase(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	expr := b.Case().
		When(a.ObjectType().Eq(b.Lit("document")), b.Lit("doc")).
		When(a.ObjectType().Eq(b.Lit("folder")), b.Lit("dir")).
		Else(b.Lit("other")).
		As("label")
	sql, args := renderQuery(t, b.Select(expr).From(a))
	want := "SELECT CASE WHEN a.object_type = ? THEN ? WHEN a.object_type = ? THEN ? ELSE ? END AS label FROM tuple a"
	assertSQL(t, sql, want, args, "document", "doc", "folder", "dir", "other")
}

func TestPredicateAlgebra(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	p := a.ObjectID().In(b.Lit(1), b.Lit(2)).Not().
		Or(a.ObjectType().Between(b.Lit("a"), b.Lit("z")))
	sql, args := renderQuery(t, b.Select(a.ObjectID()).From(a).Where(p))
	want := "SELECT a.object_id FROM tuple a WHERE (NOT (a.object_id IN (?, ?)) OR a.object_type BETWEEN ? AND ?)"
	assertSQL(t, sql, want, args, 1, 2, "a", "z")
}

func TestLikeAndIsNull(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	p := a.ObjectID().Like(b.Lit("doc%")).And(a.Condition().IsNull().Not())
	sql, args := renderQuery(t, b.Select(a.ObjectID()).From(a).Where(p))
	want := "SELECT a.object_id FROM tuple a WHERE (a.object_id LIKE ? AND NOT (a.condition_name IS NULL))"
	assertSQL(t, sql, want, args, "doc%")
}

func TestCaseSearched(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	c := b.Case().
		When(a.ObjectType().Eq(b.Lit("doc")), b.Lit(1)).
		Else(b.Lit(0)).
		As("kind")
	sql, args := renderQuery(t, b.Select(c).From(a))
	want := "SELECT CASE WHEN a.object_type = ? THEN ? ELSE ? END AS kind FROM tuple a"
	assertSQL(t, sql, want, args, "doc", 1, 0)
}

func TestCastAndCoalesce(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	coalesced := b.Func(adapter.FuncCoalesce, a.Condition(), b.Lit(""))
	casted := b.Cast(a.ConditionContext(), "text")
	sql, args := renderQuery(t, b.Select(coalesced.As("c"), casted.As("ctx")).From(a))
	want := "SELECT COALESCE(a.condition_name, ?) AS c, CAST(a.condition_context AS text) AS ctx FROM tuple a"
	assertSQL(t, sql, want, args, "")
}

func TestDistinctOn(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(a.ObjectID()).Distinct(a.ObjectType()).From(a)
	sql, args := renderQuery(t, q)
	want := "SELECT DISTINCT ON (a.object_type) a.object_id FROM tuple a"
	assertSQL(t, sql, want, args)
}

func TestSetOperationUnionAll(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	g := b.Tuple("b")
	left := b.Select(a.ObjectID()).From(a)
	right := b.Select(g.ObjectID()).From(g)
	q := left.Set(adapter.SetUnion, true, right).OrderBy(a.ObjectID().Asc()).Limit(5)
	sql, args := renderQuery(t, q)
	want := "SELECT a.object_id FROM tuple a UNION ALL SELECT b.object_id FROM tuple b ORDER BY a.object_id ASC LIMIT 5"
	assertSQL(t, sql, want, args)
}

func TestScalarSubqueryAndExists(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	g := b.Tuple("b")
	sub := b.Select(g.ObjectID()).From(g).Where(g.Store().Eq(a.Store()))
	q := b.Select(a.ObjectID(), sub.ScalarExpr().As("sub")).
		From(a).
		Where(sub.Exists())
	sql, args := renderQuery(t, q)
	want := "SELECT a.object_id, (SELECT b.object_id FROM tuple b WHERE b.store = a.store) AS sub " +
		"FROM tuple a WHERE EXISTS (SELECT b.object_id FROM tuple b WHERE b.store = a.store)"
	assertSQL(t, sql, want, args)
}

func TestQuantifiedAny(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	g := b.Tuple("b")
	sub := b.Select(g.ObjectID()).From(g)
	p := a.ObjectID().Quantified(adapter.OpEq, adapter.QuantifierAny, sub)
	sql, args := renderQuery(t, b.Select(a.ObjectID()).From(a).Where(p))
	want := "SELECT a.object_id FROM tuple a WHERE a.object_id = ANY (SELECT b.object_id FROM tuple b)"
	assertSQL(t, sql, want, args)
}

func TestGroupByHavingOffset(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	cnt := b.Aggregate(adapter.AggCount, a.ObjectID())
	q := b.Select(a.ObjectType(), cnt.As("n")).
		From(a).
		GroupBy(a.ObjectType()).
		Having(cnt.Gt(b.Lit(1))).
		Offset(20)
	sql, args := renderQuery(t, q)
	want := "SELECT a.object_type, COUNT(a.object_id) AS n FROM tuple a GROUP BY a.object_type " +
		"HAVING COUNT(a.object_id) > ? OFFSET 20"
	assertSQL(t, sql, want, args, 1)
}
