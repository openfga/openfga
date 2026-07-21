package mysql

import (
	"testing"

	"github.com/openfga/openfga/pkg/storage/adapter"
)

// build renders a statement or query through the mysql builder, returning its SQL and bind
// arguments. It uses a render-only builder (nil db), exercising the same rendering New
// installs. It accepts a SelectBuilder or a lowered Query.
func build(t *testing.T, q any) (string, []any) {
	t.Helper()
	bq, ok := q.(Query)
	if !ok {
		t.Fatalf("query %T does not implement mysql.Query", q)
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

// newBuilder returns a render-only mysql builder (nil db); queries render but never execute.
func newBuilder() adapter.Builder { return New(nil) }

// TestPlaceholders verifies the mysql builder renders MySQL's native "?" placeholders.
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

// TestSubjectView verifies the subject view uses MySQL's SUBSTRING_INDEX / SUBSTRING /
// LOCATE / IF functions.
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

// TestStandardColumns verifies the non-subject columns render as plain "<alias>.<column>"
// references, with the condition column mapping to condition_name.
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

// TestLike verifies LIKE renders with "?" placeholders.
func TestLike(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(a.ObjectID()).From(a).Where(a.ObjectID().Like(b.Bind("doc%")))
	sql, args := build(t, q)
	want := "SELECT a.object_id FROM tuple a WHERE a.object_id LIKE ?"
	assertSQL(t, sql, want, args, "doc%")
}

// --- MySQL-specific divergences from ANSI ---

// TestAggregateFilterNoArgs verifies an argument-less COUNT with a Filter renders a CASE
// over the count, as MySQL lacks a FILTER clause.
func TestAggregateFilterNoArgs(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	count := b.Aggregate(adapter.AggCount).Filter(a.ObjectType().Eq(b.Bind("doc")))
	q := b.Select(count.As("n")).From(a)
	sql, args := build(t, q)
	want := "SELECT COUNT(CASE WHEN a.object_type = ? THEN 1 END) AS n FROM tuple a"
	assertSQL(t, sql, want, args, "doc")
}

// TestAggregateFilterWithArg verifies an aggregate over a column with a Filter wraps the
// argument in a CASE, so filtered-out rows contribute NULL and are skipped.
func TestAggregateFilterWithArg(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	count := b.Aggregate(adapter.AggCount, a.ObjectID()).
		Filter(a.ObjectType().Eq(b.Bind("doc")))
	q := b.Select(count.As("n")).From(a)
	sql, args := build(t, q)
	want := "SELECT COUNT(CASE WHEN a.object_type = ? THEN a.object_id END) AS n FROM tuple a"
	assertSQL(t, sql, want, args, "doc")
}

// TestAggregateNoFilter verifies an unfiltered aggregate renders as a plain call.
func TestAggregateNoFilter(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(b.Aggregate(adapter.AggCount).As("n")).From(a)
	sql, _ := build(t, q)
	assertSQL(t, sql, "SELECT COUNT() AS n FROM tuple a", nil)
}

// TestJSONObjectCommaForm verifies JSON_OBJECT uses MySQL's comma form JSON_OBJECT(k, v)
// rather than the ANSI "k VALUE v" pairs.
func TestJSONObjectCommaForm(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	obj := b.Func(adapter.FuncJSONObject, b.Bind("k"), a.ObjectID())
	q := b.Select(obj.As("o")).From(a)
	sql, args := build(t, q)
	want := "SELECT JSON_OBJECT(?, a.object_id) AS o FROM tuple a"
	assertSQL(t, sql, want, args, "k")
}

// TestOrderByNullsLast verifies NULLS LAST is emulated with a leading "IS NULL" sort key.
func TestOrderByNullsLast(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(a.ObjectID()).From(a).
		OrderBy(a.ObjectID().Asc().Nulls(adapter.NullsLast))
	sql, _ := build(t, q)
	want := "SELECT a.object_id FROM tuple a ORDER BY a.object_id IS NULL, a.object_id ASC"
	assertSQL(t, sql, want, nil)
}

// TestOrderByNullsFirst verifies NULLS FIRST is emulated with a leading "IS NOT NULL" sort
// key.
func TestOrderByNullsFirst(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(a.ObjectID()).From(a).
		OrderBy(a.ObjectID().Desc().Nulls(adapter.NullsFirst))
	sql, _ := build(t, q)
	want := "SELECT a.object_id FROM tuple a ORDER BY a.object_id IS NOT NULL, a.object_id DESC"
	assertSQL(t, sql, want, nil)
}

// TestOrderByNullsDefault verifies the default null ordering renders a plain sort term.
func TestOrderByNullsDefault(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(a.ObjectID()).From(a).OrderBy(a.ObjectID().Desc())
	sql, _ := build(t, q)
	assertSQL(t, sql, "SELECT a.object_id FROM tuple a ORDER BY a.object_id DESC", nil)
}

// --- Ported render coverage: constructs shared with ANSI must render identically ---

// TestSelectFull exercises GROUP BY / HAVING / ORDER BY / LIMIT / OFFSET together.
func TestSelectFull(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("t")
	q := b.Select(a.ObjectID()).
		From(a).
		Where(a.ObjectType().Eq(b.Bind("document"))).
		GroupBy(a.ObjectID()).
		Having(b.Aggregate(adapter.AggCount).Gt(b.Bind(1))).
		OrderBy(a.ObjectID().Desc()).
		Limit(10).
		Offset(5)
	sql, args := build(t, q)
	want := "SELECT t.object_id FROM tuple t WHERE t.object_type = ? " +
		"GROUP BY t.object_id HAVING COUNT() > ? ORDER BY t.object_id DESC LIMIT 10 OFFSET 5"
	assertSQL(t, sql, want, args, "document", 1)
}

// TestJoin exercises an INNER JOIN self-join with an ON predicate.
func TestJoin(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	c := b.Tuple("c")
	q := b.Select(a.ObjectID()).
		From(a).
		JoinClause(b.Join(adapter.InnerJoin, c).On(a.ObjectID().Eq(c.ObjectID())))
	sql, _ := build(t, q)
	want := "SELECT a.object_id FROM tuple a INNER JOIN tuple c ON a.object_id = c.object_id"
	assertSQL(t, sql, want, nil)
}

// TestSearchedCase exercises a searched CASE expression.
func TestSearchedCase(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	expr := b.Case().
		When(a.ObjectType().Eq(b.Bind("doc")), b.Bind(1)).
		Else(b.Bind(0))
	q := b.Select(expr.As("kind")).From(a)
	sql, args := build(t, q)
	want := "SELECT CASE WHEN a.object_type = ? THEN ? ELSE ? END AS kind FROM tuple a"
	assertSQL(t, sql, want, args, "doc", 1, 0)
}

// TestExistsSubquery exercises a NOT EXISTS correlated subquery.
func TestExistsSubquery(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	c := b.Tuple("c")
	sub := b.Build(b.Select(c.ObjectID()).From(c).Where(c.ObjectID().Eq(a.ObjectID())))
	q := b.Select(a.ObjectID()).From(a).Where(sub.Exists().Not())
	sql, _ := build(t, q)
	want := "SELECT a.object_id FROM tuple a WHERE NOT (EXISTS (SELECT c.object_id FROM tuple c WHERE c.object_id = a.object_id))"
	assertSQL(t, sql, want, nil)
}

// TestInList exercises an IN predicate over a list of literals.
func TestInList(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	q := b.Select(a.ObjectID()).From(a).
		Where(a.ObjectRelation().In(b.Bind("viewer"), b.Bind("editor")))
	sql, args := build(t, q)
	want := "SELECT a.object_id FROM tuple a WHERE a.relation IN (?, ?)"
	assertSQL(t, sql, want, args, "viewer", "editor")
}

// TestLitInline verifies Lit renders its value inline in the SQL text — strings
// single-quoted (with embedded quotes doubled), numbers bare — and contributes nothing to
// the bind-argument list, in contrast to Bind's "?" placeholder.
func TestLitInline(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	p := a.ObjectRelation().Eq(b.Lit("viewer")).
		And(a.ObjectID().Gt(b.Lit(1))).
		And(a.ObjectType().Eq(b.Lit("O'Brien")))
	sql, args := build(t, b.Select(a.ObjectID()).From(a).Where(p))
	want := "SELECT a.object_id FROM tuple a WHERE ((a.relation = 'viewer' AND a.object_id > 1) AND a.object_type = 'O''Brien')"
	assertSQL(t, sql, want, args)
}

// TestLitUnsupportedTypePanics verifies Lit rejects a type it cannot safely inline, steering
// callers to Bind for such values.
func TestLitUnsupportedTypePanics(t *testing.T) {
	b := newBuilder()
	a := b.Tuple("a")
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for unsupported Lit type")
		}
	}()
	build(t, b.Select(a.ObjectID()).From(a).Where(a.ObjectType().Eq(b.Lit([]byte("x")))))
}
