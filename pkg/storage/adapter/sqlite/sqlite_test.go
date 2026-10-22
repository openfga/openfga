package sqlite_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/openfga/openfga/pkg/storage/adapter/query"
	"github.com/openfga/openfga/pkg/storage/adapter/sqlite"
)

func assertSQL(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("SQL mismatch:\n got: %s\nwant: %s", got, want)
	}
}

// TestSQLiteSubjectColumnsAreDiscrete verifies a subject field is a plain column reference:
// SQLite stores the subject across three physical columns, unlike the packed _user in
// MySQL/PostgreSQL.
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

// TestSQLiteSelfJoinAndCast exercises a self-join and a CAST; SQLite spells TypeVarchar as TEXT.
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

// TestSQLiteNativeFilter verifies SQLite emits a real `FILTER (WHERE ...)` clause for a filtered
// aggregate, with no CASE emulation. Relation literals are inlined, so no binds appear.
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

// TestSQLitePlainDistinct covers a plain SELECT DISTINCT.
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

// TestSQLiteHavingWithoutGroupBy covers the EXISTS rewrite: ORDER BY is dropped, LIMIT stays
// outside, and binds keep their order.
func TestSQLiteHavingWithoutGroupBy(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(query.Bind[int64](7)).
		From(a).
		Where(query.Eq(a.ObjectID(), query.Bind("1"))).
		Having(query.Gt(query.Count(query.Star, query.AggFilter(query.Eq(a.ObjectRelation(), query.Bind("viewer")))), query.Lit[int64](0))).
		OrderBy(query.Asc(query.Count(query.Star, query.AggFilter(query.Eq(a.ObjectRelation(), query.Bind("dropped")))))).
		Limit(1)

	sql, args := sqlite.Render(stmt)
	assertSQL(t, sql, "SELECT ? WHERE EXISTS (SELECT COUNT(*) FROM tuple a WHERE a.object_id = ? "+
		"HAVING COUNT(*) FILTER (WHERE a.relation = ?) > 0) LIMIT 1")
	if len(args) != 3 || args[0] != int64(7) || args[1] != "1" || args[2] != "viewer" {
		t.Errorf("args: got %v", args)
	}
}

// TestSQLiteHavingAlreadyAggregate covers shapes SQLite accepts as-is, so they are not rewritten.
func TestSQLiteHavingAlreadyAggregate(t *testing.T) {
	a := query.NewTuple("a")
	having := query.Gt(query.Count(query.Star), query.Lit[int64](0))

	sql, _ := sqlite.Render(query.Select(query.Cast[string](query.Count(query.Star), query.TypeVarchar)).From(a).Having(having))
	assertSQL(t, sql, "SELECT CAST(COUNT(*) AS TEXT) FROM tuple a HAVING COUNT(*) > 0")

	sql, _ = sqlite.Render(query.Select(a.ObjectID()).From(a).GroupBy(a.ObjectID()).Having(having))
	assertSQL(t, sql, "SELECT a.object_id FROM tuple a GROUP BY a.object_id HAVING COUNT(*) > 0")
}

// TestSQLiteHavingWithoutGroupByExecutes pins that there is one group even over no rows.
func TestSQLiteHavingWithoutGroupByExecutes(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec("CREATE TABLE tuple (relation TEXT)")
	require.NoError(t, err)

	a := query.NewTuple("a")
	banned := query.Count(query.Star, query.AggFilter(query.Eq(a.ObjectRelation(), query.Lit("banned"))))
	tests := []struct {
		name   string
		rows   []string
		having query.Predicate
		want   bool
	}{
		{name: "empty_input_true", having: query.Not(query.Gt(banned, query.Lit[int64](0))), want: true},
		{name: "empty_input_false", having: query.Gt(banned, query.Lit[int64](0)), want: false},
		{name: "match_true", rows: []string{"banned"}, having: query.Gt(banned, query.Lit[int64](0)), want: true},
		{name: "match_false", rows: []string{"banned"}, having: query.Not(query.Gt(banned, query.Lit[int64](0))), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec("DELETE FROM tuple")
			require.NoError(t, err)
			for _, rel := range tc.rows {
				_, err := db.Exec("INSERT INTO tuple (relation) VALUES (?)", rel)
				require.NoError(t, err)
			}

			stmt := query.Select(query.Lit[int64](1)).From(a).Having(tc.having).Limit(1)
			rows, err := sqlite.New(db).Execute(context.Background(), stmt)
			require.NoError(t, err)
			defer rows.Close()
			require.Equal(t, tc.want, rows.Next())
			require.NoError(t, rows.Err())
		})
	}
}
