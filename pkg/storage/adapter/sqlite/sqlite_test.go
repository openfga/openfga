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

// newDB returns an in-memory tuple table with one row per object ID; condition_name is NULL.
func newDB(t *testing.T, objectIDs ...string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec("CREATE TABLE tuple (object_id TEXT, relation TEXT, condition_name TEXT)")
	require.NoError(t, err)
	for _, id := range objectIDs {
		_, err := db.Exec("INSERT INTO tuple (object_id) VALUES (?)", id)
		require.NoError(t, err)
	}
	return db
}

func queryStrings(t *testing.T, db *sql.DB, stmt *query.Statement) []string {
	t.Helper()
	rows, err := sqlite.New(db).Execute(context.Background(), stmt)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		out = append(out, s)
	}
	require.NoError(t, rows.Err())
	return out
}

// Unlike MySQL/PostgreSQL's packed _user, SQLite stores the subject in discrete columns.
func TestSQLiteSubjectColumnsAreDiscrete(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.SubjectID()).
		From(a).
		Where(query.Eq(a.SubjectID(), query.Bind("bob")))
	sqlText, args, err := sqlite.Render(stmt)
	require.NoError(t, err)
	want := `SELECT "a".user_object_id FROM tuple "a" WHERE "a".user_object_id = ?`
	assertSQL(t, sqlText, want)
	if len(args) != 1 || args[0] != "bob" {
		t.Errorf("args: got %v", args)
	}
}

// Guards sqliteColumn's exhaustive switch: an unmapped column panics.
func TestSQLiteMapsEveryLogicalColumn(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(
		a.ObjectType(), a.ObjectID(), a.ObjectRelation(),
		a.SubjectType(), a.SubjectID(), a.SubjectRelation(), a.SubjectKind(),
		a.Store(), a.Condition(), a.ConditionContext(),
	).From(a)
	sqlText, _, err := sqlite.Render(stmt)
	require.NoError(t, err)
	want := `SELECT "a".object_type, "a".object_id, "a".relation, ` +
		`"a".user_object_type, "a".user_object_id, "a".user_relation, "a".user_type, ` +
		`"a".store, COALESCE("a".condition_name, ''), "a".condition_context FROM tuple "a"`
	assertSQL(t, sqlText, want)
}

func TestSQLiteSelfJoinAndCast(t *testing.T) {
	a := query.NewTuple("a")
	g := query.NewTuple("g")
	ctxText := query.Cast[string](a.ConditionContext(), query.TypeVarchar)
	stmt := query.Select(a.ObjectID(), ctxText).
		From(a).
		Join(g, query.Eq(a.SubjectID(), g.ObjectID())).
		Where(query.Eq(ctxText, query.Bind("{}")))

	sqlText, _, err := sqlite.Render(stmt)
	require.NoError(t, err)
	want := `SELECT "a".object_id, CAST("a".condition_context AS TEXT) ` +
		`FROM tuple "a" INNER JOIN tuple "g" ON "a".user_object_id = "g".object_id ` +
		`WHERE CAST("a".condition_context AS TEXT) = ?`
	assertSQL(t, sqlText, want)
}

func TestSQLiteNativeFilter(t *testing.T) {
	a := query.NewTuple("a")
	match := query.Eq(a.ObjectRelation(), query.Lit("viewer"))
	stmt := query.Select(query.Count(a.ObjectID(), query.AggFilter(match))).From(a)

	sqlText, _, err := sqlite.Render(stmt)
	require.NoError(t, err)
	assertSQL(t, sqlText,
		`SELECT COUNT("a".object_id) FILTER (WHERE "a".relation = 'viewer') FROM tuple "a"`)
}

func TestSQLiteFilteredCountStar(t *testing.T) {
	a := query.NewTuple("a")
	match := query.Eq(a.ObjectRelation(), query.Lit("viewer"))
	stmt := query.Select(query.Count(query.Star, query.AggFilter(match))).From(a)

	sqlText, _, err := sqlite.Render(stmt)
	require.NoError(t, err)
	assertSQL(t, sqlText, `SELECT COUNT(*) FILTER (WHERE "a".relation = 'viewer') FROM tuple "a"`)
}

func TestSQLitePlainDistinct(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.ObjectID()).From(a).Distinct()

	sqlText, _, err := sqlite.Render(stmt)
	require.NoError(t, err)
	assertSQL(t, sqlText, `SELECT DISTINCT "a".object_id FROM tuple "a"`)
}

func TestSQLiteOffsetUsesNegativeLimit(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.ObjectID()).From(a).Offset(20)

	sqlText, _, err := sqlite.Render(stmt)
	require.NoError(t, err)
	assertSQL(t, sqlText, `SELECT "a".object_id FROM tuple "a" LIMIT -1 OFFSET 20`)
}

func TestSQLiteJSONObject(t *testing.T) {
	a := query.NewTuple("a")
	obj := query.JSONObject(query.Pair(query.Lit("rel"), a.ObjectRelation()))
	stmt := query.Select(obj).From(a)

	sqlText, _, err := sqlite.Render(stmt)
	require.NoError(t, err)
	assertSQL(t, sqlText, `SELECT json_object('rel', "a".relation) FROM tuple "a"`)
}

func TestSQLiteExpandsBoundSet(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(a.ObjectID()).
		From(a).
		Where(query.And(
			query.Eq(a.ObjectType(), query.Bind("document")),
			query.Quantified(a.ObjectRelation(), query.OpEq, query.Any,
				query.BindAll([]string{"viewer", "editor"})),
		))

	sqlText, args, err := sqlite.Render(stmt)
	require.NoError(t, err)
	assertSQL(t, sqlText,
		`SELECT "a".object_id FROM tuple "a" WHERE ("a".object_type = ? AND "a".relation IN (?, ?))`)
	if len(args) != 3 {
		t.Errorf("args: got %v", args)
	}
}

// ORDER BY's bind must be dropped with it, keeping the remaining binds in order.
func TestSQLiteHavingWithoutGroupBy(t *testing.T) {
	a := query.NewTuple("a")
	stmt := query.Select(query.Bind[int64](7)).
		From(a).
		Where(query.Eq(a.ObjectID(), query.Bind("1"))).
		Having(query.Gt(query.Count(query.Star, query.AggFilter(query.Eq(a.ObjectRelation(), query.Bind("viewer")))), query.Lit[int64](0))).
		OrderBy(query.Asc(query.Count(query.Star, query.AggFilter(query.Eq(a.ObjectRelation(), query.Bind("dropped")))))).
		Limit(1)

	sqlText, args, err := sqlite.Render(stmt)
	require.NoError(t, err)
	assertSQL(t, sqlText, `SELECT ? WHERE EXISTS (SELECT COUNT(*) FROM tuple "a" WHERE "a".object_id = ? `+
		`HAVING COUNT(*) FILTER (WHERE "a".relation = ?) > 0) LIMIT 1`)
	if len(args) != 3 || args[0] != int64(7) || args[1] != "1" || args[2] != "viewer" {
		t.Errorf("args: got %v", args)
	}
}

// Shapes SQLite already accepts must not be rewritten.
func TestSQLiteHavingAlreadyAggregate(t *testing.T) {
	a := query.NewTuple("a")
	having := query.Gt(query.Count(query.Star), query.Lit[int64](0))

	sqlText, _, err := sqlite.Render(query.Select(query.Cast[string](query.Count(query.Star), query.TypeVarchar)).From(a).Having(having))
	require.NoError(t, err)
	assertSQL(t, sqlText, `SELECT CAST(COUNT(*) AS TEXT) FROM tuple "a" HAVING COUNT(*) > 0`)

	sqlText, _, err = sqlite.Render(query.Select(a.ObjectID()).From(a).GroupBy(a.ObjectID()).Having(having))
	require.NoError(t, err)
	assertSQL(t, sqlText, `SELECT "a".object_id FROM tuple "a" GROUP BY "a".object_id HAVING COUNT(*) > 0`)
}

// The rewrite must keep the single implicit group even over no rows.
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

// Each alias is only valid when quoted.
func TestSQLiteQuotesAliases(t *testing.T) {
	db := newDB(t, "doc1")
	for _, alias := range []string{"group", "tuple alias", `we"ird`} {
		t.Run(alias, func(t *testing.T) {
			g := query.NewTuple(alias)
			h := query.NewTuple(alias + "2")
			stmt := query.Select(query.As(g.ObjectID(), alias)).
				From(g).
				Join(h, query.Eq(g.ObjectID(), h.ObjectID()))
			require.Equal(t, []string{"doc1"}, queryStrings(t, db, stmt))
		})
	}
}

// SQLite itself rejects 1-arg COALESCE.
func TestSQLiteSingleArgCoalesce(t *testing.T) {
	db := newDB(t)
	sqlText, _, err := sqlite.Render(query.Select(query.Coalesce(query.Lit("x"))))
	require.NoError(t, err)
	assertSQL(t, sqlText, "SELECT 'x'")
	require.Equal(t, []string{"x"}, queryStrings(t, db, query.Select(query.Coalesce(query.Lit("x")))))
}

func TestSQLiteQuantifiedSubquery(t *testing.T) {
	db := newDB(t, "doc1", "doc2")
	a := query.NewTuple("a")
	b := query.NewTuple("b")
	sub := query.SetSubExpr[string](query.Select(b.ObjectID()).From(b).Where(query.Eq(b.ObjectID(), query.Bind("doc1"))))

	anyStmt := query.Select(a.ObjectID()).From(a).Where(query.Quantified(a.ObjectID(), query.OpEq, query.Any, sub))
	sqlText, _, err := sqlite.Render(anyStmt)
	require.NoError(t, err)
	assertSQL(t, sqlText, `SELECT "a".object_id FROM tuple "a" WHERE "a".object_id IN `+
		`(SELECT "b".object_id FROM tuple "b" WHERE "b".object_id = ?)`)
	require.Equal(t, []string{"doc1"}, queryStrings(t, db, anyStmt))

	allStmt := query.Select(a.ObjectID()).From(a).Where(query.Quantified(a.ObjectID(), query.OpNe, query.All, sub))
	sqlText, _, err = sqlite.Render(allStmt)
	require.NoError(t, err)
	assertSQL(t, sqlText, `SELECT "a".object_id FROM tuple "a" WHERE "a".object_id NOT IN `+
		`(SELECT "b".object_id FROM tuple "b" WHERE "b".object_id = ?)`)
	require.Equal(t, []string{"doc2"}, queryStrings(t, db, allStmt))

	for _, unsupported := range []query.Predicate{
		query.Quantified(a.ObjectID(), query.OpLt, query.All, sub),
		query.Quantified(a.ObjectID(), query.OpEq, query.All, sub),
	} {
		stmt := query.Select(a.ObjectID()).From(a).Where(unsupported)
		_, _, err := sqlite.Render(stmt)
		require.ErrorIs(t, err, sqlite.ErrUnsupported)
		_, err = sqlite.New(db).Execute(context.Background(), stmt)
		require.ErrorIs(t, err, sqlite.ErrUnsupported)
	}
}

// Matches the SQLite datastore, which reads a NULL condition_name as "".
func TestSQLiteNullConditionIsEmpty(t *testing.T) {
	db := newDB(t, "doc1")
	a := query.NewTuple("a")
	stmt := query.Select(a.Condition()).From(a).Where(query.Eq(a.Condition(), query.Lit("")))
	require.Equal(t, []string{""}, queryStrings(t, db, stmt))
}
