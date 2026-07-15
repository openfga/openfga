package check

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/adaptertest"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

// sqlDatastore is a minimal storage.RelationshipTupleReader whose only meaningful method is
// Builder: weight1 obtains its adapter.Builder from here and touches nothing else, so the
// other reads panic if ever called (they must not be, for the SQL path).
type sqlDatastore struct {
	storage.RelationshipTupleReader
	builder adapter.Builder
}

func (d *sqlDatastore) Builder(openfgav1.ConsistencyPreference) adapter.Builder { return d.builder }

// The subject is stored packed as "type:id[#relation]" in the _user column, so the standard
// ANSI dialect decodes each logical field with string functions. These reproduce those
// decoded expressions for the table alias "t" the strategy always uses, so the golden SQL
// stays readable; the decoding itself is pinned in the ansi package's render tests.
var (
	subjType = "SUBSTRING(t._user FROM 1 FOR POSITION(':' IN t._user) - 1)"

	subjIDRest = "SUBSTRING(t._user FROM POSITION(':' IN t._user) + 1)"
	subjID     = "CASE WHEN POSITION('#' IN " + subjIDRest + ") = 0 THEN " + subjIDRest +
		" ELSE SUBSTRING(" + subjIDRest + " FROM 1 FOR POSITION('#' IN " + subjIDRest + ") - 1) END"

	subjRel = "CASE WHEN POSITION('#' IN t._user) = 0 THEN '' ELSE SUBSTRING(t._user" +
		" FROM POSITION('#' IN t._user) + 1) END"
)

func sqlSharedWhere() string {
	return "t.store = ? AND t.object_type = ? AND t.object_id = ? AND " +
		subjType + " = ? AND " + subjRel + " = ? AND " + subjID + " IN (?, ?)"
}

func sqlRelFilter(n int) string {
	if n == 1 {
		return "t.relation = ?"
	}
	marks := make([]string, n)
	for i := range marks {
		marks[i] = "?"
	}
	return "t.relation IN (" + strings.Join(marks, ", ") + ")"
}

func sqlCountAtom() string {
	return "COUNT(CASE WHEN ((t.relation = ? AND (" + subjID + " = ? OR " + subjID + " = ?)) " +
		"AND (t.condition_name IS NULL OR t.condition_name = ?)) THEN ? END) >= ?"
}

// entryEdge returns the single outgoing edge of the entry node objectType#relation, matching
// what ResolveCheck forwards to the strategy.
func entryEdge(t *testing.T, g *modelgraph.AuthorizationModelGraph, objectType, relation string) *graph.WeightedAuthorizationModelEdge {
	t.Helper()
	node, ok := g.GetNodeByID(tuple.ToObjectRelationString(objectType, relation))
	require.True(t, ok)
	edges, ok := g.GetEdgesFromNode(node)
	require.True(t, ok)
	require.NotEmpty(t, edges)
	return edges[0]
}

func sqlModel(t *testing.T, dsl string) *modelgraph.AuthorizationModelGraph {
	t.Helper()
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(dsl))
	require.NoError(t, err)
	return g
}

func sqlRequest(t *testing.T, g *modelgraph.AuthorizationModelGraph, object, relation, user string) *Request {
	t.Helper()
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey(object, relation, user),
	})
	require.NoError(t, err)
	return req
}

// recordSQL runs weight1 through a Recorder-backed builder and returns the SQL and bind args
// it handed the executor — the same path a live datastore takes, minus the database.
func recordSQL(t *testing.T, dsl, object, relation, user string) (string, []any) {
	t.Helper()
	g := sqlModel(t, dsl)
	rec := &adaptertest.Recorder{}
	s := NewSql(g, &sqlDatastore{builder: adaptertest.New(rec)})
	req := sqlRequest(t, g, object, relation, user)
	_, err := s.weight1(context.Background(), req, entryEdge(t, g, tuple.GetType(object), relation))
	require.NoError(t, err)
	return rec.SQL, rec.Parameters
}

func TestSqlWeight1_DirectHaving(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]`

	sql, args := recordSQL(t, model, "document:1", "viewer", "user:alice")

	want := "SELECT ? FROM tuple t WHERE " + sqlSharedWhere() + " AND " + sqlRelFilter(1) +
		" GROUP BY t.object_id HAVING " + sqlCountAtom()
	require.Equal(t, want, sql)
	require.Equal(t, []any{
		1, "store1", "document", "1", "user", "", "alice", "*",
		"viewer",
		"viewer", "alice", "*", "", 1, 1,
	}, args)
}

func TestSqlWeight1_UsersetSubjectHaving(t *testing.T) {
	// Checking a userset subject (group:eng#member) against a relation that grants it directly is
	// a weight-1 leaf: the subject relation is bound to "member" and the wildcard is not admitted
	// (the shared subject-id bound carries only the exact id).
	model := `
		model
			schema 1.1
		type user
		type group
			relations
				define member: [user]
		type document
			relations
				define viewer: [group#member]`

	sql, args := recordSQL(t, model, "document:1", "viewer", "group:eng#member")

	want := "SELECT ? FROM tuple t WHERE t.store = ? AND t.object_type = ? AND t.object_id = ? AND " +
		subjType + " = ? AND " + subjRel + " = ? AND " + subjID + " IN (?) AND " + sqlRelFilter(1) +
		" GROUP BY t.object_id HAVING " +
		"COUNT(CASE WHEN ((t.relation = ? AND " + subjID + " = ?) " +
		"AND (t.condition_name IS NULL OR t.condition_name = ?)) THEN ? END) >= ?"
	require.Equal(t, want, sql)
	require.Equal(t, []any{
		1, "store1", "document", "1", "group", "member", "eng", // shared WHERE: no wildcard for a userset subject
		"viewer",
		"viewer", "eng", "", 1, 1,
	}, args)
}

func TestSqlWeight1_WildcardHaving(t *testing.T) {
	// A public-access terminal ([user:*]) is a leaf whose subject id is "*". The bound subject id
	// is still alice, so the leaf match tests subject_id = "*" only (the wildcard-terminal form),
	// while the shared WHERE admits alice or the wildcard tuple.
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user:*]`

	sql, args := recordSQL(t, model, "document:1", "viewer", "user:alice")

	want := "SELECT ? FROM tuple t WHERE " + sqlSharedWhere() + " AND " + sqlRelFilter(1) +
		" GROUP BY t.object_id HAVING " +
		"COUNT(CASE WHEN ((t.relation = ? AND " + subjID + " = ?) " +
		"AND (t.condition_name IS NULL OR t.condition_name = ?)) THEN ? END) >= ?"
	require.Equal(t, want, sql)
	require.Equal(t, []any{
		1, "store1", "document", "1", "user", "", "alice", "*",
		"viewer",
		"viewer", "*", "", 1, 1, // leaf subject id is the wildcard
	}, args)
}

func TestSqlWeight1_UnionHaving(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define editor: [user]
				define viewer: [user] or editor`

	sql, args := recordSQL(t, model, "document:1", "viewer", "user:alice")

	want := "SELECT ? FROM tuple t WHERE " + sqlSharedWhere() + " AND " + sqlRelFilter(2) +
		" GROUP BY t.object_id HAVING (" + sqlCountAtom() + " OR " + sqlCountAtom() + ")"
	require.Equal(t, want, sql)
	require.Equal(t, []any{
		1, "store1", "document", "1", "user", "", "alice", "*",
		"editor", "viewer", // relation filter (sorted)
		"viewer", "alice", "*", "", 1, 1, // [user] leaf
		"editor", "alice", "*", "", 1, 1, // editor leaf
	}, args)
}

func TestSqlWeight1_IntersectionHaving(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define editor: [user]
				define viewer: [user] and editor`

	sql, args := recordSQL(t, model, "document:1", "viewer", "user:alice")

	want := "SELECT ? FROM tuple t WHERE " + sqlSharedWhere() + " AND " + sqlRelFilter(2) +
		" GROUP BY t.object_id HAVING (" + sqlCountAtom() + " AND " + sqlCountAtom() + ")"
	require.Equal(t, want, sql)
	require.Equal(t, []any{
		1, "store1", "document", "1", "user", "", "alice", "*",
		"editor", "viewer",
		"viewer", "alice", "*", "", 1, 1,
		"editor", "alice", "*", "", 1, 1,
	}, args)
}

func TestSqlWeight1_ExclusionHaving(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define banned: [user]
				define viewer: [user] but not banned`

	sql, args := recordSQL(t, model, "document:1", "viewer", "user:alice")

	want := "SELECT ? FROM tuple t WHERE " + sqlSharedWhere() + " AND " + sqlRelFilter(2) +
		" GROUP BY t.object_id HAVING (" + sqlCountAtom() + " AND NOT (" + sqlCountAtom() + "))"
	require.Equal(t, want, sql)
	require.Equal(t, []any{
		1, "store1", "document", "1", "user", "", "alice", "*",
		"banned", "viewer", // relation filter (sorted)
		"viewer", "alice", "*", "", 1, 1, // base leaf
		"banned", "alice", "*", "", 1, 1, // subtract leaf
	}, args)
}

func TestSqlWeight1_ConditionedGather(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user, user with cond]
		condition cond(x: int) { x > 0 }`

	sql, args := recordSQL(t, model, "document:1", "viewer", "user:alice")

	// The model deduplicates [user, user with cond] into one edge to the user node carrying both
	// conditions, so it is a single leaf whose condition membership is a nested OR (unconditioned
	// OR the named condition). The presence of the named condition switches the whole subtree to
	// the gather shape: no HAVING, project the attribution columns.
	membership := "((t.condition_name IS NULL OR t.condition_name = ?) OR t.condition_name = ?)"
	want := "SELECT t.relation, " + subjID + ", t.condition_name, t.condition_context FROM tuple t WHERE " +
		sqlSharedWhere() + " AND ((t.relation = ? AND " + membership + "))"
	require.Equal(t, want, sql)
	require.Equal(t, []any{
		"store1", "document", "1", "user", "", "alice", "*",
		"viewer", "", "cond", // single leaf: relation + unconditioned sentinel + named condition
	}, args)
}

// fakeExecutor returns canned rows for a gather scan and reports whether a boolean HAVING
// query returned a row.
type fakeExecutor struct {
	rows    [][]any
	hasRow  bool
	lastSQL string
}

func (e *fakeExecutor) Query(_ context.Context, sql string, _ []any) (adapter.Rows, error) {
	e.lastSQL = sql
	if strings.HasPrefix(sql, "SELECT ? FROM") {
		if e.hasRow {
			return &fakeRows{rows: [][]any{{1}}}, nil
		}
		return &fakeRows{}, nil
	}
	return &fakeRows{rows: e.rows}, nil
}

type fakeRows struct {
	rows [][]any
	i    int
}

func (r *fakeRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	row := r.rows[r.i-1]
	for i := range dest {
		if err := scanInto(dest[i], row[i]); err != nil {
			return err
		}
	}
	return nil
}

// scanInto assigns a canned value into a *sql.NullString or *[]byte scan destination,
// matching the columns gatherQuery projects (relation, subject id, condition name are
// strings; condition context is raw bytes).
func scanInto(dest, val any) error {
	switch d := dest.(type) {
	case *sql.NullString:
		if val == nil {
			*d = sql.NullString{}
			return nil
		}
		*d = sql.NullString{String: val.(string), Valid: true}
	case *[]byte:
		if val == nil {
			*d = nil
			return nil
		}
		*d = val.([]byte)
	default:
		return fmt.Errorf("sql_test: unexpected scan destination %T", dest)
	}
	return nil
}

func (r *fakeRows) Close() error { return nil }
func (r *fakeRows) Err() error   { return nil }

func runWeight1(t *testing.T, g *modelgraph.AuthorizationModelGraph, exec adaptertest.Executor, object, relation, user string) (bool, error) {
	t.Helper()
	s := NewSql(g, &sqlDatastore{builder: adaptertest.New(exec)})
	req := sqlRequest(t, g, object, relation, user)
	res, err := s.weight1(context.Background(), req, entryEdge(t, g, tuple.GetType(object), relation))
	if err != nil {
		return false, err
	}
	return res.GetAllowed(), nil
}

func TestSqlWeight1_ExecuteHaving(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]`
	g := sqlModel(t, model)

	t.Run("granted", func(t *testing.T) {
		allowed, err := runWeight1(t, g, &fakeExecutor{hasRow: true}, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.True(t, allowed)
	})
	t.Run("denied", func(t *testing.T) {
		allowed, err := runWeight1(t, g, &fakeExecutor{hasRow: false}, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.False(t, allowed)
	})
}

func TestSqlWeight1_ExecuteGatherCondition(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user with cond]
		condition cond(x: int) { x > 0 }`
	g := sqlModel(t, model)

	t.Run("cel_passes", func(t *testing.T) {
		exec := &fakeExecutor{rows: [][]any{{"viewer", "alice", "cond", condCtx(t, map[string]any{"x": 1})}}}
		allowed, err := runWeight1(t, g, exec, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.True(t, allowed)
	})
	t.Run("cel_fails", func(t *testing.T) {
		exec := &fakeExecutor{rows: [][]any{{"viewer", "alice", "cond", condCtx(t, map[string]any{"x": 0})}}}
		allowed, err := runWeight1(t, g, exec, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.False(t, allowed)
	})
	t.Run("no_rows", func(t *testing.T) {
		allowed, err := runWeight1(t, g, &fakeExecutor{}, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.False(t, allowed)
	})
}

func TestSqlWeight1_ExecuteGatherExclusion(t *testing.T) {
	// viewer: [user with cond] but not banned. The base leaf is conditioned so the whole tree
	// gathers; the subtract (banned) must remove the grant.
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define banned: [user]
				define viewer: [user with cond] but not banned
		condition cond(x: int) { x > 0 }`
	g := sqlModel(t, model)

	t.Run("granted_when_not_banned", func(t *testing.T) {
		exec := &fakeExecutor{rows: [][]any{{"viewer", "alice", "cond", condCtx(t, map[string]any{"x": 1})}}}
		allowed, err := runWeight1(t, g, exec, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.True(t, allowed)
	})
	t.Run("denied_when_banned", func(t *testing.T) {
		exec := &fakeExecutor{rows: [][]any{
			{"viewer", "alice", "cond", condCtx(t, map[string]any{"x": 1})},
			{"banned", "alice", "", nil},
		}}
		allowed, err := runWeight1(t, g, exec, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.False(t, allowed)
	})
}

// condCtx marshals a request-context map the way condition_context is stored, so the gather
// fold's proto.Unmarshal round-trips it.
func condCtx(t *testing.T, m map[string]any) []byte {
	t.Helper()
	s := testutils.MustNewStruct(t, m)
	b, err := proto.Marshal(s)
	require.NoError(t, err)
	return b
}
