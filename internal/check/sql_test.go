package check

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/internal/planner"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/query"
	"github.com/openfga/openfga/pkg/storage/adapter/sqlite"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	sqlitestorage "github.com/openfga/openfga/pkg/storage/sqlite"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

// Unit tests (up to the integration banner below): weight1 against fakeQuerier, asserting the
// emitted SQL and the Go logic around it.

// sqlDatastore is a minimal RelationshipTupleReader: weight1 uses only Querier, so any other
// read panics if the SQL path unexpectedly touches it.
type sqlDatastore struct {
	storage.RelationshipTupleReader
	querier adapter.Querier
}

func (d *sqlDatastore) Querier(openfgav1.ConsistencyPreference) adapter.Querier { return d.querier }

// entryEdges returns the flattened weight-1 edges out of the entry object#relation node,
// as ResolveUnion would forward them to the group strategy.
func entryEdges(t *testing.T, g *modelgraph.AuthorizationModelGraph, req *Request, object, relation string) []*graph.WeightedAuthorizationModelEdge {
	t.Helper()
	node, ok := g.GetNodeByID(tuple.ToObjectRelationString(tuple.GetType(object), relation))
	require.True(t, ok)
	e, err := g.FlattenNode(node, req.GetUserType(), req.IsTypedWildcard(), "")
	require.NoError(t, err)
	return e
}

func runWeight1(t *testing.T, g *modelgraph.AuthorizationModelGraph, q *fakeQuerier, object, relation, user string, ctxTuples ...*openfgav1.TupleKey) (bool, error) {
	t.Helper()
	s := NewSQL(g, &sqlDatastore{querier: q})
	req, err := NewRequest(RequestParams{
		StoreID:          "store1",
		Model:            g,
		TupleKey:         tuple.NewTupleKey(object, relation, user),
		ContextualTuples: ctxTuples,
	})
	require.NoError(t, err)
	edges := entryEdges(t, g, req, object, relation)
	res, err := s.weight1(context.Background(), req, q, edges, graph.UnionOperator)
	if err != nil {
		return false, err
	}
	return res.GetAllowed(), nil
}

// --- fakes -----------------------------------------------------------------

// fakeQuerier renders and records each statement via the sqlite adapter, answering unconditioned
// queries by hasRow and conditioned queries with canned rows.
type fakeQuerier struct {
	rows   [][]any
	hasRow bool

	// SQL and Parameters hold the most recently rendered statement, for structural assertions.
	SQL        string
	Parameters []any
	// sqls records every statement seen, letting a test assert the executor was never called.
	sqls []string
}

func (q *fakeQuerier) Execute(_ context.Context, stmt *query.Statement) (adapter.Rows, error) {
	sqlText, args, err := sqlite.Render(stmt)
	if err != nil {
		return nil, err
	}
	q.SQL = sqlText
	q.Parameters = args
	q.sqls = append(q.sqls, sqlText)
	if strings.HasPrefix(sqlText, "SELECT 1 ") { // unconditioned query projects an inline literal
		if q.hasRow {
			return &fakeRows{rows: [][]any{{1}}}, nil
		}
		return &fakeRows{}, nil
	}
	return &fakeRows{rows: q.rows}, nil
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

func scanInto(dest, val any) error {
	switch d := dest.(type) {
	case *string:
		if val == nil {
			*d = ""
			return nil
		}
		*d = val.(string)
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

// --- unconditioned path (no conditions) ------------------------------------

func TestSqlWeight1_Direct(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	t.Run("granted", func(t *testing.T) {
		allowed, err := runWeight1(t, g, &fakeQuerier{hasRow: true}, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.True(t, allowed)
	})
	t.Run("denied", func(t *testing.T) {
		allowed, err := runWeight1(t, g, &fakeQuerier{hasRow: false}, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.False(t, allowed)
	})
}

func TestSqlWeight1_DirectSQLShape(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)
	q := &fakeQuerier{}
	s := NewSQL(g, &sqlDatastore{querier: q})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, q, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)

	require.Contains(t, q.SQL, "SELECT 1 FROM tuple \"t\" WHERE")
	// A single leaf needs no counts: the WHERE alone answers, so LIMIT 1 can stop at the first row.
	require.NotContains(t, q.SQL, "HAVING")
	require.NotContains(t, q.SQL, "GROUP BY")
	require.Contains(t, q.SQL, "LIMIT 1")
	require.NotContains(t, q.SQL, "COUNT(")
	// Relation names are inlined as literals; store id and object type stay bound (store first).
	require.Equal(t, "store1", q.Parameters[0])
	require.Contains(t, q.Parameters, "document")
	require.Contains(t, q.SQL, "'viewer'")
	require.NotContains(t, q.Parameters, "viewer")
}

func TestSqlWeight1_Union(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define owner: [user]
				define editor: [user] or owner`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	q := &fakeQuerier{hasRow: true}
	allowed, err := runWeight1(t, g, q, "document:2", "editor", "user:alice")
	require.NoError(t, err)
	require.True(t, allowed)
	// A pure union (here nested, through the computed owner arm) is answered by the WHERE alone, so
	// LIMIT 1 can stop at the first matching row.
	require.Contains(t, q.SQL, "\"t\".relation IN ('editor', 'owner')")
	require.NotContains(t, q.SQL, "HAVING")
	require.NotContains(t, q.SQL, "COUNT(")

	allowed, err = runWeight1(t, g, &fakeQuerier{hasRow: false}, "document:2", "editor", "user:alice")
	require.NoError(t, err)
	require.False(t, allowed)
}

func TestSqlWeight1_Wildcard(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user:*]`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	q := &fakeQuerier{}
	s := NewSQL(g, &sqlDatastore{querier: q})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, q, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)
	// The WHERE admits both alice and the wildcard.
	require.Contains(t, q.Parameters, "*")
	require.Contains(t, q.Parameters, "alice")
}

func TestSqlWeight1_Userset(t *testing.T) {
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
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	allowed, err := runWeight1(t, g, &fakeQuerier{hasRow: true}, "document:1", "viewer", "group:eng#member")
	require.NoError(t, err)
	require.True(t, allowed)
}

func TestSqlWeight1_Intersection(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define editor: [user]
				define viewer: [user] and editor`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	q := &fakeQuerier{}
	s := NewSQL(g, &sqlDatastore{querier: q})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, q, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)
	require.Contains(t, q.SQL, " AND ") // two COUNT atoms ANDed in HAVING
	require.Contains(t, q.SQL, "HAVING")

	allowed, err := runWeight1(t, g, &fakeQuerier{hasRow: true}, "document:1", "viewer", "user:alice")
	require.NoError(t, err)
	require.True(t, allowed)
}

func TestSqlWeight1_Exclusion(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define banned: [user]
				define viewer: [user] but not banned`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	q := &fakeQuerier{}
	s := NewSQL(g, &sqlDatastore{querier: q})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, q, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)
	require.Contains(t, q.SQL, "NOT (")
}

// --- conditioned path ------------------------------------------------------

func TestSqlWeight1_ConditionedShape(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user with cond]
		condition cond(x: int) { x > 0 }`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	q := &fakeQuerier{}
	s := NewSQL(g, &sqlDatastore{querier: q})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, q, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)
	require.Contains(t, q.SQL, "\"t\".relation")
	require.Contains(t, q.SQL, "COALESCE(\"t\".condition_name, '')")
	require.Contains(t, q.SQL, "\"t\".condition_context")
	// The conditioned query narrows entirely in the WHERE; no aggregate.
	require.Contains(t, q.SQL, "\"t\".relation =")
	require.NotContains(t, q.SQL, "GROUP BY")
	require.NotContains(t, q.SQL, "HAVING")
	require.NotContains(t, q.SQL, "COUNT(1) FILTER (WHERE")
	// Narrowed to the model's condition, so out-of-model conditions in the DB are excluded.
	require.Contains(t, q.SQL, "COALESCE(\"t\".condition_name, '') =")
	require.Contains(t, q.SQL, "'cond'")
	require.NotContains(t, q.Parameters, "cond")
	// This leaf admits only the named condition, so there is no IS NULL disjunct.
	require.NotContains(t, q.SQL, "COALESCE(\"t\".condition_name, '') IS NULL")
}

// TestSqlWeight1_ConditionedFiltersUnknownCondition pins that the conditioned query narrows to
// the model's condition, so a tuple with an out-of-model condition is filtered out in SQL.
func TestSqlWeight1_ConditionedFiltersUnknownCondition(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user with cond]
		condition cond(x: int) { x > 0 }`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	q := &fakeQuerier{}
	s := NewSQL(g, &sqlDatastore{querier: q})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, q, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)

	require.Contains(t, q.SQL, "COALESCE(\"t\".condition_name, '') =")
	require.Contains(t, q.SQL, "'cond'")
	require.NotContains(t, q.Parameters, "cond")
}

// TestSqlWeight1_ConditionedMixedConditionFilter pins that a relation admitting both an
// unconditioned and a conditioned assignment gathers the named condition or an unconditioned tuple.
func TestSqlWeight1_ConditionedMixedConditionFilter(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user, user with cond]
		condition cond(x: int) { x > 0 }`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	q := &fakeQuerier{}
	s := NewSQL(g, &sqlDatastore{querier: q})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, q, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)

	require.Contains(t, q.SQL, "COALESCE(\"t\".condition_name, '') =")
	require.Contains(t, q.SQL, "COALESCE(\"t\".condition_name, '') IS NULL")
	require.Contains(t, q.SQL, "'cond'")
	require.NotContains(t, q.Parameters, "cond")
}

// TestSqlWeight1_ConditionedPairsConditionsPerRelation pins that the conditioned filter is a
// disjunction of per-relation clauses, each relation paired only with its own conditions, rather
// than a single global `relation IN (...) AND condition IN (...)`.
func TestSqlWeight1_ConditionedPairsConditionsPerRelation(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define rel1: [user, user with condA]
				define rel2: [user with condB]
				define viewer: rel1 or rel2
		condition condA(x: int) { x > 0 }
		condition condB(x: int) { x > 0 }`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	q := &fakeQuerier{}
	s := NewSQL(g, &sqlDatastore{querier: q})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, q, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)

	// Only rel1 admits unconditioned tuples, so only it gets the IS NULL disjunct.
	require.Contains(t, q.SQL, "\"t\".relation IN")
	require.Contains(t, q.SQL, "\"t\".relation = 'rel1'")
	require.Contains(t, q.SQL, "\"t\".relation = 'rel2'")
	require.Contains(t, q.SQL, "'condA'")
	require.Contains(t, q.SQL, "'condB'")
	require.Contains(t, q.SQL, "COALESCE(\"t\".condition_name, '') IS NULL")
	require.NotContains(t, q.SQL, "HAVING")
	require.NotContains(t, q.SQL, "COUNT(1) FILTER (WHERE")

	// Search after `relation IN (...)`, whose list also names the relations.
	pairing := q.SQL[strings.LastIndex(q.SQL, "\"t\".relation IN"):]
	rel1 := strings.Index(pairing, "\"t\".relation = 'rel1'")
	rel2 := strings.Index(pairing, "\"t\".relation = 'rel2'")
	condA := strings.Index(pairing, "'condA'")
	condB := strings.Index(pairing, "'condB'")
	require.Less(t, rel1, condA, "condA must be inlined within rel1's clause")
	require.Less(t, condA, rel2, "rel2's clause must follow rel1's")
	require.Less(t, rel2, condB, "condB must be inlined within rel2's clause")
}

func TestSqlWeight1_ConditionedBehavior(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user with cond]
		condition cond(x: int) { x > 0 }`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	t.Run("cel_passes", func(t *testing.T) {
		exec := &fakeQuerier{rows: [][]any{{"viewer", "alice", "cond", condCtx(t, map[string]any{"x": 1})}}}
		allowed, err := runWeight1(t, g, exec, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.True(t, allowed)
	})
	t.Run("cel_fails", func(t *testing.T) {
		exec := &fakeQuerier{rows: [][]any{{"viewer", "alice", "cond", condCtx(t, map[string]any{"x": 0})}}}
		allowed, err := runWeight1(t, g, exec, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.False(t, allowed)
	})
	t.Run("no_rows", func(t *testing.T) {
		allowed, err := runWeight1(t, g, &fakeQuerier{}, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.False(t, allowed)
	})
}

func TestSqlWeight1_ConditionedExclusion(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define banned: [user]
				define viewer: [user with cond] but not banned
		condition cond(x: int) { x > 0 }`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	t.Run("granted_when_not_banned", func(t *testing.T) {
		exec := &fakeQuerier{rows: [][]any{{"viewer", "alice", "cond", condCtx(t, map[string]any{"x": 1})}}}
		allowed, err := runWeight1(t, g, exec, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.True(t, allowed)
	})
	t.Run("denied_when_banned", func(t *testing.T) {
		exec := &fakeQuerier{rows: [][]any{
			{"viewer", "alice", "cond", condCtx(t, map[string]any{"x": 1})},
			{"banned", "alice", "", nil},
		}}
		allowed, err := runWeight1(t, g, exec, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.False(t, allowed)
	})
}

// TestSqlWeight1_ConditionedLazyContextDecode pins that a gathered row's condition context is
// decoded only when its leaf is visited: a malformed context on a row whose leaf is pruned by
// short-circuiting must not fail the check, while one on a visited leaf still errors.
func TestSqlWeight1_ConditionedLazyContextDecode(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define banned: [user with cond]
				define viewer: [user with cond] but not banned
		condition cond(x: int) { x > 0 }`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	malformed := []byte{0xff}

	t.Run("unvisited_row_not_decoded", func(t *testing.T) {
		// The base fails, so the exclusion short-circuits and the banned leaf is never visited.
		exec := &fakeQuerier{rows: [][]any{
			{"viewer", "alice", "cond", condCtx(t, map[string]any{"x": 0})},
			{"banned", "alice", "cond", malformed},
		}}
		allowed, err := runWeight1(t, g, exec, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.False(t, allowed)
	})
	t.Run("visited_row_decode_error", func(t *testing.T) {
		exec := &fakeQuerier{rows: [][]any{{"viewer", "alice", "cond", malformed}}}
		_, err := runWeight1(t, g, exec, "document:1", "viewer", "user:alice")
		require.Error(t, err)
	})
}

// TestSqlWeight1_ConditionedRepeatedLeaf covers a relation reached by two paths in one subtree
// (editor directly and via can_edit), so the conditioned pass visits the same leaf, and row, twice.
func TestSqlWeight1_ConditionedRepeatedLeaf(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define owner: [user]
				define editor: [user with cond]
				define can_edit: editor and owner
				define can_view: editor or can_edit
		condition cond(x: int) { x > 0 }`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	t.Run("granted_when_condition_passes", func(t *testing.T) {
		exec := &fakeQuerier{rows: [][]any{{"editor", "alice", "cond", condCtx(t, map[string]any{"x": 1})}}}
		allowed, err := runWeight1(t, g, exec, "document:1", "can_view", "user:alice")
		require.NoError(t, err)
		require.True(t, allowed)
	})
	t.Run("denied_when_condition_fails_on_both_paths", func(t *testing.T) {
		exec := &fakeQuerier{rows: [][]any{
			{"editor", "alice", "cond", condCtx(t, map[string]any{"x": 0})},
			{"owner", "alice", "", nil},
		}}
		allowed, err := runWeight1(t, g, exec, "document:1", "can_view", "user:alice")
		require.NoError(t, err)
		require.False(t, allowed)
	})
}

// TestSqlWeight1_ConditionedLeafSatisfiedCachesRowEvaluation pins that a gathered row's condition
// is evaluated at most once per request: after the first evaluation, changing the request context
// (which would flip a fresh CEL result) does not change the verdict a second visit returns.
func TestSqlWeight1_ConditionedLeafSatisfiedCachesRowEvaluation(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user with cond]
		condition cond(x: int) { x > 0 }`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		Context:  testutils.MustNewStruct(t, map[string]any{"x": 1}),
	})
	require.NoError(t, err)
	w := &walker{model: g, req: req, subjType: "user", subjID: "alice"}
	l := leaf{relation: "viewer", conditions: []string{"cond"}}
	gathered := []gatheredRow{{relation: "viewer", subjectID: "alice", condName: "cond"}}

	ok, err := w.conditionedLeafSatisfied(context.Background(), l, gathered)
	require.NoError(t, err)
	require.True(t, ok)

	req.Context = testutils.MustNewStruct(t, map[string]any{"x": 0})
	ok, err = w.conditionedLeafSatisfied(context.Background(), l, gathered)
	require.NoError(t, err)
	require.True(t, ok, "second visit must reuse the cached evaluation")
}

// TestSqlWeight1_ConditionedWildcardMatrix exercises wildcard-vs-direct row routing across the
// conditioned/unconditioned combos: an exclusion whose base unions direct and wildcard leaves,
// queried by a direct user so the conditioned query returns both '*' and exact-subject rows.
func TestSqlWeight1_ConditionedWildcardMatrix(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type resource
			relations
				define blocked: [user]
				define direct_conditional: [user with cond]
				define direct_unconditional: [user]
				define wildcard_conditional: [user:* with cond]
				define wildcard_unconditional: [user:*]
				define allowed: direct_conditional or direct_unconditional or wildcard_conditional or wildcard_unconditional
				define combined: allowed but not blocked
		condition cond(x: int) { x > 0 }`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	run := func(t *testing.T, rows [][]any) bool {
		t.Helper()
		allowed, err := runWeight1(t, g, &fakeQuerier{rows: rows}, "resource:A", "combined", "user:alice")
		require.NoError(t, err)
		return allowed
	}

	t.Run("wildcard_conditional_passing_grants", func(t *testing.T) {
		// A passing '*' row must match the wildcard leaf, not be rejected as != the exact subject.
		require.True(t, run(t, [][]any{{"wildcard_conditional", "*", "cond", condCtx(t, map[string]any{"x": 1})}}))
	})
	t.Run("wildcard_conditional_failing_denies", func(t *testing.T) {
		require.False(t, run(t, [][]any{{"wildcard_conditional", "*", "cond", condCtx(t, map[string]any{"x": 0})}}))
	})
	t.Run("wildcard_unconditional_grants", func(t *testing.T) {
		// Direct-user query, unconditioned wildcard leaf: the '*' row satisfies it.
		require.True(t, run(t, [][]any{{"wildcard_unconditional", "*", "", nil}}))
	})
	t.Run("direct_conditional_passing_grants", func(t *testing.T) {
		require.True(t, run(t, [][]any{{"direct_conditional", "alice", "cond", condCtx(t, map[string]any{"x": 1})}}))
	})
	t.Run("direct_unconditional_grants", func(t *testing.T) {
		require.True(t, run(t, [][]any{{"direct_unconditional", "alice", "", nil}}))
	})
	t.Run("blocked_denies_even_with_wildcard_grant", func(t *testing.T) {
		// A blocked row subtracts the wildcard grant.
		require.False(t, run(t, [][]any{
			{"wildcard_unconditional", "*", "", nil},
			{"blocked", "alice", "", nil},
		}))
	})
	t.Run("no_matching_rows_denies", func(t *testing.T) {
		require.False(t, run(t, nil))
	})
}

// --- contextual-tuple short-circuit ----------------------------------------

func TestSqlWeight1_ContextualShortCircuit(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define editor: [user]
				define viewer: [user] or editor`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	// A contextual tuple satisfying the [user] leaf makes the union true without any DB read.
	ct := tuple.NewTupleKey("document:1", "viewer", "user:alice")
	q := &fakeQuerier{hasRow: false}
	allowed, err := runWeight1(t, g, q, "document:1", "viewer", "user:alice", ct)
	require.NoError(t, err)
	require.True(t, allowed)
	require.Empty(t, q.sqls, "executor must not be called when contextual tuples decide the result")
}

// TestSqlWeight1_ContextualNarrowsUnconditionedFilter pins that when a contextual tuple satisfies one
// arm of an intersection, that arm drops out and only the undetermined arm's relation enters the
// unconditioned query.
func TestSqlWeight1_ContextualNarrowsUnconditionedFilter(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define editor: [user]
				define owner: [user]
				define viewer: editor and owner`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	q := &fakeQuerier{}
	s := NewSQL(g, &sqlDatastore{querier: q})
	// A contextual tuple satisfies the editor arm, leaving only owner to be queried.
	ct := tuple.NewTupleKey("document:1", "editor", "user:alice")
	req, err := NewRequest(RequestParams{
		StoreID:          "store1",
		Model:            g,
		TupleKey:         tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		ContextualTuples: []*openfgav1.TupleKey{ct},
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, q, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)

	// Only owner is queried; the contextually-satisfied editor arm is pruned from the SQL entirely.
	require.Contains(t, q.SQL, "'owner'")
	require.NotContains(t, q.SQL, "'editor'")
}

// TestSqlWeight1_ContextualCollapsedUnionBranch pins that HAVING is kept for a union containing an
// exclusion, even once a contextual tuple collapses it. The viewer leaf enters relConds before the
// contextual banned tuple collapses its exclusion to false, so the WHERE still admits viewer rows;
// only HAVING (which counts editor alone) stops a stored viewer row from granting a banned user.
func TestSqlWeight1_ContextualCollapsedUnionBranch(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define banned: [user]
				define editor: [user]
				define viewer: ([user] but not banned) or editor`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	banned := tuple.NewTupleKey("document:1", "banned", "user:alice")
	q := &fakeQuerier{}
	_, err = runWeight1(t, g, q, "document:1", "viewer", "user:alice", banned)
	require.NoError(t, err)
	require.Contains(t, q.SQL, "\"t\".relation IN ('editor', 'viewer')")
	require.Contains(t, q.SQL, "HAVING COUNT(1) FILTER (WHERE \"t\".relation = 'editor') > 0) LIMIT 1")
}

// TestSqlWeight1_ContextualSatisfiesArmOnConditionedPath covers the carry-forward from the first
// pass to the conditioned pass: an intersection where one arm is conditioned (forcing the
// conditioned path) and the other arm is satisfied only by a contextual tuple. The contextual arm
// contributes no row and is never queried, so the conditioned pass can only find it true by
// reusing the first pass's verdict.
func TestSqlWeight1_ContextualSatisfiesArmOnConditionedPath(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define editor: [user]
				define viewer: [user with cond] and editor
		condition cond(x: int) { x > 0 }`
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)

	// A contextual tuple satisfies the (unconditioned) editor arm; it is never queried. The
	// conditioned viewer arm forces the conditioned path and is answered by the gathered row.
	ct := tuple.NewTupleKey("document:1", "editor", "user:alice")

	t.Run("granted_when_conditioned_arm_row_passes", func(t *testing.T) {
		q := &fakeQuerier{rows: [][]any{{"viewer", "alice", "cond", condCtx(t, map[string]any{"x": 1})}}}
		allowed, err := runWeight1(t, g, q, "document:1", "viewer", "user:alice", ct)
		require.NoError(t, err)
		require.True(t, allowed)
	})
	t.Run("denied_when_conditioned_arm_row_fails", func(t *testing.T) {
		q := &fakeQuerier{rows: [][]any{{"viewer", "alice", "cond", condCtx(t, map[string]any{"x": 0})}}}
		allowed, err := runWeight1(t, g, q, "document:1", "viewer", "user:alice", ct)
		require.NoError(t, err)
		require.False(t, allowed)
	})
	t.Run("denied_without_contextual_tuple", func(t *testing.T) {
		// Without the contextual tuple the editor arm has no row either, so the intersection fails
		// even though the conditioned arm's row passes.
		q := &fakeQuerier{rows: [][]any{{"viewer", "alice", "cond", condCtx(t, map[string]any{"x": 1})}}}
		allowed, err := runWeight1(t, g, q, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.False(t, allowed)
	})
}

// condCtx marshals a context map the way condition_context is stored, so the conditioned path's
// proto.Unmarshal round-trips it.
func condCtx(t *testing.T, m map[string]any) []byte {
	t.Helper()
	s := testutils.MustNewStruct(t, m)
	b, err := proto.Marshal(s)
	require.NoError(t, err)
	return b
}

// --- subject scoping -------------------------------------------------------

// TestSqlWeight1_SubjectScopingSQL pins the exact SQL for each combination of request subject
// (direct or wildcard), wildcard leaf, and conditions. Only a direct-user request with a reachable
// wildcard leaf widens the WHERE to the public wildcard, so only then does each relation filter
// (HAVING count on the unconditioned path, per-relation clause on the conditioned path) also name
// its subject; every other combination keeps a single pinned subject and no per-relation subject
// term.
func TestSqlWeight1_SubjectScopingSQL(t *testing.T) {
	const (
		selectOne = "SELECT 1 FROM tuple \"t\" WHERE (\"t\".store = ? AND \"t\".object_type = ? AND \"t\".object_id = ? AND \"t\".user_object_type = ? AND \"t\".user_relation = ? AND "
		// sqlite nests a HAVING without GROUP BY in an EXISTS.
		selectHaving      = "SELECT 1 WHERE EXISTS (SELECT COUNT(*) FROM tuple \"t\" WHERE (\"t\".store = ? AND \"t\".object_type = ? AND \"t\".object_id = ? AND \"t\".user_object_type = ? AND \"t\".user_relation = ? AND "
		selectConditioned = "SELECT \"t\".relation, \"t\".user_object_id, COALESCE(\"t\".condition_name, ''), \"t\".condition_context FROM tuple \"t\" WHERE (\"t\".store = ? AND \"t\".object_type = ? AND \"t\".object_id = ? AND \"t\".user_object_type = ? AND \"t\".user_relation = ? AND "
		uncond            = "(COALESCE(\"t\".condition_name, '') IS NULL OR COALESCE(\"t\".condition_name, '') = '')"
	)
	tests := []struct {
		name   string
		rels   string
		user   string
		sql    string
		params []any
	}{
		{
			name:   "direct_user",
			rels:   "define can_view: [user]",
			user:   "user:alice",
			sql:    selectOne + "\"t\".user_object_id = ? AND \"t\".user_type = 'user' AND \"t\".relation = 'can_view' AND " + uncond + ") LIMIT 1",
			params: []any{"store1", "document", "1", "user", "", "alice"},
		},
		{
			name:   "direct_user_conditioned",
			rels:   "define can_view: [user with cond]",
			user:   "user:alice",
			sql:    selectConditioned + "\"t\".user_object_id = ? AND \"t\".user_type = 'user' AND \"t\".relation = 'can_view' AND (\"t\".relation = 'can_view' AND COALESCE(\"t\".condition_name, '') = 'cond'))",
			params: []any{"store1", "document", "1", "user", "", "alice"},
		},
		{
			name: "direct_user_wildcard_leaf",
			rels: "define viewer: [user]\n define editor: [user:*]\n define can_view: viewer and editor",
			user: "user:alice",
			sql: selectHaving + "\"t\".user_object_id IN (?, ?) AND \"t\".user_type IN ('user', 'userset') AND \"t\".relation IN ('editor', 'viewer') AND " + uncond + ") HAVING (" +
				"COUNT(1) FILTER (WHERE (\"t\".relation = 'viewer' AND \"t\".user_object_id = ?)) > 0 AND " +
				"COUNT(1) FILTER (WHERE (\"t\".relation = 'editor' AND \"t\".user_object_id = '*')) > 0)) LIMIT 1",
			params: []any{"store1", "document", "1", "user", "", "alice", "*", "alice"},
		},
		{
			name: "direct_user_wildcard_leaf_same_relation",
			rels: "define can_view: [user, user:*]",
			user: "user:alice",
			sql: selectHaving + "\"t\".user_object_id IN (?, ?) AND \"t\".user_type IN ('user', 'userset') AND \"t\".relation = 'can_view' AND " + uncond + ") HAVING (" +
				"COUNT(1) FILTER (WHERE (\"t\".relation = 'can_view' AND \"t\".user_object_id = ?)) > 0 OR " +
				"COUNT(1) FILTER (WHERE (\"t\".relation = 'can_view' AND \"t\".user_object_id = '*')) > 0)) LIMIT 1",
			params: []any{"store1", "document", "1", "user", "", "alice", "*", "alice"},
		},
		{
			// HAVING is kept despite the pure union: it pins each relation to its subject ID.
			name: "direct_user_wildcard_leaf_pure_union",
			rels: "define viewer: [user]\n define editor: [user:*]\n define can_view: viewer or editor",
			user: "user:alice",
			sql: selectHaving + "\"t\".user_object_id IN (?, ?) AND \"t\".user_type IN ('user', 'userset') AND \"t\".relation IN ('editor', 'viewer') AND " + uncond + ") HAVING (" +
				"COUNT(1) FILTER (WHERE (\"t\".relation = 'viewer' AND \"t\".user_object_id = ?)) > 0 OR " +
				"COUNT(1) FILTER (WHERE (\"t\".relation = 'editor' AND \"t\".user_object_id = '*')) > 0)) LIMIT 1",
			params: []any{"store1", "document", "1", "user", "", "alice", "*", "alice"},
		},
		{
			name: "union_of_intersections",
			rels: "define a: [user]\n define b: [user]\n define c: [user]\n define d: [user]\n define can_view: (a and b) or (c and d)",
			user: "user:alice",
			sql: selectHaving + "\"t\".user_object_id = ? AND \"t\".user_type = 'user' AND \"t\".relation IN ('a', 'b', 'c', 'd') AND " + uncond + ") HAVING (" +
				"(COUNT(1) FILTER (WHERE \"t\".relation = 'a') > 0 AND COUNT(1) FILTER (WHERE \"t\".relation = 'b') > 0) OR " +
				"(COUNT(1) FILTER (WHERE \"t\".relation = 'c') > 0 AND COUNT(1) FILTER (WHERE \"t\".relation = 'd') > 0))) LIMIT 1",
			params: []any{"store1", "document", "1", "user", "", "alice"},
		},
		{
			// One intersection under the union keeps the whole tree in HAVING, the bare leaf included.
			name: "union_of_intersection_and_leaf",
			rels: "define a: [user]\n define b: [user]\n define c: [user]\n define can_view: (a and b) or c",
			user: "user:alice",
			sql: selectHaving + "\"t\".user_object_id = ? AND \"t\".user_type = 'user' AND \"t\".relation IN ('a', 'b', 'c') AND " + uncond + ") HAVING (" +
				"(COUNT(1) FILTER (WHERE \"t\".relation = 'a') > 0 AND COUNT(1) FILTER (WHERE \"t\".relation = 'b') > 0) OR " +
				"COUNT(1) FILTER (WHERE \"t\".relation = 'c') > 0)) LIMIT 1",
			params: []any{"store1", "document", "1", "user", "", "alice"},
		},
		{
			name: "direct_user_wildcard_leaf_conditioned",
			rels: "define viewer: [user with cond]\n define editor: [user:*]\n define can_view: viewer and editor",
			user: "user:alice",
			sql: selectConditioned + "\"t\".user_object_id IN (?, ?) AND \"t\".user_type IN ('user', 'userset') AND \"t\".relation IN ('editor', 'viewer') AND (" +
				"(\"t\".relation = 'editor' AND \"t\".user_object_id = '*' AND " + uncond + ") OR " +
				"(\"t\".relation = 'viewer' AND \"t\".user_object_id = ? AND COALESCE(\"t\".condition_name, '') = 'cond')))",
			params: []any{"store1", "document", "1", "user", "", "alice", "*", "alice"},
		},
		{
			// Every leaf is pruned for a wildcard request without a wildcard leaf: no query.
			name: "wildcard_user",
			rels: "define can_view: [user]",
			user: "user:*",
		},
		{
			name: "wildcard_user_conditioned",
			rels: "define can_view: [user with cond]",
			user: "user:*",
		},
		{
			name:   "wildcard_user_wildcard_leaf",
			rels:   "define viewer: [user]\n define editor: [user:*]\n define can_view: viewer or editor",
			user:   "user:*",
			sql:    selectOne + "\"t\".user_object_id = ? AND \"t\".user_type = 'userset' AND \"t\".relation = 'editor' AND " + uncond + ") LIMIT 1",
			params: []any{"store1", "document", "1", "user", "", "*"},
		},
		{
			name:   "wildcard_user_wildcard_leaf_conditioned",
			rels:   "define viewer: [user with cond]\n define editor: [user:* with cond]\n define can_view: viewer or editor",
			user:   "user:*",
			sql:    selectConditioned + "\"t\".user_object_id = ? AND \"t\".user_type = 'userset' AND \"t\".relation = 'editor' AND (\"t\".relation = 'editor' AND COALESCE(\"t\".condition_name, '') = 'cond'))",
			params: []any{"store1", "document", "1", "user", "", "*"},
		},
		{
			name:   "userset_subject",
			rels:   "define can_view: [group#member]",
			user:   "group:eng#member",
			sql:    selectOne + "\"t\".user_object_id = ? AND \"t\".user_type = 'userset' AND \"t\".relation = 'can_view' AND " + uncond + ") LIMIT 1",
			params: []any{"store1", "document", "1", "group", "member", "eng"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			model := `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
				type document
					relations
						` + tc.rels + `
				condition cond(x: int) {
					x > 0
				}`
			g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
			require.NoError(t, err)
			q := &fakeQuerier{}
			s := NewSQL(g, &sqlDatastore{querier: q})
			req, err := NewRequest(RequestParams{
				StoreID:  "store1",
				Model:    g,
				TupleKey: tuple.NewTupleKey("document:1", "can_view", tc.user),
			})
			require.NoError(t, err)
			_, err = s.weight1(context.Background(), req, q, entryEdges(t, g, req, "document:1", "can_view"), graph.UnionOperator)
			require.NoError(t, err)
			if tc.sql == "" {
				require.Empty(t, q.sqls)
				return
			}
			require.Equal(t, tc.sql, q.SQL)
			require.Equal(t, tc.params, q.Parameters)
		})
	}
}

// =============================================================================
// Integration tests: weight1 against a real sqlite datastore. Tuples are written
// through the datastore and the generated SQL actually executes, so these verify
// decisions rather than SQL text. Everything above this line uses fakeQuerier.
// =============================================================================

// sqliteEnv is one sqlite datastore, used both to seed tuples and, via its Querier, to run weight1.
type sqliteEnv struct {
	ds *sqlitestorage.Datastore
}

func setupSQLiteEnv(t *testing.T) *sqliteEnv {
	t.Helper()

	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")
	ds, err := sqlitestorage.New(testDatastore.GetConnectionURI(true), sqlcommon.NewConfig())
	require.NoError(t, err)
	t.Cleanup(ds.Close)

	return &sqliteEnv{ds: ds}
}

// run seeds tuples under a fresh store id (so cases never collide), evaluates weight1 for
// object#relation@user, and returns the decision. When set, reqCtx feeds CEL on the conditioned
// path.
func (e *sqliteEnv) run(t *testing.T, g *modelgraph.AuthorizationModelGraph, tuples []*openfgav1.TupleKey, object, relation, user string, reqCtx map[string]any, ctxTuples ...*openfgav1.TupleKey) bool {
	t.Helper()
	ctx := context.Background()
	store := ulid.Make().String()

	if len(tuples) > 0 {
		require.NoError(t, e.ds.Write(ctx, store, nil, tuples))
	}

	params := RequestParams{
		StoreID:          store,
		Model:            g,
		TupleKey:         tuple.NewTupleKey(object, relation, user),
		ContextualTuples: ctxTuples,
	}
	if reqCtx != nil {
		params.Context = testutils.MustNewStruct(t, reqCtx)
	}
	req, err := NewRequest(params)
	require.NoError(t, err)

	s := NewSQL(g, e.ds)
	edges := entryEdges(t, g, req, object, relation)
	res, err := s.weight1(ctx, req, e.ds.Querier(req.GetConsistency()), edges, graph.UnionOperator)
	require.NoError(t, err)
	return res.GetAllowed()
}

func mustGraph(t *testing.T, model string) *modelgraph.AuthorizationModelGraph {
	t.Helper()
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)
	return g
}

// --- unconditioned path ----------------------------------------------------

func TestSQLWeight1Integration_Direct(t *testing.T) {
	e := setupSQLiteEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]`)

	t.Run("granted", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "viewer", "user:alice")}
		require.True(t, e.run(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("denied_no_tuple", func(t *testing.T) {
		require.False(t, e.run(t, g, nil, "document:2", "viewer", "user:alice", nil))
	})
	t.Run("denied_other_user", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "viewer", "user:bob")}
		require.False(t, e.run(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("denied_other_object", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:2", "viewer", "user:alice")}
		require.False(t, e.run(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
}

func TestSQLWeight1Integration_Union(t *testing.T) {
	e := setupSQLiteEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type document
			relations
				define editor: [user]
				define viewer: [user] or editor`)

	t.Run("via_direct", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "viewer", "user:alice")}
		require.True(t, e.run(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("via_editor", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "editor", "user:alice")}
		require.True(t, e.run(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("denied", func(t *testing.T) {
		require.False(t, e.run(t, g, nil, "document:1", "viewer", "user:alice", nil))
	})
}

// TestSQLWeight1Integration_UnionOfIntersections pins that each intersection under a union still
// needs all of its relations, so one relation from each arm does not grant.
func TestSQLWeight1Integration_UnionOfIntersections(t *testing.T) {
	e := setupSQLiteEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type document
			relations
				define a: [user]
				define b: [user]
				define c: [user]
				define d: [user]
				define viewer: (a and b) or (c and d)`)

	tk := func(rel string) *openfgav1.TupleKey { return tuple.NewTupleKey("document:1", rel, "user:alice") }
	tests := []struct {
		name    string
		tuples  []*openfgav1.TupleKey
		allowed bool
	}{
		{name: "granted_first_arm", tuples: []*openfgav1.TupleKey{tk("a"), tk("b")}, allowed: true},
		{name: "granted_second_arm", tuples: []*openfgav1.TupleKey{tk("c"), tk("d")}, allowed: true},
		{name: "denied_one_from_each_arm", tuples: []*openfgav1.TupleKey{tk("a"), tk("d")}, allowed: false},
		{name: "denied_single_relation", tuples: []*openfgav1.TupleKey{tk("b")}, allowed: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.allowed, e.run(t, g, tc.tuples, "document:1", "viewer", "user:alice", nil))
		})
	}
}

func TestSQLWeight1Integration_Intersection(t *testing.T) {
	e := setupSQLiteEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type document
			relations
				define editor: [user]
				define viewer: [user] and editor`)

	t.Run("both", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:alice"),
			tuple.NewTupleKey("document:1", "editor", "user:alice"),
		}
		require.True(t, e.run(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("only_viewer", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "viewer", "user:alice")}
		require.False(t, e.run(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("only_editor", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "editor", "user:alice")}
		require.False(t, e.run(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
}

func TestSQLWeight1Integration_Exclusion(t *testing.T) {
	e := setupSQLiteEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type document
			relations
				define banned: [user]
				define viewer: [user] but not banned`)

	t.Run("granted_not_banned", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "viewer", "user:alice")}
		require.True(t, e.run(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("denied_banned", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:alice"),
			tuple.NewTupleKey("document:1", "banned", "user:alice"),
		}
		require.False(t, e.run(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("denied_no_base", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "banned", "user:alice")}
		require.False(t, e.run(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
}

func TestSQLWeight1Integration_Wildcard(t *testing.T) {
	e := setupSQLiteEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user, user:*]`)

	t.Run("granted_via_wildcard", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "viewer", "user:*")}
		require.True(t, e.run(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("granted_via_direct", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "viewer", "user:alice")}
		require.True(t, e.run(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("denied", func(t *testing.T) {
		require.False(t, e.run(t, g, nil, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("granted_wildcard_request", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "viewer", "user:*")}
		require.True(t, e.run(t, g, tuples, "document:1", "viewer", "user:*", nil))
	})
	t.Run("denied_wildcard_request_direct_tuple", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "viewer", "user:alice")}
		require.False(t, e.run(t, g, tuples, "document:1", "viewer", "user:*", nil))
	})
}

// TestSQLWeight1Integration_UsersetSubject pins that a userset request subject, whose direct edge
// is weight-1 for its own user type, reads its stored tuple, whose user_type is 'userset'.
func TestSQLWeight1Integration_UsersetSubject(t *testing.T) {
	e := setupSQLiteEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type group
			relations
				define member: [user]
		type document
			relations
				define viewer: [group#member]`)

	t.Run("granted", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "viewer", "group:eng#member")}
		require.True(t, e.run(t, g, tuples, "document:1", "viewer", "group:eng#member", nil))
	})
	t.Run("denied", func(t *testing.T) {
		require.False(t, e.run(t, g, nil, "document:1", "viewer", "group:eng#member", nil))
	})
}

// TestSQLWeight1Integration_NestedTree pins that the single unconditioned query's HAVING tree
// evaluates a deeply nested boolean model correctly.
func TestSQLWeight1Integration_NestedTree(t *testing.T) {
	e := setupSQLiteEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type employee
		type document
			relations
				define rel1: [user]
				define rel2: [user]
				define rel3: [user]
				define rel4: [user]
				define rel5: [user]
				define rel6: rel7 and rel8
				define rel7: [user]
				define rel8: [user]
				define rel9: [user]
				define rel10: [employee]
				define complex: [user, employee] or (rel1 and (rel2 or (rel3 but not (rel4 or (rel5 and (rel6 but not rel9)))))) or rel10`)

	rels := func(names ...string) []*openfgav1.TupleKey {
		out := make([]*openfgav1.TupleKey, len(names))
		for i, n := range names {
			out[i] = tuple.NewTupleKey("document:2", n, "user:alice")
		}
		return out
	}

	t.Run("direct_viewer_grants", func(t *testing.T) {
		require.True(t, e.run(t, g, rels("complex"), "document:2", "complex", "user:alice", nil))
	})
	t.Run("rel1_and_rel2_grants", func(t *testing.T) {
		require.True(t, e.run(t, g, rels("rel1", "rel2"), "document:2", "complex", "user:alice", nil))
	})
	t.Run("rel1_alone_denies", func(t *testing.T) {
		require.False(t, e.run(t, g, rels("rel1"), "document:2", "complex", "user:alice", nil))
	})
	t.Run("rel1_rel3_grants_via_but_not", func(t *testing.T) {
		// rel1 and (rel2:false or (rel3 but not (rel4:false or ...))) => rel1 and rel3 => granted.
		require.True(t, e.run(t, g, rels("rel1", "rel3"), "document:2", "complex", "user:alice", nil))
	})
	t.Run("rel1_rel3_rel4_denies", func(t *testing.T) {
		// the rel4 branch of the exclusion fires, so (rel3 but not (rel4 or ...)) is false.
		require.False(t, e.run(t, g, rels("rel1", "rel3", "rel4"), "document:2", "complex", "user:alice", nil))
	})
	t.Run("rel1_rel3_rel5_rel7_rel8_denies", func(t *testing.T) {
		// rel5 and (rel6=rel7 and rel8) but not rel9 => the inner subtract is true, so the rel4-or
		// branch is true, so (rel3 but not ...) is false.
		require.False(t, e.run(t, g, rels("rel1", "rel3", "rel5", "rel7", "rel8"), "document:2", "complex", "user:alice", nil))
	})
	t.Run("rel1_rel3_rel5_rel7_rel8_rel9_grants", func(t *testing.T) {
		// rel9 present flips (rel6 but not rel9) to false, so rel5-and is false, so the exclusion's
		// subtract collapses and (rel3 but not ...) is true again.
		require.True(t, e.run(t, g, rels("rel1", "rel3", "rel5", "rel7", "rel8", "rel9"), "document:2", "complex", "user:alice", nil))
	})
	t.Run("rel10_grants_for_employee", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:2", "rel10", "employee:e1")}
		require.True(t, e.run(t, g, tuples, "document:2", "complex", "employee:e1", nil))
	})
}

// --- conditioned path ------------------------------------------------------

func TestSQLWeight1Integration_Conditioned(t *testing.T) {
	e := setupSQLiteEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user with cond]
		condition cond(x: int) { x > 0 }`)

	condTuple := tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:alice", "cond", nil)

	t.Run("cel_passes", func(t *testing.T) {
		require.True(t, e.run(t, g, []*openfgav1.TupleKey{condTuple}, "document:1", "viewer", "user:alice", map[string]any{"x": 1}))
	})
	t.Run("cel_fails", func(t *testing.T) {
		require.False(t, e.run(t, g, []*openfgav1.TupleKey{condTuple}, "document:1", "viewer", "user:alice", map[string]any{"x": 0}))
	})
	t.Run("no_tuple", func(t *testing.T) {
		require.False(t, e.run(t, g, nil, "document:1", "viewer", "user:alice", map[string]any{"x": 1}))
	})
}

// TestSQLWeight1Integration_ConditionedExclusion drives the conditioned path through an exclusion
// with a conditioned base and an unconditioned subtract.
func TestSQLWeight1Integration_ConditionedExclusion(t *testing.T) {
	e := setupSQLiteEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type document
			relations
				define banned: [user]
				define viewer: [user with cond] but not banned
		condition cond(x: int) { x > 0 }`)

	condTuple := tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:alice", "cond", nil)

	t.Run("granted_when_not_banned", func(t *testing.T) {
		require.True(t, e.run(t, g, []*openfgav1.TupleKey{condTuple}, "document:1", "viewer", "user:alice", map[string]any{"x": 1}))
	})
	t.Run("denied_when_banned", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{condTuple, tuple.NewTupleKey("document:1", "banned", "user:alice")}
		require.False(t, e.run(t, g, tuples, "document:1", "viewer", "user:alice", map[string]any{"x": 1}))
	})
	t.Run("denied_when_condition_fails", func(t *testing.T) {
		require.False(t, e.run(t, g, []*openfgav1.TupleKey{condTuple}, "document:1", "viewer", "user:alice", map[string]any{"x": 0}))
	})
}

// TestSQLWeight1Integration_StaleConditionOnPlainRelation pins that a conditioned tuple stored
// (directly, bypassing model validation) against a plain [user] relation does not satisfy the
// unconditioned plan, whose count atoms match only unconditioned rows.
func TestSQLWeight1Integration_StaleConditionOnPlainRelation(t *testing.T) {
	e := setupSQLiteEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]
		condition cond(x: int) { x > 0 }`)

	stale := tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:alice", "cond", nil)
	require.False(t, e.run(t, g, []*openfgav1.TupleKey{stale}, "document:1", "viewer", "user:alice", nil))
}

// TestSQLWeight1Integration_StaleWildcardOnDirectLeaf pins that a wildcard tuple stored (directly,
// bypassing model validation) against a relation the model types as plain [user] does not satisfy
// that relation's direct leaf, even though another leaf's wildcard widens the shared WHERE to '*'.
func TestSQLWeight1Integration_StaleWildcardOnDirectLeaf(t *testing.T) {
	e := setupSQLiteEnv(t)
	tests := []struct {
		name, viewer string
		reqCtx       map[string]any
	}{
		{name: "unconditioned", viewer: "[user]"},
		{name: "conditioned", viewer: "[user with cond]", reqCtx: map[string]any{"x": 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := mustGraph(t, `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: `+tc.viewer+`
						define editor: [user:*]
						define can_view: viewer and editor
						define can_view_any: viewer or editor
				condition cond(x: int) { x > 0 }`)

			editor := tuple.NewTupleKey("document:1", "editor", "user:*")
			t.Run("denied_stale_wildcard", func(t *testing.T) {
				stale := tuple.NewTupleKey("document:1", "viewer", "user:*")
				require.False(t, e.run(t, g, []*openfgav1.TupleKey{stale, editor}, "document:1", "can_view", "user:alice", tc.reqCtx))
			})
			t.Run("denied_stale_wildcard_union", func(t *testing.T) {
				// The unconditioned union is answered without HAVING, so the WHERE must pin the subject.
				stale := tuple.NewTupleKey("document:1", "viewer", "user:*")
				require.False(t, e.run(t, g, []*openfgav1.TupleKey{stale}, "document:1", "can_view_any", "user:alice", tc.reqCtx))
			})
			t.Run("granted_wildcard_union", func(t *testing.T) {
				require.True(t, e.run(t, g, []*openfgav1.TupleKey{editor}, "document:1", "can_view_any", "user:alice", tc.reqCtx))
			})
			t.Run("granted_direct", func(t *testing.T) {
				direct := tuple.NewTupleKey("document:1", "viewer", "user:alice")
				if tc.reqCtx != nil {
					direct = tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:alice", "cond", nil)
				}
				require.True(t, e.run(t, g, []*openfgav1.TupleKey{direct, editor}, "document:1", "can_view", "user:alice", tc.reqCtx))
			})
		})
	}
}

// TestSQLWeight1Integration_ConditionedFiltersUnknownCondition pins that the conditioned filter
// excludes a tuple carrying a condition the edge does not admit, while a tuple with the admitted
// condition still grants (so the filter does not over-narrow).
func TestSQLWeight1Integration_ConditionedFiltersUnknownCondition(t *testing.T) {
	e := setupSQLiteEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user with cond]
		condition cond(x: int) { x > 0 }
		condition other(x: int) { x > 0 }`)

	t.Run("denied_for_unadmitted_condition", func(t *testing.T) {
		wrong := tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:alice", "other", nil)
		require.False(t, e.run(t, g, []*openfgav1.TupleKey{wrong}, "document:1", "viewer", "user:alice", map[string]any{"x": 1}))
	})
	t.Run("granted_for_admitted_condition", func(t *testing.T) {
		right := tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:alice", "cond", nil)
		require.True(t, e.run(t, g, []*openfgav1.TupleKey{right}, "document:1", "viewer", "user:alice", map[string]any{"x": 1}))
	})
}

// TestSQLWeight1Integration_ConditionedPairsConditionsPerRelation pins per-relation condition
// pairing: a rel1 tuple carrying rel2's condB and an unconditioned rel2 tuple are each excluded
// even though their relation and condition appear elsewhere in the model, while correctly-paired
// tuples grant.
func TestSQLWeight1Integration_ConditionedPairsConditionsPerRelation(t *testing.T) {
	e := setupSQLiteEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type document
			relations
				define rel1: [user, user with condA]
				define rel2: [user with condB]
				define viewer: rel1 or rel2
		condition condA(x: int) { x > 0 }
		condition condB(x: int) { x > 0 }`)

	t.Run("denied_rel1_with_rel2s_condition", func(t *testing.T) {
		wrong := tuple.NewTupleKeyWithCondition("document:1", "rel1", "user:alice", "condB", nil)
		require.False(t, e.run(t, g, []*openfgav1.TupleKey{wrong}, "document:1", "viewer", "user:alice", map[string]any{"x": 1}))
	})
	t.Run("denied_rel2_unconditioned", func(t *testing.T) {
		wrong := tuple.NewTupleKey("document:1", "rel2", "user:alice")
		require.False(t, e.run(t, g, []*openfgav1.TupleKey{wrong}, "document:1", "viewer", "user:alice", map[string]any{"x": 1}))
	})
	t.Run("granted_rel1_with_condA", func(t *testing.T) {
		right := tuple.NewTupleKeyWithCondition("document:1", "rel1", "user:alice", "condA", nil)
		require.True(t, e.run(t, g, []*openfgav1.TupleKey{right}, "document:1", "viewer", "user:alice", map[string]any{"x": 1}))
	})
	t.Run("granted_rel1_unconditioned", func(t *testing.T) {
		right := tuple.NewTupleKey("document:1", "rel1", "user:alice")
		require.True(t, e.run(t, g, []*openfgav1.TupleKey{right}, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("granted_rel2_with_condB", func(t *testing.T) {
		right := tuple.NewTupleKeyWithCondition("document:1", "rel2", "user:alice", "condB", nil)
		require.True(t, e.run(t, g, []*openfgav1.TupleKey{right}, "document:1", "viewer", "user:alice", map[string]any{"x": 1}))
	})
}

// --- contextual-tuple short-circuit ----------------------------------------

func TestSQLWeight1Integration_ContextualShortCircuit(t *testing.T) {
	e := setupSQLiteEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type document
			relations
				define editor: [user]
				define viewer: [user] or editor`)

	// A contextual tuple satisfies the union with no stored tuples at all.
	ct := tuple.NewTupleKey("document:1", "viewer", "user:alice")
	require.True(t, e.run(t, g, nil, "document:1", "viewer", "user:alice", nil, ct))
}

// =============================================================================
// Resolver integration tests: ResolveCheck against a real sqlite datastore with SQLStrategy
// forced for every GroupEdge, cross-checked against DefaultStrategy. These verify that the
// GroupEdges built by GatherLogicalEdges reach weight1 intact when mixed with weight-2+ edges
// or nested under intersections and exclusions.
// =============================================================================

// forceSQLPlanner selects SQLPlan whenever it is a candidate and otherwise delegates, so edge
// strategies keep their usual selection.
type forceSQLPlanner struct {
	planner.Manager
	sqlSelections atomic.Int64
}

func (p *forceSQLPlanner) GetPlanSelector(key keys.Key) planner.Selector {
	return &forceSQLSelector{Selector: p.Manager.GetPlanSelector(key), p: p}
}

type forceSQLSelector struct {
	planner.Selector
	p *forceSQLPlanner
}

func (s *forceSQLSelector) Select(candidates map[string]*planner.PlanConfig) *planner.PlanConfig {
	if plan, ok := candidates[SQLStrategyName]; ok {
		s.p.sqlSelections.Add(1)
		return plan
	}
	return s.Selector.Select(candidates)
}

type resolveEnv struct {
	*sqliteEnv
	base   *planner.Planner
	forced *forceSQLPlanner
}

// setupResolveEnv fails the test at cleanup if SQLStrategy was never selected. The check is
// per test rather than per resolve because a union may short-circuit before its GroupEdge is
// dispatched; denied cases always evaluate it.
func setupResolveEnv(t *testing.T) *resolveEnv {
	t.Helper()
	base := planner.New(&planner.Config{})
	e := &resolveEnv{sqliteEnv: setupSQLiteEnv(t), base: base, forced: &forceSQLPlanner{Manager: base}}
	t.Cleanup(func() {
		base.Stop()
		require.Positive(t, e.forced.sqlSelections.Load(), "SQLStrategy was never selected")
	})
	return e
}

// resolve seeds tuples under a fresh store id and runs ResolveCheck for object#relation@user
// with SQLStrategy forced and with SQL optimizations off, requires the two to agree, and
// returns the decision.
func (e *resolveEnv) resolve(t *testing.T, g *modelgraph.AuthorizationModelGraph, tuples []*openfgav1.TupleKey, object, relation, user string, reqCtx map[string]any, ctxTuples ...*openfgav1.TupleKey) bool {
	t.Helper()
	ctx := context.Background()
	store := ulid.Make().String()

	if len(tuples) > 0 {
		require.NoError(t, e.ds.Write(ctx, store, nil, tuples))
	}

	resolveWith := func(p planner.Manager, sqlEnabled bool) bool {
		params := RequestParams{
			StoreID:          store,
			Model:            g,
			TupleKey:         tuple.NewTupleKey(object, relation, user),
			ContextualTuples: ctxTuples,
		}
		if reqCtx != nil {
			params.Context = testutils.MustNewStruct(t, reqCtx)
		}
		req, err := NewRequest(params)
		require.NoError(t, err)

		res, err := New(Config{
			Model:                   g,
			Datastore:               e.ds,
			Planner:                 p,
			ConcurrencyLimit:        10,
			SQLOptimizationsEnabled: sqlEnabled,
		}).ResolveCheck(ctx, req)
		require.NoError(t, err)
		return res.GetAllowed()
	}

	allowed := resolveWith(e.forced, true)
	require.Equal(t, resolveWith(e.base, false), allowed, "SQLStrategy disagrees with DefaultStrategy")
	return allowed
}

func TestSQLResolverIntegration_UnionGroupWithTTU(t *testing.T) {
	e := setupResolveEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type folder
			relations
				define viewer: [user]
		type document
			relations
				define parent: [folder]
				define editor: [user]
				define viewer: [user] or editor or viewer from parent`)

	parent := tuple.NewTupleKey("document:1", "parent", "folder:1")

	t.Run("granted_via_direct", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{parent, tuple.NewTupleKey("document:1", "viewer", "user:alice")}
		require.True(t, e.resolve(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("granted_via_editor", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{parent, tuple.NewTupleKey("document:1", "editor", "user:alice")}
		require.True(t, e.resolve(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("granted_via_parent", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{parent, tuple.NewTupleKey("folder:1", "viewer", "user:alice")}
		require.True(t, e.resolve(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("granted_via_contextual_editor", func(t *testing.T) {
		ct := tuple.NewTupleKey("document:1", "editor", "user:alice")
		require.True(t, e.resolve(t, g, []*openfgav1.TupleKey{parent}, "document:1", "viewer", "user:alice", nil, ct))
	})
	t.Run("denied", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{parent, tuple.NewTupleKey("document:1", "editor", "user:bob")}
		require.False(t, e.resolve(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("denied_other_object", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{parent, tuple.NewTupleKey("document:1", "editor", "user:alice")}
		require.False(t, e.resolve(t, g, tuples, "document:2", "viewer", "user:alice", nil))
	})
}

func TestSQLResolverIntegration_UnionGroupWithUserset(t *testing.T) {
	e := setupResolveEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type group
			relations
				define member: [user]
		type document
			relations
				define editor: [user]
				define viewer: [user, group#member] or editor`)

	t.Run("granted_via_direct", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "viewer", "user:alice")}
		require.True(t, e.resolve(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("granted_via_editor", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "editor", "user:alice")}
		require.True(t, e.resolve(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("granted_via_group", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			tuple.NewTupleKey("group:eng", "member", "user:alice"),
		}
		require.True(t, e.resolve(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
	t.Run("denied", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "viewer", "group:eng#member")}
		require.False(t, e.resolve(t, g, tuples, "document:1", "viewer", "user:alice", nil))
	})
}

func TestSQLResolverIntegration_IntersectionGroupWithTTU(t *testing.T) {
	e := setupResolveEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type folder
			relations
				define viewer: [user]
		type document
			relations
				define parent: [folder]
				define allowed: [user]
				define can_view: [user] and allowed and viewer from parent`)

	all := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "parent", "folder:1"),
		tuple.NewTupleKey("document:1", "can_view", "user:alice"),
		tuple.NewTupleKey("document:1", "allowed", "user:alice"),
		tuple.NewTupleKey("folder:1", "viewer", "user:alice"),
	}
	without := func(i int) []*openfgav1.TupleKey {
		out := append([]*openfgav1.TupleKey{}, all[:i]...)
		return append(out, all[i+1:]...)
	}

	t.Run("granted_all_operands", func(t *testing.T) {
		require.True(t, e.resolve(t, g, all, "document:1", "can_view", "user:alice", nil))
	})
	t.Run("denied_missing_direct", func(t *testing.T) {
		require.False(t, e.resolve(t, g, without(1), "document:1", "can_view", "user:alice", nil))
	})
	t.Run("denied_missing_allowed", func(t *testing.T) {
		require.False(t, e.resolve(t, g, without(2), "document:1", "can_view", "user:alice", nil))
	})
	t.Run("denied_missing_parent_viewer", func(t *testing.T) {
		require.False(t, e.resolve(t, g, without(3), "document:1", "can_view", "user:alice", nil))
	})
	t.Run("denied_other_user", func(t *testing.T) {
		require.False(t, e.resolve(t, g, all, "document:1", "can_view", "user:bob", nil))
	})
}

func TestSQLResolverIntegration_ExclusionGroupBase(t *testing.T) {
	e := setupResolveEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type folder
			relations
				define blocked: [user]
		type document
			relations
				define parent: [folder]
				define editor: [user]
				define viewer: [user]
				define can_view: (viewer or editor) but not blocked from parent`)

	parent := tuple.NewTupleKey("document:1", "parent", "folder:1")
	blocked := tuple.NewTupleKey("folder:1", "blocked", "user:alice")

	t.Run("granted_via_viewer", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{parent, tuple.NewTupleKey("document:1", "viewer", "user:alice")}
		require.True(t, e.resolve(t, g, tuples, "document:1", "can_view", "user:alice", nil))
	})
	t.Run("granted_via_editor", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{parent, tuple.NewTupleKey("document:1", "editor", "user:alice")}
		require.True(t, e.resolve(t, g, tuples, "document:1", "can_view", "user:alice", nil))
	})
	t.Run("denied_blocked", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{parent, blocked, tuple.NewTupleKey("document:1", "editor", "user:alice")}
		require.False(t, e.resolve(t, g, tuples, "document:1", "can_view", "user:alice", nil))
	})
	t.Run("denied_no_base", func(t *testing.T) {
		require.False(t, e.resolve(t, g, []*openfgav1.TupleKey{parent}, "document:1", "can_view", "user:alice", nil))
	})
}

// TestSQLResolverIntegration_ExclusionGroupSubtract pins that a weight-1 group in the subtract
// position stays the subtract even though the base is heavier.
func TestSQLResolverIntegration_ExclusionGroupSubtract(t *testing.T) {
	e := setupResolveEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type folder
			relations
				define viewer: [user]
		type document
			relations
				define parent: [folder]
				define blocked: [user]
				define banned: [user]
				define can_view: viewer from parent but not (blocked or banned)`)

	base := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "parent", "folder:1"),
		tuple.NewTupleKey("folder:1", "viewer", "user:alice"),
	}

	t.Run("granted", func(t *testing.T) {
		require.True(t, e.resolve(t, g, base, "document:1", "can_view", "user:alice", nil))
	})
	t.Run("denied_blocked", func(t *testing.T) {
		tuples := append([]*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "blocked", "user:alice")}, base...)
		require.False(t, e.resolve(t, g, tuples, "document:1", "can_view", "user:alice", nil))
	})
	t.Run("denied_banned", func(t *testing.T) {
		tuples := append([]*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "banned", "user:alice")}, base...)
		require.False(t, e.resolve(t, g, tuples, "document:1", "can_view", "user:alice", nil))
	})
	t.Run("denied_subtract_without_base", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{tuple.NewTupleKey("document:1", "blocked", "user:alice")}
		require.False(t, e.resolve(t, g, tuples, "document:1", "can_view", "user:alice", nil))
	})
}

// TestSQLResolverIntegration_ExclusionGroupInUnion covers an all-weight-1 exclusion, resolved as
// GroupEdge{base, subtract}, alongside a TTU in a union.
func TestSQLResolverIntegration_ExclusionGroupInUnion(t *testing.T) {
	e := setupResolveEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type folder
			relations
				define viewer: [user]
		type document
			relations
				define parent: [folder]
				define blocked: [user]
				define viewer: [user]
				define can_view: (viewer but not blocked) or viewer from parent`)

	parent := tuple.NewTupleKey("document:1", "parent", "folder:1")
	viewer := tuple.NewTupleKey("document:1", "viewer", "user:alice")
	blocked := tuple.NewTupleKey("document:1", "blocked", "user:alice")

	t.Run("granted_via_exclusion", func(t *testing.T) {
		require.True(t, e.resolve(t, g, []*openfgav1.TupleKey{parent, viewer}, "document:1", "can_view", "user:alice", nil))
	})
	t.Run("denied_blocked", func(t *testing.T) {
		require.False(t, e.resolve(t, g, []*openfgav1.TupleKey{parent, viewer, blocked}, "document:1", "can_view", "user:alice", nil))
	})
	t.Run("denied_blocked_only", func(t *testing.T) {
		require.False(t, e.resolve(t, g, []*openfgav1.TupleKey{parent, blocked}, "document:1", "can_view", "user:alice", nil))
	})
	t.Run("granted_blocked_but_via_parent", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{parent, viewer, blocked, tuple.NewTupleKey("folder:1", "viewer", "user:alice")}
		require.True(t, e.resolve(t, g, tuples, "document:1", "can_view", "user:alice", nil))
	})
}

func TestSQLResolverIntegration_ConditionedGroupWithTTU(t *testing.T) {
	e := setupResolveEnv(t)
	g := mustGraph(t, `
		model
			schema 1.1
		type user
		type folder
			relations
				define viewer: [user]
		type document
			relations
				define parent: [folder]
				define editor: [user]
				define viewer: [user with xcond] or editor or viewer from parent
		condition xcond(x: int) {
			x < 100
		}`)

	parent := tuple.NewTupleKey("document:1", "parent", "folder:1")
	conditioned := tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:alice", "xcond", nil)

	t.Run("granted_condition_met", func(t *testing.T) {
		require.True(t, e.resolve(t, g, []*openfgav1.TupleKey{parent, conditioned}, "document:1", "viewer", "user:alice", map[string]any{"x": 1}))
	})
	t.Run("denied_condition_unmet", func(t *testing.T) {
		require.False(t, e.resolve(t, g, []*openfgav1.TupleKey{parent, conditioned}, "document:1", "viewer", "user:alice", map[string]any{"x": 500}))
	})
	t.Run("granted_condition_unmet_but_editor", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{parent, conditioned, tuple.NewTupleKey("document:1", "editor", "user:alice")}
		require.True(t, e.resolve(t, g, tuples, "document:1", "viewer", "user:alice", map[string]any{"x": 500}))
	})
}
