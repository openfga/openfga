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
	"github.com/openfga/openfga/pkg/storage/adapter/query"
	"github.com/openfga/openfga/pkg/storage/adapter/sqlite"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

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

// fakeQuerier renders each statement via the sqlite adapter (chosen because its FILTER/discrete
// columns keep the text closest to what these assertions pin), records it, and answers unconditioned
// queries by hasRow and gather queries with canned rows.
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
	sqlText, args := sqlite.Render(stmt)
	q.SQL = sqlText
	q.Parameters = args
	q.sqls = append(q.sqls, sqlText)
	if strings.Contains(sqlText, "SELECT 1 FROM") { // unconditioned query projects an inline literal
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

	require.Contains(t, q.SQL, "SELECT 1 FROM tuple t WHERE")
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
	require.Contains(t, q.SQL, "t.relation IN ('editor', 'owner')")
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

	// gather-free unconditioned path: the wildcard leaf matches subject_id = '*'.
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
	// shared WHERE admits alice or the wildcard tuple.
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

// --- conditioned path (gather) ---------------------------------------------

func TestSqlWeight1_ConditionedGatherShape(t *testing.T) {
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
	// gather projects attribution + condition columns.
	require.Contains(t, q.SQL, "t.relation")
	require.Contains(t, q.SQL, "t.condition_name")
	require.Contains(t, q.SQL, "t.condition_context")
	// WHERE keeps a flat `relation = ...` (single surviving relation) alongside the (relation,
	// condition) pairing; there is no GROUP BY / HAVING aggregate — everything narrows in the WHERE.
	require.Contains(t, q.SQL, "t.relation =")
	require.NotContains(t, q.SQL, "GROUP BY")
	require.NotContains(t, q.SQL, "HAVING")
	require.NotContains(t, q.SQL, "COUNT(1) FILTER (WHERE")
	// The gather is narrowed to the model's single condition, excluding out-of-model conditions in the DB.
	require.Contains(t, q.SQL, "t.condition_name =")
	// Condition names are inlined as literals rather than bound.
	require.Contains(t, q.SQL, "'cond'")
	require.NotContains(t, q.Parameters, "cond")
	// This leaf admits only the named condition, so there is no IS NULL disjunct.
	require.NotContains(t, q.SQL, "t.condition_name IS NULL")
}

// TestSqlWeight1_ConditionedGatherFiltersUnknownCondition pins that the gather query narrows to
// the model's condition, so a tuple with an out-of-model condition is filtered out in SQL.
func TestSqlWeight1_ConditionedGatherFiltersUnknownCondition(t *testing.T) {
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

	require.Contains(t, q.SQL, "t.condition_name =")
	require.Contains(t, q.SQL, "'cond'")
	require.NotContains(t, q.Parameters, "cond")
}

// TestSqlWeight1_ConditionedGatherMixedConditionFilter pins that a relation admitting both an
// unconditioned and a conditioned assignment gathers the named condition or an unconditioned tuple.
func TestSqlWeight1_ConditionedGatherMixedConditionFilter(t *testing.T) {
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

	require.Contains(t, q.SQL, "t.condition_name =")
	require.Contains(t, q.SQL, "t.condition_name IS NULL")
	require.Contains(t, q.SQL, "'cond'")
	require.NotContains(t, q.Parameters, "cond")
}

// TestSqlWeight1_ConditionedGatherPairsConditionsPerRelation pins that the gather filter is a
// disjunction of per-relation clauses, each relation paired only with its own conditions, rather
// than a single global `relation IN (...) AND condition IN (...)`.
func TestSqlWeight1_ConditionedGatherPairsConditionsPerRelation(t *testing.T) {
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

	// WHERE carries the flat `relation IN (...)` and the per-relation pairing, each relation in its
	// own clause and only rel1 carrying the unconditioned (IS NULL) disjunct. No GROUP BY / HAVING.
	require.Contains(t, q.SQL, "t.relation IN")
	require.Contains(t, q.SQL, "t.relation = 'rel1'")
	require.Contains(t, q.SQL, "t.relation = 'rel2'")
	require.Contains(t, q.SQL, "'condA'")
	require.Contains(t, q.SQL, "'condB'")
	require.Contains(t, q.SQL, "t.condition_name IS NULL")
	require.NotContains(t, q.SQL, "HAVING")
	require.NotContains(t, q.SQL, "COUNT(1) FILTER (WHERE")

	// condA inlined within rel1's clause, condB within rel2's (relations sorted). Measure after the
	// flat `relation IN (...)`, whose sorted list also names the relations.
	pairing := q.SQL[strings.LastIndex(q.SQL, "t.relation IN"):]
	rel1 := strings.Index(pairing, "t.relation = 'rel1'")
	rel2 := strings.Index(pairing, "t.relation = 'rel2'")
	condA := strings.Index(pairing, "'condA'")
	condB := strings.Index(pairing, "'condB'")
	require.Less(t, rel1, condA, "condA must be inlined within rel1's clause")
	require.Less(t, condA, rel2, "rel2's clause must follow rel1's")
	require.Less(t, rel2, condB, "condB must be inlined within rel2's clause")
}

func TestSqlWeight1_ConditionedGatherBehavior(t *testing.T) {
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
// (editor directly and via can_edit), so the gather pass visits the same leaf, and row, twice.
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

// TestSqlWeight1_LeafSatisfiedCachesRowEvaluation pins that a gathered row's condition is
// evaluated at most once per request: after the first evaluation, changing the request context
// (which would flip a fresh CEL result) does not change the verdict a second visit returns.
func TestSqlWeight1_LeafSatisfiedCachesRowEvaluation(t *testing.T) {
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
	w := &walker{s: NewSQL(g, nil), req: req, subjType: "user", subjID: "alice"}
	l := leaf{relation: "viewer", conditions: []string{"cond"}}
	gathered := []gatheredRow{{relation: "viewer", subjectID: "alice", condName: "cond"}}

	ok, err := w.leafSatisfied(context.Background(), l, gathered)
	require.NoError(t, err)
	require.True(t, ok)

	req.Context = testutils.MustNewStruct(t, map[string]any{"x": 0})
	ok, err = w.leafSatisfied(context.Background(), l, gathered)
	require.NoError(t, err)
	require.True(t, ok, "second visit must reuse the cached evaluation")
}

// TestSqlWeight1_ConditionedWildcardMatrix exercises wildcard-vs-direct row routing across the
// conditioned/unconditioned combos: an exclusion whose base unions direct and wildcard leaves,
// queried by a direct user so the gather returns both '*' and exact-subject rows.
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
	require.Contains(t, q.SQL, "t.relation IN ('editor', 'viewer')")
	require.Contains(t, q.SQL, "HAVING COUNT(1) FILTER (WHERE t.relation = 'editor') > 0 LIMIT 1")
}

// TestSqlWeight1_ContextualSatisfiesArmOnGatherPath covers the carry-forward from the first pass
// to the gather pass: an intersection where one arm is conditioned (forcing the gather path) and
// the other arm is satisfied only by a contextual tuple. The contextual arm contributes no row and
// is never queried, so the gather pass can only find it true by reusing the first pass's verdict.
func TestSqlWeight1_ContextualSatisfiesArmOnGatherPath(t *testing.T) {
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
	// conditioned viewer arm forces the gather path and is answered by the gathered row.
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

// condCtx marshals a context map the way condition_context is stored, so the gather path's
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
// (HAVING count on the unconditioned path, per-relation clause on the gather path) also name its
// subject; every other combination keeps a single pinned subject and no per-relation subject term.
func TestSqlWeight1_SubjectScopingSQL(t *testing.T) {
	const (
		selectOne = "SELECT 1 FROM tuple t WHERE (t.store = ? AND t.object_type = ? AND t.object_id = ? AND t.user_object_type = ? AND t.user_relation = ? AND "
		gather    = "SELECT t.relation, t.user_object_id, t.condition_name, t.condition_context FROM tuple t WHERE (t.store = ? AND t.object_type = ? AND t.object_id = ? AND t.user_object_type = ? AND t.user_relation = ? AND "
		uncond    = "(t.condition_name IS NULL OR t.condition_name = '')"
	)
	tests := []struct {
		name   string
		rels   string
		user   string
		sql    string
		params []any
	}{
		{
			name:   "case1_direct_user",
			rels:   "define can_view: [user]",
			user:   "user:alice",
			sql:    selectOne + "t.user_object_id = ? AND t.relation = 'can_view' AND " + uncond + ") LIMIT 1",
			params: []any{"store1", "document", "1", "user", "", "alice"},
		},
		{
			name:   "case2_direct_user_conditioned",
			rels:   "define can_view: [user with cond]",
			user:   "user:alice",
			sql:    gather + "t.user_object_id = ? AND t.relation = 'can_view' AND (t.relation = 'can_view' AND t.condition_name = 'cond'))",
			params: []any{"store1", "document", "1", "user", "", "alice"},
		},
		{
			name: "case3_direct_user_wildcard_leaf",
			rels: "define viewer: [user]\n define editor: [user:*]\n define can_view: viewer and editor",
			user: "user:alice",
			sql: selectOne + "t.user_object_id IN (?, ?) AND t.relation IN ('editor', 'viewer') AND " + uncond + ") HAVING (" +
				"COUNT(1) FILTER (WHERE (t.relation = 'viewer' AND t.user_object_id = ?)) > 0 AND " +
				"COUNT(1) FILTER (WHERE (t.relation = 'editor' AND t.user_object_id = '*')) > 0) LIMIT 1",
			params: []any{"store1", "document", "1", "user", "", "alice", "*", "alice"},
		},
		{
			name: "case3_direct_user_wildcard_leaf_same_relation",
			rels: "define can_view: [user, user:*]",
			user: "user:alice",
			sql: selectOne + "t.user_object_id IN (?, ?) AND t.relation = 'can_view' AND " + uncond + ") HAVING (" +
				"COUNT(1) FILTER (WHERE (t.relation = 'can_view' AND t.user_object_id = ?)) > 0 OR " +
				"COUNT(1) FILTER (WHERE (t.relation = 'can_view' AND t.user_object_id = '*')) > 0) LIMIT 1",
			params: []any{"store1", "document", "1", "user", "", "alice", "*", "alice"},
		},
		{
			// Like case3 but a union: HAVING is kept even though the tree is a pure union, since its
			// per-relation filters are what keep a stale viewer@user:* row from matching the viewer leaf.
			name: "case3_direct_user_wildcard_leaf_union",
			rels: "define viewer: [user]\n define editor: [user:*]\n define can_view: viewer or editor",
			user: "user:alice",
			sql: selectOne + "t.user_object_id IN (?, ?) AND t.relation IN ('editor', 'viewer') AND " + uncond + ") HAVING (" +
				"COUNT(1) FILTER (WHERE (t.relation = 'viewer' AND t.user_object_id = ?)) > 0 OR " +
				"COUNT(1) FILTER (WHERE (t.relation = 'editor' AND t.user_object_id = '*')) > 0) LIMIT 1",
			params: []any{"store1", "document", "1", "user", "", "alice", "*", "alice"},
		},
		{
			name: "union_of_intersections",
			rels: "define a: [user]\n define b: [user]\n define c: [user]\n define d: [user]\n define can_view: (a and b) or (c and d)",
			user: "user:alice",
			sql: selectOne + "t.user_object_id = ? AND t.relation IN ('a', 'b', 'c', 'd') AND " + uncond + ") HAVING (" +
				"(COUNT(1) FILTER (WHERE t.relation = 'a') > 0 AND COUNT(1) FILTER (WHERE t.relation = 'b') > 0) OR " +
				"(COUNT(1) FILTER (WHERE t.relation = 'c') > 0 AND COUNT(1) FILTER (WHERE t.relation = 'd') > 0)) LIMIT 1",
			params: []any{"store1", "document", "1", "user", "", "alice"},
		},
		{
			// One intersection under the union keeps the whole tree in HAVING, the bare leaf included.
			name: "union_of_intersection_and_leaf",
			rels: "define a: [user]\n define b: [user]\n define c: [user]\n define can_view: (a and b) or c",
			user: "user:alice",
			sql: selectOne + "t.user_object_id = ? AND t.relation IN ('a', 'b', 'c') AND " + uncond + ") HAVING (" +
				"(COUNT(1) FILTER (WHERE t.relation = 'a') > 0 AND COUNT(1) FILTER (WHERE t.relation = 'b') > 0) OR " +
				"COUNT(1) FILTER (WHERE t.relation = 'c') > 0) LIMIT 1",
			params: []any{"store1", "document", "1", "user", "", "alice"},
		},
		{
			name: "case4_direct_user_wildcard_leaf_conditioned",
			rels: "define viewer: [user with cond]\n define editor: [user:*]\n define can_view: viewer and editor",
			user: "user:alice",
			sql: gather + "t.user_object_id IN (?, ?) AND t.relation IN ('editor', 'viewer') AND (" +
				"(t.relation = 'editor' AND t.user_object_id = '*' AND " + uncond + ") OR " +
				"(t.relation = 'viewer' AND t.user_object_id = ? AND t.condition_name = 'cond')))",
			params: []any{"store1", "document", "1", "user", "", "alice", "*", "alice"},
		},
		{
			// Every leaf is pruned for a wildcard request without a wildcard leaf: no query.
			name: "case5_wildcard_user",
			rels: "define can_view: [user]",
			user: "user:*",
		},
		{
			name: "case6_wildcard_user_conditioned",
			rels: "define can_view: [user with cond]",
			user: "user:*",
		},
		{
			name:   "case7_wildcard_user_wildcard_leaf",
			rels:   "define viewer: [user]\n define editor: [user:*]\n define can_view: viewer or editor",
			user:   "user:*",
			sql:    selectOne + "t.user_object_id = ? AND t.relation = 'editor' AND " + uncond + ") LIMIT 1",
			params: []any{"store1", "document", "1", "user", "", "*"},
		},
		{
			name:   "case8_wildcard_user_wildcard_leaf_conditioned",
			rels:   "define viewer: [user with cond]\n define editor: [user:* with cond]\n define can_view: viewer or editor",
			user:   "user:*",
			sql:    gather + "t.user_object_id = ? AND t.relation = 'editor' AND (t.relation = 'editor' AND t.condition_name = 'cond'))",
			params: []any{"store1", "document", "1", "user", "", "*"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			model := "model\n schema 1.1\ntype user\ntype document\n relations\n " + tc.rels + "\ncondition cond(x: int) { x > 0 }"
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
