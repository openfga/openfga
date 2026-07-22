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

// entryEdges reproduces what ResolveUnion forwards to the group strategy: the flattened
// weight-1 edges out of the entry object#relation node, combined by union.
func entryEdges(t *testing.T, g *modelgraph.AuthorizationModelGraph, req *Request, object, relation string) []*graph.WeightedAuthorizationModelEdge {
	t.Helper()
	node, ok := g.GetNodeByID(tuple.ToObjectRelationString(tuple.GetType(object), relation))
	require.True(t, ok)
	e, err := g.FlattenNode(node, req.GetUserType(), req.IsTypedWildcard(), false)
	require.NoError(t, err)
	return e
}

func runWeight1(t *testing.T, g *modelgraph.AuthorizationModelGraph, exec adaptertest.Executor, object, relation, user string, ctxTuples ...*openfgav1.TupleKey) (bool, error) {
	t.Helper()
	b := adaptertest.New(exec)
	s := NewSQL(g, &sqlDatastore{builder: b})
	req, err := NewRequest(RequestParams{
		StoreID:          "store1",
		Model:            g,
		TupleKey:         tuple.NewTupleKey(object, relation, user),
		ContextualTuples: ctxTuples,
	})
	require.NoError(t, err)
	edges := entryEdges(t, g, req, object, relation)
	res, err := s.weight1(context.Background(), req, b, edges, graph.UnionOperator)
	if err != nil {
		return false, err
	}
	return res.GetAllowed(), nil
}

// --- fakes -----------------------------------------------------------------

// fakeExecutor answers existence queries by hasRow and gather queries with canned rows. It
// records the SQL it saw so structural assertions can be made without pinning exact text.
type fakeExecutor struct {
	rows   [][]any
	hasRow bool

	sqls []string
}

func (e *fakeExecutor) Query(_ context.Context, sql string, _ []any) (adapter.Rows, error) {
	e.sqls = append(e.sqls, sql)
	if strings.Contains(sql, "SELECT 1 FROM") { // existence query projects an inline literal
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

// --- existence path (no conditions) ---------------------------------------

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
	rec := &adaptertest.Recorder{}
	b := adaptertest.New(rec)
	s := NewSQL(g, &sqlDatastore{builder: b})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, b, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)

	require.Contains(t, rec.SQL, "SELECT 1 FROM tuple t WHERE")
	// object_id is pinned to a single value by the shared WHERE, so the existence query relies on
	// one implicit group and emits HAVING without a GROUP BY.
	require.Contains(t, rec.SQL, "HAVING")
	require.NotContains(t, rec.SQL, "GROUP BY")
	require.Contains(t, rec.SQL, "LIMIT 1")
	require.Contains(t, rec.SQL, "COUNT(1) FILTER (WHERE")
	// The inline SELECT/COUNT/comparison constants bind no parameters, so the store is the
	// first bound parameter, followed by object type/id, subject narrowing, and the relation
	// filter (the HAVING relation match).
	require.Equal(t, "store1", rec.Parameters[0])
	require.Contains(t, rec.Parameters, "document")
	require.Contains(t, rec.Parameters, "viewer")
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

	allowed, err := runWeight1(t, g, &fakeExecutor{hasRow: true}, "document:2", "editor", "user:alice")
	require.NoError(t, err)
	require.True(t, allowed)

	allowed, err = runWeight1(t, g, &fakeExecutor{hasRow: false}, "document:2", "editor", "user:alice")
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

	// gather-free existence: the wildcard leaf matches subject_id = '*'.
	rec := &adaptertest.Recorder{}
	b := adaptertest.New(rec)
	s := NewSQL(g, &sqlDatastore{builder: b})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, b, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)
	// shared WHERE admits alice or the wildcard tuple.
	require.Contains(t, rec.Parameters, "*")
	require.Contains(t, rec.Parameters, "alice")
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

	allowed, err := runWeight1(t, g, &fakeExecutor{hasRow: true}, "document:1", "viewer", "group:eng#member")
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

	rec := &adaptertest.Recorder{}
	b := adaptertest.New(rec)
	s := NewSQL(g, &sqlDatastore{builder: b})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, b, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)
	require.Contains(t, rec.SQL, " AND ") // two COUNT atoms ANDed in HAVING
	require.Contains(t, rec.SQL, "HAVING")

	allowed, err := runWeight1(t, g, &fakeExecutor{hasRow: true}, "document:1", "viewer", "user:alice")
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

	rec := &adaptertest.Recorder{}
	b := adaptertest.New(rec)
	s := NewSQL(g, &sqlDatastore{builder: b})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, b, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)
	require.Contains(t, rec.SQL, "NOT (")
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

	rec := &adaptertest.Recorder{}
	b := adaptertest.New(rec)
	s := NewSQL(g, &sqlDatastore{builder: b})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, b, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)
	// gather projects attribution + condition columns, and has no HAVING/GROUP BY.
	require.Contains(t, rec.SQL, "t.relation")
	require.Contains(t, rec.SQL, "t.condition_name")
	require.Contains(t, rec.SQL, "t.condition_context")
	require.NotContains(t, rec.SQL, "HAVING")
	require.NotContains(t, rec.SQL, "GROUP BY")
	// The gather is narrowed to the condition the model admits, so tuples carrying an
	// out-of-model condition are excluded in the database rather than gathered and rejected.
	require.Contains(t, rec.SQL, "t.condition_name IN")
	require.Contains(t, rec.Parameters, "cond")
	// This leaf admits only the named condition (no unconditioned assignment), so there is no
	// IS NULL disjunct.
	require.NotContains(t, rec.SQL, "t.condition_name IS NULL")
}

// TestSqlWeight1_ConditionedGatherFiltersUnknownCondition proves the gather query narrows to the
// model's condition: a tuple whose condition is absent from the model is filtered out in SQL, so
// it is never gathered nor evaluated in-app.
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

	rec := &adaptertest.Recorder{}
	b := adaptertest.New(rec)
	s := NewSQL(g, &sqlDatastore{builder: b})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, b, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)

	require.Contains(t, rec.SQL, "t.condition_name IN")
	require.Contains(t, rec.Parameters, "cond")
}

// TestSqlWeight1_ConditionedGatherMixedConditionFilter proves that when a relation admits both an
// unconditioned assignment and a conditioned one, the gather admits the named condition or an
// unconditioned tuple (condition IS NULL / empty) — and nothing else.
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

	rec := &adaptertest.Recorder{}
	b := adaptertest.New(rec)
	s := NewSQL(g, &sqlDatastore{builder: b})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, b, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)

	require.Contains(t, rec.SQL, "t.condition_name IN")
	require.Contains(t, rec.SQL, "t.condition_name IS NULL")
	require.Contains(t, rec.Parameters, "cond")
}

// TestSqlWeight1_ConditionedGatherPairsConditionsPerRelation guards the per-relation pairing: with
// rel1 admitting [user, user with condA] and rel2 admitting [user with condB], the gather must not
// admit a rel1 tuple carrying condB nor a rel2 tuple with no condition. The filter is therefore a
// disjunction of per-relation clauses, each relation paired only with its own conditions — never a
// single global `relation IN (...) AND condition IN (...)`.
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

	rec := &adaptertest.Recorder{}
	b := adaptertest.New(rec)
	s := NewSQL(g, &sqlDatastore{builder: b})
	req, err := NewRequest(RequestParams{
		StoreID:  "store1",
		Model:    g,
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	_, err = s.weight1(context.Background(), req, b, entryEdges(t, g, req, "document:1", "viewer"), graph.UnionOperator)
	require.NoError(t, err)

	// Each relation is paired with its own conditions in a dedicated clause, so both conditions
	// bind but only rel1 carries the unconditioned (IS NULL) disjunct.
	require.Contains(t, rec.SQL, "t.relation = ?")
	require.Contains(t, rec.Parameters, "condA")
	require.Contains(t, rec.Parameters, "condB")
	require.Contains(t, rec.SQL, "t.condition_name IS NULL")
	// A single global `relation IN (...)` would signal the conditions are not paired per relation.
	require.NotContains(t, rec.SQL, "t.relation IN")

	// The pairing binds condA adjacent to rel1 and condB adjacent to rel2 (relations sorted), so a
	// rel1-with-condB or rel2-with-condA row cannot match.
	rel1 := indexOf(t, rec.Parameters, "rel1")
	rel2 := indexOf(t, rec.Parameters, "rel2")
	condA := indexOf(t, rec.Parameters, "condA")
	condB := indexOf(t, rec.Parameters, "condB")
	require.Less(t, rel1, condA, "condA must be bound within rel1's clause")
	require.Less(t, condA, rel2, "rel2's clause must follow rel1's")
	require.Less(t, rel2, condB, "condB must be bound within rel2's clause")
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
		exec := &fakeExecutor{rows: [][]any{{"viewer", "alice", "", "cond", condCtx(t, map[string]any{"x": 1})}}}
		allowed, err := runWeight1(t, g, exec, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.True(t, allowed)
	})
	t.Run("cel_fails", func(t *testing.T) {
		exec := &fakeExecutor{rows: [][]any{{"viewer", "alice", "", "cond", condCtx(t, map[string]any{"x": 0})}}}
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
		exec := &fakeExecutor{rows: [][]any{{"viewer", "alice", "", "cond", condCtx(t, map[string]any{"x": 1})}}}
		allowed, err := runWeight1(t, g, exec, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.True(t, allowed)
	})
	t.Run("denied_when_banned", func(t *testing.T) {
		exec := &fakeExecutor{rows: [][]any{
			{"viewer", "alice", "", "cond", condCtx(t, map[string]any{"x": 1})},
			{"banned", "alice", "", "", nil},
		}}
		allowed, err := runWeight1(t, g, exec, "document:1", "viewer", "user:alice")
		require.NoError(t, err)
		require.False(t, allowed)
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
	exec := &fakeExecutor{hasRow: false}
	allowed, err := runWeight1(t, g, exec, "document:1", "viewer", "user:alice", ct)
	require.NoError(t, err)
	require.True(t, allowed)
	require.Empty(t, exec.sqls, "executor must not be called when contextual tuples decide the result")
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

// indexOf returns the position of want in the recorded bind parameters, failing the test if it
// is absent. It lets a test assert the relative ordering of binds (e.g. that a condition binds
// within its relation's clause).
func indexOf(t *testing.T, params []any, want string) int {
	t.Helper()
	for i, p := range params {
		if s, ok := p.(string); ok && s == want {
			return i
		}
	}
	require.Failf(t, "parameter not found", "expected %q in %v", want, params)
	return -1
}
