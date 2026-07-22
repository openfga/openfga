//go:build docker

// These integration tests drive weight1 end to end against a real PostgreSQL database in a test
// container. Each case turns a model DSL into a weighted graph, seeds tuples through the real
// postgres datastore, then runs weight1 against the datastore's own adapter.Builder and asserts
// the boolean decision.
//
// sql_test.go pins the emitted SQL shape and folding behavior against a fake executor; this suite
// closes the remaining gap by proving the emitted SQL returns correct authorization decisions when
// Postgres actually runs it — the existence (HAVING) path, the conditioned (gather) path, and the
// contextual-tuple short-circuit.
//
// Gated behind the `docker` build tag so the check package stays Docker-free in the
// `make test-unit` / `make test` lanes; run with `go test -tags=docker ./internal/check/...`.
package check

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

// pgEnv is the shared PostgreSQL backing for the suite: a single datastore used both to seed
// tuples and, via its Builder, to run weight1. Storagefixtures bootstraps the container once and
// tears it down in TestMain; the datastore is closed via t.Cleanup.
type pgEnv struct {
	ds *postgres.Datastore
}

func setupPgEnv(t *testing.T) *pgEnv {
	t.Helper()

	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")
	ds, err := postgres.New(testDatastore.GetConnectionURI(true), sqlcommon.NewConfig())
	require.NoError(t, err)
	t.Cleanup(ds.Close)

	return &pgEnv{ds: ds}
}

// run seeds tuples under a fresh store id (so cases sharing the container's database never
// collide), then evaluates weight1 for object#relation@user and returns the decision. ReqCtx may
// be nil; when set it is supplied for CEL evaluation on the gather path.
func (e *pgEnv) run(t *testing.T, g *modelgraph.AuthorizationModelGraph, tuples []*openfgav1.TupleKey, object, relation, user string, reqCtx map[string]any, ctxTuples ...*openfgav1.TupleKey) bool {
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
	res, err := s.weight1(ctx, req, e.ds.Builder(req.GetConsistency()), edges, graph.UnionOperator)
	require.NoError(t, err)
	return res.GetAllowed()
}

func mustGraph(t *testing.T, model string) *modelgraph.AuthorizationModelGraph {
	t.Helper()
	g, err := modelgraph.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)
	return g
}

// --- existence path (no conditions) ---------------------------------------

func TestSqlWeight1Integration_Direct(t *testing.T) {
	e := setupPgEnv(t)
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

func TestSqlWeight1Integration_Union(t *testing.T) {
	e := setupPgEnv(t)
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

func TestSqlWeight1Integration_Intersection(t *testing.T) {
	e := setupPgEnv(t)
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

func TestSqlWeight1Integration_Exclusion(t *testing.T) {
	e := setupPgEnv(t)
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

func TestSqlWeight1Integration_Wildcard(t *testing.T) {
	e := setupPgEnv(t)
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
}

// TestSqlWeight1Integration_NestedTree drives a deeply nested weight 1 set-operation model.
// It proves the single existence query's HAVING tree evaluates the whole boolean structure correctly against Postgres.
func TestSqlWeight1Integration_NestedTree(t *testing.T) {
	e := setupPgEnv(t)
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

// --- conditioned path (gather) ---------------------------------------------

func TestSqlWeight1Integration_Conditioned(t *testing.T) {
	e := setupPgEnv(t)
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

// TestSqlWeight1Integration_ConditionedExclusion drives the gather path through an exclusion: a
// conditioned base and an unconditioned subtract.
func TestSqlWeight1Integration_ConditionedExclusion(t *testing.T) {
	e := setupPgEnv(t)
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

// TestSqlWeight1Integration_StaleConditionOnPlainRelation guards a data-integrity edge: a
// conditioned tuple stored against a plain [user] relation must not satisfy the existence (HAVING)
// plan, whose count atoms match only unconditioned rows. The stale row is written directly through
// the datastore, which persists at the storage layer without model validation.
func TestSqlWeight1Integration_StaleConditionOnPlainRelation(t *testing.T) {
	e := setupPgEnv(t)
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

// TestSqlWeight1Integration_GatherFiltersUnknownCondition drives the gather path over a relation
// admitting one condition while a stored tuple carries a different model condition the edge does
// not admit. The gather query's condition filter must exclude that tuple, so the request is
// denied and the tuple never reaches in-app CEL evaluation. A tuple with the admitted condition
// still satisfies the request, proving the filter does not over-narrow.
func TestSqlWeight1Integration_GatherFiltersUnknownCondition(t *testing.T) {
	e := setupPgEnv(t)
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

// TestSqlWeight1Integration_GatherPairsConditionsPerRelation drives the gather path over two
// relations with disjoint condition sets — rel1 admits [user, user with condA], rel2 admits
// [user with condB] — and proves conditions are paired per relation. A rel1 tuple carrying condB
// and a rel2 tuple carrying no condition are each individually within the referenced relation and
// condition sets, yet neither pairing is admitted by the model, so both must be excluded by the
// gather query. Tuples with the correct pairing still grant, proving no over-narrowing.
func TestSqlWeight1Integration_GatherPairsConditionsPerRelation(t *testing.T) {
	e := setupPgEnv(t)
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

func TestSqlWeight1Integration_ContextualShortCircuit(t *testing.T) {
	e := setupPgEnv(t)
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
