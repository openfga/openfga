package graph

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/openfga/openfga/internal/planner"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// countingReadStartingWithUser wraps a datastore and counts calls to
// ReadStartingWithUser, the leaf read the weight-two fast path issues eagerly
// for every direct leaf of its left-hand walk (see fastPathDirect).
type countingReadStartingWithUser struct {
	storage.RelationshipTupleReader
	total int64
}

// ReadStartingWithUser counts every call and delegates to the wrapped
// RelationshipTupleReader, so tests can assert how many eager leaf reads a
// resolve actually issued.
func (c *countingReadStartingWithUser) ReadStartingWithUser(ctx context.Context, store string, filter storage.ReadStartingWithUserFilter, options storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
	atomic.AddInt64(&c.total, 1)
	return c.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
}

// stubPlanner forces the named strategy whenever it is offered, so tests do
// not depend on the planner randomized selection. When the forced strategy
// is not offered, it returns whatever remains (the default), mirroring the
// real planner contract.
type stubPlanner struct{ want string }

// GetPlanSelector returns a selector that always picks the stub's wanted
// strategy when offered, mirroring the real planner contract.
func (p *stubPlanner) GetPlanSelector(keys.Key) planner.Selector { return &stubSelector{want: p.want} }

// Stop satisfies the planner interface; the stub holds no resources.
func (p *stubPlanner) Stop() {}

type stubSelector struct{ want string }

// Select returns the wanted strategy when it is offered, falling back to
// whatever strategy remains (the default) so the test can observe whether
// weight2 was gated off.
func (s *stubSelector) Select(resolvers map[string]*planner.PlanConfig) *planner.PlanConfig {
	if p, ok := resolvers[s.want]; ok {
		return p
	}
	for _, p := range resolvers {
		return p
	}
	return nil
}

// UpdateStats satisfies the selector interface; selection is stateless here.
func (s *stubSelector) UpdateStats(*planner.PlanConfig, time.Duration) {}

// DSL fixtures for the leaf-read counter tests.
const DSL_DIRECT = `model
  schema 1.1
  type user
  type group
    relations
      define members: [user]
  type document
    relations
      define viewer: [group#members]`
const DSL_UNION2 = `model
  schema 1.1
  type user
  type group
    relations
      define members: [user]
      define admins: [user]
      define all: members or admins
  type document
    relations
      define viewer: [group#all]`
const DSL_DIFF = `model
  schema 1.1
  type user
  type group
    relations
      define members: [user]
      define admins: [user]
      define some: members but not admins
  type document
    relations
      define viewer: [group#some]`
const DSL_INTER = `model
  schema 1.1
  type user
  type group
    relations
      define members: [user]
      define admins: [user]
      define both: members and admins
  type document
    relations
      define viewer: [group#both]`
const DSL_COMPUTED = `model
  schema 1.1
  type user
  type group
    relations
      define members: [user]
      define admins: [user]
      define inner: members or admins
      define outer: inner
  type document
    relations
      define viewer: [group#outer]`

// union26DSL is a model whose document#viewer expands through group#all, a
// union of 26 directly assignable relations. The weight-two fast path
// qualifies for it (every path to user has weight <= 2) and its left-hand walk
// starts one ReadStartingWithUser per union child.
const union26DSL = `model
  schema 1.1
  type user
  type group
    relations
      define a: [user]
      define b: [user]
      define c: [user]
      define d: [user]
      define e: [user]
      define f: [user]
      define g: [user]
      define h: [user]
      define i: [user]
      define j: [user]
      define k: [user]
      define l: [user]
      define m: [user]
      define n: [user]
      define o: [user]
      define p: [user]
      define q: [user]
      define r: [user]
      define s: [user]
      define t: [user]
      define u: [user]
      define v: [user]
      define w: [user]
      define x: [user]
      define y: [user]
      define z: [user]
      define all: a or b or c or d or e or f or g or h or i or j or k or l or m or n or o or p or q or r or s or t or u or v or w or x or y or z
  type document
    relations
      define viewer: [group#all]`

// mustTypesystem builds and validates a TypeSystem from the given DSL
// fixture, failing the test on any validation error.
func mustTypesystem(t *testing.T, dsl string) *typesystem.TypeSystem {
	t.Helper()
	ts, err := typesystem.NewAndValidate(context.Background(), testutils.MustTransformDSLToProtoWithID(dsl))
	require.NoError(t, err)
	return ts
}

// weight2CheckRequest builds the minimal check request the leaf-read
// counter needs: a store id, the fixture's authorization model, and a
// document:1#viewer@user:1 tuple key with fresh request metadata.
func weight2CheckRequest(ts *typesystem.TypeSystem) *ResolveCheckRequest {
	return &ResolveCheckRequest{
		StoreID:              "s1",
		AuthorizationModelID: ts.GetAuthorizationModelID(),
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		RequestMetadata:      NewCheckRequestMetadata(),
	}
}

// TestWeight2UsersetFanOutIsBoundedByResolveNodeBreadthLimit pins the fix for
// issue #3305: when the weight-two fast path left-hand walk would start more
// eager leaf reads than the configured resolve node breadth limit, the
// strategy must not be offered and the node must fall back to the default
// resolver (which enforces the limit through its bounded dispatch channel), so
// the operator knob is honored on both paths.
//
// On main, the forced weight2 strategy fans out to all 26 leaf reads despite
// the limit of 5 (verified by reverting this fix: 26 reads > 5).
func TestWeight2UsersetFanOutIsBoundedByResolveNodeBreadthLimit(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ts := mustTypesystem(t, union26DSL)

	// the shape must genuinely qualify for the weight-two fast path
	require.True(t, ts.UsersetUseWeight2Resolver("document", "viewer", "user", &openfgav1.RelationReference{
		Type:               "group",
		RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "all"},
	}), "the union26 model must qualify for the weight2 resolver")

	// and the walk must genuinely be over-budget for the limit used below
	ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
	checker := NewLocalChecker()
	reads, ok := checker.weight2UsersetLeafReadBudget(ctx, weight2CheckRequest(ts), &openfgav1.RelationReference{
		Type:               "group",
		RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "all"},
	})
	require.True(t, ok)
	require.Equal(t, 26, reads)

	cr := &countingReadStartingWithUser{RelationshipTupleReader: memory.New()}
	ctx = storage.ContextWithRelationshipTupleReader(ctx, cr)

	limited := NewLocalChecker(
		WithPlanner(&stubPlanner{want: weightTwoResolver}),
		WithResolveNodeBreadthLimit(5),
	)
	resp, err := limited.ResolveCheck(ctx, weight2CheckRequest(ts))
	require.NoError(t, err)
	require.False(t, resp.GetAllowed())
	require.LessOrEqual(t, atomic.LoadInt64(&cr.total), int64(5),
		"the weight2 walk (26 eager leaf reads) must not run when the resolve node breadth limit is 5")
}

// TestWeight2UsersetRunsWhenWithinBreadthLimit pins the other side: when the
// walk fits within the limit, weight2 is still offered and runs, preserving
// the fast path for the models it was built for.
func TestWeight2UsersetRunsWhenWithinBreadthLimit(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ts := mustTypesystem(t, `model
  schema 1.1
  type user
  type group
    relations
      define members: [user]
      define admins: [user]
      define all: members or admins
  type document
    relations
      define viewer: [group#all]`)

	ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
	cr := &countingReadStartingWithUser{RelationshipTupleReader: memory.New()}
	ctx = storage.ContextWithRelationshipTupleReader(ctx, cr)

	checker := NewLocalChecker(
		WithPlanner(&stubPlanner{want: weightTwoResolver}),
		WithResolveNodeBreadthLimit(10),
	)
	resp, err := checker.ResolveCheck(ctx, weight2CheckRequest(ts))
	require.NoError(t, err)
	require.False(t, resp.GetAllowed())
	require.Equal(t, int64(2), atomic.LoadInt64(&cr.total),
		"within the limit, weight2 must run and issue one leaf read per union child")
}

// TestWeight2TTUFanOutIsBoundedByResolveNodeBreadthLimit is the TTU twin: the
// weight2TTU left-hand walk (one fastPathRewrite per directly related user
// type of the tupleset) is offered only when it fits the breadth limit.
func TestWeight2TTUFanOutIsBoundedByResolveNodeBreadthLimit(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ts := mustTypesystem(t, `model
  schema 1.1
  type user
  type folder
    relations
      define can_view: [user]
  type group
    relations
      define member: [user]
      define can_view: member
  type document
    relations
      define parent: [folder, group]
      define viewer: can_view from parent`)
	ctx := typesystem.ContextWithTypesystem(context.Background(), ts)

	require.True(t, ts.TTUUseWeight2Resolver("document", "viewer", "user", &openfgav1.TupleToUserset{
		Tupleset:        &openfgav1.ObjectRelation{Object: "document", Relation: "parent"},
		ComputedUserset: &openfgav1.ObjectRelation{Object: "document", Relation: "can_view"},
	}), "the TTU model must qualify for the weight2 resolver")

	cr := &countingReadStartingWithUser{RelationshipTupleReader: memory.New()}
	ctx = storage.ContextWithRelationshipTupleReader(ctx, cr)

	req := &ResolveCheckRequest{
		StoreID:              "s1",
		AuthorizationModelID: ts.GetAuthorizationModelID(),
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		RequestMetadata:      NewCheckRequestMetadata(),
	}

	// The left-hand walk spans two parent types (folder#can_view and
	// group#can_view), so it needs a budget of at least 2; with a limit of 1
	// it must be refused and the default resolver must handle the node.
	limited := NewLocalChecker(
		WithPlanner(&stubPlanner{want: weightTwoResolver}),
		WithResolveNodeBreadthLimit(1),
	)
	resp, err := limited.ResolveCheck(ctx, req)
	require.NoError(t, err)
	require.False(t, resp.GetAllowed())
	require.LessOrEqual(t, atomic.LoadInt64(&cr.total), int64(1),
		"the weight2 TTU walk (2 eager leaf reads across parent types) must not run when the limit is 1")
}

// TestWeight2FastPathLeafReadsCounts pins the static leaf-read counter over
// concrete rewrite trees, through the same entry the strategy gate uses, so
// the counting rules (a directly assignable leaf is one read, a computed
// userset follows the computed relation, set operations sum their children)
// cannot drift from the walk they model.
func TestWeight2FastPathLeafReadsCounts(t *testing.T) {
	groupRef := func(relation string) *openfgav1.RelationReference {
		return &openfgav1.RelationReference{
			Type:               "group",
			RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: relation},
		}
	}

	budget := func(ts *typesystem.TypeSystem, ctx context.Context, relation string) (int, bool) {
		checker := NewLocalChecker()
		return checker.weight2UsersetLeafReadBudget(ctx, weight2CheckRequest(ts), groupRef(relation))
	}

	t.Run("direct_leaf_counts_one", func(t *testing.T) {
		ts := mustTypesystem(t, DSL_DIRECT)
		ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
		reads, ok := budget(ts, ctx, "members")
		require.True(t, ok)
		require.Equal(t, 1, reads)
	})

	t.Run("union_of_direct_leaves_sums", func(t *testing.T) {
		ts := mustTypesystem(t, DSL_UNION2)
		ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
		reads, ok := budget(ts, ctx, "all")
		require.True(t, ok)
		require.Equal(t, 2, reads)
	})

	t.Run("difference_sums_base_and_subtract", func(t *testing.T) {
		ts := mustTypesystem(t, DSL_DIFF)
		ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
		reads, ok := budget(ts, ctx, "some")
		require.True(t, ok)
		require.Equal(t, 2, reads)
	})

	t.Run("intersection_sums_children", func(t *testing.T) {
		ts := mustTypesystem(t, DSL_INTER)
		ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
		reads, ok := budget(ts, ctx, "both")
		require.True(t, ok)
		require.Equal(t, 2, reads)
	})

	t.Run("computed_userset_follows_the_computed_relation", func(t *testing.T) {
		ts := mustTypesystem(t, DSL_COMPUTED)
		ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
		reads, ok := budget(ts, ctx, "outer")
		require.True(t, ok)
		require.Equal(t, 2, reads)
	})
}
