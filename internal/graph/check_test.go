package graph

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestResolveCheckDeterministic(t *testing.T) {
	t.Run("resolution_depth_resolves_deterministically", func(t *testing.T) {
		t.Parallel()

		ds := memory.New()

		storeID := ulid.Make().String()

		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			tuple.NewTupleKey("document:1", "editor", "group:other1#member"),
			tuple.NewTupleKey("document:2", "editor", "group:eng#member"),
			tuple.NewTupleKey("document:2", "allowed", "user:jon"),
			tuple.NewTupleKey("document:2", "allowed", "user:x"),
			tuple.NewTupleKey("group:eng", "member", "group:fga#member"),
			tuple.NewTupleKey("group:eng", "member", "user:jon"),
			tuple.NewTupleKey("group:other1", "member", "group:other2#member"),
		})
		require.NoError(t, err)

		checker := NewLocalCheckerWithCycleDetection()
		t.Cleanup(checker.Close)

		model := testutils.MustTransformDSLToProtoWithID(`model
	schema 1.1
type user

type group
  relations
	define member: [user, group#member]

type document
  relations
	define allowed: [user]
	define viewer: [group#member] or editor
	define editor: [group#member] and allowed`)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			typesystem.New(model),
		)

		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:            storeID,
			TupleKey:           tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			ResolutionMetadata: &ResolutionMetadata{Depth: 2},
		})
		require.NoError(t, err)
		require.True(t, resp.Allowed)

		resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:            storeID,
			TupleKey:           tuple.NewTupleKey("document:2", "editor", "user:x"),
			ResolutionMetadata: &ResolutionMetadata{Depth: 2},
		})
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("exclusion_resolves_deterministically_1", func(t *testing.T) {
		t.Parallel()

		ds := memory.New()

		storeID := ulid.Make().String()

		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:budget", "admin", "user:*"),
			tuple.NewTupleKeyWithCondition("document:budget", "viewer", "user:maria", "condX", nil),
		})
		require.NoError(t, err)

		model := testutils.MustTransformDSLToProtoWithID(`model
	schema 1.1
type user

type document
  relations
	define admin: [user:*]
	define viewer: [user with condX] but not admin

condition condX(x: int) {
	x < 100
}
`)
		checker := NewLocalCheckerWithCycleDetection()
		t.Cleanup(checker.Close)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			typesystem.New(model),
		)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		for i := 0; i < 2000; i++ {
			// subtract branch resolves to {allowed: true} even though the base branch
			// results in an error. Outcome should be falsey, not an error.
			resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
				StoreID:            storeID,
				TupleKey:           tuple.NewTupleKey("document:budget", "viewer", "user:maria"),
				ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
			})
			require.NoError(t, err)
			require.False(t, resp.GetAllowed())
		}
	})

	t.Run("exclusion_resolves_deterministically_2", func(t *testing.T) {
		t.Parallel()

		ds := memory.New()

		storeID := ulid.Make().String()

		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKeyWithCondition("document:budget", "admin", "user:maria", "condX", nil),
		})
		require.NoError(t, err)

		model := testutils.MustTransformDSLToProtoWithID(`model
			schema 1.1
		type user

		type document
		  relations
			define admin: [user with condX]
			define viewer: [user] but not admin

		condition condX(x: int) {
			x < 100
		}
		`)

		checker := NewLocalCheckerWithCycleDetection()
		t.Cleanup(checker.Close)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			typesystem.New(model),
		)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		for i := 0; i < 2000; i++ {
			// base should resolve to {allowed: false} even though the subtract branch
			// results in an error. Outcome should be falsey, not an error.
			resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
				StoreID:            storeID,
				TupleKey:           tuple.NewTupleKey("document:budget", "viewer", "user:maria"),
				ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
			})
			require.NoError(t, err)
			require.False(t, resp.GetAllowed())
		}
	})
}

func TestCheckWithOneConcurrentGoroutineCausesNoDeadlock(t *testing.T) {
	const concurrencyLimit = 1
	ds := memory.New()
	defer ds.Close()

	storeID := ulid.Make().String()

	err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "group:1#member"),
		tuple.NewTupleKey("document:1", "viewer", "group:2#member"),
		tuple.NewTupleKey("group:1", "member", "group:1a#member"),
		tuple.NewTupleKey("group:1", "member", "group:1b#member"),
		tuple.NewTupleKey("group:2", "member", "group:2a#member"),
		tuple.NewTupleKey("group:2", "member", "group:2b#member"),
		tuple.NewTupleKey("group:2b", "member", "user:jon"),
	})
	require.NoError(t, err)

	checker := NewLocalCheckerWithCycleDetection(WithResolveNodeBreadthLimit(concurrencyLimit))
	t.Cleanup(checker.Close)

	model := testutils.MustTransformDSLToProtoWithID(`model
	schema 1.1
type user
type group
  relations
	define member: [user, group#member]
type document
  relations
	define viewer: [group#member]`)

	ctx := typesystem.ContextWithTypesystem(
		context.Background(),
		typesystem.New(model),
	)
	ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

	resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:            storeID,
		TupleKey:           tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		ResolutionMetadata: &ResolutionMetadata{Depth: 25},
	})
	require.NoError(t, err)
	require.True(t, resp.Allowed)
}

func TestCheckDatastoreQueryCount(t *testing.T) {
	ds := memory.New()
	defer ds.Close()

	storeID := ulid.Make().String()

	err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:x", "a", "user:jon"),
		tuple.NewTupleKey("document:x", "a", "user:maria"),
		tuple.NewTupleKey("document:x", "b", "user:maria"),
		tuple.NewTupleKey("document:x", "parent", "org:fga"),
		tuple.NewTupleKey("org:fga", "member", "user:maria"),
	})
	require.NoError(t, err)

	model := parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type org
  relations
	define member: [user]

type document
  relations
	define a: [user]
	define b: [user]
	define union: a or b
	define union_rewrite: union
	define intersection: a and b
	define difference: a but not b
	define ttu: member from parent
	define union_and_ttu: union and ttu
	define union_or_ttu: union or ttu or union_rewrite
	define intersection_of_ttus: union_or_ttu and union_and_ttu
	define parent: [org]
`)

	ctx := typesystem.ContextWithTypesystem(
		context.Background(),
		typesystem.New(model),
	)

	tests := []struct {
		name             string
		check            *openfgav1.TupleKey
		contextualTuples []*openfgav1.TupleKey
		allowed          bool
		minDBReads       uint32
		maxDBReads       uint32
	}{
		{
			name:       "no_direct_access",
			check:      tuple.NewTupleKey("document:x", "a", "user:unknown"),
			allowed:    false,
			minDBReads: 1, // both checkDirectUserTuple
			maxDBReads: 1,
		},
		{
			name:       "direct_access",
			check:      tuple.NewTupleKey("document:x", "a", "user:maria"),
			allowed:    true,
			minDBReads: 1, // checkDirectUserTuple needs to run
			maxDBReads: 1,
		},
		{
			name:             "direct_access_thanks_to_contextual_tuple", // NOTE: this is counting the read from memory as a database read!
			check:            tuple.NewTupleKey("document:x", "a", "user:unknown"),
			contextualTuples: []*openfgav1.TupleKey{tuple.NewTupleKey("document:x", "a", "user:unknown")},
			allowed:          true,
			minDBReads:       1, // checkDirectUserTuple needs to run
			maxDBReads:       1,
		},
		{
			name:       "union",
			check:      tuple.NewTupleKey("document:x", "union", "user:maria"),
			allowed:    true,
			minDBReads: 1, // checkDirectUserTuple needs to run
			maxDBReads: 1,
		},
		{
			name:       "union_no_access",
			check:      tuple.NewTupleKey("document:x", "union", "user:unknown"),
			allowed:    false,
			minDBReads: 2, // need to check all the conditions in the union
			maxDBReads: 2,
		},
		{
			name:       "intersection",
			check:      tuple.NewTupleKey("document:x", "intersection", "user:maria"),
			allowed:    true,
			minDBReads: 2, // need at minimum two direct tuple checks
			maxDBReads: 2, // at most two tuple checks
		},
		{
			name:       "intersection_no_access",
			check:      tuple.NewTupleKey("document:x", "intersection", "user:unknown"),
			allowed:    false,
			minDBReads: 1, // need at minimum one direct tuple checks (short circuit the ohter path)
			maxDBReads: 1,
		},
		{
			name:       "difference",
			check:      tuple.NewTupleKey("document:x", "difference", "user:jon"),
			allowed:    true,
			minDBReads: 2, // need at minimum two direct tuple checks
			maxDBReads: 2,
		},
		{
			name:       "difference_no_access",
			check:      tuple.NewTupleKey("document:x", "difference", "user:maria"),
			allowed:    false,
			minDBReads: 1, // if the "but not" condition returns quickly with "false", no need to evaluate the first branch
			maxDBReads: 2, // at most two tuple checks
		},
		{
			name:       "ttu",
			check:      tuple.NewTupleKey("document:x", "ttu", "user:maria"),
			allowed:    true,
			minDBReads: 2, // one read to find org:fga + one direct check if user:maria is a member of org:fga
			maxDBReads: 3, // one read to find org:fga + (one direct check + userset check) if user:maria is a member of org:fga
		},
		{
			name:       "ttu_no_access",
			check:      tuple.NewTupleKey("document:x", "ttu", "user:jon"),
			allowed:    false,
			minDBReads: 2, // one read to find org:fga + (one direct check) to see if user:jon is a member of org:fga
			maxDBReads: 2,
		},
		// more complex scenarios
		{
			name:       "union_and_ttu",
			check:      tuple.NewTupleKey("document:x", "union_and_ttu", "user:maria"),
			allowed:    true,
			minDBReads: 3, // union (1 read) + ttu (2 reads)
			maxDBReads: 5, // union (2 reads) + ttu (3 reads)
		},
		{
			name:       "union_and_ttu_no_access",
			check:      tuple.NewTupleKey("document:x", "union_and_ttu", "user:unknown"),
			allowed:    false,
			minDBReads: 2, // min(union (2 reads), ttu (4 reads))
			maxDBReads: 4, // max(union (2 reads), ttu (4 reads))
		},
		{
			name:       "union_or_ttu",
			check:      tuple.NewTupleKey("document:x", "union_or_ttu", "user:maria"),
			allowed:    true,
			minDBReads: 1, // min(union (1 read), ttu (2 reads))
			maxDBReads: 3, // max(union (2 reads), ttu (3 reads))
		},
		{
			name:       "union_or_ttu_no_access",
			check:      tuple.NewTupleKey("document:x", "union_or_ttu", "user:unknown"),
			allowed:    false,
			minDBReads: 6, // union (2 reads) + ttu (2 reads) + union rewrite (2 reads)
			maxDBReads: 6,
		},
		{
			name:       "intersection_of_ttus", // union_or_ttu and union_and_ttu
			check:      tuple.NewTupleKey("document:x", "intersection_of_ttus", "user:maria"),
			allowed:    true,
			minDBReads: 4, // union_or_ttu (1 read) + union_and_ttu (3 reads)
			maxDBReads: 8, // union_or_ttu (3 reads) + union_and_ttu (5 reads)
		},
	}

	checker := NewLocalCheckerWithCycleDetection(
		WithMaxConcurrentReads(1),
	)
	t.Cleanup(checker.Close)

	// run the test many times to exercise all the possible DBReads
	for i := 1; i < 1000; i++ {
		t.Run(fmt.Sprintf("iteration_%v", i), func(t *testing.T) {
			t.Parallel()
			for _, test := range tests {
				test := test
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()

					ctx := storage.ContextWithRelationshipTupleReader(
						ctx,
						storagewrappers.NewCombinedTupleReader(
							ds,
							test.contextualTuples,
						),
					)

					res, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
						StoreID:            storeID,
						TupleKey:           test.check,
						ContextualTuples:   test.contextualTuples,
						ResolutionMetadata: &ResolutionMetadata{Depth: 25},
					})
					require.NoError(t, err)
					require.Equal(t, res.Allowed, test.allowed)
					// minDBReads <= dbReads <= maxDBReads
					require.GreaterOrEqual(t, res.ResolutionMetadata.DatastoreQueryCount, test.minDBReads)
					require.LessOrEqual(t, res.ResolutionMetadata.DatastoreQueryCount, test.maxDBReads)
				})
			}
		})
	}
}

// TestCheckWithUnexpectedCycle tests the LocalChecker to make sure that if a model includes a cycle
// that should have otherwise been invalid according to the typesystem, then the check resolution will
// avoid the cycle and return an error indicating a cycle was detected.
func TestCheckWithUnexpectedCycle(t *testing.T) {
	ds := memory.New()
	defer ds.Close()

	storeID := ulid.Make().String()

	err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("resource:1", "parent", "resource:1"),
	})
	require.NoError(t, err)

	tests := []struct {
		name     string
		model    string
		tupleKey *openfgav1.TupleKey
	}{
		{
			name: "test_1",
			model: `model
	schema 1.1
type user

type resource
  relations
	define x: [user] but not y
	define y: [user] but not z
	define z: [user] or x`,
			tupleKey: tuple.NewTupleKey("resource:1", "x", "user:jon"),
		},
		{
			name: "test_2",
			model: `model
	schema 1.1
type user

type resource
  relations
	define x: [user] and y
	define y: [user] and z
	define z: [user] or x`,
			tupleKey: tuple.NewTupleKey("resource:1", "x", "user:jon"),
		},
		{
			name: "test_3",
			model: `model
	schema 1.1
type resource
  relations
	define x: y
	define y: x`,
			tupleKey: tuple.NewTupleKey("resource:1", "x", "user:jon"),
		},
		{
			name: "test_4",
			model: `model
	schema 1.1
type resource
  relations
	define parent: [resource]
	define x: [user] or x from parent`,
			tupleKey: tuple.NewTupleKey("resource:1", "x", "user:jon"),
		},
	}

	checker := NewLocalCheckerWithCycleDetection()
	t.Cleanup(checker.Close)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)

			ctx := typesystem.ContextWithTypesystem(
				context.Background(),
				typesystem.New(model),
			)
			ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

			resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
				StoreID:            storeID,
				TupleKey:           test.tupleKey,
				ResolutionMetadata: &ResolutionMetadata{Depth: 25},
			})

			// if the branch producing the cycle is reached first, then an error is returned, otherwise
			// a result is returned if some other terminal path of evaluation was reached before the cycle
			if err != nil {
				require.ErrorIs(t, err, ErrCycleDetected)
			} else {
				require.False(t, resp.GetAllowed())
				require.GreaterOrEqual(t, resp.ResolutionMetadata.DatastoreQueryCount, uint32(1)) // min of 1 (x) if x isn't found and it returns quickly
				require.LessOrEqual(t, resp.ResolutionMetadata.DatastoreQueryCount, uint32(3))    // max of 3 (x, y, z) before the cycle
			}
		})
	}
}

func TestCheckConditions(t *testing.T) {
	ds := memory.New()

	storeID := ulid.Make().String()

	tkConditionContext, err := structpb.NewStruct(map[string]interface{}{
		"param1": "ok",
	})
	require.NoError(t, err)

	model := parser.MustTransformDSLToProto(`model
  schema 1.1

type user

type folder
  relations
    define viewer: [user]

type group
  relations
    define member: [user, group#member with condition1]

type document
  relations
    define parent: [folder with condition1]
	define viewer: [group#member] or viewer from parent

condition condition1(param1: string) {
  param1 == "ok"
}`)

	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKeyWithCondition("document:x", "parent", "folder:x", "condition1", tkConditionContext),
		tuple.NewTupleKeyWithCondition("document:x", "parent", "folder:y", "condition1", nil),
		tuple.NewTupleKey("folder:y", "viewer", "user:bob"),
		tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
		tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
		tuple.NewTupleKeyWithCondition("group:eng", "member", "group:fga#member", "condition1", nil),
		tuple.NewTupleKey("group:fga", "member", "user:jon"),
	}

	err = ds.Write(context.Background(), storeID, nil, tuples)
	require.NoError(t, err)

	checker := NewLocalCheckerWithCycleDetection()
	t.Cleanup(checker.Close)

	typesys, err := typesystem.NewAndValidate(
		context.Background(),
		model,
	)
	require.NoError(t, err)

	ctx := typesystem.ContextWithTypesystem(context.Background(), typesys)
	ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

	conditionContext, err := structpb.NewStruct(map[string]interface{}{
		"param1": "notok",
	})
	require.NoError(t, err)

	resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: model.GetId(),
		TupleKey:             tuple.NewTupleKey("document:x", "parent", "folder:x"),
		ResolutionMetadata:   &ResolutionMetadata{Depth: 1},
		Context:              conditionContext,
	})
	require.NoError(t, err)
	require.True(t, resp.Allowed)

	resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: model.GetId(),
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		ResolutionMetadata:   &ResolutionMetadata{Depth: defaultResolveNodeLimit},
		Context:              conditionContext,
	})
	require.NoError(t, err)
	require.False(t, resp.Allowed)

	resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: model.GetId(),
		TupleKey:             tuple.NewTupleKey("document:x", "viewer", "user:bob"),
		ResolutionMetadata:   &ResolutionMetadata{Depth: defaultResolveNodeLimit},
		Context:              conditionContext,
	})
	require.NoError(t, err)
	require.False(t, resp.Allowed)
}
