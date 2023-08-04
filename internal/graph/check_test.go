package graph

import (
	"context"
	"fmt"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
)

func TestResolveCheckDeterministic(t *testing.T) {

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

	checker := NewLocalChecker(ds)

	typedefs := parser.MustParse(`
	type user

	type group
	  relations
	    define member: [user, group#member] as self

	type document
	  relations
	    define allowed: [user] as self
	    define viewer: [group#member] as self or editor
	    define editor: [group#member] as self and allowed
	    
	`)

	ctx := typesystem.ContextWithTypesystem(context.Background(), typesystem.New(
		&openfgav1.AuthorizationModel{
			Id:              ulid.Make().String(),
			TypeDefinitions: typedefs,
			SchemaVersion:   typesystem.SchemaVersion1_1,
		},
	))

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

	checker := NewLocalChecker(ds, WithResolveNodeBreadthLimit(concurrencyLimit))

	typedefs := parser.MustParse(`
	type user
	type group
	  relations
		define member: [user, group#member] as self
	type document
	  relations
		define viewer: [group#member] as self
	`)

	ctx := typesystem.ContextWithTypesystem(context.Background(), typesystem.New(
		&openfgav1.AuthorizationModel{
			Id:              ulid.Make().String(),
			TypeDefinitions: typedefs,
			SchemaVersion:   typesystem.SchemaVersion1_1,
		},
	))

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

	typedefs := parser.MustParse(`
	type user

	type org
      relations
		define member: [user] as self

	type document
	  relations
		define a: [user] as self
		define b: [user] as self
		define union as a or b
		define union_rewrite as union
		define intersection as a and b
		define difference as a but not b
		define ttu as member from parent
        define union_and_ttu as union and ttu
		define union_or_ttu as union or ttu or union_rewrite
		define intersection_of_ttus as union_or_ttu and union_and_ttu
		define parent: [org] as self
	`)

	ctx := typesystem.ContextWithTypesystem(context.Background(), typesystem.New(
		&openfgav1.AuthorizationModel{
			Id:              ulid.Make().String(),
			TypeDefinitions: typedefs,
			SchemaVersion:   typesystem.SchemaVersion1_1,
		},
	))

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
			minDBReads: 2, // both checkDirectUserTuple and checkDirectUsersetTuples need to run
			maxDBReads: 2,
		},
		{
			name:       "direct_access",
			check:      tuple.NewTupleKey("document:x", "a", "user:maria"),
			allowed:    true,
			minDBReads: 1, // checkDirectUserTuple needs to run
			maxDBReads: 2, // maybe checkDirectUsersetTuples also runs
		},
		{
			name:             "direct_access_thanks_to_contextual_tuple", // NOTE: this is counting the read from memory as a database read!
			check:            tuple.NewTupleKey("document:x", "a", "user:unknown"),
			contextualTuples: []*openfgav1.TupleKey{tuple.NewTupleKey("document:x", "a", "user:unknown")},
			allowed:          true,
			minDBReads:       1, // checkDirectUserTuple needs to run
			maxDBReads:       2, // maybe checkDirectUsersetTuples also runs
		},
		{
			name:       "union",
			check:      tuple.NewTupleKey("document:x", "union", "user:maria"),
			allowed:    true,
			minDBReads: 1, // checkDirectUserTuple needs to run
			maxDBReads: 2, // maybe checkDirectUsersetTuples also runs
		},
		{
			name:       "union_no_access",
			check:      tuple.NewTupleKey("document:x", "union", "user:unknown"),
			allowed:    false,
			minDBReads: 4, // need to check all the conditions in the union, so two direct tuple lookups + two userset lookups
			maxDBReads: 4,
		},
		{
			name:       "intersection",
			check:      tuple.NewTupleKey("document:x", "intersection", "user:maria"),
			allowed:    true,
			minDBReads: 2, // need at minimum two direct tuple checks
			maxDBReads: 4, // at most two tuple checks + two userset checks
		},
		{
			name:       "intersection_no_access",
			check:      tuple.NewTupleKey("document:x", "intersection", "user:unknown"),
			allowed:    false,
			minDBReads: 2, // need at minimum two direct tuple checks
			maxDBReads: 4, // at most two tuple checks + two userset checks
		},
		{
			name:       "difference",
			check:      tuple.NewTupleKey("document:x", "difference", "user:jon"),
			allowed:    true,
			minDBReads: 2, // need at minimum two direct tuple checks
			maxDBReads: 4, // at most two tuple checks + two userset checks
		},
		{
			name:       "difference_no_access",
			check:      tuple.NewTupleKey("document:x", "difference", "user:maria"),
			allowed:    false,
			minDBReads: 1, // if the "but not" condition returns quickly with "false", no need to evaluate the first branch
			maxDBReads: 4, // at most two tuple checks + two userset checks
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
			minDBReads: 3, // one read to find org:fga + (one direct check + userset check) to see if user:jon is a member of org:fga
			maxDBReads: 3,
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
			minDBReads: 3, // min(union (4 reads), ttu (3 reads))
			maxDBReads: 4, // max(union (4 reads), ttu (3 reads))
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
			minDBReads: 11, // union (4 reads) + ttu (3 reads) + union rewrite (4 reads)
			maxDBReads: 11,
		},
		{
			name:       "intersection_of_ttus", //union_or_ttu and union_and_ttu
			check:      tuple.NewTupleKey("document:x", "intersection_of_ttus", "user:maria"),
			allowed:    true,
			minDBReads: 4, // union_or_ttu (1 read) + union_and_ttu (3 reads)
			maxDBReads: 8, // union_or_ttu (3 reads) + union_and_ttu (5 reads)
		},
	}

	// run the test many times to exercise all the possible DBReads
	for i := 1; i < 1000; i++ {
		t.Run(fmt.Sprintf("iteration_%v", i), func(t *testing.T) {
			t.Parallel()
			for _, test := range tests {
				test := test
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()

					checker := NewLocalChecker(
						// TODO build this wrapper inside ResolveCheck so that we don't need to construct a new Checker per test
						storagewrappers.NewCombinedTupleReader(ds, test.contextualTuples),
						WithMaxConcurrentReads(1))

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
