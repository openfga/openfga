package graph

import (
	"context"
	"fmt"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgav1 "go.buf.build/openfga/go/openfga/api/openfga/v1"
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

func TestCheckDbReads(t *testing.T) {
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
		define intersection as a and b
		define difference as a but not b
		define ttu as member from parent
        define intersection_and_ttu as union and ttu 
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
			minDBReads: 1, // checkDirectUserTuple returns success before checkDirectUsersetTuples starts
			maxDBReads: 2, // checkDirectUsersetTuples returns false, then checkDirectUserTuple returns true
		},
		{
			name:             "direct_access_thanks_to_contextual_tuple", // NOTE: this is counting the read from memory as a database read!
			check:            tuple.NewTupleKey("document:x", "a", "user:unknown"),
			contextualTuples: []*openfgav1.TupleKey{tuple.NewTupleKey("document:x", "a", "user:unknown")},
			allowed:          true,
			minDBReads:       1, // checkDirectUserTuple returns success from contextual tuples before checkDirectUsersetTuples starts
			maxDBReads:       2,
		},
		{
			name:       "union",
			check:      tuple.NewTupleKey("document:x", "union", "user:maria"),
			allowed:    true,
			minDBReads: 1, // one direct tuple lookup
			maxDBReads: 4, // very unlikely but possible, depending on goroutine scheduling
		},
		{
			name:       "union_no_access",
			check:      tuple.NewTupleKey("document:x", "union", "user:unknown"),
			allowed:    false,
			minDBReads: 2, // need to check all the conditions in the union, so at minimum two direct tuple lookups
			maxDBReads: 4, // very unlikely but possible, depending on goroutine scheduling
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
			minDBReads: 2, // one read to find org:fga and another direct check to check membership
			maxDBReads: 4,
		},
		{
			name:       "intersection_and_ttu",
			check:      tuple.NewTupleKey("document:x", "intersection_and_ttu", "user:maria"),
			allowed:    true,
			minDBReads: 3, // TODO should this not be four? one read + one check direct for TTU and two check directs to solve the intersection
			maxDBReads: 4 + 4,
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
					require.GreaterOrEqual(t, res.ResolutionMetadata.DatastoreCallCount, test.minDBReads)
					require.LessOrEqual(t, res.ResolutionMetadata.DatastoreCallCount, test.maxDBReads)
				})
			}
		})
	}
}
