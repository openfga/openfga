package graph

import (
	"context"
	"fmt"
	"testing"
	"time"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage/memory"
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
		define parent: [org] as self
	`)

	ctx := typesystem.ContextWithTypesystem(context.Background(), typesystem.New(
		&openfgav1.AuthorizationModel{
			Id:              ulid.Make().String(),
			TypeDefinitions: typedefs,
			SchemaVersion:   typesystem.SchemaVersion1_1,
		},
	))
	t.Run("direct_check_fast", func(t *testing.T) {
		t.Parallel()
		ds = mocks.NewMockSlowDataStorage(ds,
			mocks.WithReadUsersetTuplesDelay(1*time.Second))

		checker := NewLocalChecker(ds)
		res, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:            storeID,
			TupleKey:           tuple.NewTupleKey("document:x", "a", "user:maria"),
			ResolutionMetadata: &ResolutionMetadata{Depth: 25},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, res.ResolutionMetadata.DatabaseReads)
	})
	t.Run("direct_check_slow", func(t *testing.T) {
		t.Parallel()
		ds = mocks.NewMockSlowDataStorage(ds,
			mocks.WithReadUserTupleDelay(1*time.Second))

		checker := NewLocalChecker(ds)
		fmt.Println(time.Now())
		res, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:            storeID,
			TupleKey:           tuple.NewTupleKey("document:x", "a", "user:maria"),
			ResolutionMetadata: &ResolutionMetadata{Depth: 25},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, res.ResolutionMetadata.DatabaseReads)
	})

	t.Run("union_fast", func(t *testing.T) {
		t.Parallel()
		ds = mocks.NewMockSlowDataStorage(ds,
			mocks.WithReadUsersetTuplesDelay(1*time.Second))

		checker := NewLocalChecker(ds)
		res, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:            storeID,
			TupleKey:           tuple.NewTupleKey("document:x", "union", "user:maria"),
			ResolutionMetadata: &ResolutionMetadata{Depth: 25},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, res.ResolutionMetadata.DatabaseReads)
	})

	t.Run("union_slow", func(t *testing.T) {
		t.Parallel()
		ds = mocks.NewMockSlowDataStorage(ds,
			mocks.WithReadUserTupleDelay(1*time.Second))

		checker := NewLocalChecker(ds)
		res, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:            storeID,
			TupleKey:           tuple.NewTupleKey("document:x", "union", "user:maria"),
			ResolutionMetadata: &ResolutionMetadata{Depth: 25},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, res.ResolutionMetadata.DatabaseReads)
	})
}
