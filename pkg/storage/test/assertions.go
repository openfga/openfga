package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

func AssertionsTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	t.Run("small_request_succeeds", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := ulid.Make().String()
		assertions := []*openfgav1.Assertion{
			{
				TupleKey:    tupleUtils.NewAssertionTupleKey("doc:readme", "owner", "10"),
				Expectation: false,
			},
			{
				TupleKey:    tupleUtils.NewAssertionTupleKey("doc:readme", "viewer", "11"),
				Expectation: true,
			},
		}

		err := datastore.WriteAssertions(ctx, store, modelID, assertions)
		require.NoError(t, err)

		gotAssertions, err := datastore.ReadAssertions(ctx, store, modelID)
		require.NoError(t, err)

		if diff := cmp.Diff(assertions, gotAssertions, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("big_request_succeeds", func(t *testing.T) {
		storeID := ulid.Make().String()
		modelID := ulid.Make().String()

		maxAssertionsPerRequest := 100
		maxContextualTuplesPerAssertion := 20

		contextualTuples := make([]*openfgav1.TupleKey, 0, maxContextualTuplesPerAssertion)
		assertions := make([]*openfgav1.Assertion, 0, maxAssertionsPerRequest)

		for i := 0; i < maxContextualTuplesPerAssertion; i++ {
			contextualTuples = append(contextualTuples, &openfgav1.TupleKey{
				Object:   "doc:readme",
				Relation: "owner",
				User:     fmt.Sprintf("user:%d", i),
			})
		}

		for i := 0; i < maxAssertionsPerRequest; i++ {
			assertions = append(assertions, &openfgav1.Assertion{
				TupleKey:         tupleUtils.NewAssertionTupleKey("doc:readme", "owner", fmt.Sprintf("user:%d", i)),
				Expectation:      true,
				ContextualTuples: contextualTuples,
			})
		}

		err := datastore.WriteAssertions(ctx, storeID, modelID, assertions)
		require.NoError(t, err)

		gotAssertions, err := datastore.ReadAssertions(ctx, storeID, modelID)
		require.NoError(t, err)
		require.Len(t, gotAssertions, len(assertions))
	})

	t.Run("writing_twice_overwrites_assertions", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := ulid.Make().String()
		assertions := []*openfgav1.Assertion{{TupleKey: tupleUtils.NewAssertionTupleKey("doc:readme", "viewer", "11"), Expectation: true}}

		err := datastore.WriteAssertions(ctx, store, modelID, []*openfgav1.Assertion{
			{
				TupleKey:    tupleUtils.NewAssertionTupleKey("doc:readme", "owner", "10"),
				Expectation: false,
			},
		})
		require.NoError(t, err)

		err = datastore.WriteAssertions(ctx, store, modelID, assertions)
		require.NoError(t, err)

		gotAssertions, err := datastore.ReadAssertions(ctx, store, modelID)
		require.NoError(t, err)

		if diff := cmp.Diff(assertions, gotAssertions, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("writing_to_one_modelID_and_reading_from_other_returns_nothing", func(t *testing.T) {
		store := ulid.Make().String()
		oldModelID := ulid.Make().String()
		newModelID := ulid.Make().String()
		assertions := []*openfgav1.Assertion{
			{
				TupleKey:    tupleUtils.NewAssertionTupleKey("doc:readme", "owner", "10"),
				Expectation: false,
			},
		}

		err := datastore.WriteAssertions(ctx, store, oldModelID, assertions)
		require.NoError(t, err)

		gotAssertions, err := datastore.ReadAssertions(ctx, store, newModelID)
		require.NoError(t, err)

		require.Empty(t, gotAssertions)
	})
}
