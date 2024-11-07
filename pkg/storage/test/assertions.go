package test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

func AssertionsTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	t.Run("writing_and_reading_assertions_succeeds", func(t *testing.T) {
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

	t.Run("64kb_request_succeeds", func(t *testing.T) {
		storeID := ulid.Make().String()
		modelID := ulid.Make().String()

		maxBytesPerUser := 512
		maxBytesPerRelation := 50
		maxBytesPerObject := 256
		numAssertions := 45

		longRelationName := strings.Repeat("a", maxBytesPerRelation)

		assertions := make([]*openfgav1.Assertion, 0, numAssertions)

		protoSize := 0
		for a := 0; a < numAssertions; a++ {
			newAssertion := &openfgav1.Assertion{
				TupleKey: &openfgav1.AssertionTupleKey{
					Object:   "document:" + strings.Repeat(strconv.Itoa(a), maxBytesPerObject-len("document:")),
					Relation: longRelationName,
					User:     "user:" + strings.Repeat(strconv.Itoa(a), maxBytesPerUser-len("user:")),
				},
				Expectation:      true,
				ContextualTuples: nil,
			}
			assertions = append(assertions, newAssertion)
			protoSize += proto.Size(newAssertion)
		}

		require.LessOrEqual(t, protoSize, 64*1024) // 64 kb

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
