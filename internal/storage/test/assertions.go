package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/internal/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func AssertionsTest(t *testing.T, datastore storage.OpenFGADatastore) {

	ctx := context.Background()

	t.Run("writing_and_reading_assertions_succeeds", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := ulid.Make().String()
		assertions := []*openfgapb.Assertion{
			{
				TupleKey:    &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"},
				Expectation: false,
			},
			{
				TupleKey:    &openfgapb.TupleKey{Object: "doc:readme", Relation: "viewer", User: "11"},
				Expectation: true,
			},
		}

		err := datastore.WriteAssertions(ctx, store, modelID, assertions)
		require.NoError(t, err)

		gotAssertions, err := datastore.ReadAssertions(ctx, store, modelID)
		require.NoError(t, err)

		if diff := cmp.Diff(assertions, gotAssertions, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("writing_twice_overwrites_assertions", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := ulid.Make().String()
		assertions := []*openfgapb.Assertion{{TupleKey: &openfgapb.TupleKey{Object: "doc:readme", Relation: "viewer", User: "11"}, Expectation: true}}

		err := datastore.WriteAssertions(ctx, store, modelID, []*openfgapb.Assertion{
			{
				TupleKey:    &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"},
				Expectation: false,
			},
		})
		require.NoError(t, err)

		err = datastore.WriteAssertions(ctx, store, modelID, assertions)
		require.NoError(t, err)

		gotAssertions, err := datastore.ReadAssertions(ctx, store, modelID)
		require.NoError(t, err)

		if diff := cmp.Diff(assertions, gotAssertions, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("writing_to_one_modelID_and_reading_from_other_returns_nothing", func(t *testing.T) {
		store := ulid.Make().String()
		oldModelID := ulid.Make().String()
		newModelID := ulid.Make().String()
		assertions := []*openfgapb.Assertion{
			{
				TupleKey:    &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"},
				Expectation: false,
			},
		}

		err := datastore.WriteAssertions(ctx, store, oldModelID, assertions)
		require.NoError(t, err)

		gotAssertions, err := datastore.ReadAssertions(ctx, store, newModelID)
		require.NoError(t, err)

		require.Len(t, gotAssertions, 0)
	})
}
