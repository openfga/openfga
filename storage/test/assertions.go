package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func AssertionsTest(t *testing.T, datastore storage.OpenFGADatastore) {

	ctx := context.Background()

	t.Run("writing and reading assertions succeeds", func(t *testing.T) {
		store := id.Must(id.New()).String()
		modelID := id.Must(id.New()).String()
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

	t.Run("writing twice overwrites assertions", func(t *testing.T) {
		store := id.Must(id.New()).String()
		modelID := id.Must(id.New()).String()
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

	t.Run("writing to one modelID and reading from other returns nothing", func(t *testing.T) {
		store := id.Must(id.New()).String()
		oldModelID := id.Must(id.New()).String()
		newModelID := id.Must(id.New()).String()
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
