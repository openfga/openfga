package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func AssertionsTest(t *testing.T, datastore storage.OpenFGADatastore) {

	ctx := context.Background()

	t.Run("writing and reading assertions succeeds", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		modelID, err := id.NewString()
		if err != nil {
			t.Fatal(err)
		}
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

		err = datastore.WriteAssertions(ctx, store, modelID, assertions)
		if err != nil {
			t.Fatal(err)
		}

		gotAssertions, err := datastore.ReadAssertions(ctx, store, modelID)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(assertions, gotAssertions, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("writing twice overwrites assertions", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		modelID, err := id.NewString()
		if err != nil {
			t.Fatal(err)
		}

		assertions := []*openfgapb.Assertion{{TupleKey: &openfgapb.TupleKey{Object: "doc:readme", Relation: "viewer", User: "11"}, Expectation: true}}

		err = datastore.WriteAssertions(ctx, store, modelID, []*openfgapb.Assertion{
			{
				TupleKey:    &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"},
				Expectation: false,
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		err = datastore.WriteAssertions(ctx, store, modelID, assertions)
		if err != nil {
			t.Fatal(err)
		}

		gotAssertions, err := datastore.ReadAssertions(ctx, store, modelID)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(assertions, gotAssertions, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("writing to one modelID and reading from other returns nothing", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		oldModelID, err := id.NewString()
		if err != nil {
			t.Fatal(err)
		}
		newModelID, err := id.NewString()
		if err != nil {
			t.Fatal(err)
		}
		assertions := []*openfgapb.Assertion{
			{
				TupleKey:    &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"},
				Expectation: false,
			},
		}

		err = datastore.WriteAssertions(ctx, store, oldModelID, assertions)
		if err != nil {
			t.Fatal(err)
		}

		gotAssertions, err := datastore.ReadAssertions(ctx, store, newModelID)
		if err != nil {
			t.Fatal(err)
		}
		if len(gotAssertions) != 0 {
			t.Fatalf("got assertions, but expected none: %v", gotAssertions)
		}
	})
}
