package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"
)

func RelationshipTupleReaderTest(t *testing.T, datastore storage.OpenFGADatastore) {

	t.Run("ReadRelationshipTuples", func(t *testing.T) {

		tuples := []string{
			"document:1#viewer@user:jon",
			"document:1#viewer@group:eng#member",
			"document:1#viewer@user:*",
			"document:1#editor@user:jon",

			"document:2#viewer@user:jon",
			"document:3#viewer@user:will",

			"folder:1#editor@org:acme#member",
			"folder:2#owner@user:andres",

			"org:acme#member@group:eng#member",
		}

		storeID := BootstrapFGATuples(t, datastore, tuples)

		t.Run("empty_store_id", func(t *testing.T) {
			filter := storage.ReadRelationshipTuplesFilter{}

			iter, err := datastore.ReadRelationshipTuples(context.Background(), "", filter)
			require.Errorf(t, err, "store id should not be an empty string")
			require.Nil(t, iter)
		})

		t.Run("empty_filter", func(t *testing.T) {
			filter := storage.ReadRelationshipTuplesFilter{}

			iter, err := datastore.ReadRelationshipTuples(context.Background(), storeID, filter)
			require.NoError(t, err)

			actual := storage.RelationshipTupleIteratorToStringSlice(iter)
			expected := tuples
			require.ElementsMatch(t, expected, actual)
		})

		t.Run("filter_out_non_matching_objectType", func(t *testing.T) {
			filter := storage.ReadRelationshipTuplesFilter{
				ObjectType: "org",
			}

			iter, err := datastore.ReadRelationshipTuples(context.Background(), storeID, filter)
			require.NoError(t, err)

			actual := storage.RelationshipTupleIteratorToStringSlice(iter)
			expected := []string{
				"org:acme#member@group:eng#member",
			}
			require.ElementsMatch(t, expected, actual)
		})

		t.Run("filter_out_non_matching_objectType_and_objectIDs", func(t *testing.T) {
			filter := storage.ReadRelationshipTuplesFilter{
				ObjectType: "document",
				ObjectIDs:  []string{"1", "2"},
			}

			iter, err := datastore.ReadRelationshipTuples(context.Background(), storeID, filter)
			require.NoError(t, err)

			actual := storage.RelationshipTupleIteratorToStringSlice(iter)
			expected := []string{
				"document:1#viewer@user:jon",
				"document:1#viewer@group:eng#member",
				"document:1#viewer@user:*",
				"document:1#editor@user:jon",
				"document:2#viewer@user:jon",
			}
			require.ElementsMatch(t, expected, actual)
		})

		t.Run("filter_out_non_matching_objectIDs", func(t *testing.T) {
			filter := storage.ReadRelationshipTuplesFilter{
				ObjectIDs: []string{"1", "2"},
			}

			iter, err := datastore.ReadRelationshipTuples(context.Background(), storeID, filter)
			require.NoError(t, err)

			actual := storage.RelationshipTupleIteratorToStringSlice(iter)
			expected := []string{
				"document:1#viewer@user:jon",
				"document:1#viewer@group:eng#member",
				"document:1#viewer@user:*",
				"document:1#editor@user:jon",
				"document:2#viewer@user:jon",
				"folder:1#editor@org:acme#member",
				"folder:2#owner@user:andres",
			}
			require.ElementsMatch(t, expected, actual)
		})

		t.Run("filter_out_non_matching_objectType_objectIDs_relation", func(t *testing.T) {
			filter := storage.ReadRelationshipTuplesFilter{
				ObjectType: "document",
				ObjectIDs:  []string{"1"},
				Relation:   "editor",
			}

			iter, err := datastore.ReadRelationshipTuples(context.Background(), storeID, filter)
			require.NoError(t, err)

			actual := storage.RelationshipTupleIteratorToStringSlice(iter)
			expected := []string{
				"document:1#editor@user:jon",
			}
			require.ElementsMatch(t, expected, actual)
		})

		t.Run("filter_out_non_matching_objectIDs_relation", func(t *testing.T) {
			filter := storage.ReadRelationshipTuplesFilter{
				ObjectIDs: []string{"1"},
				Relation:  "editor",
			}

			iter, err := datastore.ReadRelationshipTuples(context.Background(), storeID, filter)
			require.NoError(t, err)

			actual := storage.RelationshipTupleIteratorToStringSlice(iter)
			expected := []string{
				"document:1#editor@user:jon",
				"folder:1#editor@org:acme#member",
			}
			require.ElementsMatch(t, expected, actual)
		})

		t.Run("filter_out_non_matching_relation", func(t *testing.T) {
			filter := storage.ReadRelationshipTuplesFilter{
				Relation: "owner",
			}

			iter, err := datastore.ReadRelationshipTuples(context.Background(), storeID, filter)
			require.NoError(t, err)

			actual := storage.RelationshipTupleIteratorToStringSlice(iter)
			expected := []string{
				"folder:2#owner@user:andres",
			}
			require.ElementsMatch(t, expected, actual)
		})
	})
}
