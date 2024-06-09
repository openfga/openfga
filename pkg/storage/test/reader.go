package test

import (
	"context"
	"fmt"
	"strconv"
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

		t.Run("filter_out_non_matching_subjects", func(t *testing.T) {
			filter := storage.ReadRelationshipTuplesFilter{
				SubjectsFilter: []storage.SubjectsFilter{
					{
						SubjectType:     "user",
						SubjectIDs:      []string{"will", "andres"},
						SubjectRelation: "",
					},
					{
						SubjectType:     "user",
						SubjectIDs:      []string{"*"},
						SubjectRelation: "",
					},
					{
						SubjectType:     "group",
						SubjectIDs:      []string{"eng"},
						SubjectRelation: "member",
					},
				},
			}

			iter, err := datastore.ReadRelationshipTuples(context.Background(), storeID, filter)
			require.NoError(t, err)

			actual := storage.RelationshipTupleIteratorToStringSlice(iter)
			expected := []string{
				"document:3#viewer@user:will",
				"folder:2#owner@user:andres",
				"document:1#viewer@group:eng#member",
				"org:acme#member@group:eng#member",
				"document:1#viewer@user:*",
			}
			require.ElementsMatch(t, expected, actual)
		})

		t.Run("all_filter_fields_provided", func(t *testing.T) {
			filter := storage.ReadRelationshipTuplesFilter{
				ObjectType: "document",
				ObjectIDs:  []string{"1", "3"},
				Relation:   "viewer",
				SubjectsFilter: []storage.SubjectsFilter{
					{
						SubjectType:     "user",
						SubjectIDs:      []string{"will", "andres"},
						SubjectRelation: "",
					},
					{
						SubjectType:     "user",
						SubjectIDs:      []string{"*"},
						SubjectRelation: "",
					},
					{
						SubjectType:     "group",
						SubjectIDs:      []string{"eng"},
						SubjectRelation: "member",
					},
				},
			}

			iter, err := datastore.ReadRelationshipTuples(context.Background(), storeID, filter)
			require.NoError(t, err)

			actual := storage.RelationshipTupleIteratorToStringSlice(iter)
			expected := []string{
				"document:3#viewer@user:will",
				"document:1#viewer@group:eng#member",
				"document:1#viewer@user:*",
			}
			require.ElementsMatch(t, expected, actual)
		})

		t.Run("all_filter_fields_provided_with_large_object_id_list", func(t *testing.T) {
			// this test is for the scenario(s) where the number of elements in the ObjectID slice
			// is roughly approaching the size that some range filter can efficiently support.

			var tuples []string
			for i := 0; i < 5000; i++ {
				tuples = append(tuples, fmt.Sprintf("document:%d#viewer@user:jon", i))
			}

			storeID := BootstrapFGATuples(t, datastore, tuples)

			var objectIDs []string
			var expected []string
			for i := 0; i < 5000; i++ {
				if i%2 == 0 {
					objectID := strconv.Itoa(i)
					objectIDs = append(objectIDs, objectID) // only the even objectIDs (e.g. 2500 of them)
					expected = append(expected, fmt.Sprintf("document:%s#viewer@user:jon", objectID))
				}
			}

			filter := storage.ReadRelationshipTuplesFilter{
				ObjectType: "document",
				ObjectIDs:  objectIDs,
				Relation:   "viewer",
				SubjectsFilter: []storage.SubjectsFilter{
					{
						SubjectType:     "user",
						SubjectIDs:      []string{"jon"},
						SubjectRelation: "",
					},
				},
			}

			iter, err := datastore.ReadRelationshipTuples(context.Background(), storeID, filter)
			require.NoError(t, err)

			actual := storage.RelationshipTupleIteratorToStringSlice(iter)
			require.ElementsMatch(t, expected, actual)
		})

		t.Run("all_filter_fields_provided_with_large_object_id_list_and_subject_id_list", func(t *testing.T) {
			// depending on the implementation, the number of reads may be proportional to:
			// 	len(ObjectIDs) * len(SubjectFilters[0].SubjectIDs) * ... * len(SubjectFilters[N].SubjectIDs)
			//
			// in this example (500) * (5) * (5) = 12,500
			var objectIDs []string
			for i := 0; i < 500; i++ {
				objectIDs = append(objectIDs, strconv.Itoa(i))
			}

			var tuples []string

			// jon, andres - view access to documents [0-99]
			for i := 0; i < 100; i++ {
				objectID := strconv.Itoa(i)

				tuples = append(tuples, []string{
					fmt.Sprintf("document:%s#viewer@user:jon", objectID),
					fmt.Sprintf("document:%s#viewer@user:andres", objectID),
				}...)
			}

			// will, maria, adrian - view access to documents [100-200]
			for i := 100; i <= 200; i++ {
				objectID := strconv.Itoa(i)

				tuples = append(tuples, []string{
					fmt.Sprintf("document:%s#viewer@user:will", objectID),
					fmt.Sprintf("document:%s#viewer@user:jon", objectID),
					fmt.Sprintf("document:%s#viewer@user:maria", objectID),
				}...)
			}

			// eng, product, marketing - have access to all documents [201-300]
			for i := 201; i <= 300; i++ {
				objectID := strconv.Itoa(i)

				tuples = append(tuples, []string{
					fmt.Sprintf("document:%s#viewer@group:eng#member", objectID),
					fmt.Sprintf("document:%s#viewer@group:product#member", objectID),
					fmt.Sprintf("document:%s#viewer@group:marketing#member", objectID),
				}...)
			}

			// sales, finance - have access to no documents

			storeID := BootstrapFGATuples(t, datastore, tuples)

			filter := storage.ReadRelationshipTuplesFilter{
				ObjectType: "document",
				ObjectIDs:  objectIDs,
				Relation:   "viewer",
				SubjectsFilter: []storage.SubjectsFilter{
					{
						SubjectType:     "user",
						SubjectIDs:      []string{"jon", "will", "andres", "maria", "adrian"},
						SubjectRelation: "",
					},
					{
						SubjectType:     "group",
						SubjectIDs:      []string{"eng", "marketing", "sales", "product", "finance"},
						SubjectRelation: "member",
					},
				},
			}

			iter, err := datastore.ReadRelationshipTuples(context.Background(), storeID, filter)
			require.NoError(t, err)

			actual := storage.RelationshipTupleIteratorToStringSlice(iter)
			require.ElementsMatch(t, tuples, actual) // all of the tuples in the store match the whole query directly
		})
	})
}
