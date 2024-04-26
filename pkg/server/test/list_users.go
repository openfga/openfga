package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/server/commands/listusers"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func BenchmarkListUsers(b *testing.B, ds storage.OpenFGADatastore) {
	var oneResultIterations, allResultsIterations int

	benchmarkScenarios := map[string]struct {
		inputModel            string
		tupleGenerator        func() []*openfgav1.TupleKey
		inputConfigMaxResults uint32
		inputRequest          *openfgav1.ListUsersRequest
		expectedResults       int
	}{
		`one_result_without_conditions`: {
			inputModel: `model
							schema 1.1
						type user
						type folder
							relations
								define viewer: [user]
						type document
							relations
								define viewer: [user]
								define parent: [folder]
								define can_view: viewer or viewer from parent`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				// same as the next benchmark, so that later we can compare times.
				var tuples []*openfgav1.TupleKey
				for i := 0; i < 100; i++ {
					for j := 0; j < 50; j++ {
						user := fmt.Sprintf("user:%s", ulid.Make().String())
						// one document accessible by many users
						tuples = append(tuples, tuple.NewTupleKey("document:1", "viewer", user))
					}
				}
				return tuples
			},
			inputRequest: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "document", Id: "1"},
				Relation:    "can_view",
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			inputConfigMaxResults: 1,
			expectedResults:       1,
		},
		`all_results_without_conditions`: {
			inputModel: `model
							schema 1.1
						type user
						type folder
							relations
								define viewer: [user]
						type document
							relations
								define viewer: [user]
								define parent: [folder]
								define can_view: viewer or viewer from parent`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				// same as the previous benchmark, so that later we can compare times.
				var tuples []*openfgav1.TupleKey
				for i := 0; i < 100; i++ {
					for j := 0; j < 50; j++ {
						user := fmt.Sprintf("user:%s", ulid.Make().String())
						// one document accessible by many users
						tuples = append(tuples, tuple.NewTupleKey("document:1", "viewer", user))
					}
				}
				return tuples
			},
			inputRequest: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "document", Id: "1"},
				Relation:    "can_view",
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			inputConfigMaxResults: 0, // infinite
			expectedResults:       100 * 50,
		},
		`all_results_with_conditions`: {
			inputModel: `model
							schema 1.1
						type user
						type folder
							relations
								define viewer: [user]
						type document
							relations
								define viewer: [user with condTrue]
								define parent: [folder]
								define can_view_conditional: viewer or viewer from parent

						condition condTrue(param: bool) {
							param == true
						}`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				var tuples []*openfgav1.TupleKey
				for i := 0; i < 100; i++ {
					for j := 0; j < 50; j++ {
						// one document accessible by many users
						user := fmt.Sprintf("user:%s", ulid.Make().String())
						tuples = append(tuples, tuple.NewTupleKeyWithCondition("document:1", "viewer", user, "condTrue", nil))
					}
				}
				return tuples
			},
			inputRequest: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "document", Id: "1"},
				Relation:    "can_view_conditional",
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
				Context:     testutils.MustNewStruct(b, map[string]interface{}{"param": true}),
			},
			inputConfigMaxResults: 0, // infinite
			expectedResults:       100 * 50,
		},
		`exclusion_without_conditions`: {
			inputModel: `model
							schema 1.1
						type user
						type document
							relations
								define blocked: [user]
								define public_but_not: [user:*] but not blocked`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				return []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "public_but_not", "user:*"),
				}
			},
			inputRequest: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "document", Id: "1"},
				Relation:    "public_but_not",
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			inputConfigMaxResults: 0, // infinite
			expectedResults:       1,
		},
	}

	for name, bm := range benchmarkScenarios {
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			storeID := ulid.Make().String()

			// arrange: write model
			model := testutils.MustTransformDSLToProtoWithID(bm.inputModel)
			typeSystem, err := typesystem.NewAndValidate(context.Background(), model)
			require.NoError(b, err)
			err = ds.WriteAuthorizationModel(context.Background(), storeID, model)
			require.NoError(b, err)

			// arrange: write tuples
			tuples := bm.tupleGenerator()
			for i := 0; i < len(tuples); {
				var tuplesToWrite []*openfgav1.TupleKey
				for j := 0; j < ds.MaxTuplesPerWrite(); j++ {
					if i == len(tuples) {
						break
					}
					tuplesToWrite = append(tuplesToWrite, tuples[i])
					i++
				}
				err = ds.Write(context.Background(), storeID, nil, tuplesToWrite)
				require.NoError(b, err)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				bm.inputRequest.StoreId = storeID
				ctx = typesystem.ContextWithTypesystem(ctx, typeSystem)
				resp, err := listusers.NewListUsersQuery(ds,
					listusers.WithListUserMaxResults(bm.inputConfigMaxResults)).
					ListUsers(ctx, bm.inputRequest)
				require.NoError(b, err)
				require.NotNil(b, resp)
				require.Len(b, resp.GetUsers(), bm.expectedResults)
			}
			if name == "all_results_without_conditions" {
				allResultsIterations = b.N
			} else if name == "one_result_without_conditions" {
				oneResultIterations = b.N
			}
		})
	}
	require.Greater(b, oneResultIterations, allResultsIterations)
}
