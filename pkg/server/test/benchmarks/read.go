package test

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func BenchmarkRead(b *testing.B, ds storage.OpenFGADatastore) {
	benchmarkScenarios := map[string]struct {
		inputModel      string
		tupleGenerator  func() []*openfgav1.TupleKey
		inputRequest    *openfgav1.ReadRequest
		expectedResults int
	}{
		`object:id no relation or user`: {
			inputModel: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user]
						define editor: [user]`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				var tuples []*openfgav1.TupleKey
				document := "document:1"
				for j := 0; j < 500; j++ {
					user := "user:" + ulid.Make().String()
					tuples = append(tuples, tuple.NewTupleKey(document, "viewer", user))
				}
				for j := 0; j < 500; j++ {
					user := "user:" + ulid.Make().String()
					tuples = append(tuples, tuple.NewTupleKey(document, "editor", user))
				}
				return tuples
			},
			inputRequest: &openfgav1.ReadRequest{
				TupleKey: &openfgav1.ReadRequestTupleKey{
					Object: "document:1",
				},
			},
			// expect the query to return all viewers and editors as relation is omitted
			expectedResults: 1_000,
		},
		`object:id with relation and no user`: {
			inputModel: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user]
						define editor: [user]`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				var tuples []*openfgav1.TupleKey
				document := "document:1"
				for j := 0; j < 500; j++ {
					user := "user:" + ulid.Make().String()
					tuples = append(tuples, tuple.NewTupleKey(document, "viewer", user))
				}
				for j := 0; j < 500; j++ {
					user := "user:" + ulid.Make().String()
					tuples = append(tuples, tuple.NewTupleKey(document, "editor", user))
				}
				return tuples
			},
			inputRequest: &openfgav1.ReadRequest{
				TupleKey: &openfgav1.ReadRequestTupleKey{
					Object:   "document:1",
					Relation: "viewer",
				},
			},
			// expect the query to return all viewers as relation is specified
			expectedResults: 500,
		},
		`object type with user:id and no relation`: {
			inputModel: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user]
						define editor: [user]`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				var tuples []*openfgav1.TupleKey
				user := "user:1"
				for j := 0; j < 500; j++ {
					document := "document:" + ulid.Make().String()
					tuples = append(tuples, tuple.NewTupleKey(document, "viewer", user))
				}
				for j := 0; j < 500; j++ {
					document := "document:" + ulid.Make().String()
					tuples = append(tuples, tuple.NewTupleKey(document, "editor", user))
				}
				return tuples
			},
			inputRequest: &openfgav1.ReadRequest{
				TupleKey: &openfgav1.ReadRequestTupleKey{
					Object: "document:",
					User:   "user:1",
				},
			},
			// expect the query to return all documents as user is specified
			expectedResults: 1_000,
		},
		`object type with user:id and relation`: {
			inputModel: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user]
						define editor: [user]`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				var tuples []*openfgav1.TupleKey
				user := "user:1"
				for j := 0; j < 500; j++ {
					document := "document:" + ulid.Make().String()
					tuples = append(tuples, tuple.NewTupleKey(document, "viewer", user))
				}
				for j := 0; j < 500; j++ {
					document := "document:" + ulid.Make().String()
					tuples = append(tuples, tuple.NewTupleKey(document, "editor", user))
				}
				return tuples
			},
			inputRequest: &openfgav1.ReadRequest{
				TupleKey: &openfgav1.ReadRequestTupleKey{
					Object:   "document:",
					Relation: "viewer",
					User:     "user:1",
				},
			},
			// expect the query to return all documents the user can view specified and relation is specified
			expectedResults: 500,
		},
	}

	for name, bm := range benchmarkScenarios {
		ctx := context.Background()
		storeID := ulid.Make().String()

		// arrange: write model
		model := testutils.MustTransformDSLToProtoWithID(bm.inputModel)
		typeSystem, err := typesystem.NewAndValidate(context.Background(), model)
		require.NoError(b, err)
		err = ds.WriteAuthorizationModel(context.Background(), storeID, model)
		require.NoError(b, err)

		// arrange: write tuples in random order
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
			tuplesToWrite = testutils.Shuffle(tuplesToWrite)
			err = ds.Write(context.Background(), storeID, nil, tuplesToWrite)
			require.NoError(b, err)
		}

		bm.inputRequest.StoreId = storeID
		ctx = typesystem.ContextWithTypesystem(ctx, typeSystem)

		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var tuples []*openfgav1.Tuple
				continuationToken := ""
				for {
					bm.inputRequest.ContinuationToken = continuationToken

					resp, err := commands.NewReadQuery(ds).
						Execute(ctx, bm.inputRequest)
					require.NoError(b, err)
					require.NotNil(b, resp)
					tuples = append(tuples, resp.GetTuples()...)

					continuationToken = resp.GetContinuationToken()
					if continuationToken == "" {
						break
					}
				}
				actualResultCount := len(tuples)
				require.Equal(b, bm.expectedResults, actualResultCount, "total number of records returned should match")
			}
		})
	}
}
