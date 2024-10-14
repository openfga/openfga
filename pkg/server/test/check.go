package test

import (
	"context"
	"fmt"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	"testing"
)

func BenchmarkCheck(b *testing.B, ds storage.OpenFGADatastore) {
	benchmarkScenarios := map[string]struct {
		inputModel      string
		tupleGenerator  func() []*openfgav1.TupleKey
		checker         graph.CheckResolver
		inputRequest    *openfgav1.CheckRequest
		expectedResults any // not sure what goes here yet
	}{
		`check_direct_and_userset`: {
			inputModel: `
				model
					schema 1.1
				type user
				type team
					relations
						define member: [user,team#member]
				type repo
					relations
						define admin: [user,team#member] or member from owner
						define owner: [organization]
				type organization
					relations
						define member: [user]`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				var tuples []*openfgav1.TupleKey
				for i := 0; i < 1000; i++ { // add user:anne to many teams
					tuples = append(tuples, &openfgav1.TupleKey{
						User:     "user:anne",
						Relation: "member",
						Object:   fmt.Sprintf("team:%d", i),
					})
				}
				return tuples
			},
			checker:         graph.NewLocalChecker(),
			inputRequest:    &openfgav1.CheckRequest{},
			expectedResults: "hello",
		},
	}

	for name, bm := range benchmarkScenarios {
		ctx := context.Background()
		storeID := ulid.Make().String()
		//bm.inputRequest.StoreId = storeID
		model := testutils.MustTransformDSLToProtoWithID(bm.inputModel)
		typeSystem, err := typesystem.NewAndValidate(context.Background(), model)
		require.NoError(b, err)
		err = ds.WriteAuthorizationModel(context.Background(), storeID, model)

		// create and write necessary tuples
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
			err := ds.Write(context.Background(), storeID, nil, tuplesToWrite)
			require.NoError(b, err)
		}

		ctx = typesystem.ContextWithTypesystem(ctx, typeSystem)

		// Write Command
		cmd := commands.NewWriteCommand(ds)

		// give both a team and anne the 'admin' relation
		writes := &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{Object: "repo:openfga", Relation: "admin", User: "user:anne"},
				{Object: "repo:openfga", Relation: "admin", User: "team:123#member"},
			},
		}

		_, err = cmd.Execute(ctx, &openfgav1.WriteRequest{
			StoreId:              storeID,
			AuthorizationModelId: typeSystem.GetAuthorizationModelID(), // the resolved model id
			Writes:               writes,
			Deletes:              nil,
		})
		require.NoError(b, err)

		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				checkQuery := commands.NewCheckCommand(ds, bm.checker, typeSystem)
				_, _, err = checkQuery.Execute(ctx, &openfgav1.CheckRequest{
					StoreId: storeID,
					TupleKey: &openfgav1.CheckRequestTupleKey{
						Object:   "repo:openfga",
						Relation: "admin",
						User:     "user:anne",
					},
				})

				require.NoError(b, err)
			}
		})
	}
}
