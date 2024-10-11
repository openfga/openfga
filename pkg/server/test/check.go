package test

import (
	"context"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	"log"
	"testing"
)

func BenchmarkCheck(b *testing.B, ds storage.OpenFGADatastore) {
	benchmarkScenarios := map[string]struct {
		inputModel      string
		tupleGenerator  func() []*openfgav1.TupleKey
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
				return []*openfgav1.TupleKey{} // TODO
			},
			inputRequest:    &openfgav1.CheckRequest{},
			expectedResults: "hello",
		},
	}

	//for name, bench := range benchmarkScenarios {
	for _, bench := range benchmarkScenarios {
		//ctx := context.Background()
		storeID := ulid.Make().String()
		// write model
		model := testutils.MustTransformDSLToProtoWithID(bench.inputModel)
		//typeSystem, err := typesystem.NewAndValidate(context.Background(), model)
		_, err := typesystem.NewAndValidate(context.Background(), model)
		require.NoError(b, err)
		err = ds.WriteAuthorizationModel(context.Background(), storeID, model)

		log.Println("justinnnnnnnnnnnnnnn")
		// create tuples
		tuples := bench.tupleGenerator()
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
		// run check
	}
}
