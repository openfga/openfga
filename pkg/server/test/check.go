package test

import (
	"context"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"testing"
)

func BenchmarkCheck(b *testing.B, ds storage.OpenFGADatastore) {
	benchmarkScenarios := map[string]struct {
		inputModel      string
		tupleGenerator  func() []*openfgav1.TupleKey
		inputRequest    *openfgav1.CheckRequest
		expectedResults any // not sure what goes here yet
	}{
		`first_one`: {
			inputModel: `fake model placeholder`,
			tupleGenerator: func() []*openfgav1.TupleKey {
				return []*openfgav1.TupleKey{} // TODO
			},
			inputRequest:    &openfgav1.CheckRequest{},
			expectedResults: "hello",
		},
	}

	for name, bench := range benchmarkScenarios {
		ctx := context.Background()
		storeID := ulid.Make().String()

		// write model
		// create tuples
		// run check
	}
}
