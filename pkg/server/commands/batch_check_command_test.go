package commands

import (
	"context"
	"fmt"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/cachecontroller"
	"log"
	"testing"

	"github.com/openfga/openfga/internal/graph"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBatchCheckCommand(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()
	ds := mockstorage.NewMockOpenFGADatastore(mockController)

	mockCheckResolver := graph.NewMockCheckResolver(mockController)
	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1
		type user
		type doc
			relations
				define viewer: [user]
	`)
	ts, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)

	cmd := NewBatchCheckCommand(
		ds,
		mockCheckResolver,
		ts,
		50,
		WithBatchCheckCommandCacheController(cachecontroller.NewNoopCacheController()),
	)

	numChecks := 10
	checks := make([]*openfgav1.BatchCheckItem, numChecks)
	for i := 0; i < numChecks; i++ {
		checks[i] = &openfgav1.BatchCheckItem{
			TupleKey: &openfgav1.CheckRequestTupleKey{
				Object:   "doc:doc1",
				Relation: "viewer",
				User:     "user:justin",
			},
			CorrelationId: fmt.Sprintf("fakeid%d", i),
		}
	}

	mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
		Times(numChecks).
		Return(nil, nil)

	t.Run("first test", func(t *testing.T) {
		params := &BatchCheckCommandParams{
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			Checks:               checks,
			// Can i get a store id without a server?
			// I need the mock datastore to return something here
			StoreID: ulid.Make().String(),
		}

		result, err := cmd.Execute(context.Background(), params)
		//err := ds.Write(context.Background(), storeID, nil, tuplesToWrite)
		log.Printf("justin results: %+v", result)
		log.Printf("justin results1: %+v", result["fakeid1"])
		require.NoError(t, err)
	})
}

// simple test case for lots of tuples, under limit
// then test case for over limit
// then some where we assert a specific allowed true and allowed false
// then some where we hit a known error for individual check (like bad relation?)
// I kinda need a "assert check was called N times" test
