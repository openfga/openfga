package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/openfga/openfga/pkg/tuple"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
)

var (
	cmpOpts = []cmp.Option{
		protocmp.IgnoreFields(protoadapt.MessageV2Of(&openfgav1.Tuple{}), "timestamp"),
		protocmp.IgnoreFields(protoadapt.MessageV2Of(&openfgav1.TupleChange{}), "timestamp"),
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}
)

func RunAllTests(t *testing.T, ds storage.OpenFGADatastore) {
	t.Run("TestDatastoreIsReady", func(t *testing.T) {
		status, err := ds.IsReady(context.Background())
		require.NoError(t, err)
		require.True(t, status.IsReady)
	})
	// Tuples.
	t.Run("TestTupleWriteAndRead", func(t *testing.T) { TupleWritingAndReadingTest(t, ds) })
	t.Run("TestReadChanges", func(t *testing.T) { ReadChangesTest(t, ds) })
	t.Run("TestReadStartingWithUser", func(t *testing.T) { ReadStartingWithUserTest(t, ds) })

	// TODO I suspect there is overlap in test scenarios. Consolidate them into one
	t.Run("ReadPageTestCorrectnessOfContinuationTokens", func(t *testing.T) { ReadPageTestCorrectnessOfContinuationTokens(t, ds) })
	t.Run("ReadPageTestCorrectnessOfContinuationTokensV2", func(t *testing.T) { ReadPageTestCorrectnessOfContinuationTokensV2(t, ds) })

	// TODO Consolidate them into one, since both Read and ReadPage should respect the same set of filters
	t.Run("ReadTestCorrectnessOfTuples", func(t *testing.T) { ReadTestCorrectnessOfTuples(t, ds) })
	t.Run("ReadPageTestCorrectnessOfTuples", func(t *testing.T) { ReadPageTestCorrectnessOfTuples(t, ds) })

	// Authorization models.
	t.Run("TestWriteAndReadAuthorizationModel", func(t *testing.T) { WriteAndReadAuthorizationModelTest(t, ds) })
	t.Run("TestReadAuthorizationModels", func(t *testing.T) { ReadAuthorizationModelsTest(t, ds) })
	t.Run("TestFindLatestAuthorizationModel", func(t *testing.T) { FindLatestAuthorizationModelTest(t, ds) })

	// Assertions.
	t.Run("TestWriteAndReadAssertions", func(t *testing.T) { AssertionsTest(t, ds) })

	// Stores.
	t.Run("TestStore", func(t *testing.T) { StoreTest(t, ds) })
}

// BootstrapFGAStore is a utility which can used to quickly bootstrap an OpenFGADatastore
// with an FGA model and relationship tuples. It returns the (store_id, model_id), respectively.
func BootstrapFGAStore(
	t require.TestingT,
	ds storage.OpenFGADatastore,
	model string,
	tupleStrs []string,
) (string, string) {
	storeID := ulid.Make().String()

	fgaModel := testutils.MustTransformDSLToProtoWithID(model)
	err := ds.WriteAuthorizationModel(context.Background(), storeID, fgaModel)
	require.NoError(t, err)

	tuples := tuple.MustParseTupleStrings(tupleStrs...)

	batchSize := ds.MaxTuplesPerWrite()
	for batch := 0; batch < len(tuples); batch += batchSize {
		batchEnd := batch + batchSize

		if batchEnd > len(tuples) {
			batchEnd = len(tuples)
		}

		err := ds.Write(context.Background(), storeID, nil, tuples[batch:batchEnd])
		require.NoError(t, err)
	}

	return storeID, fgaModel.GetId()
}
