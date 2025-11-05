package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/testing/protocmp"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
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
	t.Run("TestReadAndReadPages", func(t *testing.T) { ReadAndReadPageTest(t, ds) })

	// Authorization models.
	t.Run("TestWriteAndReadAuthorizationModel", func(t *testing.T) { WriteAndReadAuthorizationModelTest(t, ds) })
	t.Run("TestReadAuthorizationModels", func(t *testing.T) { ReadAuthorizationModelsTest(t, ds) })
	t.Run("TestFindLatestAuthorizationModel", func(t *testing.T) { FindLatestAuthorizationModelTest(t, ds) })

	// Assertions.
	t.Run("TestWriteAndReadAssertions", func(t *testing.T) { AssertionsTest(t, ds) })

	// Stores.
	t.Run("TestStore", func(t *testing.T) { StoreTest(t, ds) })
}

// BootstrapFGAStore is a utility to write an FGA model and relationship tuples to a datastore.
// It doesn't validate the model. It validates the format of the tuples, but not the types within them.
// It returns the store_id and FGA AuthorizationModel, respectively.
func BootstrapFGAStore(
	t require.TestingT,
	ds storage.OpenFGADatastore,
	model string,
	tupleStrs []string,
) (string, *openfgav1.AuthorizationModel) {
	storeID := ulid.Make().String()

	fgaModel := testutils.MustTransformDSLToProtoWithID(model)
	fgaModelId, err := ds.WriteAuthorizationModel(context.Background(), storeID, fgaModel, "fakehash")
	require.NoError(t, err)
	require.Equal(t, fgaModelId, fgaModel.GetId())

	tuples := tuple.MustParseTupleStrings(tupleStrs...)
	tuples = testutils.Shuffle(tuples)
	batchSize := ds.MaxTuplesPerWrite()
	for batch := 0; batch < len(tuples); batch += batchSize {
		batchEnd := min(batch+batchSize, len(tuples))

		err := ds.Write(context.Background(), storeID, nil, tuples[batch:batchEnd])
		require.NoError(t, err)
	}

	return storeID, fgaModel
}
