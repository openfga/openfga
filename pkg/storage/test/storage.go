package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/testing/protocmp"
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
		ready, err := ds.IsReady(context.Background())
		require.NoError(t, err)
		require.True(t, ready)
	})
	// tuples
	t.Run("TestTupleWriteAndRead", func(t *testing.T) { TupleWritingAndReadingTest(t, ds) })
	t.Run("TestTuplePaginationOptions", func(t *testing.T) { TuplePaginationOptionsTest(t, ds) })
	t.Run("TestReadChanges", func(t *testing.T) { ReadChangesTest(t, ds) })
	t.Run("TestReadStartingWithUser", func(t *testing.T) { ReadStartingWithUserTest(t, ds) })
	t.Run("TestRead", func(t *testing.T) { ReadTest(t, ds) })

	// authorization models
	t.Run("TestWriteAndReadAuthorizationModel", func(t *testing.T) { WriteAndReadAuthorizationModelTest(t, ds) })
	t.Run("TestReadAuthorizationModels", func(t *testing.T) { ReadAuthorizationModelsTest(t, ds) })
	t.Run("TestFindLatestAuthorizationModelID", func(t *testing.T) { FindLatestAuthorizationModelIDTest(t, ds) })

	// assertions
	t.Run("TestWriteAndReadAssertions", func(t *testing.T) { AssertionsTest(t, ds) })

	// stores
	t.Run("TestStore", func(t *testing.T) { StoreTest(t, ds) })
}
