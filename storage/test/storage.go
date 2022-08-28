package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

var (
	cmpOpts = []cmp.Option{
		cmpopts.IgnoreUnexported(openfgapb.TupleKey{}, openfgapb.Tuple{}, openfgapb.TupleChange{}, openfgapb.Assertion{}),
		cmpopts.IgnoreFields(openfgapb.Tuple{}, "Timestamp"),
		cmpopts.IgnoreFields(openfgapb.TupleChange{}, "Timestamp"),
		testutils.TupleKeyCmpTransformer,
	}
)

func RunAllTests(t *testing.T, ds storage.OpenFGADatastore) {
	t.Run("TestTupleWriteAndRead", func(t *testing.T) { TupleWritingAndReadingTest(t, ds) })
	t.Run("TestTuplePaginationOptions", func(t *testing.T) { TuplePaginationOptionsTest(t, ds) })
	t.Run("TestWriteAndReadAuthorizationModel", func(t *testing.T) { TestWriteAndReadAuthorizationModel(t, ds) })
	t.Run("TestReadAuthorizationModels", func(t *testing.T) { ReadAuthorizationModelsTest(t, ds) })
	t.Run("TestReadTypeDefinition", func(t *testing.T) { ReadTypeDefinitionTest(t, ds) })
	t.Run("TestFindLatestAuthorizationModelID", func(t *testing.T) { FindLatestAuthorizationModelIDTest(t, ds) })
	t.Run("TestReadChanges", func(t *testing.T) { ReadChangesTest(t, ds) })
	t.Run("TestWriteAndReadAssertions", func(t *testing.T) { AssertionsTest(t, ds) })
	t.Run("TestStore", func(t *testing.T) { TestStore(t, ds) })
	t.Run("TestListObjectsByType", func(t *testing.T) { TestListObjects(t, ds) })
}

func TestListObjects(t *testing.T, ds storage.OpenFGADatastore) {

	expected := []string{"document:doc1", "document:doc2"}

	store := id.MustNewString()

	err := ds.Write(context.Background(), store, nil, []*openfgapb.TupleKey{
		tuple.NewTupleKey("document:doc1", "viewer", "jon"),
		tuple.NewTupleKey("document:doc1", "viewer", "elbuo"),
		tuple.NewTupleKey("document:doc2", "editor", "maria"),
	})
	require.NoError(t, err)

	iter, err := ds.ListObjectsByType(context.Background(), store, "document")
	require.NoError(t, err)

	var actual []string
	for {
		obj, err := iter.Next()
		if err != nil {
			if err == storage.ErrIteratorDone {
				break
			}
		}

		actual = append(actual, tuple.ObjectKey(obj))
	}

	require.Equal(t, expected, actual)
}
