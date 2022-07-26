package test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/storage"
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

// DatastoreConstructor provides a generic mechanism to construct a datastore instance.
type DatastoreConstructor[T any] interface {
	// New creates a new datastore instance for a single test.
	New() (T, error)
}

// OpenFGADatastoreConstructor implements the DatastoreConstructor interface
// specifically for the OpenFGADatastore storage interface.
type OpenFGADatastoreConstructor func() (storage.OpenFGADatastore, error)

// New returns the OpenFGADatastore implementation coming from the underlying function
// provided.
func (fn OpenFGADatastoreConstructor) New() (storage.OpenFGADatastore, error) {
	return fn()
}

// TestAll runs all datastore tests against the datastore constructed by the provided
// DatastoreConstructor.
func TestAll(t *testing.T, dc DatastoreConstructor[storage.OpenFGADatastore]) {
	t.Run("TestTupleWriteAndRead", func(t *testing.T) { TupleWritingAndReadingTest(t, dc) })
	t.Run("TestTuplePaginationOptions", func(t *testing.T) { TuplePaginationOptionsTest(t, dc) })
	t.Run("TestWriteAndReadAuthorizationModel", func(t *testing.T) { TestWriteAndReadAuthorizationModel(t, dc) })
	t.Run("TestReadAuthorizationModels", func(t *testing.T) { ReadAuthorizationModelsTest(t, dc) })
	t.Run("TestReadTypeDefinition", func(t *testing.T) { ReadTypeDefinitionTest(t, dc) })
	t.Run("TestFindLatestAuthorizationModelID", func(t *testing.T) { FindLatestAuthorizationModelIDTest(t, dc) })
	t.Run("TestReadChanges", func(t *testing.T) { ReadChangesTest(t, dc) })
	t.Run("TestWriteAndReadAssertions", func(t *testing.T) { AssertionsTest(t, dc) })
	t.Run("TestStore", func(t *testing.T) { TestStore(t, dc) })
}
