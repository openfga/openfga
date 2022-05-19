package test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

var (
	cmpOpts = []cmp.Option{
		cmpopts.IgnoreUnexported(openfgapb.TupleKey{}, openfgapb.Tuple{}, openfgapb.TupleChange{}, openfgapb.Assertion{}),
		cmpopts.IgnoreFields(openfgapb.Tuple{}, "Timestamp"),
		cmpopts.IgnoreFields(openfgapb.TupleChange{}, "Timestamp"),
	}
)

// DatastoreTester provides a generic datastore suite a means of initializing
// a particular datastore.
type DatastoreTester interface {
	// New creates a new datastore instance for a single test.
	New() (storage.OpenFGADatastore, error)
}

type DatastoreTesterFunc func() (storage.OpenFGADatastore, error)

func (f DatastoreTesterFunc) New() (storage.OpenFGADatastore, error) {
	return f()
}

// All runs all generic datastore tests on a DatastoreTester.
func TestAll(t *testing.T, dbTester DatastoreTester) {
	t.Run("TestTupleWriteAndRead", func(t *testing.T) { TupleWritingAndReadingTest(t, dbTester) })
	t.Run("TestTuplePaginationOptions", func(t *testing.T) { TuplePaginationOptionsTest(t, dbTester) })
	t.Run("TestWriteAndReadAuthorizationModel", func(t *testing.T) { TestWriteAndReadAuthorizationModel(t, dbTester) })
	t.Run("TestReadAuthorizationModels", func(t *testing.T) { ReadAuthorizationModelsTest(t, dbTester) })
	t.Run("TestReadTypeDefinition", func(t *testing.T) { ReadTypeDefinitionTest(t, dbTester) })
	t.Run("TestFindLatestAuthorizationModelID", func(t *testing.T) { FindLatestAuthorizationModelIDTest(t, dbTester) })
	t.Run("TestReadChanges", func(t *testing.T) { ReadChangesTest(t, dbTester) })
	t.Run("TestWriteAndReadAssertions", func(t *testing.T) { AssertionsTest(t, dbTester) })
	t.Run("TestStore", func(t *testing.T) { TestStore(t, dbTester) })
}
