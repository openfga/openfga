package test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

var (
	cmpOpts = []cmp.Option{
		cmpopts.IgnoreUnexported(
			openfgapb.AuthorizationModel{},
			openfgapb.TypeDefinition{},
			openfgapb.Userset{},
			openfgapb.Userset_This{},
			openfgapb.DirectUserset{},
			openfgapb.TupleKey{},
			openfgapb.Tuple{},
			openfgapb.TupleChange{},
			openfgapb.Assertion{},
		),
		cmpopts.IgnoreFields(openfgapb.Tuple{}, "Timestamp"),
		cmpopts.IgnoreFields(openfgapb.TupleChange{}, "Timestamp"),
		testutils.TupleKeyCmpTransformer,
	}
)

func RunAllTests(t *testing.T, ds storage.OpenFGADatastore) {
	// tuples
	t.Run("TestTupleWriteAndRead", func(t *testing.T) { TupleWritingAndReadingTest(t, ds) })
	t.Run("TestTuplePaginationOptions", func(t *testing.T) { TuplePaginationOptionsTest(t, ds) })
	t.Run("TestListObjectsByType", func(t *testing.T) { ListObjectsByTypeTest(t, ds) })
	t.Run("TestReadChanges", func(t *testing.T) { ReadChangesTest(t, ds) })
	t.Run("TestReadStartingWithUser", func(t *testing.T) { ReadStartingWithUserTest(t, ds) })

	// authorization models
	t.Run("TestWriteAndReadAuthorizationModel", func(t *testing.T) { WriteAndReadAuthorizationModelTest(t, ds) })
	t.Run("TestReadAuthorizationModels", func(t *testing.T) { ReadAuthorizationModelsTest(t, ds) })
	t.Run("TestReadTypeDefinition", func(t *testing.T) { ReadTypeDefinitionTest(t, ds) })
	t.Run("TestFindLatestAuthorizationModelID", func(t *testing.T) { FindLatestAuthorizationModelIDTest(t, ds) })

	// assertions
	t.Run("TestWriteAndReadAssertions", func(t *testing.T) { AssertionsTest(t, ds) })

	// stores
	t.Run("TestStore", func(t *testing.T) { StoreTest(t, ds) })
}
