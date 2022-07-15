package test

import (
	"testing"

	"github.com/openfga/openfga/storage"
	teststorage "github.com/openfga/openfga/storage/test"
)

func TestAll(t *testing.T, dbTester teststorage.DatastoreTester[storage.OpenFGADatastore]) {
	QueryTests(t, dbTester)
	CommandTests(t, dbTester)
}

func QueryTests(t *testing.T, dbTester teststorage.DatastoreTester[storage.OpenFGADatastore]) {
	t.Run("TestCheck", func(t *testing.T) { TestCheck(t, dbTester) })
	t.Run("TestCheckQuery", func(t *testing.T) { TestCheckQuery(t, dbTester) })
	t.Run("TestReadAuthorizationModelQueryErrors", func(t *testing.T) { TestReadAuthorizationModelQueryErrors(t, dbTester) })
	t.Run("TestReadAuthorizationModelByIDAndOneTypeDefinitionReturnsAuthorizationModel",
		func(t *testing.T) {
			TestReadAuthorizationModelByIDAndOneTypeDefinitionReturnsAuthorizationModel(t, dbTester)
		},
	)
	t.Run("TestReadAuthorizationModelByIDAndTypeDefinitionsReturnsError",
		func(t *testing.T) {
			TestReadAuthorizationModelByIDAndTypeDefinitionsReturnsError(t, dbTester)
		},
	)

	t.Run("TestExpandQuery", func(t *testing.T) { TestExpandQuery(t, dbTester) })
	t.Run("TestExpandQueryErrors", func(t *testing.T) { TestExpandQueryErrors(t, dbTester) })

	t.Run("TestGetStoreQuery", func(t *testing.T) { TestGetStoreQuery(t, dbTester) })
	t.Run("TestGetStoreSucceeds", func(t *testing.T) { TestGetStoreSucceeds(t, dbTester) })
	t.Run("TestListStores", func(t *testing.T) { TestListStores(t, dbTester) })

	t.Run("TestReadAssertionQuery", func(t *testing.T) { TestReadAssertionQuery(t, dbTester) })

	t.Run("TestReadQuery", func(t *testing.T) { TestReadQuery(t, dbTester) })

	t.Run("TestReadTuplesQuery", func(t *testing.T) { TestReadTuplesQuery(t, dbTester) })
	t.Run("TestReadTuplesQueryInvalidContinuationToken",
		func(t *testing.T) { TestReadTuplesQueryInvalidContinuationToken(t, dbTester) },
	)

	t.Run("TestReadAuthorizationModelsWithoutPaging",
		func(t *testing.T) { TestReadAuthorizationModelsWithoutPaging(t, dbTester) },
	)

	t.Run("TestReadAuthorizationModelsWithPaging",
		func(t *testing.T) { TestReadAuthorizationModelsWithPaging(t, dbTester) },
	)

	t.Run("TestReadAuthorizationModelsInvalidContinuationToken",
		func(t *testing.T) { TestReadAuthorizationModelsInvalidContinuationToken(t, dbTester) },
	)

	t.Run("TestReadChanges", func(t *testing.T) { TestReadChanges(t, dbTester) })
	t.Run("TestReadChangesReturnsSameContTokenWhenNoChanges",
		func(t *testing.T) { TestReadChangesReturnsSameContTokenWhenNoChanges(t, dbTester) },
	)
}

func CommandTests(t *testing.T, dbTester teststorage.DatastoreTester[storage.OpenFGADatastore]) {

	t.Run("TestWriteCommand", func(t *testing.T) { TestWriteCommand(t, dbTester) })

	t.Run("TestWriteAuthorizationModel",
		func(t *testing.T) { TestWriteAuthorizationModel(t, dbTester) },
	)

	t.Run("TestWriteAssertions", func(t *testing.T) { TestWriteAssertions(t, dbTester) })
	t.Run("TestCreateStore", func(t *testing.T) { TestCreateStore(t, dbTester) })
	t.Run("TestDeleteStore", func(t *testing.T) { TestDeleteStore(t, dbTester) })
}

func BenchmarkAll(b *testing.B, dbTester teststorage.DatastoreTester[storage.OpenFGADatastore]) {
	BenchmarkCheck(b, dbTester)
}

func BenchmarkCheck(b *testing.B, dbTester teststorage.DatastoreTester[storage.OpenFGADatastore]) {
	b.Run("BenchmarkCheckWithoutTrace", func(b *testing.B) { BenchmarkCheckWithoutTrace(b, dbTester) })
	b.Run("BenchmarkWithTrace", func(b *testing.B) { BenchmarkWithTrace(b, dbTester) })
}
