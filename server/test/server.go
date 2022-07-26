package test

import (
	"testing"

	"github.com/openfga/openfga/storage"
	teststorage "github.com/openfga/openfga/storage/test"
)

func TestAll(t *testing.T, dc teststorage.DatastoreConstructor[storage.OpenFGADatastore]) {
	QueryTests(t, dc)
	CommandTests(t, dc)
}

func QueryTests(t *testing.T, dc teststorage.DatastoreConstructor[storage.OpenFGADatastore]) {
	t.Run("TestCheckQuery", func(t *testing.T) { TestCheckQuery(t, dc) })
	t.Run("TestReadAuthorizationModelQueryErrors", func(t *testing.T) { TestReadAuthorizationModelQueryErrors(t, dc) })
	t.Run("TestReadAuthorizationModelByIDAndOneTypeDefinitionReturnsAuthorizationModel",
		func(t *testing.T) {
			TestReadAuthorizationModelByIDAndOneTypeDefinitionReturnsAuthorizationModel(t, dc)
		},
	)
	t.Run("TestReadAuthorizationModelByIDAndTypeDefinitionsReturnsError",
		func(t *testing.T) {
			TestReadAuthorizationModelByIDAndTypeDefinitionsReturnsError(t, dc)
		},
	)

	t.Run("TestExpandQuery", func(t *testing.T) { TestExpandQuery(t, dc) })
	t.Run("TestExpandQueryErrors", func(t *testing.T) { TestExpandQueryErrors(t, dc) })

	t.Run("TestGetStoreQuery", func(t *testing.T) { TestGetStoreQuery(t, dc) })
	t.Run("TestGetStoreSucceeds", func(t *testing.T) { TestGetStoreSucceeds(t, dc) })
	t.Run("TestListStores", func(t *testing.T) { TestListStores(t, dc) })

	t.Run("TestReadAssertionQuery", func(t *testing.T) { TestReadAssertionQuery(t, dc) })

	t.Run("TestReadQuery", func(t *testing.T) { TestReadQuery(t, dc) })

	t.Run("TestReadTuplesQuery", func(t *testing.T) { TestReadTuplesQuery(t, dc) })
	t.Run("TestReadTuplesQueryInvalidContinuationToken",
		func(t *testing.T) { TestReadTuplesQueryInvalidContinuationToken(t, dc) },
	)

	t.Run("TestReadAuthorizationModelsWithoutPaging",
		func(t *testing.T) { TestReadAuthorizationModelsWithoutPaging(t, dc) },
	)

	t.Run("TestReadAuthorizationModelsWithPaging",
		func(t *testing.T) { TestReadAuthorizationModelsWithPaging(t, dc) },
	)

	t.Run("TestReadAuthorizationModelsInvalidContinuationToken",
		func(t *testing.T) { TestReadAuthorizationModelsInvalidContinuationToken(t, dc) },
	)

	t.Run("TestReadChanges", func(t *testing.T) { TestReadChanges(t, dc) })
	t.Run("TestReadChangesReturnsSameContTokenWhenNoChanges",
		func(t *testing.T) { TestReadChangesReturnsSameContTokenWhenNoChanges(t, dc) },
	)
}

func CommandTests(t *testing.T, dc teststorage.DatastoreConstructor[storage.OpenFGADatastore]) {

	t.Run("TestWriteCommand", func(t *testing.T) { TestWriteCommand(t, dc) })

	t.Run("TestWriteAuthorizationModel",
		func(t *testing.T) { TestWriteAuthorizationModel(t, dc) },
	)

	t.Run("TestWriteAssertions", func(t *testing.T) { TestWriteAssertions(t, dc) })
	t.Run("TestCreateStore", func(t *testing.T) { TestCreateStore(t, dc) })
	t.Run("TestDeleteStore", func(t *testing.T) { TestDeleteStore(t, dc) })
}

func BenchmarkAll(b *testing.B, dc teststorage.DatastoreConstructor[storage.OpenFGADatastore]) {
	BenchmarkCheck(b, dc)
}

func BenchmarkCheck(b *testing.B, dc teststorage.DatastoreConstructor[storage.OpenFGADatastore]) {
	b.Run("BenchmarkCheckWithoutTrace", func(b *testing.B) { BenchmarkCheckWithoutTrace(b, dc) })
	b.Run("BenchmarkWithTrace", func(b *testing.B) { BenchmarkWithTrace(b, dc) })
}
