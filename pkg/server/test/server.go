package test

import (
	"testing"

	"github.com/openfga/openfga/pkg/storage"
)

func RunAllTests(t *testing.T, ds storage.OpenFGADatastore) {
	RunQueryTests(t, ds)
	RunCommandTests(t, ds)
}

func RunQueryTests(t *testing.T, ds storage.OpenFGADatastore) {
	t.Run("TestExpandQuery", func(t *testing.T) { TestExpandQuery(t, ds) })
	t.Run("TestExpandQueryErrors", func(t *testing.T) { TestExpandQueryErrors(t, ds) })

	t.Run("TestReadAuthorizationModelsWithoutPaging",
		func(t *testing.T) { TestReadAuthorizationModelsWithoutPaging(t, ds) },
	)

	t.Run("TestReadAuthorizationModelsWithPaging",
		func(t *testing.T) { TestReadAuthorizationModelsWithPaging(t, ds) },
	)

	t.Run("TestReadAuthorizationModelsInvalidContinuationToken",
		func(t *testing.T) { TestReadAuthorizationModelsInvalidContinuationToken(t, ds) },
	)

	t.Run("TestListObjects", func(t *testing.T) { TestListObjects(t, ds) })
	t.Run("TestReverseExpand", func(t *testing.T) { TestReverseExpand(t, ds) })
}

func RunCommandTests(t *testing.T, ds storage.OpenFGADatastore) {
	t.Run("TestWriteAuthorizationModel", func(t *testing.T) { WriteAuthorizationModelTest(t, ds) })
}

func RunAllBenchmarks(b *testing.B, ds storage.OpenFGADatastore) {
	b.Run("BenchmarkListObjects", func(b *testing.B) { BenchmarkListObjects(b, ds) })
	b.Run("BenchmarkListUsers", func(b *testing.B) { BenchmarkListUsers(b, ds) })
	b.Run("BenchmarkCheck", func(b *testing.B) { BenchmarkCheck(b, ds) })
	b.Run("BenchmarkReadChanges", func(b *testing.B) { BenchmarkReadChanges(b, ds) })
}
