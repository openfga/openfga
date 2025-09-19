package test

import (
	"testing"

	benchmarks "github.com/openfga/openfga/pkg/server/test/benchmarks"
	"github.com/openfga/openfga/pkg/storage"
)

func RunAllTests(t *testing.T, ds storage.OpenFGADatastore) {
	RunQueryTests(t, ds)
}

func RunQueryTests(t *testing.T, ds storage.OpenFGADatastore) {
	t.Run("TestListObjects", func(t *testing.T) { TestListObjects(t, ds) })
	t.Run("TestReverseExpand", func(t *testing.T) { TestReverseExpand(t, ds) })
}

func RunAllBenchmarks(b *testing.B, ds storage.OpenFGADatastore) {
	b.Run("BenchmarkListObjects", func(b *testing.B) { benchmarks.BenchmarkListObjects(b, ds) })
	b.Run("BenchmarkListUsers", func(b *testing.B) { benchmarks.BenchmarkListUsers(b, ds) })
	b.Run("BenchmarkCheck", func(b *testing.B) { benchmarks.BenchmarkCheck(b, ds) })
	b.Run("BenchmarkReadChanges", func(b *testing.B) { benchmarks.BenchmarkReadChanges(b, ds) })
}
