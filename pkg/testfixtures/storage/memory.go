package storage

import (
	"testing"

	"github.com/openfga/openfga/storage"
)

type memoryTest struct{}

// RunMemoryForTesting returns a RunningEngineForTest for the in-memory driver.
func RunMemoryForTesting(t testing.TB) RunningEngineForTest {
	return &memoryTest{}
}

func (mdbt *memoryTest) NewDatabase(t testing.TB) string {
	// Does nothing.
	return ""
}

func (mdbt *memoryTest) NewDatastore(t testing.TB, initFunc InitFunc) storage.OpenFGADatastore {
	return initFunc("memory", "")
}
