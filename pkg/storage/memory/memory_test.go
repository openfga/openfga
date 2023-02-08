package memory_test

import (
	"testing"

	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/test"
)

var memoryStorage *memory.MemoryBackend

func init() {
	memoryStorage = memory.New(10000, 10000)
}

func TestMemdbStorage(t *testing.T) {
	ds := memory.New(10, 24)
	test.RunAllTests(t, ds)
}
