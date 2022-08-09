package memory_test

import (
	"testing"

	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/storage/memory"
	"github.com/openfga/openfga/storage/test"
)

var (
	memoryStorage *memory.MemoryBackend
)

func init() {
	memoryStorage = memory.New(telemetry.NewNoopTracer(), 10000, 10000)
}

func TestMemdbStorage(t *testing.T) {
	ds := memory.New(telemetry.NewNoopTracer(), 10, 24)
	test.RunAllTests(t, ds)
}
