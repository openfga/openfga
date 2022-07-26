package memory_test

import (
	"testing"

	"github.com/openfga/openfga/pkg/telemetry"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/storage"
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
	testEngine := storagefixtures.RunOpenFGADatastoreTestEngine(t, "memory")

	test.TestAll(t, test.OpenFGADatastoreConstructor(func() (storage.OpenFGADatastore, error) {
		ds := testEngine.NewDatastore(t, func(engine, uri string) storage.OpenFGADatastore {
			return memory.New(telemetry.NewNoopTracer(), 10, 24)
		})

		return ds, nil
	}))
}
