package storagewrappers

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/mocks"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestBoundedConcurrencyWrapper(t *testing.T) {
	store := ulid.Make().String()
	t.Logf("create a slow backend that takes 1 second per read call")
	slowBackend := mocks.NewMockSlowDataStorage(memory.New(), time.Second)

	t.Logf("write a tuple")
	err := slowBackend.Write(context.Background(), store, []*openfgapb.TupleKey{}, []*openfgapb.TupleKey{
		tuple.NewTupleKey("obj:1", "viewer", "user:anne"),
	})
	require.NoError(t, err)

	t.Logf("create a limited tuple reader that allows 1 concurrent read a time")
	limitedTupleReader := NewBoundedConcurrencyTupleReader(slowBackend, 1)
	defer limitedTupleReader.Close()

	t.Logf("Read the tuple from 3 goroutines: Each should be run serially")
	var wg sync.WaitGroup
	wg.Add(3)

	start := time.Now()

	go func() {
		_, err := limitedTupleReader.ReadUserTuple(context.Background(), store, tuple.NewTupleKey("obj:1", "viewer", "user:anne"))
		require.NoError(t, err)
		wg.Done()
	}()

	go func() {
		_, err := limitedTupleReader.ReadUsersetTuples(context.Background(), store, storage.ReadUsersetTuplesFilter{
			Object:   "obj:1",
			Relation: "viewer",
		})
		require.NoError(t, err)
		wg.Done()
	}()

	go func() {
		_, err := limitedTupleReader.Read(context.Background(), store, nil)
		require.NoError(t, err)
		wg.Done()
	}()

	wg.Wait()

	end := time.Now()

	require.True(t, end.Sub(start) >= 3*time.Second, "Expected all reads to take at least 3 seconds")
}
