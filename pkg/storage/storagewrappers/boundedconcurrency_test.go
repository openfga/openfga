package storagewrappers

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestBoundedConcurrencyWrapper(t *testing.T) {
	store := ulid.Make().String()
	slowBackend := mocks.NewMockSlowDataStorage(memory.New(),
		mocks.WithReadUserTupleDelay(time.Second),
		mocks.WithReadUsersetTuplesDelay(time.Second),
		mocks.WithReadDelay(time.Second),
	)

	err := slowBackend.Write(context.Background(), store, []*openfgapb.TupleKey{}, []*openfgapb.TupleKey{
		tuple.NewTupleKey("obj:1", "viewer", "user:anne"),
	})
	require.NoError(t, err)

	// create a limited tuple reader that allows 1 concurrent read a time
	limitedTupleReader := NewBoundedConcurrencyTupleReader(slowBackend, 1)

	// do reads from 3 goroutines - each should be run serially
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
