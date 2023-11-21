package memory

import (
	"context"
	"testing"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
)

func TestMemdbStorage(t *testing.T) {
	ds := New()
	test.RunAllTests(t, ds)
}

func TestStaticTupleIteratorNoRace(t *testing.T) {
	iter := &staticIterator{
		tuples: []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			},
			{
				Key: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			},
		},
	}
	defer iter.Stop()

	go func() {
		_, err := iter.Next(context.Background())
		require.NoError(t, err)
	}()

	go func() {
		_, err := iter.Next(context.Background())
		require.NoError(t, err)
	}()
}

func TestStaticTupleIteratorContextCanceled(t *testing.T) {
	iter := &staticIterator{
		tuples: []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			},
		},
	}
	defer iter.Stop()

	ctx, cancel := context.WithCancel(context.Background())

	_, err := iter.Next(ctx)
	require.NoError(t, err)

	cancel()

	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestStaticTupleIteratorContextDeadlineExceeded(t *testing.T) {
	iter := &staticIterator{
		tuples: []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			},
		},
	}
	defer iter.Stop()

	deadlineCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := iter.Next(deadlineCtx)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	_, err = iter.Next(deadlineCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
