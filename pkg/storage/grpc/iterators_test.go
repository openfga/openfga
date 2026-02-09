package grpc

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/testing/protocmp"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	storagev1 "github.com/openfga/api/proto/storage/v1beta1"
	"github.com/openfga/openfga/pkg/storage"
)

type mockReadStream struct {
	tuples []*openfgav1.Tuple
	index  int
	closed bool
	grpc.ClientStream
}

func (m *mockReadStream) Recv() (*storagev1.ReadResponse, error) {
	if m.closed {
		return nil, io.EOF
	}
	if m.index >= len(m.tuples) {
		return nil, io.EOF
	}
	tuple := m.tuples[m.index]
	m.index++
	return &storagev1.ReadResponse{Tuple: tuple}, nil
}

func (m *mockReadStream) CloseSend() error {
	m.closed = true
	return nil
}

func TestStreamTupleIterator(t *testing.T) {
	t.Run("empty_stream", func(t *testing.T) {
		stream := &mockReadStream{
			tuples: []*openfgav1.Tuple{},
		}
		iter := newStreamTupleIterator(stream)
		defer iter.Stop()

		_, err := iter.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("multiple_tuples", func(t *testing.T) {
		tuples := []*openfgav1.Tuple{
			{
				Key: &openfgav1.TupleKey{
					Object:   "doc:1",
					Relation: "viewer",
					User:     "user:anne",
				},
			},
			{
				Key: &openfgav1.TupleKey{
					Object:   "doc:2",
					Relation: "editor",
					User:     "user:bob",
				},
			},
			{
				Key: &openfgav1.TupleKey{
					Object:   "doc:3",
					Relation: "owner",
					User:     "user:charlie",
				},
			},
		}
		stream := &mockReadStream{
			tuples: tuples,
		}
		iter := newStreamTupleIterator(stream)
		defer iter.Stop()

		for i, expected := range tuples {
			got, err := iter.Next(context.Background())
			require.NoError(t, err, "tuple %d", i)
			if diff := cmp.Diff(expected, got, protocmp.Transform()); diff != "" {
				t.Errorf("tuple %d mismatch (-want +got):\n%s", i, diff)
			}
		}

		_, err := iter.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("head_then_next_returns_same_tuple", func(t *testing.T) {
		tuples := []*openfgav1.Tuple{
			{
				Key: &openfgav1.TupleKey{
					Object:   "doc:1",
					Relation: "viewer",
					User:     "user:anne",
				},
			},
			{
				Key: &openfgav1.TupleKey{
					Object:   "doc:2",
					Relation: "editor",
					User:     "user:bob",
				},
			},
		}
		stream := &mockReadStream{
			tuples: tuples,
		}
		iter := newStreamTupleIterator(stream)
		defer iter.Stop()

		// Head should return first tuple
		head1, err := iter.Head(context.Background())
		require.NoError(t, err)
		if diff := cmp.Diff(tuples[0], head1, protocmp.Transform()); diff != "" {
			t.Errorf("head1 mismatch (-want +got):\n%s", diff)
		}

		// Multiple Head calls should be idempotent
		head2, err := iter.Head(context.Background())
		require.NoError(t, err)
		if diff := cmp.Diff(head1, head2, protocmp.Transform()); diff != "" {
			t.Errorf("head2 should equal head1 (-want +got):\n%s", diff)
		}

		// Next should return the same tuple as Head
		next1, err := iter.Next(context.Background())
		require.NoError(t, err)
		if diff := cmp.Diff(tuples[0], next1, protocmp.Transform()); diff != "" {
			t.Errorf("next1 should equal first tuple (-want +got):\n%s", diff)
		}

		// Next call should return second tuple
		next2, err := iter.Next(context.Background())
		require.NoError(t, err)
		if diff := cmp.Diff(tuples[1], next2, protocmp.Transform()); diff != "" {
			t.Errorf("next2 mismatch (-want +got):\n%s", diff)
		}

		_, err = iter.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("stop_releases_resources", func(t *testing.T) {
		stream := &mockReadStream{
			tuples: []*openfgav1.Tuple{
				{
					Key: &openfgav1.TupleKey{
						Object:   "doc:1",
						Relation: "viewer",
						User:     "user:anne",
					},
				},
			},
		}
		iter := newStreamTupleIterator(stream)

		iter.Stop()
		require.True(t, stream.closed)

		_, err := iter.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("stop_idempotent", func(t *testing.T) {
		stream := &mockReadStream{
			tuples: []*openfgav1.Tuple{},
		}
		iter := newStreamTupleIterator(stream)

		iter.Stop()
		iter.Stop()
		iter.Stop()

		_, err := iter.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("context_cancellation", func(t *testing.T) {
		stream := &mockReadStream{
			tuples: []*openfgav1.Tuple{
				{
					Key: &openfgav1.TupleKey{
						Object:   "doc:1",
						Relation: "viewer",
						User:     "user:anne",
					},
				},
			},
		}
		iter := newStreamTupleIterator(stream)
		defer iter.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		// Both Next and Head should respect context cancellation
		_, err := iter.Next(ctx)
		require.ErrorIs(t, err, context.Canceled)

		_, err = iter.Head(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("context_timeout", func(t *testing.T) {
		stream := &mockReadStream{
			tuples: []*openfgav1.Tuple{
				{
					Key: &openfgav1.TupleKey{
						Object:   "doc:1",
						Relation: "viewer",
						User:     "user:anne",
					},
				},
			},
		}
		iter := newStreamTupleIterator(stream)
		defer iter.Stop()

		ctx, cancel := context.WithTimeout(context.Background(), 0) // Already expired
		defer cancel()

		// Both Next and Head should respect context deadline
		_, err := iter.Next(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)

		_, err = iter.Head(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("done_state_checked_before_cache", func(t *testing.T) {
		tuples := []*openfgav1.Tuple{
			{
				Key: &openfgav1.TupleKey{
					Object:   "doc:1",
					Relation: "viewer",
					User:     "user:anne",
				},
			},
		}
		stream := &mockReadStream{
			tuples: tuples,
		}
		iter := newStreamTupleIterator(stream)

		// Call Head to cache the tuple
		_, err := iter.Head(context.Background())
		require.NoError(t, err)

		// Stop the iterator (marks it as done)
		iter.Stop()

		// Both Head and Next should return ErrIteratorDone despite cached value
		_, err = iter.Head(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)

		_, err = iter.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})
}

type errorStream struct {
	grpc.ClientStream
}

func (e *errorStream) Recv() (*storagev1.ReadResponse, error) {
	return nil, errors.New("stream error")
}

func (e *errorStream) CloseSend() error {
	return nil
}

func TestStreamErrors(t *testing.T) {
	t.Run("stream_error_propagates", func(t *testing.T) {
		stream := &errorStream{}
		iter := newStreamTupleIterator(stream)
		defer iter.Stop()

		_, err := iter.Next(context.Background())
		require.Error(t, err)
		require.NotErrorIs(t, err, storage.ErrIteratorDone)
	})
}

type storageErrorStream struct {
	storageErr error
	grpc.ClientStream
}

func (e *storageErrorStream) Recv() (*storagev1.ReadResponse, error) {
	// Convert storage error to gRPC error (simulating what server does)
	return nil, toGRPCError(e.storageErr)
}

func (e *storageErrorStream) CloseSend() error {
	return nil
}

func TestIteratorErrorConversion(t *testing.T) {
	// Test representative error types to verify error conversion is wired up correctly.
	tests := []struct {
		name  string
		error error
	}{
		{
			name:  "NotFound error is converted",
			error: storage.ErrNotFound,
		},
		{
			name:  "TransactionThrottled error is converted",
			error: storage.ErrTransactionThrottled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := &storageErrorStream{storageErr: tt.error}
			iter := newStreamTupleIterator(stream)
			defer iter.Stop()

			// Test Next
			_, err := iter.Next(context.Background())
			require.Error(t, err)
			require.ErrorIs(t, err, tt.error, "Next should preserve storage error")

			// Test Head
			_, err = iter.Head(context.Background())
			require.Error(t, err)
			require.ErrorIs(t, err, tt.error, "Head should preserve storage error")
		})
	}
}
