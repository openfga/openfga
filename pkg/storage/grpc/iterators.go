package grpc

import (
	"context"
	"errors"
	"io"
	"sync"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	storagev1 "github.com/openfga/openfga/pkg/storage/grpc/proto/storage/v1"
)

// streamTupleIterator adapts a gRPC stream to a TupleIterator.
type streamTupleIterator struct {
	mu      sync.Mutex
	stream  streamRecv
	current *openfgav1.Tuple
	done    bool
}

var _ storage.TupleIterator = (*streamTupleIterator)(nil)

type streamRecv interface {
	Recv() (*storagev1.ReadResponse, error)
	CloseSend() error
}

func newStreamTupleIterator(stream streamRecv) *streamTupleIterator {
	return &streamTupleIterator{
		stream: stream,
	}
}

func (s *streamTupleIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	// Check for context cancellation or timeout
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.done {
		return nil, storage.ErrIteratorDone
	}

	// Fetch into cache if empty
	if s.current == nil {
		resp, err := s.stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.done = true
				return nil, storage.ErrIteratorDone
			}
			return nil, err
		}
		s.current = fromStorageTuple(resp.Tuple)
	}

	// Return cached item and clear it (consume)
	tuple := s.current
	s.current = nil
	return tuple, nil
}

func (s *streamTupleIterator) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.done {
		return
	}

	s.done = true
	s.stream.CloseSend()
}

func (s *streamTupleIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	// Check for context cancellation or timeout
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.done {
		return nil, storage.ErrIteratorDone
	}

	if s.current != nil {
		return s.current, nil
	}

	// Fetch the first item
	resp, err := s.stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			s.done = true
			return nil, storage.ErrIteratorDone
		}
		return nil, err
	}

	tuple := fromStorageTuple(resp.Tuple)
	s.current = tuple

	return tuple, nil
}
