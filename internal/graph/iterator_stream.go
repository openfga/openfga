package graph

import (
	"context"
	"slices"

	"github.com/openfga/openfga/pkg/storage"
)

type iteratorStream struct {
	idx    int
	buffer storage.TupleKeyIterator
	done   bool
	source chan *iteratorMsg
}

// helper function to drain the stream until the head's object is larger than target.
func (s *iteratorStream) skipToTarget(ctx context.Context, target string) error {
	t, err := s.buffer.Head(ctx)
	if err != nil {
		if storage.IterIsDoneOrCancelled(err) {
			s.buffer.Stop()
			s.buffer = nil
			return nil
		}
		return err
	}
	tmpKey := t.GetObject()
	for tmpKey < target {
		_, _ = s.buffer.Next(ctx)
		t, err = s.buffer.Head(ctx)
		if err != nil {
			if storage.IterIsDoneOrCancelled(err) {
				s.buffer.Stop()
				s.buffer = nil
				break
			}
			return err
		}
		tmpKey = t.GetObject()
	}
	return nil
}

type iteratorStreams struct {
	streams []*iteratorStream
}

// getActiveStreamsCount will return the active streams from the last time getActiveStreams was called.
func (s *iteratorStreams) getActiveStreamsCount() int {
	return len(s.streams)
}

// Stop will drain all streams completely to avoid leaving dangling resources
// NOTE: caller should consider running this in a goroutine to not block.
func (s *iteratorStreams) Stop() {
	for _, stream := range s.streams {
		if stream.buffer != nil {
			stream.buffer.Stop()
		}
		for msg := range stream.source {
			if msg.iter != nil {
				msg.iter.Stop()
			}
		}
	}
}

// getActiveStreams will return a list of the remaining active streams.
// To be considered active your source channel must still be open.
func (s *iteratorStreams) getActiveStreams(ctx context.Context) ([]*iteratorStream, error) {
	for _, stream := range s.streams {
		if stream.buffer != nil || stream.done {
			// no need to poll further
			continue
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case i, ok := <-stream.source:
			if !ok {
				stream.done = true
				break
			}
			if i.err != nil {
				return nil, i.err
			}
			stream.buffer = i.iter
		}
	}
	// TODO: in go1.23 compare performance vs slices.Collect
	// clean up all empty entries that are both done and drained
	s.streams = slices.DeleteFunc(s.streams, func(entry *iteratorStream) bool {
		return entry.done && entry.buffer == nil
	})
	return s.streams, nil
}
