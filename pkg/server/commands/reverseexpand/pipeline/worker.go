package pipeline

import (
	"context"
	"sync"
)

type sender struct {
	Key *Edge
	C   <-chan *message
}

type listener struct {
	closed bool
	Key    *Edge
	C      chan<- *message
}

func (l *listener) Close() error {
	if l.closed {
		return nil
	}
	l.closed = true
	close(l.C)
	return nil
}

// worker processes messages for a single node in the authorization graph.
type worker struct {
	senders        []*sender
	listeners      []*listener
	Resolver       resolver
	bufferCapacity int
	started        bool
}

func (w *worker) Listen(s *sender) {
	w.senders = append(w.senders, s)
}

func (w *worker) Start(ctx context.Context) {
	if w.started {
		return
	}
	ctx, span := pipelineTracer.Start(ctx, "worker")
	defer span.End()

	w.started = true

	w.Resolver.Resolve(ctx, w.senders, w.listeners)

	for _, lst := range w.listeners {
		lst.Close()
	}
}

func (w *worker) Subscribe(key *Edge) *sender {
	ch := make(chan *message, w.bufferCapacity)

	w.listeners = append(w.listeners, &listener{
		Key: key,
		C:   ch,
	})

	return &sender{
		key,
		ch,
	}
}

// listenForInitialValue seeds the traversal from the user's starting point.
func (w *worker) listenForInitialValue(value string, membership *membership) {
	items := []string{value}
	membership.Tracker().Inc()
	m := message{
		Value:   items,
		tracker: membership.Tracker(),
	}
	ch := make(chan *message, 1)
	ch <- &m
	close(ch)
	w.Listen(&sender{Key: nil, C: ch})
}

type workerPool map[*Node]*worker

// Start launches all workers and blocks until they begin processing.
func (wp workerPool) Start(ctx context.Context) {
	var wg sync.WaitGroup
	for _, w := range wp {
		wg.Add(1)

		go func() {
			defer wg.Done()
			w.Start(ctx)
		}()
	}
	wg.Wait()
}
