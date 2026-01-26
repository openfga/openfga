package pipeline

import (
	"context"
	"sync"

	"github.com/openfga/openfga/internal/pipe"
)

type sender struct {
	key *Edge
	pipe.RxCloser[*message]
}

func (s *sender) Key() *Edge {
	return s.key
}

type listener struct {
	key *Edge
	pipe.TxCloser[*message]
}

func (l *listener) Key() *Edge {
	return l.key
}

// worker processes messages for a single node in the authorization graph.
type worker struct {
	senders      []*sender
	listeners    []*listener
	Resolver     resolver
	bufferConfig pipe.Config
	started      bool
}

// Close shuts down outputs. Inputs are owned by upstream workers.
func (w *worker) Close() {
	for _, lst := range w.listeners {
		lst.Close()
	}
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
}

func (w *worker) Subscribe(key *Edge) *sender {
	p := pipe.Must[*message](w.bufferConfig)

	w.listeners = append(w.listeners, &listener{
		key,
		p,
	})

	return &sender{
		key,
		p,
	}
}

// listenForInitialValue seeds the traversal from the user's starting point.
func (w *worker) listenForInitialValue(value string, membership *membership) {
	items := []Item{Item{Value: value}}
	membership.Tracker().Inc()
	m := &message{
		Value:   items,
		tracker: membership.Tracker(),
	}
	w.Listen(&sender{nil, pipe.StaticRx(m)})
}

type workerPool map[*Node]*worker

// Start launches all workers and blocks until they begin processing.
func (wp workerPool) Start(ctx context.Context) {
	var wg sync.WaitGroup
	for _, w := range wp {
		wg.Add(1)
		go func(ctx context.Context, wg *sync.WaitGroup, fn func(context.Context)) {
			defer wg.Done()
			fn(ctx)
		}(ctx, &wg, w.Start)
	}
	wg.Wait()
}

func (wp workerPool) Stop() {
	for _, w := range wp {
		w.Close()
	}
}
