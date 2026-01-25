package pipeline

import (
	"context"
	"sync"

	"github.com/openfga/openfga/internal/pipe"
)

// sender is a struct that contains fields relevant to the producing
// end of a pipeline connection.
type sender struct {
	key *Edge
	pipe.RxCloser[*message]
}

func (s *sender) Key() *Edge {
	return s.key
}

// listener is a struct that contains fields relevant to the listening
// end of a pipeline connection.
type listener struct {
	key *Edge
	pipe.TxCloser[*message]
}

func (l *listener) Key() *Edge {
	return l.key
}

// worker represents a processing node in the pipeline corresponding to a relation in the
// authorization model. It consumes messages from upstream workers (senders), processes them
// through its resolver, and sends results to downstream workers (listeners).
type worker struct {
	senders      []*sender
	listeners    []*listener
	Resolver     resolver
	bufferConfig pipe.Config
	started      bool
}

// Close shuts down the worker's output connections.
// Only closes listeners (not senders) because this worker owns its output but not its input;
// upstream workers own the senders and are responsible for closing them.
func (w *worker) Close() {
	for _, lst := range w.listeners {
		lst.Close()
	}
}

// Listen registers an input connection from an upstream worker.
// Called during pipeline construction to wire the worker graph together.
func (w *worker) Listen(s *sender) {
	w.senders = append(w.senders, s)
}

// Start begins processing messages through this worker's resolver.
// Only the first call actually starts the resolver.
func (w *worker) Start(ctx context.Context) {
	if w.started {
		return
	}
	ctx, span := pipelineTracer.Start(ctx, "worker")
	defer span.End()

	w.started = true
	w.Resolver.Resolve(ctx, w.senders, w.listeners)
}

// Subscribe creates an output connection for downstream workers to consume this worker's results.
// The edge parameter identifies which authorization model edge this connection represents.
// Returns a sender that the downstream worker will listen to.
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

// listenForInitialValue seeds the worker with a starting value for graph traversal.
// Called when the worker's node matches the query's user node; this initial value
// propagates through the worker graph to find all reachable objects.
func (w *worker) listenForInitialValue(value string, membership *membership) {
	items := []Object{Item{Value: value}}
	membership.Tracker().Inc()
	m := &message{
		Value: items,

		// Stored for cleanup
		tracker: membership.Tracker(),
	}
	w.Listen(&sender{nil, pipe.StaticRx(m)})
}

type workerPool map[*Node]*worker

// Start launches all workers concurrently.
// Blocks until all workers have begun processing; this ensures the pipeline
// is fully operational before streaming results to the caller.
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

// Stop closes all worker output connections to signal shutdown.
func (wp workerPool) Stop() {
	for _, w := range wp {
		w.Close()
	}
}
