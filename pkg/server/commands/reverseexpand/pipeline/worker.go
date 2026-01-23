package pipeline

import (
	"context"
	"sync"

	"github.com/openfga/openfga/internal/pipe"
	"github.com/openfga/openfga/pkg/server/commands/reverseexpand/pipeline/track"
)

// sender is a struct that contains fields relevant to the producing
// end of a pipeline connection.
type sender struct {
	key *Edge
	pipe.Rx[*message]
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

type worker struct {
	senders      []*sender
	listeners    []*listener
	Resolver     resolver
	bufferConfig pipe.Config
	started      bool
}

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

func (w *worker) listenForInitialValue(value string, tracker *track.Tracker) {
	items := []Item{{Value: value}}
	tracker.Inc()
	m := &message{
		Value: items,

		// Stored for cleanup
		tracker: tracker,
	}
	w.Listen(&sender{nil, pipe.StaticRx(m)})
}

type workerPool map[*Node]*worker

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
