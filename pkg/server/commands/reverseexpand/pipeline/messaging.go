package pipeline

import (
	"iter"
	"sync"

	"github.com/openfga/openfga/internal/containers"
	"github.com/openfga/openfga/pkg/server/commands/reverseexpand/pipeline/track"
)

// bufferPool manages reusable Item slice allocations to reduce GC pressure.
// Workers allocate many short-lived slices for batching results; pooling
// these allocations significantly reduces garbage collection overhead.
type bufferPool struct {
	size int
	pool sync.Pool
}

func (b *bufferPool) Get() *[]Object {
	return b.pool.Get().(*[]Object)
}

func (b *bufferPool) Put(buffer *[]Object) {
	b.pool.Put(buffer)
}

func (b *bufferPool) create() any {
	tmp := make([]Object, b.size)
	return &tmp
}

func newBufferPool(size int) *bufferPool {
	var b bufferPool
	b.size = size
	b.pool.New = b.create
	return &b
}

// Item is a struct that contains an object `string` as its `Value` or an
// encountered error as its `Err`. Item is the primary container used to
// communicate values as they pass through a `Pipeline`.
type Item struct {
	Value string
	Err   error
}

func (i Item) Object() (string, error) {
	return i.Value, i.Err
}

// message is the unit of communication between workers.
// It carries result items and cleanup state needed to properly release resources
// when the message is processed.
type message struct {
	Value []Object

	// stored for cleanup
	buffer     *[]Object
	bufferPool *bufferPool
	tracker    *track.Tracker
}

// Done releases the message's resources back to their pools.
// Must decrement the tracker first to ensure proper shutdown coordination;
// cycle groups wait for tracker counts to reach zero before completing shutdown.
func (m *message) Done() {
	if m.tracker != nil {
		m.tracker.Dec()
	}
	if m.bufferPool != nil && m.buffer != nil {
		m.bufferPool.Put(m.buffer)
	}
}

// txBag adapts containers.Bag to the pipe.Tx interface.
// Operator resolvers need to collect all items from their senders before computing results
// (e.g., intersection, exclusion). Bag provides unordered collection while Tx provides
// the interface expected by processor code.
type txBag[T any] containers.Bag[T]

// Send implements the pipe.Tx[T] interface.
func (tx *txBag[T]) Send(t T) bool {
	(*containers.Bag[T])(tx).Add(t)
	return true
}

// Seq returns an iter.Seq of all items within the txBag.
func (tx *txBag[T]) Seq() iter.Seq[T] {
	return (*containers.Bag[T])(tx).Seq()
}
