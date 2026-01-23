package pipeline

import (
	"iter"
	"sync"

	"github.com/openfga/openfga/internal/containers"
	"github.com/openfga/openfga/pkg/server/commands/reverseexpand/pipeline/track"
)

type bufferPool struct {
	size int
	pool sync.Pool
}

func (b *bufferPool) Get() *[]Item {
	return b.pool.Get().(*[]Item)
}

func (b *bufferPool) Put(buffer *[]Item) {
	b.pool.Put(buffer)
}

func (b *bufferPool) create() any {
	tmp := make([]Item, b.size)
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

type message struct {
	Value []Item

	// stored for cleanup
	buffer     *[]Item
	bufferPool *bufferPool
	tracker    *track.Tracker
}

func (m *message) Done() {
	if m.tracker != nil {
		m.tracker.Dec()
	}
	if m.bufferPool != nil && m.buffer != nil {
		m.bufferPool.Put(m.buffer)
	}
}

// txBag is a type that implements the interface pipe.Tx[T]
// for the type containers.Bag[T].
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
