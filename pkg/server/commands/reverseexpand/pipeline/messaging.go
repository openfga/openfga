package pipeline

import (
	"sync"

	"github.com/openfga/openfga/pkg/server/commands/reverseexpand/pipeline/track"
)

// bufferPool reduces GC pressure by reusing slice allocations across messages.
type bufferPool struct {
	size int
	pool sync.Pool
}

func (b *bufferPool) Get() *[]string {
	return b.pool.Get().(*[]string)
}

func (b *bufferPool) Put(buffer *[]string) {
	b.pool.Put(buffer)
}

func (b *bufferPool) create() any {
	tmp := make([]string, b.size)
	return &tmp
}

func newBufferPool(size int) *bufferPool {
	var b bufferPool
	b.size = size
	b.pool.New = b.create
	return &b
}

// Item holds either an object ID or an error encountered during processing.
type Item struct {
	Value string
	Err   error
}

func (i Item) Object() (string, error) {
	return i.Value, i.Err
}

// message carries items between workers along with cleanup state.
type message struct {
	Value      []string
	buffer     *[]string
	bufferPool *bufferPool
	tracker    *track.Tracker
}

// Done releases resources. Tracker must decrement first for correct cycle shutdown.
func (m *message) Done() {
	if m.tracker != nil {
		m.tracker.Dec()
	}
	if m.bufferPool != nil && m.buffer != nil {
		m.bufferPool.Put(m.buffer)
	}
}
