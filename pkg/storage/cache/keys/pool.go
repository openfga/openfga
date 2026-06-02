package keys

import (
	"sync"
)

// PooledBuilder is a Builder obtained from a sync.Pool. Callers must
// call Close when finished to return it to the pool. The typical pattern:
//
//	b := keys.GetBuilder()
//	defer b.Close()
type PooledBuilder struct {
	*Builder
	closed bool
}

// Close resets the builder and returns it to the pool. Close is
// idempotent; subsequent calls are no-ops.
func (b *PooledBuilder) Close() {
	if b.closed {
		return
	}
	b.Reset()
	builderPool.Put(b.Builder)
	b.Builder = nil
	b.closed = true
}

var builderPool = sync.Pool{
	New: func() any {
		return &Builder{}
	},
}

// GetBuilder retrieves a Builder from the pool, ready for use. The returned
// wrapper must not be copied: each copy carries its own closed flag, so
// calling Close on more than one copy would return the same Builder to the
// pool twice.
func GetBuilder() *PooledBuilder {
	return &PooledBuilder{Builder: builderPool.Get().(*Builder)}
}

// PooledDigest is a Digest obtained from a sync.Pool. Callers must
// call Close when finished to return it to the pool.
type PooledDigest struct {
	*Digest
	closed bool
}

// Close resets the digest and returns it to the pool. Close is
// idempotent; subsequent calls are no-ops.
func (d *PooledDigest) Close() {
	if d.closed {
		return
	}
	d.Reset()
	digestPool.Put(d.Digest)
	d.Digest = nil
	d.closed = true
}

var digestPool = sync.Pool{
	New: func() any {
		return NewDigest()
	},
}

// GetDigest retrieves a Digest from the pool, ready for use. Reset is
// called to ensure the digest uses the current package-level Seed. The
// returned wrapper must not be copied: each copy carries its own closed
// flag, so calling Close on more than one copy would return the same
// Digest to the pool twice.
func GetDigest() *PooledDigest {
	d := digestPool.Get().(*Digest)
	d.Reset()
	return &PooledDigest{Digest: d}
}
