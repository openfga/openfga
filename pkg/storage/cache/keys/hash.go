package keys

import (
	"crypto/rand"
	"encoding/binary"
	"hash"

	"github.com/cespare/xxhash/v2"
)

// Seed is the per-process random seed applied to all Digest instances.
// Randomizing prevents external observers from predicting or deliberately
// colliding hash outputs (hash-flooding mitigation).
//
// Exported solely so tests can pin it to a known value for deterministic
// hash output. Production code must not modify Seed: Digest.Reset re-reads
// it on every call, so changing Seed at runtime invalidates the continuity
// of any in-flight or already-emitted digests.
var Seed uint64

func init() {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("keys: failed to seed digest: " + err.Error())
	}
	Seed = binary.NativeEndian.Uint64(b[:])
}

var _ hash.Hash64 = &Digest{}

// Digest wraps xxhash.Digest with the package-level Seed applied
// automatically on construction and reset.
type Digest struct {
	inner *xxhash.Digest
}

// NewDigest returns a new Digest initialized with the package-level Seed.
func NewDigest() *Digest {
	return &Digest{
		inner: xxhash.NewWithSeed(Seed),
	}
}

func (hasher *Digest) Size() int {
	return hasher.inner.Size()
}

func (hasher *Digest) BlockSize() int {
	return hasher.inner.BlockSize()
}

func (hasher *Digest) Write(b []byte) (int, error) {
	return hasher.inner.Write(b)
}

func (hasher *Digest) WriteString(s string) (int, error) {
	return hasher.inner.WriteString(s)
}

// Reset re-initializes the digest while preserving the package-level Seed.
func (hasher *Digest) Reset() {
	hasher.inner.ResetWithSeed(Seed)
}

func (hasher *Digest) Sum(b []byte) []byte {
	return hasher.inner.Sum(b)
}

func (hasher *Digest) Sum64() uint64 {
	return hasher.inner.Sum64()
}
