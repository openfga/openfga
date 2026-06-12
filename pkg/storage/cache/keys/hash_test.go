package keys

import (
	"encoding/binary"
	"hash"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

func TestSeed_InitProducedNonZero(t *testing.T) {
	// The init() must populate Seed from crypto/rand. A zero value would
	// indicate either that init() failed silently or that entropy collapsed,
	// either of which removes the hash-flooding protection.
	require.NotZero(t, Seed, "package Seed must be initialized to a non-zero value")
}

func TestDigest_ImplementsHashHash64(t *testing.T) {
	// Compile-time assertion (var _ hash.Hash64 = &Digest{}) already exists,
	// but assert at runtime that a NewDigest is usable through the interface.
	var h hash.Hash64 = NewDigest()
	_, err := h.Write([]byte("x"))
	require.NoError(t, err)
	require.NotZero(t, h.Sum64())
}

func TestDigest_NewUsesPackageSeed(t *testing.T) {
	// A Digest produced by NewDigest must hash equivalently to a bare xxhash
	// digest seeded with the same package-level Seed.
	d := NewDigest()
	_, _ = d.Write([]byte("hello"))

	ref := xxhash.NewWithSeed(Seed)
	_, _ = ref.Write([]byte("hello"))

	require.Equal(t, ref.Sum64(), d.Sum64())
}

func TestDigest_NewWithDifferentSeedDiffers(t *testing.T) {
	// Swap Seed and confirm a freshly constructed Digest picks up the new
	// value (i.e., NewDigest reads Seed, doesn't snapshot it elsewhere).
	original := Seed
	defer func() { Seed = original }()

	Seed = 1
	d1 := NewDigest()
	_, _ = d1.Write([]byte("payload"))

	Seed = 2
	d2 := NewDigest()
	_, _ = d2.Write([]byte("payload"))

	require.NotEqual(t, d1.Sum64(), d2.Sum64())
}

func TestDigest_WriteAndWriteStringAreEquivalent(t *testing.T) {
	// WriteString must be a transparent forward to Write — the digest of a
	// string and the digest of its bytes must match.
	const payload = "the quick brown fox"

	d1 := NewDigest()
	_, err := d1.Write([]byte(payload))
	require.NoError(t, err)

	d2 := NewDigest()
	n, err := d2.WriteString(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)

	require.Equal(t, d1.Sum64(), d2.Sum64())
}

func TestDigest_Sum_AppendsBigEndianSum64(t *testing.T) {
	// Sum must append the 8-byte big-endian encoding of Sum64 to the
	// provided buffer (xxhash.Digest semantics, which Digest delegates to)
	// without mutating the supplied prefix bytes.
	d := NewDigest()
	_, _ = d.WriteString("abc")

	prefix := []byte{0xde, 0xad}
	out := d.Sum(prefix)

	require.Len(t, out, len(prefix)+8)
	require.Equal(t, prefix, out[:len(prefix)])
	require.Equal(t, d.Sum64(), binary.BigEndian.Uint64(out[len(prefix):]))
}

func TestDigest_Size(t *testing.T) {
	// xxhash is a 64-bit hash; Size must report 8 bytes.
	require.Equal(t, 8, NewDigest().Size())
}

func TestDigest_BlockSize(t *testing.T) {
	// BlockSize is delegated to xxhash; assert it matches the underlying
	// implementation so a future xxhash change is caught here, not via a
	// surprising downstream regression.
	require.Equal(t, xxhash.New().BlockSize(), NewDigest().BlockSize())
}

func TestDigest_ResetRestoresInitialState(t *testing.T) {
	// After Write + Reset, Sum64 must equal the digest of an empty input
	// (with the current Seed) — i.e., Reset truly clears accumulated state.
	d := NewDigest()
	_, _ = d.WriteString("garbage")
	d.Reset()

	empty := NewDigest()
	require.Equal(t, empty.Sum64(), d.Sum64())
}
