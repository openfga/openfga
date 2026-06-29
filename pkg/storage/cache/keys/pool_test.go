package keys

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBuilder_ReturnsUsableBuilder(t *testing.T) {
	b := GetBuilder()
	require.NotNil(t, b)
	require.NotNil(t, b.Builder)

	b.EncodeString("hello")
	require.NotEmpty(t, b.Bytes())

	b.Close()
}

func TestPooledBuilder_CloseClearsAndDetaches(t *testing.T) {
	b := GetBuilder()
	b.EncodeString("payload")
	require.NotEmpty(t, b.Bytes())

	b.Close()

	// After Close, the wrapper no longer holds a Builder reference.
	require.Nil(t, b.Builder)
}

func TestPooledBuilder_CloseIdempotent(t *testing.T) {
	b := GetBuilder()
	b.EncodeString("x")

	require.NotPanics(t, func() { b.Close() })
	// A second Close must be a no-op — calling Reset/Put on a nil Builder
	// would panic, so the absence of a panic confirms the closed flag
	// short-circuits as intended.
	require.NotPanics(t, func() { b.Close() })
	require.Nil(t, b.Builder)
}

func TestPooledBuilder_ResetBeforeReturn(t *testing.T) {
	// Acquire a builder, write to it, and close it. Then drain the pool
	// until we get the same underlying *Builder back, and verify its
	// buffer was reset (length 0) before being returned to the pool.
	first := GetBuilder()
	first.EncodeString("not-empty")
	original := first.Builder
	first.Close()

	// sync.Pool doesn't guarantee LIFO order or even that Put-then-Get
	// returns the same instance; drain a bounded number of times looking
	// for our pointer. If we don't see it (GC eviction, contention), the
	// test still asserts the invariant on whichever builder we did see:
	// any builder pulled from the pool must be empty.
	for range 32 {
		b := GetBuilder()
		require.Empty(t, b.Bytes(), "pooled builder must be empty on retrieval")
		if b.Builder == original {
			b.Close()
			return
		}
		b.Close()
	}
}

func TestGetDigest_ReturnsUsableDigest(t *testing.T) {
	d := GetDigest()
	require.NotNil(t, d)
	require.NotNil(t, d.Digest)

	_, _ = d.Write([]byte("hello"))
	require.NotZero(t, d.Sum64())

	d.Close()
}

func TestPooledDigest_CloseClearsAndDetaches(t *testing.T) {
	d := GetDigest()
	_, _ = d.Write([]byte("payload"))

	d.Close()

	require.Nil(t, d.Digest)
}

func TestPooledDigest_CloseIdempotent(t *testing.T) {
	d := GetDigest()
	_, _ = d.Write([]byte("x"))

	require.NotPanics(t, func() { d.Close() })
	require.NotPanics(t, func() { d.Close() })
	require.Nil(t, d.Digest)
}

func TestGetDigest_ResetOnRetrieval(t *testing.T) {
	// GetDigest must call Reset on the returned digest so its state is
	// fresh regardless of whether it came from the pool or was newly
	// constructed. Write to one, return it, then verify any subsequent
	// Get yields a digest whose Sum64 over an empty input equals that
	// of a freshly-constructed digest.
	want := NewDigest().Sum64()

	used := GetDigest()
	_, _ = used.Write([]byte("dirty state"))
	used.Close()

	fresh := GetDigest()
	require.Equal(t, want, fresh.Sum64(), "GetDigest must Reset before returning")
	fresh.Close()
}

func TestPooledBuilder_ConcurrentGetAndClose(t *testing.T) {
	const goroutines = 50
	const iterations = 100

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for j := range iterations {
				b := GetBuilder()
				b.EncodeString(fmt.Sprintf("goroutine-%d", j))
				key := b.Key()
				assert.NotEmpty(t, key.String())
				b.Close()
			}
		})
	}
	wg.Wait()
}

func TestPooledDigest_ConcurrentGetAndClose(t *testing.T) {
	const goroutines = 50
	const iterations = 100

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for j := range iterations {
				d := GetDigest()
				_, _ = fmt.Fprintf(d, "data-%d", j)
				assert.NotZero(t, d.Sum64())
				d.Close()
			}
		})
	}
	wg.Wait()
}

func TestDigest_ResetUsesCurrentSeed(t *testing.T) {
	original := Seed
	defer func() { Seed = original }()

	Seed = 1
	d := NewDigest()
	_, _ = d.Write([]byte("test"))
	hash1 := d.Sum64()

	Seed = 2
	d.Reset()
	_, _ = d.Write([]byte("test"))
	hash2 := d.Sum64()

	require.NotEqual(t, hash1, hash2, "Reset must re-seed with current Seed value")
}

func TestGetDigest_StateCleanBetweenUsers(t *testing.T) {
	d1 := GetDigest()
	_, _ = d1.Write([]byte("user-a-data"))
	hash1 := d1.Sum64()
	d1.Close()

	d2 := GetDigest()
	_, _ = d2.Write([]byte("user-a-data"))
	hash2 := d2.Sum64()
	d2.Close()

	require.Equal(t, hash1, hash2, "same data must produce same hash across pool reuses")

	d3 := GetDigest()
	_, _ = d3.Write([]byte("different-data"))
	hash3 := d3.Sum64()
	d3.Close()

	require.NotEqual(t, hash1, hash3, "different data must produce different hashes")
}
