package keys

import (
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

func TestCacheKeyHasher(t *testing.T) {
	hasher1 := NewCacheKeyHasher(xxhash.New())
	hasher1.WriteString("a")

	hasher2 := NewCacheKeyHasher(xxhash.New())
	hasher2.WriteString("b")

	require.NotEqual(t, hasher1.Key().ToUInt64(), hasher2.Key().ToUInt64())
}
