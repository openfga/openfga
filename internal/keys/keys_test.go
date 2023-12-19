package keys

import (
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

func TestCacheKeyHasher(t *testing.T) {
	hasher1 := NewCacheKeyHasher(xxhash.New())
	err := hasher1.WriteString("a")
	require.NoError(t, err)

	hasher2 := NewCacheKeyHasher(xxhash.New())
	err = hasher2.WriteString("b")
	require.NoError(t, err)

	require.NotEqual(t, hasher1.Key().ToUInt64(), hasher2.Key().ToUInt64())
}
