package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTypedSyncMap_BasicIntString(t *testing.T) {
	// Create a map with int keys and string values
	tm := &TypedSyncMap[int, string]{}

	key := 101
	value := "hello world"

	// --- 1. Store Test ---
	tm.Store(key, value)

	// --- 2. Load Success Test ---
	t.Run("Load_Success", func(t *testing.T) {
		loadedValue, ok := tm.Load(key)
		require.True(t, ok)
		require.Equal(t, value, loadedValue)
	})

	// --- 3. Load Missing Key Test ---
	t.Run("Load_MissingKey", func(t *testing.T) {
		missingKey := 999
		loadedValue, ok := tm.Load(missingKey)
		var zeroValue string // Zero value for string is ""
		require.False(t, ok)
		require.Equal(t, zeroValue, loadedValue)
	})

	// --- 4. Delete Test ---
	t.Run("Delete", func(t *testing.T) {
		tm.Delete(key)

		// Verify deletion
		_, ok := tm.Load(key)
		require.False(t, ok)
	})
}
