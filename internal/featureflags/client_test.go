package featureflags

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewDefaultClient checks if the provider is initialized correctly.
func TestNewDefaultClient(t *testing.T) {
	flags := []string{"test-flag-1", "test-flag-2"}
	client := NewDefaultClient(flags).(*defaultClient)

	require.Len(t, client.flags, len(flags))

	for _, flag := range flags {
		_, ok := client.flags[flag]
		require.True(t, ok)
	}
}

// TestBoolean tests the Boolean method of the defaultClient.
func TestBoolean(t *testing.T) {
	client := NewDefaultClient([]string{"enabled-flag", "another-enabled-flag"})

	// Test case where the flag is enabled and defaultValue is true.
	t.Run("enabled flag, true default", func(t *testing.T) {
		result := client.Boolean("enabled-flag", true, nil)
		require.True(t, result)
	})

	// Test case where the flag is enabled and defaultValue is false.
	t.Run("enabled flag, false default", func(t *testing.T) {
		result := client.Boolean("enabled-flag", false, nil)
		require.True(t, result)
	})

	// Test case where the flag is not enabled and defaultValue is false.
	t.Run("disabled flag, false default", func(t *testing.T) {
		result := client.Boolean("disabled-flag", false, nil)
		require.False(t, result)
	})

	// Test case with an empty flag name.
	t.Run("empty flag name", func(t *testing.T) {
		result := client.Boolean("", false, nil)
		require.False(t, result)
	})

	// The `featureCtx` is not used by this implementation, so we test that it doesn't affect the result.
	t.Run("with feature context", func(t *testing.T) {
		ctx := map[string]any{"user_id": "123", "plan": "premium"}
		result := client.Boolean("enabled-flag", false, ctx)
		require.True(t, result)
	})
}
