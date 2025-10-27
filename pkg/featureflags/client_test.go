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

	t.Run("enabled flag", func(t *testing.T) {
		result := client.Boolean("enabled-flag", nil)
		require.True(t, result)
	})

	t.Run("disabled flag", func(t *testing.T) {
		result := client.Boolean("disabled-flag", nil)
		require.False(t, result)
	})

	// The `featureCtx` is not used by this implementation, so we test that it doesn't affect the result.
	t.Run("with feature context", func(t *testing.T) {
		ctx := map[string]any{"user_id": "123", "plan": "premium"}
		result := client.Boolean("enabled-flag", ctx)
		require.True(t, result)

		result = client.Boolean("disabled-flag", ctx)
		require.False(t, result)
	})
}
