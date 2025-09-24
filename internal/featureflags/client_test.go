package featureflags

import (
	"testing"
)

// TestNewDefaultClient checks if the provider is initialized correctly.
func TestNewDefaultClient(t *testing.T) {
	flags := []string{"test-flag-1", "test-flag-2"}
	client := NewDefaultClient(flags).(*defaultClient)

	if len(client.flags) != len(flags) {
		t.Errorf("client has incorrect number of flags: expected %d, got %d", len(flags), len(client.flags))
		return
	}

	for _, flag := range flags {
		if _, ok := client.flags[flag]; !ok {
			t.Errorf("expected flag %s to be in the map, but it was not", flag)
		}
	}
}

// TestBoolean tests the Boolean method of the defaultClient.
func TestBoolean(t *testing.T) {
	client := NewDefaultClient([]string{"enabled-flag", "another-enabled-flag"})

	// Test case where the flag is enabled and defaultValue is true.
	t.Run("enabled flag, true default", func(t *testing.T) {
		result := client.Boolean("enabled-flag", true, nil)
		if !result {
			t.Errorf("expected true, got %t", result)
		}
	})

	// Test case where the flag is enabled and defaultValue is false.
	t.Run("enabled flag, false default", func(t *testing.T) {
		result := client.Boolean("enabled-flag", false, nil)
		if !result {
			t.Errorf("expected true, got %t", result)
		}
	})

	// Test case where the flag is not enabled and defaultValue is false.
	t.Run("disabled flag, false default", func(t *testing.T) {
		result := client.Boolean("disabled-flag", false, nil)
		if result {
			t.Errorf("expected false, got %t", result)
		}
	})

	// Test case with an empty flag name.
	t.Run("empty flag name", func(t *testing.T) {
		result := client.Boolean("", false, nil)
		if result {
			t.Errorf("expected false, got %t", result)
		}
	})

	// The `featureCtx` is not used by this implementation, so we test that it doesn't affect the result.
	t.Run("with feature context", func(t *testing.T) {
		ctx := map[string]any{"user_id": "123", "plan": "premium"}
		result := client.Boolean("enabled-flag", false, ctx)
		if !result {
			t.Errorf("expected true with context, got %t", result)
		}
	})
}
