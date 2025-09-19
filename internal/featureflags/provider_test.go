package featureflags

import (
	"context"
	"testing"

	"github.com/open-feature/go-sdk/openfeature"
)

// TestNewDefaultProvider checks if the provider is initialized correctly.
func TestNewDefaultProvider(t *testing.T) {
	flags := []string{"test-flag-1", "test-flag-2"}
	provider := NewDefaultProvider(flags)

	if provider == nil {
		t.Errorf("NewDefaultProvider returned nil")
		return // to make linter happy
	}

	// Verify that the flags map is correctly populated
	if _, ok := provider.flags["test-flag-1"]; !ok {
		t.Errorf("Expected flag 'test-flag-1' to be in the map, but it wasn't")
	}
	if _, ok := provider.flags["test-flag-2"]; !ok {
		t.Errorf("Expected flag 'test-flag-2' to be in the map, but it wasn't")
	}
	if _, ok := provider.flags["non-existent-flag"]; ok {
		t.Errorf("Did not expect 'non-existent-flag' to be in the map, but it was")
	}
}

// TestBooleanEvaluation checks if boolean flag evaluation works for both true and false cases.
func TestBooleanEvaluation(t *testing.T) {
	flags := []string{"enabled-flag", "another-enabled-flag"}
	provider := NewDefaultProvider(flags)
	ctx := context.Background()
	flatCtx := make(openfeature.FlattenedContext)

	// Test case for an enabled flag
	t.Run("Enabled Flag", func(t *testing.T) {
		detail := provider.BooleanEvaluation(ctx, "enabled-flag", false, flatCtx)
		if detail.Value != true {
			t.Errorf("Expected true for 'enabled-flag', but got %v", detail.Value)
		}
	})

	// Test case for a disabled flag
	t.Run("Disabled Flag", func(t *testing.T) {
		detail := provider.BooleanEvaluation(ctx, "disabled-flag", false, flatCtx)
		if detail.Value != false {
			t.Errorf("Expected false for 'disabled-flag', but got %v", detail.Value)
		}
	})
}

// TestOtherEvaluations checks that the other evaluation methods return the default values.
// All non-boolean evaluations currently just return defaults.
func TestOtherEvaluations(t *testing.T) {
	provider := NewDefaultProvider([]string{})
	ctx := context.Background()
	flatCtx := make(openfeature.FlattenedContext)

	t.Run("StringEvaluation", func(t *testing.T) {
		defaultValue := "default-value"
		detail := provider.StringEvaluation(ctx, "any-flag", defaultValue, flatCtx)
		if detail.Value != defaultValue {
			t.Errorf("Expected %s, but got %s", defaultValue, detail.Value)
		}
	})

	t.Run("FloatEvaluation", func(t *testing.T) {
		defaultValue := 3.14
		detail := provider.FloatEvaluation(ctx, "any-flag", defaultValue, flatCtx)
		if detail.Value != defaultValue {
			t.Errorf("Expected %v, but got %v", defaultValue, detail.Value)
		}
	})

	t.Run("IntEvaluation", func(t *testing.T) {
		defaultValue := int64(42)
		detail := provider.IntEvaluation(ctx, "any-flag", defaultValue, flatCtx)
		if detail.Value != defaultValue {
			t.Errorf("Expected %v, but got %v", defaultValue, detail.Value)
		}
	})

	t.Run("ObjectEvaluation", func(t *testing.T) {
		defaultValue := map[string]string{"key": "value"}
		detail := provider.ObjectEvaluation(ctx, "any-flag", defaultValue, flatCtx)
		// Can't directly compare maps, so we'll check a key
		if val, ok := detail.Value.(map[string]string); !ok || val["key"] != "value" {
			t.Errorf("Expected map with key 'key' and value 'value', but got %v", detail.Value)
		}
	})
}

// TestMetadata ensures the provider returns the correct metadata.
func TestMetadata(t *testing.T) {
	provider := NewDefaultProvider([]string{})
	metadata := provider.Metadata()
	expectedName := "Default OpenFGA feature provider"
	if metadata.Name != expectedName {
		t.Errorf("Expected metadata name '%s', but got '%s'", expectedName, metadata.Name)
	}
}

// TestHooks ensures the provider returns an empty slice for hooks.
func TestHooks(t *testing.T) {
	provider := NewDefaultProvider([]string{})
	hooks := provider.Hooks()
	if len(hooks) != 0 {
		t.Errorf("Expected an empty slice of hooks, but got a slice of length %d", len(hooks))
	}
}
