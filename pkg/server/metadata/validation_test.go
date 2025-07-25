package metadata

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateMetadata(t *testing.T) {
	tests := []struct {
		name        string
		metadata    map[string]string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "nil metadata is valid",
			metadata:    nil,
			expectError: false,
		},
		{
			name:        "empty metadata is valid",
			metadata:    map[string]string{},
			expectError: false,
		},
		{
			name: "valid metadata",
			metadata: map[string]string{
				"environment": "production",
				"team":        "platform",
				"version":     "1.2.3",
			},
			expectError: false,
		},
		{
			name: "valid metadata with dashes and dots",
			metadata: map[string]string{
				"my-custom-label": "value",
				"app.example.com": "openfga",
			},
			expectError: false,
		},
		{
			name: "reserved prefix kubernetes.io",
			metadata: map[string]string{
				"kubernetes.io/name": "openfga",
			},
			expectError: true,
			errorMsg:    "reserved prefix",
		},
		{
			name: "key too long",
			metadata: map[string]string{
				strings.Repeat("a", MaxLabelKeyLength+1): "value",
			},
			expectError: true,
			errorMsg:    "key length cannot exceed",
		},
		{
			name: "value too long",
			metadata: map[string]string{
				"key": strings.Repeat("a", MaxLabelValueLength+1),
			},
			expectError: true,
			errorMsg:    "value length cannot exceed",
		},
		{
			name: "empty key",
			metadata: map[string]string{
				"": "value",
			},
			expectError: true,
			errorMsg:    "key cannot be empty",
		},
		{
			name: "invalid key format with underscore",
			metadata: map[string]string{
				"invalid_key": "value",
			},
			expectError: true,
			errorMsg:    "lowercase alphanumeric characters, dashes, and dots",
		},
		{
			name: "key starting with dash",
			metadata: map[string]string{
				"-invalid": "value",
			},
			expectError: true,
			errorMsg:    "must start with alphanumeric",
		},
		{
			name: "key ending with dash",
			metadata: map[string]string{
				"invalid-": "value",
			},
			expectError: true,
			errorMsg:    "must end with alphanumeric",
		},
		{
			name: "reserved prefix openfga.dev",
			metadata: map[string]string{
				"openfga.dev/internal": "value",
			},
			expectError: true,
			errorMsg:    "reserved prefix",
		},
		{
			name: "too many labels",
			metadata: func() map[string]string {
				m := make(map[string]string)
				for i := 0; i < MaxLabelsCount+1; i++ {
					m[fmt.Sprintf("key%d", i)] = "value"
				}
				return m
			}(),
			expectError: true,
			errorMsg:    "cannot have more than",
		},
		{
			name: "value with control characters",
			metadata: map[string]string{
				"key": "value\x00with\x01control",
			},
			expectError: true,
			errorMsg:    "invalid control character",
		},
		{
			name: "value with allowed whitespace",
			metadata: map[string]string{
				"key": "value\twith\nallowed\rwhitespace",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMetadata(tt.metadata)
			
			if tt.expectError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					require.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNormalizeMetadata(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		expected map[string]string
	}{
		{
			name:     "nil input",
			input:    nil,
			expected: nil,
		},
		{
			name:     "empty input",
			input:    map[string]string{},
			expected: nil,
		},
		{
			name: "trim whitespace",
			input: map[string]string{
				"  key1  ": "  value1  ",
				"key2":     "value2",
			},
			expected: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		},
		{
			name: "remove empty keys",
			input: map[string]string{
				"":     "value",
				"key1": "value1",
				"  ":   "value2",
			},
			expected: map[string]string{
				"key1": "value1",
			},
		},
		{
			name: "preserve empty values",
			input: map[string]string{
				"key1": "",
				"key2": "value2",
			},
			expected: map[string]string{
				"key1": "",
				"key2": "value2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeMetadata(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestMaxMetadataSize(t *testing.T) {
	// Create metadata that exceeds the size limit
	largeValue := strings.Repeat("a", MaxMetadataSize)
	metadata := map[string]string{
		"key": largeValue,
	}
	
	err := ValidateMetadata(metadata)
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata total size cannot exceed")
}

func TestValidLabelFormats(t *testing.T) {
	validKeys := []string{
		"a",
		"abc",
		"a1b2c3",
		"app.example.com",
		"sub-domain.example.com",
		"example-key",
		"123-abc",
		"a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z",
	}
	
	for _, key := range validKeys {
		t.Run("valid_key_"+key, func(t *testing.T) {
			metadata := map[string]string{key: "value"}
			err := ValidateMetadata(metadata)
			require.NoError(t, err, "key '%s' should be valid", key)
		})
	}
	
	invalidKeys := []string{
		"",
		"-abc",
		"abc-",
		".abc",
		"abc.",
		"ab..c",
		"ab--c",
		"abc_def",
		"abc@def",
		"abc def",
		"Abc",
		"ABC",
	}
	
	for _, key := range invalidKeys {
		t.Run("invalid_key_"+key, func(t *testing.T) {
			metadata := map[string]string{key: "value"}
			err := ValidateMetadata(metadata)
			require.Error(t, err, "key '%s' should be invalid", key)
		})
	}
}
