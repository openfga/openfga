package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContainsForbiddenChars(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{name: "empty_string", input: "", expected: false},
		{name: "plain_ascii", input: "hello world", expected: false},
		{name: "null_byte", input: "ab\x00cd", expected: true},
		{name: "multiple_control_chars", input: "hello\u0008world", expected: true},
		{name: "only_control_chars", input: "\x00\x01\x02\x03", expected: true},
		{name: "cjk_clean", input: "日本語テスト", expected: false},
		{name: "c1_control_char", input: "a\u0085b", expected: true},
		{name: "clean_tuple", input: "document:12345#viewer@user:alice", expected: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ContainsForbiddenChars(tc.input)
			require.Equal(t, tc.expected, got)
		})
	}
}
