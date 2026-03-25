package utils

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSanitize(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "empty_string", input: "", expected: ""},
		{name: "plain_ascii", input: "hello world", expected: "hello world"},
		{name: "null_byte", input: "ab\x00cd", expected: "ab?cd"},
		{name: "multiple_control_chars", input: "hello\u0008 \u0008 \u0008 world\u0008", expected: "hello? ? ? world?"},
		{name: "only_control_chars", input: "\x00\x01\x02\x03", expected: "????"},
		{name: "cjk_preserved", input: "日本語テスト", expected: "日本語テスト"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Sanitize(tc.input)
			require.Equal(t, tc.expected, got)
		})
	}
}

var y string

func BenchmarkSanitize(b *testing.B) {
	a := "hello\u0008 \u0008 \u0008 world\u0008"

	var x string
	for b.Loop() {
		x = Sanitize(a)
	}
	y = x
}
