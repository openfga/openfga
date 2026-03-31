package utils

import (
	"strings"
	"unicode"
)

// ContainsControlChars reports whether s contains any Unicode control characters.
func ContainsControlChars(s string) bool {
	return strings.ContainsFunc(s, unicode.IsControl)
}
