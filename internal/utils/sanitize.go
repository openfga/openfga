package utils

import (
	"strings"
	"unicode"
)

// ContainsForbiddenChars reports whether s contains any characters OpenFGA does not allow.
func ContainsForbiddenChars(s string) bool {
	return strings.ContainsFunc(s, unicode.IsControl)
}
