package utils

import (
	"unicode"
)

// ContainsControlChars reports whether s contains any Unicode control characters.
func ContainsControlChars(s string) bool {
	for _, c := range s {
		if unicode.IsControl(c) {
			return true
		}
	}
	return false
}
