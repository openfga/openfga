package utils

import (
	"unicode"
	"unicode/utf8"
)

func Sanitize(s string) string {
	for _, c := range s {
		if unicode.IsControl(c) {
			return replaceDangerousCharacters(s)
		}
	}
	// Most of the time we can just return the original string and avoid any allocs
	return s
}

func replaceDangerousCharacters(s string) string {
	cleaned := make([]byte, len(s))
	i := 0

	for _, char := range s {
		if unicode.IsControl(char) {
			cleaned[i] = '?'
			i++
			continue
		}
		bytesWritten := utf8.EncodeRune(cleaned[i:], char)
		i += bytesWritten
	}

	return string(cleaned[:i])
}
