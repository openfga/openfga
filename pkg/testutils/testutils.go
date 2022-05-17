package testutils

import (
	"math/rand"
)

const (
	AllChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

func CreateRandomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = AllChars[rand.Intn(len(AllChars))]
	}
	return string(b)
}
