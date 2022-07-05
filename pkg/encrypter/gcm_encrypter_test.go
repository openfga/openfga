package encrypter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptyDecrypt(t *testing.T) {
	encrypter, err := NewGCMEncrypter("foo")
	require.NoError(t, err)

	got, err := encrypter.Decrypt(nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestEmptyEncrypt(t *testing.T) {
	encrypter, err := NewGCMEncrypter("bar")
	require.NoError(t, err)

	got, err := encrypter.Encrypt(nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestEncryptDecrypt(t *testing.T) {
	encrypter, err := NewGCMEncrypter("baz")
	require.NoError(t, err)

	want := []byte("some random string")

	encoded, err := encrypter.Encrypt(want)
	require.NoError(t, err)

	got, err := encrypter.Decrypt(encoded)
	require.NoError(t, err)

	require.Equal(t, want, got)
}
