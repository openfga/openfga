package encoder

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptyDecrypt(t *testing.T) {
	encrypter, err := NewGCMEncrypter("key", NoopEncoder{})
	require.NoError(t, err)

	got, err := encrypter.Decrypt("")
	require.NoError(t, err)
	require.Equal(t, []byte{}, got)
}

func TestEmptyEncrypt(t *testing.T) {
	encrypter, err := NewGCMEncrypter("key", NoopEncoder{})
	require.NoError(t, err)

	got, err := encrypter.Encrypt([]byte{})
	require.NoError(t, err)
	require.Equal(t, "", got)
}

func TestEncryptDecrypt(t *testing.T) {
	encrypter, err := NewGCMEncrypter("key", NoopEncoder{})
	require.NoError(t, err)

	want := []byte("a random string")

	encoded, err := encrypter.Encrypt(want)
	require.NoError(t, err)

	got, err := encrypter.Decrypt(encoded)
	require.NoError(t, err)

	require.Equal(t, want, got)
}
