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
	t.Run("encrypt-decrypt_returns_original", func(t *testing.T) {
		encrypter, err := NewGCMEncrypter("baz")
		require.NoError(t, err)

		want := []byte("some random string")

		encoded, err := encrypter.Encrypt(want)
		require.NoError(t, err)

		got, err := encrypter.Decrypt(encoded)
		require.NoError(t, err)

		require.Equal(t, want, got)
	})

	t.Run("two_different_encrypters_do_not_work_together", func(t *testing.T) {
		e1, err := NewGCMEncrypter("somekey")
		require.NoError(t, err)

		e2, err := NewGCMEncrypter("anotherkey")
		require.NoError(t, err)

		want := []byte("some random string")

		encoded, err := e1.Encrypt(want)
		require.NoError(t, err)

		_, err = e2.Decrypt(encoded)
		require.Error(t, err)
	})
}
