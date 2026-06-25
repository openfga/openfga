package encoder

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/encrypter"
)

// failingEncrypter is a test double that fails on demand.
type failingEncrypter struct {
	encryptErr error
	decryptErr error
}

func (f failingEncrypter) Encrypt(data []byte) ([]byte, error) {
	if f.encryptErr != nil {
		return nil, f.encryptErr
	}
	return data, nil
}

func (f failingEncrypter) Decrypt(data []byte) ([]byte, error) {
	if f.decryptErr != nil {
		return nil, f.decryptErr
	}
	return data, nil
}

func TestNewTokenEncoder(t *testing.T) {
	enc := NewTokenEncoder(encrypter.NewNoopEncrypter(), NewBase64Encoder())
	require.NotNil(t, enc)
	var _ Encoder = enc
}

func TestTokenEncoder_RoundTrip(t *testing.T) {
	t.Run("noop_encrypter", func(t *testing.T) {
		enc := NewTokenEncoder(encrypter.NewNoopEncrypter(), NewBase64Encoder())

		token, err := enc.Encode([]byte("continuation-token"))
		require.NoError(t, err)
		require.NotEmpty(t, token)

		decoded, err := enc.Decode(token)
		require.NoError(t, err)
		require.Equal(t, []byte("continuation-token"), decoded)
	})

	t.Run("gcm_encrypter", func(t *testing.T) {
		gcm, err := encrypter.NewGCMEncrypter("a-secret-key")
		require.NoError(t, err)

		enc := NewTokenEncoder(gcm, NewBase64Encoder())

		token, err := enc.Encode([]byte("hello world"))
		require.NoError(t, err)
		require.NotEmpty(t, token)

		decoded, err := enc.Decode(token)
		require.NoError(t, err)
		require.Equal(t, []byte("hello world"), decoded)
	})

	t.Run("empty_payload", func(t *testing.T) {
		gcm, err := encrypter.NewGCMEncrypter("k")
		require.NoError(t, err)
		enc := NewTokenEncoder(gcm, NewBase64Encoder())

		token, err := enc.Encode([]byte{})
		require.NoError(t, err)

		decoded, err := enc.Decode(token)
		require.NoError(t, err)
		require.Empty(t, decoded)
	})
}

func TestTokenEncoder_Decode_DecoderError(t *testing.T) {
	enc := NewTokenEncoder(encrypter.NewNoopEncrypter(), NewBase64Encoder())

	// "!!!" is not valid base64 and must surface the decoder error.
	_, err := enc.Decode("!!!not-base64!!!")
	require.Error(t, err)
}

func TestTokenEncoder_Decode_DecrypterError(t *testing.T) {
	wantErr := errors.New("decrypt failed")
	enc := NewTokenEncoder(failingEncrypter{decryptErr: wantErr}, NewBase64Encoder())

	// Valid base64 so the decoder succeeds and the decrypter error is returned.
	token, err := NewBase64Encoder().Encode([]byte("anything"))
	require.NoError(t, err)

	_, err = enc.Decode(token)
	require.ErrorIs(t, err, wantErr)
}

func TestTokenEncoder_Decode_DecrypterErrorWithGCM(t *testing.T) {
	gcm, err := encrypter.NewGCMEncrypter("k")
	require.NoError(t, err)
	enc := NewTokenEncoder(gcm, NewBase64Encoder())

	// Encode a payload that is shorter than the GCM nonce size so Decrypt
	// returns "ciphertext too short".
	token, err := NewBase64Encoder().Encode([]byte("short"))
	require.NoError(t, err)

	_, err = enc.Decode(token)
	require.Error(t, err)
}

func TestTokenEncoder_Encode_EncrypterError(t *testing.T) {
	wantErr := errors.New("encrypt failed")
	enc := NewTokenEncoder(failingEncrypter{encryptErr: wantErr}, NewBase64Encoder())

	_, err := enc.Encode([]byte("data"))
	require.ErrorIs(t, err, wantErr)
}
