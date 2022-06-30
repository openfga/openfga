package encoder

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
)

type GCMEncrypter struct {
	cipherMode cipher.AEAD
	encoder    Encoder
}

var _ Encrypter = (*GCMEncrypter)(nil)

func NewGCMEncrypter(key string, encoder Encoder) (*GCMEncrypter, error) {
	c, err := aes.NewCipher(create32ByteKey(key))
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	return &GCMEncrypter{
		cipherMode: gcm,
		encoder:    encoder,
	}, nil
}

func (e *GCMEncrypter) Decrypt(s string) ([]byte, error) {
	if s == "" {
		return []byte{}, nil
	}

	data, err := e.encoder.Decode(s)
	if err != nil {
		return nil, err
	}

	nonceSize := e.cipherMode.NonceSize()
	if len(data) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	return e.cipherMode.Open(nil, nonce, ciphertext, nil)
}

func (e *GCMEncrypter) Encrypt(data []byte) (string, error) {
	if len(data) == 0 {
		return "", nil
	}

	nonce := make([]byte, e.cipherMode.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	encrypted := e.cipherMode.Seal(nonce, nonce, data, nil)

	return e.encoder.Encode(encrypted)

}

// create32ByteKey creates a 32 byte key by taking the hex representation of the sha256 hash of a string.
func create32ByteKey(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}
