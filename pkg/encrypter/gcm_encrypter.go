package encrypter

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"io"

	"github.com/go-errors/errors"
)

type GCMEncrypter struct {
	cipherMode cipher.AEAD
}

var _ Encrypter = (*GCMEncrypter)(nil)

func NewGCMEncrypter(key string) (*GCMEncrypter, error) {
	c, err := aes.NewCipher(create32ByteKey(key))
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	return &GCMEncrypter{cipherMode: gcm}, nil
}

// Decrypt decrypts an GCM encrypted byte array
func (e *GCMEncrypter) Decrypt(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	nonceSize := e.cipherMode.NonceSize()
	if len(data) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	return e.cipherMode.Open(nil, nonce, ciphertext, nil)
}

// Encrypt encrypts the given byte array using cipher.NewGCM block cipher
func (e *GCMEncrypter) Encrypt(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	nonce := make([]byte, e.cipherMode.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	return e.cipherMode.Seal(nonce, nonce, data, nil), nil
}

// create32ByteKey creates a 32 byte key by taking the hex representation of the sha256 hash of a string.
func create32ByteKey(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}
