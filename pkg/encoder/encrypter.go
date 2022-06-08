package encoder

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"io"

	"github.com/go-errors/errors"
)

type TokenEncrypter struct {
	cipherMode cipher.AEAD
}

func NewTokenEncrypter(key string) (*TokenEncrypter, error) {
	c, err := aes.NewCipher(create32ByteKey(key))
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}
	return &TokenEncrypter{cipherMode: gcm}, nil
}

func (e TokenEncrypter) Decode(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}

	decoded, err := decode(s)
	if err != nil {
		return nil, err
	}
	return decrypt(e.cipherMode, decoded)
}

func (e TokenEncrypter) Encode(data []byte) (string, error) {
	if len(data) == 0 {
		return "", nil
	}

	encrypted, err := encrypt(e.cipherMode, data)
	if err != nil {
		return "", err
	}
	return encode(encrypted), nil
}

// create32ByteKey creates a 32 byte key by taking the hex representation of the sha256 hash of a string.
func create32ByteKey(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

func decode(s string) ([]byte, error) {
	decoded, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

func decrypt(cipherMode cipher.AEAD, data []byte) ([]byte, error) {
	nonceSize := cipherMode.NonceSize()
	if len(data) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	return cipherMode.Open(nil, nonce, ciphertext, nil)
}

func encode(data []byte) string {
	return base64.URLEncoding.EncodeToString(data)
}

func encrypt(cipherMode cipher.AEAD, data []byte) ([]byte, error) {
	nonce := make([]byte, cipherMode.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	return cipherMode.Seal(nonce, nonce, data, nil), nil
}
