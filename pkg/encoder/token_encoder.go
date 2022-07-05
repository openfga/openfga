package encoder

import (
	"github.com/openfga/openfga/pkg/encrypter"
)

type TokenEncoder struct {
	encrypter encrypter.Encrypter
	encoder   Encoder
}

var _ Encoder = (*TokenEncoder)(nil)

// NewTokenEncoder constructs an Encoder that implements a base64 encoding as specified
// by the encoding/base64 package.
func NewTokenEncoder(encrypter encrypter.Encrypter, encoder Encoder) *TokenEncoder {
	return &TokenEncoder{
		encrypter: encrypter,
		encoder:   encoder,
	}
}

// Decode base64 URL decodes the provided string.
func (e *TokenEncoder) Decode(s string) ([]byte, error) {
	decoded, err := e.encoder.Decode(s)
	if err != nil {
		return nil, err
	}

	return e.encrypter.Decrypt(decoded)
}

// Encode base64 URL encodes the provided byte slice and returns the encoded value as a string.
func (e *TokenEncoder) Encode(data []byte) (string, error) {
	encrypted, err := e.encrypter.Encrypt(data)
	if err != nil {
		return "", err
	}

	return e.encoder.Encode(encrypted)
}
