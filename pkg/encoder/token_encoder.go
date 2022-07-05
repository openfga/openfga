package encoder

import (
	"github.com/openfga/openfga/pkg/encrypter"
)

type TokenEncoder struct {
	encrypter encrypter.Encrypter
	encoder   Encoder
}

var _ Encoder = (*TokenEncoder)(nil)

// NewTokenEncoder constructs an Encoder that includes an encrypter and encoder.
func NewTokenEncoder(encrypter encrypter.Encrypter, encoder Encoder) *TokenEncoder {
	return &TokenEncoder{
		encrypter: encrypter,
		encoder:   encoder,
	}
}

// Decode will first decode the given string with its decoder, then decrypt the result with its decrypter.
func (e *TokenEncoder) Decode(s string) ([]byte, error) {
	decoded, err := e.encoder.Decode(s)
	if err != nil {
		return nil, err
	}

	return e.encrypter.Decrypt(decoded)
}

// Encode will first encrypt the given data with its encrypter, then encode the result with its encoder.
func (e *TokenEncoder) Encode(data []byte) (string, error) {
	encrypted, err := e.encrypter.Encrypt(data)
	if err != nil {
		return "", err
	}

	return e.encoder.Encode(encrypted)
}
