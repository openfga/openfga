package encoder

import "encoding/base64"

// Base64Encoder implements the Encoder interface by delegating to the encoding/base64
// base64 encoding strategy.
type Base64Encoder struct{}

var _ Encoder = (*Base64Encoder)(nil)

// NewBase64Encoder constructs an Encoder that implements a base64 encoding as specified
// by the encoding/base64 package.
func NewBase64Encoder() *Base64Encoder {
	return &Base64Encoder{}
}

func (e *Base64Encoder) Decode(s string) ([]byte, error) {
	return base64.URLEncoding.DecodeString(s)
}

func (e *Base64Encoder) Encode(data []byte) (string, error) {
	return base64.URLEncoding.EncodeToString(data), nil
}
