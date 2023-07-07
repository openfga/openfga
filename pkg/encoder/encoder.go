// Package encoder contains data encoding and continuation token encoding implementations.
package encoder

type Encoder interface {
	Decode(string) ([]byte, error)
	Encode([]byte) (string, error)
}

type NoopEncoder struct{}

var _ Encoder = (*NoopEncoder)(nil)

func (e NoopEncoder) Decode(s string) ([]byte, error) {
	return []byte(s), nil
}

func (e NoopEncoder) Encode(data []byte) (string, error) {
	return string(data), nil
}
