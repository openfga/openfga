package encoder

type Encrypter interface {
	Decrypt(string) ([]byte, error)
	Encrypt([]byte) (string, error)
}

type NoopEncrypter struct {
	encoder Encoder
}

var _ Encrypter = (*NoopEncrypter)(nil)

func NewNoopEncrypter(encoder Encoder) *NoopEncrypter {
	return &NoopEncrypter{encoder: encoder}
}

func NewNoopEncrypterEncoder() *NoopEncrypter {
	return &NoopEncrypter{encoder: NoopEncoder{}}
}

func (e *NoopEncrypter) Decrypt(s string) ([]byte, error) {
	return e.encoder.Decode(s)
}

func (e *NoopEncrypter) Encrypt(data []byte) (string, error) {
	return e.encoder.Encode(data)
}
