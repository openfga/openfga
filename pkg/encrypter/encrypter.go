package encrypter

type Encrypter interface {
	Decrypt([]byte) ([]byte, error)
	Encrypt([]byte) ([]byte, error)
}

type NoopEncrypter struct{}

var _ Encrypter = (*NoopEncrypter)(nil)

func NewNoopEncrypter() *NoopEncrypter {
	return &NoopEncrypter{}
}

func (e *NoopEncrypter) Decrypt(data []byte) ([]byte, error) {
	return data, nil
}

func (e *NoopEncrypter) Encrypt(data []byte) ([]byte, error) {
	return data, nil
}
