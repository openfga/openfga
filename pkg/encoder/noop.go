package encoder

type Noop struct{}

func NewNoopEncoder() *Noop {
	return &Noop{}
}

func (n Noop) Decode(s string) ([]byte, error) {
	return []byte(s), nil
}

func (n Noop) Encode(data []byte) (string, error) {
	return string(data), nil
}
