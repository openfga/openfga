package encoder

type Encoder interface {
	Decode(string) ([]byte, error)
	Encode([]byte) (string, error)
}
