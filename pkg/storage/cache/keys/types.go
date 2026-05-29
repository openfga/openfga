package keys

type Bytes []byte

func (b Bytes) WriteTo(kb *Builder) {
	kb.EncodeBytes(b)
}

type Byte byte

func (b Byte) WriteTo(kb *Builder) {
	kb.EncodeByte(byte(b))
}

type Bool bool

func (b Bool) WriteTo(kb *Builder) {
	kb.EncodeBool(bool(b))
}

// Null is a singleton sentinel that serializes as a bare tagNull marker.
// Use it (instead of Byte(0) or Bool(false)) when the source value is
// semantically "absence" rather than the boolean false or the byte 0x00.
type Null struct{}

func (Null) WriteTo(kb *Builder) {
	kb.EncodeNull()
}

// Unset is a singleton sentinel that serializes as a bare tagUnset marker.
// Use it when the source value is structurally absent (e.g., a oneof with
// no case selected) rather than an explicit JSON-style null, which is
// represented by Null.
type Unset struct{}

func (Unset) WriteTo(kb *Builder) {
	kb.EncodeUnset()
}

type Uint64 uint64

func (i Uint64) WriteTo(kb *Builder) {
	kb.EncodeUint64(uint64(i))
}

type String string

func (s String) WriteTo(kb *Builder) {
	kb.EncodeString(string(s))
}

type Array []Serializable

func (a Array) WriteTo(kb *Builder) {
	kb.EncodeArray(a)
}

type Map []Serializable

func (m Map) WriteTo(kb *Builder) {
	kb.EncodeMap(m)
}

type Pair struct {
	Key   Serializable
	Value Serializable
}

func (p Pair) WriteTo(kb *Builder) {
	kb.EncodePair(p.Key, p.Value)
}
