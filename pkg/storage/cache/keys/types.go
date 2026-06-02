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

// MapEntry is one entry inside a Map. Map entries use a flat key/value
// layout inside tagMap framing — no per-entry tagPair/tagKey/tagValue
// markers — so MapEntry is a plain value struct rather than a
// Serializable.
type MapEntry struct {
	Key   Serializable
	Value Serializable
}

// Map is an ordered collection of key-value entries that serializes with
// the tagMap framing (tagMap + entry count + alternating key/value TLVs).
// Use Map (rather than Array of Pairs) when the source data is a dictionary
// or struct-like object whose entries must be distinguishable from a
// positional sequence.
type Map []MapEntry

func (m Map) WriteTo(kb *Builder) {
	kb.EncodeMap(m)
}

// Pair encodes a standalone key/value with tagPair/tagKey/tagValue
// framing. It is used when a pair must appear outside a Map (for example,
// as an element of an Array or as a top-level value) and the pair
// boundary must remain unambiguous. Inside a Map, use MapEntry instead.
type Pair struct {
	Key   Serializable
	Value Serializable
}

func (p Pair) WriteTo(kb *Builder) {
	kb.EncodePair(p.Key, p.Value)
}
