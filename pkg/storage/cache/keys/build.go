package keys

import (
	"encoding/binary"
	"slices"
	"unsafe"
)

// hexTable maps each byte to a uint16 whose low byte is the high-nibble
// ASCII hex character and whose high byte is the low-nibble character.
// This packing lets Key.String pack four entries into a uint64 and emit
// them with a single little-endian 8-byte store.
var hexTable [256]uint16

func init() {
	const alphabet = "0123456789ABCDEF"
	for i := range 256 {
		hexTable[i] = uint16(alphabet[i&0x0F])<<8 | uint16(alphabet[i>>4])
	}
}

// Type tags for the encoding. Each Write method prepends a tag so that
// identical payloads of different types produce distinct byte sequences,
// eliminating ambiguity without requiring delimiters between fields.
//
// Encoding rule — the framing depends on the tag's category, not a uniform
// TLV layout:
//
//   - Zero-size markers (tagNull, tagUnset, tagPair, tagKey, tagValue): tag
//     only. The tag itself carries all the information; no length or
//     payload follows.
//   - Fixed-size payloads (tagByte: 1B, tagBool: 1B, tagUint64: 8B): tag +
//     payload. The length is implied by the tag, so it is omitted.
//   - Variable-size payloads (tagString, tagBytes, tagArray, tagMap): tag +
//     uvarint length + payload. The length disambiguates the payload boundary
//     so adjacent fields cannot smear into one another. tagMap and tagArray
//     share the same framing but use distinct tags so that a dictionary and a
//     positional sequence of identical elements never collide.
//
// The Builder is a one-way digest input — nothing parses these bytes back —
// so the encoding optimizes for compactness rather than self-description.
// Add new tags in the same category as their semantic peers; do not
// introduce a hybrid (e.g., a marker with a trailing length byte) without
// updating this comment.
const (
	tagNull byte = iota
	tagByte
	tagBool
	tagUint64
	tagString
	tagBytes
	tagArray
	tagMap
	tagPair
	tagKey
	tagValue
	tagUnset
)

// Serializable is implemented by types that can encode themselves into
// the Builder's TLV wire format.
type Serializable interface {
	WriteTo(*Builder)
}

// Builder accumulates a TLV-encoded byte sequence from which a cache key
// can be derived. Each field is self-describing (tagged with type and
// length where applicable), so keys are unambiguous without delimiters.
type Builder struct {
	data []byte
}

// Grow increases the capacity of the internal buffer by at least n bytes,
// guaranteeing space for n more bytes of writes without reallocation.
func (kb *Builder) Grow(n int) {
	kb.data = slices.Grow(kb.data, n)
}

// Reset discards all accumulated content, returning the buffer to empty
// while retaining its underlying storage for reuse.
func (kb *Builder) Reset() {
	kb.data = kb.data[:0]
}

// Write appends raw bytes to the buffer without type-tagging or
// length-prefixing. Use this for pre-encoded data or fixed prefixes
// that do not require TLV framing.
func (kb *Builder) Write(b []byte) (int, error) {
	kb.data = append(kb.data, b...)
	return len(b), nil
}

// WriteByte appends a single raw byte to the buffer without type-tagging.
func (kb *Builder) WriteByte(b byte) error {
	kb.data = append(kb.data, b)
	return nil
}

// WriteString appends the contents of s to the buffer without type-tagging
// or length-prefixing. It avoids the allocation that Write([]byte(s)) would
// incur for string-to-byte conversion.
func (kb *Builder) WriteString(s string) (int, error) {
	kb.data = append(kb.data, s...)
	return len(s), nil
}

// EncodeBytes writes b as a tagged, length-prefixed byte slice.
func (kb *Builder) EncodeBytes(b []byte) {
	kb.data = slices.Grow(kb.data, 1+binary.MaxVarintLen64+len(b))
	kb.data = append(kb.data, tagBytes)
	kb.data = binary.AppendUvarint(kb.data, uint64(len(b)))
	kb.data = append(kb.data, b...)
}

// EncodeByte writes b as a tagged single-byte value.
func (kb *Builder) EncodeByte(b byte) {
	kb.data = append(kb.data, tagByte, b)
}

// EncodeNull emits a bare tagNull marker. Distinct from EncodeBool(false)
// and EncodeByte(0) so that JSON-style null, boolean false, and the byte
// 0x00 never collide in the encoded key.
func (kb *Builder) EncodeNull() {
	kb.data = append(kb.data, tagNull)
}

// EncodeUnset emits a bare tagUnset marker. Distinct from EncodeNull so
// that "value absent / kind unset" (e.g., a structpb.Value with no Kind
// set) and an explicit JSON-style null cannot collide in the encoded key.
func (kb *Builder) EncodeUnset() {
	kb.data = append(kb.data, tagUnset)
}

// EncodeString writes s as a tagged, length-prefixed UTF-8 string.
func (kb *Builder) EncodeString(s string) {
	kb.data = slices.Grow(kb.data, 1+binary.MaxVarintLen64+len(s))
	kb.data = append(kb.data, tagString)
	kb.data = binary.AppendUvarint(kb.data, uint64(len(s)))
	kb.data = append(kb.data, s...)
}

// EncodeBool writes b as a tagged boolean (0x00 for false, 0x01 for true).
func (kb *Builder) EncodeBool(b bool) {
	var value byte
	if b {
		value = 1
	}
	kb.data = append(kb.data, tagBool, value)
}

// EncodeUint64 writes i as a tagged 8-byte little-endian integer.
func (kb *Builder) EncodeUint64(i uint64) {
	kb.data = binary.LittleEndian.AppendUint64(append(kb.data, tagUint64), i)
}

// EncodeArray writes a as a tagged sequence (tag + element count + each
// element's own TLV representation).
func (kb *Builder) EncodeArray(a []Serializable) {
	kb.EncodeArrayHeader(len(a))
	for _, e := range a {
		e.WriteTo(kb)
	}
}

// EncodeArrayHeader writes the tagArray framing (tag + element count) only.
// The caller must then write exactly n element encodings into the Builder.
// Mismatched counts produce an unparseable key — this is a discipline
// contract, not a checked invariant. Prefer EncodeArray when a
// []Serializable is already in hand; use the header when streaming
// elements directly to avoid materializing the slice.
func (kb *Builder) EncodeArrayHeader(n int) {
	kb.data = append(kb.data, tagArray)
	kb.data = binary.AppendUvarint(kb.data, uint64(n))
}

// EncodeMap writes entries as a tagged map (tag + entry count + flat
// key/value TLVs). The tagMap framing distinguishes maps from arrays,
// preventing collision between a dictionary and a positional sequence that
// happen to contain the same values. Use Pair only as a standalone
// Serializable; map entries are unframed inside tagMap.
func (kb *Builder) EncodeMap(entries []MapEntry) {
	kb.EncodeMapHeader(len(entries))
	for _, e := range entries {
		e.Key.WriteTo(kb)
		e.Value.WriteTo(kb)
	}
}

// EncodeMapHeader writes the tagMap framing (tag + entry count) only. The
// caller must then write exactly 2*n TLVs into the Builder, alternating
// key and value (flat layout — no per-entry tagPair framing inside tagMap).
// Mismatched counts or interleaving produce an unparseable key — this is
// a discipline contract, not a checked invariant.
func (kb *Builder) EncodeMapHeader(n int) {
	kb.data = append(kb.data, tagMap)
	kb.data = binary.AppendUvarint(kb.data, uint64(n))
}

// EncodePair writes a key-value pair with structural markers separating
// the key and value portions.
func (kb *Builder) EncodePair(key Serializable, value Serializable) {
	kb.data = append(kb.data, tagPair, tagKey)
	key.WriteTo(kb)
	kb.data = append(kb.data, tagValue)
	value.WriteTo(kb)
}

// Serialize delegates encoding to the value's own WriteTo implementation.
func (kb *Builder) Serialize(value Serializable) {
	value.WriteTo(kb)
}

// Bytes returns the accumulated buffer contents. The returned slice is
// valid only until the next mutating call on the Builder.
func (kb *Builder) Bytes() []byte {
	return kb.data
}

// Key snapshots the current buffer contents into an immutable Key value.
func (kb *Builder) Key() Key {
	return Key{string(kb.data)}
}

// Key is an opaque, immutable cache key derived from TLV-encoded fields.
// It is comparable and safe for use as a map key or in concurrent data
// structures.
type Key struct {
	data string
}

// Bytes returns the key contents. The returned slice must not be mutated.
// If mutation is necessary, copy the bytes into another slice.
func (k Key) Bytes() []byte {
	return unsafe.Slice(unsafe.StringData(k.data), len(k.data))
}

// String returns the key contents as a hex-encoded string suitable for
// display. The hex encoding ensures the output is printable and
// safe for any backend that restricts key character sets.
func (k Key) String() string {
	if len(k.data) == 0 {
		return ""
	}

	src := k.Bytes()
	out := make([]byte, len(src)*2)

	dst := out

	for len(src) >= 8 {
		p := (*[16]byte)(dst)

		w := uint64(hexTable[src[0]]) |
			uint64(hexTable[src[1]])<<16 |
			uint64(hexTable[src[2]])<<32 |
			uint64(hexTable[src[3]])<<48

		binary.LittleEndian.PutUint64(p[:8], w)

		w = uint64(hexTable[src[4]]) |
			uint64(hexTable[src[5]])<<16 |
			uint64(hexTable[src[6]])<<32 |
			uint64(hexTable[src[7]])<<48

		binary.LittleEndian.PutUint64(p[8:], w)

		src = src[8:]
		dst = dst[16:]
	}

	if len(src) >= 4 {
		p := (*[8]byte)(dst)

		w := uint64(hexTable[src[0]]) |
			uint64(hexTable[src[1]])<<16 |
			uint64(hexTable[src[2]])<<32 |
			uint64(hexTable[src[3]])<<48

		binary.LittleEndian.PutUint64(p[:8], w)

		src = src[4:]
		dst = dst[8:]
	}

	switch len(src) {
	case 3:
		p := (*[6]byte)(dst)

		w := uint32(hexTable[src[0]]) |
			uint32(hexTable[src[1]])<<16

		binary.LittleEndian.PutUint32(p[:4], w)
		binary.LittleEndian.PutUint16(p[4:], hexTable[src[2]])
	case 2:
		p := (*[4]byte)(dst)

		w := uint32(hexTable[src[0]]) |
			uint32(hexTable[src[1]])<<16

		binary.LittleEndian.PutUint32(p[:4], w)
	case 1:
		p := (*[2]byte)(dst)

		binary.LittleEndian.PutUint16(p[:2], hexTable[src[0]])
	}

	return unsafe.String(unsafe.SliceData(out), len(out)) // safe: out is never mutated after this point
}
