package keys

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuilder_EncodeString_TLV(t *testing.T) {
	var b Builder
	b.EncodeString("abc")

	raw := b.Bytes()
	require.Equal(t, tagString, raw[0])
	require.Equal(t, byte(3), raw[1]) // varint length
	require.Equal(t, []byte("abc"), raw[2:5])
	require.Len(t, raw, 5)
}

func TestBuilder_EncodeString_Empty(t *testing.T) {
	var b Builder
	b.EncodeString("")

	raw := b.Bytes()
	require.Equal(t, tagString, raw[0])
	require.Equal(t, byte(0), raw[1]) // varint length 0
	require.Len(t, raw, 2)
}

func TestBuilder_EncodeString_LargeLength(t *testing.T) {
	// A string of 200 bytes should use a 2-byte varint for the length.
	s := string(make([]byte, 200))
	var b Builder
	b.EncodeString(s)

	raw := b.Bytes()
	require.Equal(t, tagString, raw[0])

	length, n := binary.Uvarint(raw[1:])
	require.Equal(t, uint64(200), length)
	require.Equal(t, 2, n) // 200 requires 2 varint bytes
	require.Len(t, raw, 1+n+200)
}

func TestBuilder_EncodeUint64_TLV(t *testing.T) {
	var b Builder
	b.EncodeUint64(1)

	raw := b.Bytes()
	require.Equal(t, tagUint64, raw[0])
	require.Len(t, raw, 9)

	val := binary.LittleEndian.Uint64(raw[1:9])
	require.Equal(t, uint64(1), val)
}

func TestBuilder_EncodeUint64_MaxValue(t *testing.T) {
	var b Builder
	b.EncodeUint64(^uint64(0))

	raw := b.Bytes()
	val := binary.LittleEndian.Uint64(raw[1:9])
	require.Equal(t, ^uint64(0), val)
}

func TestBuilder_EncodeBool_TLV(t *testing.T) {
	var b Builder
	b.EncodeBool(true)

	raw := b.Bytes()
	require.Equal(t, []byte{tagBool, 1}, raw)
}

func TestBuilder_EncodeBool_False(t *testing.T) {
	var b Builder
	b.EncodeBool(false)

	raw := b.Bytes()
	require.Equal(t, []byte{tagBool, 0}, raw)
}

func TestBuilder_EncodeByte_TLV(t *testing.T) {
	var b Builder
	b.EncodeByte(0xFF)

	raw := b.Bytes()
	require.Equal(t, []byte{tagByte, 0xFF}, raw)
}

func TestBuilder_EncodeBytes_TLV(t *testing.T) {
	var b Builder
	b.EncodeBytes([]byte{0xCA, 0xFE})

	raw := b.Bytes()
	require.Equal(t, tagBytes, raw[0])
	require.Equal(t, byte(2), raw[1])
	require.Equal(t, []byte{0xCA, 0xFE}, raw[2:4])
	require.Len(t, raw, 4)
}

func TestBuilder_EncodeBytes_Nil(t *testing.T) {
	var b Builder
	b.EncodeBytes(nil)

	raw := b.Bytes()
	require.Equal(t, tagBytes, raw[0])
	require.Equal(t, byte(0), raw[1]) // varint 0
	require.Len(t, raw, 2)
}

func TestBuilder_EncodeArray_TLV(t *testing.T) {
	var b Builder
	b.EncodeArray([]Serializable{String("x"), Uint64(7)})

	raw := b.Bytes()
	require.Equal(t, tagArray, raw[0])
	require.Equal(t, byte(2), raw[1]) // 2 elements

	// First element: tagString, len=1, 'x'
	require.Equal(t, tagString, raw[2])
	require.Equal(t, byte(1), raw[3])
	require.Equal(t, byte('x'), raw[4])

	// Second element: tagUint64, 8 bytes LE
	require.Equal(t, tagUint64, raw[5])
	val := binary.LittleEndian.Uint64(raw[6:14])
	require.Equal(t, uint64(7), val)
}

func TestBuilder_EncodeArray_Empty(t *testing.T) {
	var b Builder
	b.EncodeArray([]Serializable{})

	raw := b.Bytes()
	require.Equal(t, []byte{tagArray, 0}, raw)
}

func TestBuilder_EncodeArray_Nil(t *testing.T) {
	var b Builder
	b.EncodeArray(nil)

	raw := b.Bytes()
	require.Equal(t, []byte{tagArray, 0}, raw)
}

func TestBuilder_EncodeMap_TLV(t *testing.T) {
	var b Builder
	b.EncodeMap([]MapEntry{
		{Key: String("k1"), Value: Uint64(1)},
		{Key: String("k2"), Value: Bool(true)},
	})

	raw := b.Bytes()
	require.Equal(t, tagMap, raw[0])
	require.Equal(t, byte(2), raw[1]) // 2 entries

	// Flat layout: tagString "k1", tagUint64 1, tagString "k2", tagBool 1
	require.Equal(t, tagString, raw[2])
	require.Equal(t, byte(2), raw[3]) // len("k1")
	require.Equal(t, []byte("k1"), raw[4:6])
	require.Equal(t, tagUint64, raw[6])
	val := binary.LittleEndian.Uint64(raw[7:15])
	require.Equal(t, uint64(1), val)

	require.Equal(t, tagString, raw[15])
	require.Equal(t, byte(2), raw[16]) // len("k2")
	require.Equal(t, []byte("k2"), raw[17:19])
	require.Equal(t, tagBool, raw[19])
	require.Equal(t, byte(1), raw[20])
}

func TestBuilder_EncodeMap_Empty(t *testing.T) {
	var b Builder
	b.EncodeMap([]MapEntry{})

	raw := b.Bytes()
	require.Equal(t, []byte{tagMap, 0}, raw)
}

func TestBuilder_EncodeMap_Nil(t *testing.T) {
	var b Builder
	b.EncodeMap(nil)

	raw := b.Bytes()
	require.Equal(t, []byte{tagMap, 0}, raw)
}

// TestBuilder_MapArrayDisambiguation verifies that a Map and an Array
// containing the same scalar values produce different TLV outputs.
func TestBuilder_MapArrayDisambiguation(t *testing.T) {
	var mapBuf Builder
	mapBuf.EncodeMap([]MapEntry{{Key: String("k"), Value: Uint64(1)}})

	var arrBuf Builder
	arrBuf.EncodeArray([]Serializable{String("k"), Uint64(1)})

	require.NotEqual(t, mapBuf.Bytes(), arrBuf.Bytes())
	// Specifically, they should differ in the tag byte
	require.Equal(t, tagMap, mapBuf.Bytes()[0])
	require.Equal(t, tagArray, arrBuf.Bytes()[0])
}

func TestBuilder_EncodePair_TLV(t *testing.T) {
	var b Builder
	b.EncodePair(String("key"), Uint64(42))

	raw := b.Bytes()
	require.Equal(t, tagPair, raw[0])
	require.Equal(t, tagKey, raw[1])
	// "key": tagString, len=3, "key"
	require.Equal(t, tagString, raw[2])
	require.Equal(t, byte(3), raw[3])
	require.Equal(t, []byte("key"), raw[4:7])
	require.Equal(t, tagValue, raw[7])
	// uint64(42): tagUint64, 8 bytes LE
	require.Equal(t, tagUint64, raw[8])
	val := binary.LittleEndian.Uint64(raw[9:17])
	require.Equal(t, uint64(42), val)
}

func TestBuilder_MultipleEncodes_Accumulate(t *testing.T) {
	var b Builder
	b.EncodeString("a")
	b.EncodeUint64(1)

	raw := b.Bytes()
	// String "a": tag(1) + varint(1) + 'a'(1) = 3 bytes
	// Uint64 1: tag(1) + 8 bytes = 9 bytes
	require.Len(t, raw, 12)
	require.Equal(t, tagString, raw[0])
	require.Equal(t, tagUint64, raw[3])
}

func TestBuilder_Write(t *testing.T) {
	var b Builder
	n, err := b.Write([]byte{0xDE, 0xAD})
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []byte{0xDE, 0xAD}, b.Bytes())
}

func TestBuilder_Write_Nil(t *testing.T) {
	var b Builder
	n, err := b.Write(nil)
	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.Empty(t, b.Bytes())
}

func TestBuilder_Write_NoTagging(t *testing.T) {
	// Write should not prepend any type tag.
	var b Builder
	_, _ = b.Write([]byte("hello"))
	require.Equal(t, []byte("hello"), b.Bytes())
}

func TestBuilder_WriteByte(t *testing.T) {
	var b Builder
	err := b.WriteByte(0x42)
	require.NoError(t, err)
	require.Equal(t, []byte{0x42}, b.Bytes())
}

func TestBuilder_WriteByte_NoTagging(t *testing.T) {
	// WriteByte should not prepend a type tag like EncodeByte does.
	var b Builder
	_ = b.WriteByte(0xFF)
	require.Equal(t, []byte{0xFF}, b.Bytes())
	require.Len(t, b.Bytes(), 1)
}

func TestBuilder_WriteString(t *testing.T) {
	var b Builder
	n, err := b.WriteString("hello")
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("hello"), b.Bytes())
}

func TestBuilder_WriteString_Empty(t *testing.T) {
	var b Builder
	n, err := b.WriteString("")
	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.Empty(t, b.Bytes())
}

func TestBuilder_WriteString_NoTagging(t *testing.T) {
	// WriteString should not prepend a type tag or length prefix.
	var b Builder
	_, _ = b.WriteString("abc")
	require.Equal(t, []byte("abc"), b.Bytes())
	require.Len(t, b.Bytes(), 3)
}

func TestBuilder_Grow(t *testing.T) {
	// Grow should not change the length, only ensure capacity.
	var b Builder
	b.Grow(1024)
	require.Empty(t, b.Bytes())

	// After growing, writes should succeed without reallocation.
	_, _ = b.Write(make([]byte, 1024))
	require.Len(t, b.Bytes(), 1024)
}

func TestBuilder_Reset_ThenWrite(t *testing.T) {
	var b Builder
	b.EncodeString("first")
	b.Reset()
	_, _ = b.Write([]byte("second"))
	require.Equal(t, []byte("second"), b.Bytes())
}

func TestBuilder_Reset_ClearsBuffer(t *testing.T) {
	var b Builder
	b.EncodeString("hello")
	require.NotEmpty(t, b.Bytes())

	b.Reset()
	require.Empty(t, b.Bytes())
}

func TestKey_String_EmptyBuilder(t *testing.T) {
	var b Builder
	require.Empty(t, b.Key().String())
}

func TestKey_String_HexOutput(t *testing.T) {
	var b Builder
	b.EncodeByte(0x00)

	// raw: [tagByte=0x01, 0x00]
	// hex: "0100"
	require.Equal(t, "0100", b.Key().String())
}

func TestKey_String_AllHexDigits(t *testing.T) {
	// Write a byte that exercises all hex digits in both nibbles.
	var b Builder
	b.EncodeByte(0xEF)

	// raw: [0x01, 0xEF]
	// hex: "01EF"
	require.Equal(t, "01EF", b.Key().String())
}

// TestBuilder_FieldDisambiguation verifies that different field arrangements
// with the same total byte content produce different TLV outputs.
func TestBuilder_FieldDisambiguation(t *testing.T) {
	// "ab" as one string vs "a" + "b" as two strings
	var single Builder
	single.EncodeString("ab")

	var two Builder
	two.EncodeString("a")
	two.EncodeString("b")

	require.NotEqual(t, single.Bytes(), two.Bytes())
}

// TestBuilder_TypeDisambiguation verifies that different types with the
// same underlying data produce different TLV.
func TestBuilder_TypeDisambiguation(t *testing.T) {
	// byte(1) vs bool(true) — same payload byte 0x01 but different tags
	var byteBuf Builder
	byteBuf.EncodeByte(1)

	var boolBuf Builder
	boolBuf.EncodeBool(true)

	require.NotEqual(t, byteBuf.Bytes(), boolBuf.Bytes())
}

// TestKey_String_CorrectHexForAllTailLengths verifies hex encoding correctness
// for inputs exercising every branch of the String() method: the 8-byte
// unrolled loop, the 4-byte block, and the 1/2/3 byte tail switch.
func TestKey_String_CorrectHexForAllTailLengths(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{"1_byte", []byte{0xAB}, "AB"},
		{"2_bytes", []byte{0xAB, 0xCD}, "ABCD"},
		{"3_bytes", []byte{0xAB, 0xCD, 0xEF}, "ABCDEF"},
		{"4_bytes", []byte{0x01, 0x23, 0x45, 0x67}, "01234567"},
		{"5_bytes", []byte{0x01, 0x23, 0x45, 0x67, 0x89}, "0123456789"},
		{"6_bytes", []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB}, "0123456789AB"},
		{"7_bytes", []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD}, "0123456789ABCD"},
		{"8_bytes", []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}, "0123456789ABCDEF"},
		{"9_bytes", []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFF}, "0123456789ABCDEFFF"},
		{"16_bytes_exact_multiple", []byte{
			0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
			0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF,
		}, "00112233445566778899AABBCCDDEEFF"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use raw Write to bypass TLV framing so we test exact hex output.
			var b Builder
			_, _ = b.Write(tt.input)
			got := b.Key().String()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestKey_ImmutableSnapshot(t *testing.T) {
	var b Builder
	b.EncodeString("first")
	key1 := b.Key()

	b.EncodeString("second")
	key2 := b.Key()

	require.NotEqual(t, key1, key2, "Key must be a snapshot; later writes must not affect it")

	// Verify key1 still matches a freshly-built equivalent.
	var b2 Builder
	b2.EncodeString("first")
	require.Equal(t, b2.Key(), key1)
}

func TestKey_Bytes_ReturnsCorrectContent(t *testing.T) {
	var b Builder
	b.EncodeByte(0xAB)
	k := b.Key()

	bytes := k.Bytes()
	require.Equal(t, []byte{tagByte, 0xAB}, bytes)
}

// -----------------------------------------------------------------------------
// Benchmarks: Key.String
//
// String() hex-encodes the buffer. Its hot path is a 4-byte unrolled loop with
// a switch on len%4 for the tail. The benchmarks below cover each tail arm
// (0/1/2/3) explicitly and span sizes from "empty" through "large" to
// characterize per-byte overhead vs. fixed costs.
// -----------------------------------------------------------------------------

// benchString builds a Builder containing a single EncodeBytes payload of
// `size` bytes, then benchmarks Key.String() on the resulting key. The
// Builder is populated outside the loop so the measurement reflects only
// hex-encoding cost, not encoding overhead.
func benchString(b *testing.B, size int) {
	b.Helper()

	var kb Builder
	kb.EncodeBytes(make([]byte, size))
	k := kb.Key()

	b.SetBytes(int64(size))
	b.ReportAllocs()
	for b.Loop() {
		_ = k.String()
	}
}

func BenchmarkKeyString_Empty(b *testing.B)   { benchString(b, 0) }
func BenchmarkKeyString_1Byte(b *testing.B)   { benchString(b, 1) }
func BenchmarkKeyString_2Bytes(b *testing.B)  { benchString(b, 2) }
func BenchmarkKeyString_3Bytes(b *testing.B)  { benchString(b, 3) }
func BenchmarkKeyString_4Bytes(b *testing.B)  { benchString(b, 4) }
func BenchmarkKeyString_7Bytes(b *testing.B)  { benchString(b, 7) }
func BenchmarkKeyString_64Bytes(b *testing.B) { benchString(b, 64) }

// 67 = 64 (one unrolled iteration × 16) + 3-byte tail, exercising both the
// hot loop and the 3-byte tail arm in a realistic-sized key.
func BenchmarkKeyString_67Bytes(b *testing.B) { benchString(b, 67) }
func BenchmarkKeyString_256Bytes(b *testing.B) {
	benchString(b, 256)
}
func BenchmarkKeyString_1KB(b *testing.B) {
	benchString(b, 1024)
}
func BenchmarkKeyString_4KB(b *testing.B) {
	benchString(b, 4096)
}

// BenchmarkKeyString_RealisticCacheKey measures String() on a key whose
// buffer matches what a typical iterator-cache key produces. The Builder is
// populated outside the loop so only the hex-encoding cost is measured.
func BenchmarkKeyString_RealisticCacheKey(b *testing.B) {
	const (
		storeID    = "01JABCDEFGHJKMNPQRSTVWXYZ0"
		objectType = "document"
		relation   = "viewer"
		user       = "user:alice"
	)
	suffix := make([]byte, 8) // mimics an 8-byte xxhash suffix
	for i := range suffix {
		suffix[i] = byte(i)
	}

	var kb Builder
	kb.EncodeString("IC")
	kb.EncodeString("READ")
	kb.EncodeString(storeID)
	kb.EncodeString(objectType)
	kb.EncodeString(relation)
	kb.EncodeString(user)
	kb.EncodeBytes(suffix)
	k := kb.Key()

	b.ReportAllocs()
	for b.Loop() {
		_ = k.String()
	}
}

// -----------------------------------------------------------------------------
// Benchmarks: Builder encode methods
//
// These measure the cost of populating a Builder via its Encode* methods.
// They deliberately exclude String() and Bytes() so the numbers reflect
// encoding overhead only (allocation, varint emission, payload copy, tag
// bookkeeping). Builder.Reset() is used to reuse the underlying buffer
// across iterations and isolate per-call cost from growth amortization.
// -----------------------------------------------------------------------------

func benchEncodeString(b *testing.B, size int) {
	b.Helper()

	s := string(make([]byte, size))
	var kb Builder

	b.SetBytes(int64(size))
	b.ReportAllocs()
	for b.Loop() {
		kb.Reset()
		kb.EncodeString(s)
	}
}

func BenchmarkBuilderEncodeString_Empty(b *testing.B)   { benchEncodeString(b, 0) }
func BenchmarkBuilderEncodeString_8Bytes(b *testing.B)  { benchEncodeString(b, 8) }
func BenchmarkBuilderEncodeString_64Bytes(b *testing.B) { benchEncodeString(b, 64) }
func BenchmarkBuilderEncodeString_1KB(b *testing.B)     { benchEncodeString(b, 1024) }

func benchEncodeBytes(b *testing.B, size int) {
	b.Helper()

	p := make([]byte, size)
	var kb Builder

	b.SetBytes(int64(size))
	b.ReportAllocs()
	for b.Loop() {
		kb.Reset()
		kb.EncodeBytes(p)
	}
}

func BenchmarkBuilderEncodeBytes_Empty(b *testing.B)   { benchEncodeBytes(b, 0) }
func BenchmarkBuilderEncodeBytes_8Bytes(b *testing.B)  { benchEncodeBytes(b, 8) }
func BenchmarkBuilderEncodeBytes_64Bytes(b *testing.B) { benchEncodeBytes(b, 64) }
func BenchmarkBuilderEncodeBytes_1KB(b *testing.B)     { benchEncodeBytes(b, 1024) }

func BenchmarkBuilderEncodeUint64(b *testing.B) {
	var kb Builder
	b.ReportAllocs()
	for b.Loop() {
		kb.Reset()
		kb.EncodeUint64(^uint64(0))
	}
}

func BenchmarkBuilderEncodeBool(b *testing.B) {
	var kb Builder
	b.ReportAllocs()
	for b.Loop() {
		kb.Reset()
		kb.EncodeBool(true)
	}
}

func BenchmarkBuilderEncodeByte(b *testing.B) {
	var kb Builder
	b.ReportAllocs()
	for b.Loop() {
		kb.Reset()
		kb.EncodeByte(0xFF)
	}
}

func BenchmarkBuilderEncodePair(b *testing.B) {
	var kb Builder
	k := String("key")
	v := Uint64(42)
	b.ReportAllocs()
	for b.Loop() {
		kb.Reset()
		kb.EncodePair(k, v)
	}
}

func BenchmarkBuilderEncodeArray(b *testing.B) {
	var kb Builder
	elems := []Serializable{String("x"), Uint64(7), Bool(true)}
	b.ReportAllocs()
	for b.Loop() {
		kb.Reset()
		kb.EncodeArray(elems)
	}
}

func BenchmarkBuilderEncodeMap(b *testing.B) {
	var kb Builder
	elems := []MapEntry{
		{Key: String("name"), Value: String("alice")},
		{Key: String("age"), Value: Uint64(30)},
		{Key: String("active"), Value: Bool(true)},
	}
	b.ReportAllocs()
	for b.Loop() {
		kb.Reset()
		kb.EncodeMap(elems)
	}
}

// BenchmarkBuilderEncode_RealisticCacheKey measures the cost of building a
// realistic iterator-cache key via the Encode* methods, with no String() or
// Bytes() call. A fresh Builder is constructed each iteration so the result
// includes the cold-buffer growth cost the hot path actually pays.
func BenchmarkBuilderEncode_RealisticCacheKey(b *testing.B) {
	const (
		storeID    = "01JABCDEFGHJKMNPQRSTVWXYZ0"
		objectType = "document"
		relation   = "viewer"
		user       = "user:alice"
	)
	suffix := make([]byte, 8) // mimics an 8-byte xxhash suffix
	for i := range suffix {
		suffix[i] = byte(i)
	}

	b.ReportAllocs()
	for b.Loop() {
		var kb Builder
		kb.EncodeString("IC")
		kb.EncodeString("READ")
		kb.EncodeString(storeID)
		kb.EncodeString(objectType)
		kb.EncodeString(relation)
		kb.EncodeString(user)
		kb.EncodeBytes(suffix)
	}
}
