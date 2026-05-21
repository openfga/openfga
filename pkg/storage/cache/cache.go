package cache

import (
	"io"
	"math/rand/v2"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

const hextable string = "0123456789ABCDEF"

var seed = uint64(rand.Int64())

const DefaultSeparator byte = '|'

type KeyWriter interface {
	io.Writer
	io.StringWriter
	io.ByteWriter

	WriteUint64(uint64) error
	Break()
	BreakWith(byte)
}

var _ KeyWriter = &KeyBuilder{}

// KeyBuilder is a struct that hex-encodes each value written to it, and separates them with '|'.
// Segments are unable to collide across delimited boundaries. Tradeoff: keys are 2x larger than raw
// string concatenation, increasing cache memory usage.
type KeyBuilder struct {
	buf []byte
	pos int
}

func (builder *KeyBuilder) Reset() {
	builder.buf = builder.buf[:0]
	builder.pos = 0
}

func (builder *KeyBuilder) Grow(n int) {
	available := len(builder.buf) - builder.pos
	if n <= available {
		return
	}

	needed := n - available
	target := len(builder.buf) + needed
	if cap(builder.buf) >= target {
		builder.buf = builder.buf[:target]
		return
	}

	d := make([]byte, 0, target+target/2)
	d = d[:target]
	copy(d, builder.buf)
	builder.buf = d
}

func (builder *KeyBuilder) Write(bytes []byte) (int, error) {
	builder.Grow(len(bytes) * 2)

	var w int

	for _, b := range bytes {
		builder.buf[builder.pos] = hextable[b>>4]
		builder.pos++
		builder.buf[builder.pos] = hextable[b&0x0F]
		builder.pos++
		w++
	}
	return w, nil
}

func (builder *KeyBuilder) WriteByte(val byte) error {
	builder.Write([]byte{val})
	return nil
}

func (builder *KeyBuilder) WriteUint64(i uint64) error {
	builder.Write(unsafe.Slice((*byte)(unsafe.Pointer(&i)), 8))
	return nil
}

func (builder *KeyBuilder) WriteString(val string) (int, error) {
	return builder.Write(unsafe.Slice(unsafe.StringData(val), len(val)))
}

func (builder *KeyBuilder) BreakWith(sep byte) {
	builder.Grow(1)

	builder.buf[builder.pos] = sep
	builder.pos++
}

func (builder *KeyBuilder) Break() {
	builder.BreakWith(DefaultSeparator)
}

func (builder *KeyBuilder) Bytes() []byte {
	return builder.buf
}

func (builder *KeyBuilder) String() string {
	hex := builder.Bytes()
	return unsafe.String(unsafe.SliceData(hex), len(hex))
}

func (builder *KeyBuilder) Sum64() uint64 {
	digest := xxhash.NewWithSeed(seed)
	digest.Write(builder.buf)
	return digest.Sum64()
}
