package encoder

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBase64EmptyDecode(t *testing.T) {
	encoder := NewBase64Encoder()

	got, err := encoder.Decode("")
	require.NoError(t, err)

	require.Equal(t, []byte{}, got)
}

func TestBase64EmptyEncode(t *testing.T) {
	encoder := NewBase64Encoder()

	got, err := encoder.Encode([]byte{})
	require.NoError(t, err)

	require.Empty(t, got)
}

func TestBase64DecodeEncode(t *testing.T) {
	encoder := NewBase64Encoder()
	want := "dGhlIHR2IHNob3cgJ3NjaGl0dCdzIGNyZWVrJyBpcyBncmVhdCBmdW4="

	decoded, err := encoder.Decode(want)
	require.NoError(t, err)

	got, err := encoder.Encode(decoded)
	require.NoError(t, err)

	require.Equal(t, want, got)
}

func TestBase64EncodeDecode(t *testing.T) {
	encoder := NewBase64Encoder()
	want := []byte("i hear people like the office")

	encoded, err := encoder.Encode(want)
	require.NoError(t, err)

	got, err := encoder.Decode(encoded)
	require.NoError(t, err)

	require.Equal(t, want, got)
}

func TestBase64Encode(t *testing.T) {
	encoder := NewBase64Encoder()
	data := []byte("the tv show 'schitt's creek' is great fun")
	want := "dGhlIHR2IHNob3cgJ3NjaGl0dCdzIGNyZWVrJyBpcyBncmVhdCBmdW4=" // according to https://www.base64encode.org/

	got, err := encoder.Encode(data)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
