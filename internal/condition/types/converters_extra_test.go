package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPrimitiveTypeConverterFunc(t *testing.T) {
	out, err := primitiveTypeConverterFunc[string]("hello")
	require.NoError(t, err)
	require.Equal(t, "hello", out)

	out, err = primitiveTypeConverterFunc[bool](true)
	require.NoError(t, err)
	require.Equal(t, true, out)

	_, err = primitiveTypeConverterFunc[string](42)
	require.Error(t, err)
}

func TestNumericTypeConverterFunc(t *testing.T) {
	t.Run("int64_from_string", func(t *testing.T) {
		out, err := numericTypeConverterFunc[int64]("42")
		require.NoError(t, err)
		require.Equal(t, int64(42), out)
	})

	t.Run("int64_from_float64", func(t *testing.T) {
		out, err := numericTypeConverterFunc[int64](float64(10))
		require.NoError(t, err)
		require.Equal(t, int64(10), out)
	})

	t.Run("int64_rejects_fractional", func(t *testing.T) {
		_, err := numericTypeConverterFunc[int64]("3.5")
		require.Error(t, err)
	})

	t.Run("uint64_from_string", func(t *testing.T) {
		out, err := numericTypeConverterFunc[uint64]("7")
		require.NoError(t, err)
		require.Equal(t, uint64(7), out)
	})

	t.Run("uint64_rejects_negative", func(t *testing.T) {
		_, err := numericTypeConverterFunc[uint64]("-1")
		require.Error(t, err)
	})

	t.Run("float64_from_string", func(t *testing.T) {
		// Must be exactly representable: the converter rejects values that lose
		// precision when narrowed to float64.
		out, err := numericTypeConverterFunc[float64]("3.5")
		require.NoError(t, err)
		require.InDelta(t, 3.5, out, 0.0001)
	})

	t.Run("float64_from_float64", func(t *testing.T) {
		out, err := numericTypeConverterFunc[float64](float64(2.25))
		require.NoError(t, err)
		require.InDelta(t, 2.25, out, 0.0001)
	})

	t.Run("invalid_string", func(t *testing.T) {
		_, err := numericTypeConverterFunc[int64]("not-a-number")
		require.Error(t, err)
	})

	t.Run("unsupported_value_type", func(t *testing.T) {
		_, err := numericTypeConverterFunc[int64](true)
		require.Error(t, err)
	})
}

func TestAnyTypeConverterFunc(t *testing.T) {
	out, err := anyTypeConverterFunc("anything")
	require.NoError(t, err)
	require.Equal(t, "anything", out)

	out, err = anyTypeConverterFunc(123)
	require.NoError(t, err)
	require.Equal(t, 123, out)
}

func TestDurationTypeConverterFunc(t *testing.T) {
	out, err := durationTypeConverterFunc("1h30m")
	require.NoError(t, err)
	require.Equal(t, 90*time.Minute, out)

	_, err = durationTypeConverterFunc("not-a-duration")
	require.Error(t, err)

	_, err = durationTypeConverterFunc(42)
	require.Error(t, err)
}

func TestTimestampTypeConverterFunc(t *testing.T) {
	out, err := timestampTypeConverterFunc("2023-01-02T15:04:05Z")
	require.NoError(t, err)
	require.Equal(t, time.Date(2023, 1, 2, 15, 4, 5, 0, time.UTC), out)

	_, err = timestampTypeConverterFunc("not-a-timestamp")
	require.Error(t, err)

	_, err = timestampTypeConverterFunc(42)
	require.Error(t, err)
}

func TestIPAddressTypeConverterFunc(t *testing.T) {
	t.Run("from_ipaddress_value", func(t *testing.T) {
		ip, err := ParseIPAddress("10.0.0.1")
		require.NoError(t, err)
		out, err := ipaddressTypeConverterFunc(ip)
		require.NoError(t, err)
		require.Equal(t, ip, out)
	})

	t.Run("from_valid_string", func(t *testing.T) {
		out, err := ipaddressTypeConverterFunc("192.168.1.1")
		require.NoError(t, err)
		require.IsType(t, IPAddress{}, out)
	})

	t.Run("from_invalid_string", func(t *testing.T) {
		_, err := ipaddressTypeConverterFunc("999.999.999.999")
		require.Error(t, err)
	})

	t.Run("from_non_string", func(t *testing.T) {
		_, err := ipaddressTypeConverterFunc(42)
		require.Error(t, err)
	})
}
