package types

import (
	"reflect"
	"testing"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/stretchr/testify/require"
)

func mustParseIP(t *testing.T, s string) IPAddress {
	t.Helper()
	ip, err := ParseIPAddress(s)
	require.NoError(t, err)
	return ip
}

func TestParseIPAddress(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		ip, err := ParseIPAddress("192.168.0.1")
		require.NoError(t, err)
		require.Equal(t, "192.168.0.1", ip.addr.String())
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := ParseIPAddress("not-an-ip")
		require.Error(t, err)
	})
}

func TestIPAddress_ConvertToNative(t *testing.T) {
	ip := mustParseIP(t, "10.0.0.1")

	t.Run("to_ipaddress_type", func(t *testing.T) {
		out, err := ip.ConvertToNative(reflect.TypeOf(IPAddress{}))
		require.NoError(t, err)
		require.Equal(t, ip, out)
	})

	t.Run("to_string", func(t *testing.T) {
		out, err := ip.ConvertToNative(reflect.TypeOf(""))
		require.NoError(t, err)
		require.Equal(t, "10.0.0.1", out)
	})

	t.Run("unsupported_type_errors", func(t *testing.T) {
		_, err := ip.ConvertToNative(reflect.TypeOf(42))
		require.Error(t, err)
	})
}

func TestIPAddress_ConvertToType(t *testing.T) {
	ip := mustParseIP(t, "172.16.0.5")

	t.Run("to_string", func(t *testing.T) {
		val := ip.ConvertToType(types.StringType)
		require.Equal(t, types.String("172.16.0.5"), val)
	})

	t.Run("to_type", func(t *testing.T) {
		val := ip.ConvertToType(types.TypeType)
		require.Equal(t, ipaddrCelType, val)
	})

	t.Run("unsupported_type_returns_err", func(t *testing.T) {
		val := ip.ConvertToType(types.IntType)
		require.Equal(t, types.ErrType, val.Type())
	})
}

func TestIPAddress_Equal(t *testing.T) {
	ip := mustParseIP(t, "10.0.0.1")

	t.Run("equal", func(t *testing.T) {
		require.Equal(t, types.Bool(true), ip.Equal(mustParseIP(t, "10.0.0.1")))
	})

	t.Run("not_equal", func(t *testing.T) {
		require.Equal(t, types.Bool(false), ip.Equal(mustParseIP(t, "10.0.0.2")))
	})

	t.Run("other_not_ipaddress", func(t *testing.T) {
		val := ip.Equal(types.String("10.0.0.1"))
		require.Equal(t, types.NoSuchOverloadErr(), val)
	})
}

func TestIPAddress_TypeAndValue(t *testing.T) {
	ip := mustParseIP(t, "10.0.0.1")
	require.Equal(t, ipaddrCelType, ip.Type())
	require.Equal(t, ip, ip.Value())
}

func TestStringToIPAddress(t *testing.T) {
	t.Run("valid_string", func(t *testing.T) {
		val := stringToIPAddress(types.String("192.168.1.1"))
		ip, ok := val.(IPAddress)
		require.True(t, ok)
		require.Equal(t, "192.168.1.1", ip.addr.String())
	})

	t.Run("malformed_string_returns_err", func(t *testing.T) {
		val := stringToIPAddress(types.String("999.999.999.999"))
		require.Equal(t, types.ErrType, val.Type())
	})

	t.Run("non_string_arg_returns_err", func(t *testing.T) {
		val := stringToIPAddress(types.Int(5))
		// A non-string argument is an overload error.
		require.Equal(t, types.ErrType, val.Type())
	})
}

func TestIPAddress_CompileAndProgramOptions(t *testing.T) {
	ip := IPAddress{}
	require.NotEmpty(t, ip.CompileOptions())
	require.Empty(t, ip.ProgramOptions())
	require.NotNil(t, IPAddressEnvOption())
}

var _ ref.Val = IPAddress{}
