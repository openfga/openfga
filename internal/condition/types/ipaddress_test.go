package types

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/stretchr/testify/require"
)

func TestIPaddressCELBinaryBinding(t *testing.T) {
	addr, err := ParseIPAddress("192.168.1.1")
	require.NoError(t, err)

	mappedAddr, err := ParseIPAddress("::ffff:192.168.1.1")
	require.NoError(t, err)

	tests := []struct {
		name   string
		lhs    ref.Val
		rhs    ref.Val
		result ref.Val
	}{
		{
			name:   "ip_in_cidr",
			lhs:    addr,
			rhs:    types.String("192.168.1.0/24"),
			result: types.Bool(true),
		},
		{
			name:   "ip_not_in_cidr",
			lhs:    addr,
			rhs:    types.String("10.0.0.0/8"),
			result: types.Bool(false),
		},
		{
			// IPv4-mapped IPv6 form of 192.168.1.1 must match an IPv4 CIDR.
			name:   "ipv4_mapped_in_cidr",
			lhs:    mappedAddr,
			rhs:    types.String("192.168.1.0/24"),
			result: types.Bool(true),
		},
		{
			name:   "ipv4_mapped_not_in_cidr",
			lhs:    mappedAddr,
			rhs:    types.String("10.0.0.0/8"),
			result: types.Bool(false),
		},
		{
			// The same IPv4 range expressed as an IPv4-mapped IPv6 CIDR must
			// match both an IPv4-mapped address and a plain IPv4 address.
			name:   "ipv4_mapped_in_mapped_cidr",
			lhs:    mappedAddr,
			rhs:    types.String("::ffff:192.168.1.0/120"),
			result: types.Bool(true),
		},
		{
			name:   "ip_in_mapped_cidr",
			lhs:    addr,
			rhs:    types.String("::ffff:192.168.1.0/120"),
			result: types.Bool(true),
		},
		{
			name:   "ip_not_in_mapped_cidr",
			lhs:    addr,
			rhs:    types.String("::ffff:10.0.0.0/104"),
			result: types.Bool(false),
		},
		{
			name:   "missing_cidr",
			lhs:    addr,
			rhs:    types.Bool(true),
			result: types.NewErr("a CIDR string is required for comparison"),
		},
		{
			name:   "malformed_cidr",
			lhs:    addr,
			rhs:    types.String("malformed"),
			result: types.NewErr("'malformed' is a malformed CIDR string"),
		},
		{
			name:   "missing_ip",
			lhs:    types.String("10.0.0.1"),
			rhs:    types.String("10.0.0.0/8"),
			result: types.NewErr("an IPAddress parameter value is required for comparison"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			val := ipaddressCELBinaryBinding(test.lhs, test.rhs)
			require.Equal(t, test.result, val)
		})
	}
}

func TestIPAddressWithCompileOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		expression      string
		isErrorExpected bool
	}{
		{
			name:            "valid_ipaddress_function",
			expression:      `ipaddress("192.168.1.1")`,
			isErrorExpected: false,
		},
		{
			name:            "invalid_ipaddress_function",
			expression:      `ipaddress("not-an-ip")`,
			isErrorExpected: true,
		},
	}

	// Construct a CEL environment using only the compile options exposed by the IP address library. This verifies that
	// the library registers the declarations required for the `ipaddress` function to compile.
	env, err := cel.NewCustomEnv(ipaddrLib.CompileOptions()...)
	require.NoError(t, err)

	// With the CEL environment successfully constructed, verify that each test expression can be compiled, converted
	// into an executable CEL program, and evaluated.
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// Ensure the expression successfully compiles. Validation of the runtime behavior of `ipaddress` is covered
			// by the evaluation below.
			ast, issues := env.Compile(test.expression)
			require.NoError(t, issues.Err())

			// Ensure the compiled expression can be converted into an executable CEL program using only the compile
			// options exposed by the IP address library.
			prg, err := env.Program(ast)
			require.NoError(t, err)

			// Evaluate the compiled program and verify whether evaluation succeeds or fails as expected.
			_, _, err = prg.Eval(map[string]any{})
			if test.isErrorExpected {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
