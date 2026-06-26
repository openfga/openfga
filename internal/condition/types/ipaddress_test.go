package types

import (
	"testing"

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
