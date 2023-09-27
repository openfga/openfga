package types

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPrimitives(t *testing.T) {
	var tests = []struct {
		name          string
		paramType     ParameterType
		input         any
		output        any
		expectedError error
		repr          string
	}{
		{
			name:      "valid_any",
			paramType: AnyParamType,
			input:     "hello",
			output:    "hello",
			repr:      "any",
		},
		{
			name:      "valid_bool",
			paramType: BoolParamType,
			input:     true,
			output:    true,
			repr:      "bool",
		},
		{
			name:      "valid_int",
			paramType: IntParamType,
			input:     int64(10),
			output:    int64(10),
			repr:      "int",
		},
		{
			name:          "invalid_int_to_string",
			paramType:     StringParamType,
			input:         int64(10),
			output:        nil,
			repr:          "string",
			expectedError: fmt.Errorf("for string: unexpected type value '\"int64\"', expected 'string'"),
		},
		{
			name:      "valid_uint",
			paramType: UIntParamType,
			input:     uint64(10),
			output:    uint64(10),
			repr:      "uint",
		},
		{
			name:      "valid_double",
			paramType: DoubleParamType,
			input:     10.5,
			output:    float64(10.5),
			repr:      "double",
		},
		{
			name:      "valid_double_to_uint",
			paramType: UIntParamType,
			input:     float64(10.0),
			output:    uint64(10),
			repr:      "uint",
		},
		{
			name:      "valid_double_to_int",
			paramType: IntParamType,
			input:     float64(10.0),
			output:    int64(10),
			repr:      "int",
		},
		{
			name:          "invalid_double_to_uint",
			paramType:     UIntParamType,
			input:         float64(10.5),
			output:        nil,
			repr:          "uint",
			expectedError: fmt.Errorf("for uint: a uint value is expected, but found numeric value `10.5`"),
		},
		{
			name:          "invalid_negative_double_to_uint",
			paramType:     UIntParamType,
			input:         float64(-10.5),
			output:        nil,
			repr:          "uint",
			expectedError: fmt.Errorf("for uint: a uint value is expected, but found numeric value `-10.5`"),
		},
		{
			name:          "invalid_double_to_int",
			paramType:     IntParamType,
			input:         float64(10.5),
			output:        nil,
			repr:          "int",
			expectedError: fmt.Errorf("for int: a int value is expected, but found numeric value `10.5`"),
		},
		{
			name:      "valid_string",
			paramType: StringParamType,
			input:     "hello",
			output:    "hello",
			repr:      "string",
		},
		{
			name:      "valid_string_to_int",
			paramType: IntParamType,
			input:     "10",
			output:    int64(10),
			repr:      "int",
		},
		{
			name:          "invalid_string_to_bool",
			paramType:     BoolParamType,
			input:         "hello",
			output:        nil,
			repr:          "bool",
			expectedError: fmt.Errorf("for bool: unexpected type value '\"string\"', expected 'bool'"),
		},
		{
			name:          "invalid_string_to_double",
			paramType:     DoubleParamType,
			input:         "invalid",
			output:        nil,
			repr:          "double",
			expectedError: fmt.Errorf("for double: a float64 value is expected, but found invalid string value `invalid`"),
		},
		{
			name:      "valid_duration",
			paramType: DurationParamType,
			input:     "1h",
			output:    time.Duration(1 * time.Hour),
			repr:      "duration",
		},
		{
			name:          "invalid_duration",
			paramType:     DurationParamType,
			input:         "2sm",
			output:        nil,
			repr:          "duration",
			expectedError: fmt.Errorf("for duration: failed to parse duration string '2sm': time: unknown unit \"sm\" in duration \"2sm\""),
		},
		{
			name:      "valid_timestamp",
			paramType: TimestampParamType,
			input:     "1972-01-01T10:00:20.021Z",
			output:    time.Time(time.Date(1972, time.January, 1, 10, 0, 20, 21000000, time.UTC)),
			repr:      "timestamp",
		},
		{
			name:          "invalid_timestamp",
			paramType:     TimestampParamType,
			input:         "2023-0914",
			output:        nil,
			repr:          "timestamp",
			expectedError: fmt.Errorf("for timestamp: could not parse RFC 3339 formatted timestamp string `2023-0914`: parsing time \"2023-0914\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"14\" as \"-\""),
		},
		{
			name:      "valid_ipaddress",
			paramType: IPAddressType,
			input:     "127.0.0.1",
			output:    mustParseIPAddress("127.0.0.1"),
			repr:      "ipaddress",
		},
		{
			name:          "invalid_ipaddress",
			paramType:     IPAddressType,
			input:         "invalid",
			output:        nil,
			repr:          "ipaddress",
			expectedError: fmt.Errorf("for ipaddress: could not parse string as an ipaddress `invalid`: ParseAddr(\"invalid\"): unable to parse IP"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			converted, err := test.paramType.ConvertValue(test.input)

			if test.expectedError != nil {
				require.Error(t, err)
				require.EqualError(t, err, test.expectedError.Error())
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, test.output, converted)
			require.Equal(t, test.repr, test.paramType.String())
		})
	}
}

func mustParseIPAddress(ip string) IPAddress {
	addr, err := ParseIPAddress(ip)
	if err != nil {
		panic(err)
	}

	return addr
}
