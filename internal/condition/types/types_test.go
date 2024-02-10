package types

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPrimitives(t *testing.T) {
	tests := []struct {
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
			name:      "invalid_int_to_string",
			paramType: StringParamType,
			input:     int64(10),
			output:    nil,
			repr:      "string",
			expectedError: fmt.Errorf(
				"expected type value 'string', but found 'int64'",
			),
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
			name:      "invalid_double_to_uint",
			paramType: UIntParamType,
			input:     float64(10.5),
			output:    nil,
			repr:      "uint",
			expectedError: fmt.Errorf(
				"expected a uint value, but found numeric value '10.5'",
			),
		},
		{
			name:      "invalid_negative_double_to_uint",
			paramType: UIntParamType,
			input:     float64(-10.5),
			output:    nil,
			repr:      "uint",
			expectedError: fmt.Errorf(
				"expected a uint value, but found numeric value '-10.5'",
			),
		},
		{
			name:      "invalid_double_to_int",
			paramType: IntParamType,
			input:     float64(10.5),
			output:    nil,
			repr:      "int",
			expectedError: fmt.Errorf(
				"expected an int value, but found numeric value '10.5'",
			),
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
			name:      "invalid_string_to_bool",
			paramType: BoolParamType,
			input:     "hello",
			output:    nil,
			repr:      "bool",
			expectedError: fmt.Errorf(
				"expected type value 'bool', but found 'string'",
			),
		},
		{
			name:      "invalid_string_to_double",
			paramType: DoubleParamType,
			input:     "invalid",
			output:    nil,
			repr:      "double",
			expectedError: fmt.Errorf(
				"expected a float64 value, but found invalid string value 'invalid'",
			),
		},
		{
			name:      "valid_duration",
			paramType: DurationParamType,
			input:     "1h",
			output:    1 * time.Hour,
			repr:      "duration",
		},
		{
			name:      "invalid_duration",
			paramType: DurationParamType,
			input:     "2sm",
			output:    nil,
			repr:      "duration",
			expectedError: fmt.Errorf(
				"expected a valid duration string, but found: '2sm'",
			),
		},
		{
			name:      "valid_timestamp",
			paramType: TimestampParamType,
			input:     "1972-01-01T10:00:20.021Z",
			output:    time.Date(1972, time.January, 1, 10, 0, 20, 21000000, time.UTC),
			repr:      "timestamp",
		},
		{
			name:      "invalid_timestamp",
			paramType: TimestampParamType,
			input:     "2023-0914",
			output:    nil,
			repr:      "timestamp",
			expectedError: fmt.Errorf(
				"expected RFC 3339 formatted timestamp string, but found '2023-0914'",
			),
		},
		{
			name:      "valid_ipaddress",
			paramType: IPAddressType,
			input:     "127.0.0.1",
			output:    mustParseIPAddress("127.0.0.1"),
			repr:      "ipaddress",
		},
		{
			name:      "invalid_ipaddress",
			paramType: IPAddressType,
			input:     "invalid",
			output:    nil,
			repr:      "ipaddress",
			expectedError: fmt.Errorf(
				"expected a well-formed IP address, but found: 'invalid'",
			),
		},
		{
			name:      "valid_map_string",
			paramType: mustMapParamType(StringParamType),
			input:     map[string]any{"hello": "world"},
			output:    map[string]any{"hello": "world"},
			repr:      "TYPE_NAME_MAP<string>",
		},
		{
			name:      "invalid_map",
			paramType: mustMapParamType(StringParamType),
			input:     map[int]any{123: "world"},
			expectedError: fmt.Errorf(
				"map requires a map, found: map[int]interface {}",
			),
			repr: "TYPE_NAME_MAP<string>",
		},
		{
			name:      "invalid_map_string",
			paramType: mustMapParamType(StringParamType),
			input:     map[string]any{"hello": 1},
			expectedError: fmt.Errorf(
				"found an invalid value for key 'hello': expected type value 'string', but found 'int'",
			),
			repr: "TYPE_NAME_MAP<string>",
		},
		{
			name:      "valid_list_string",
			paramType: mustListParamType(StringParamType),
			input:     []any{"hello", "world"},
			output:    []any{"hello", "world"},
			repr:      "TYPE_NAME_LIST<string>",
		},
		{
			name:      "invalid_list",
			paramType: mustListParamType(StringParamType),
			input:     "hello",
			expectedError: fmt.Errorf(
				"list requires a list, found: string",
			),
			repr: "TYPE_NAME_LIST<string>",
		},
		{
			name:      "invalid_list_string",
			paramType: mustListParamType(StringParamType),
			input:     []any{"hello", 1},
			expectedError: fmt.Errorf(
				"found an invalid list item at index `1`: expected type value 'string', but found 'int'",
			),
			repr: "TYPE_NAME_LIST<string>",
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

func mustMapParamType(genericTypes ...ParameterType) ParameterType {
	paramType, err := MapParamType(genericTypes...)
	if err != nil {
		panic(err)
	}
	return paramType
}

func mustListParamType(genericTypes ...ParameterType) ParameterType {
	paramType, err := ListParamType(genericTypes...)
	if err != nil {
		panic(err)
	}
	return paramType
}
