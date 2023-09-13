package types

import (
	"testing"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/require"
)

func TestPrimitives(t *testing.T) {
	var tests = []struct {
		name      string
		paramType ParameterType
		celType   *cel.Type
		input     any
		output    any
		repr      string
	}{
		{
			name:      "valid_any",
			paramType: AnyParamType,
			celType:   cel.AnyType,
			input:     "hello",
			output:    "hello",
			repr:      "any",
		},
		{
			name:      "valid_bool",
			paramType: BoolParamType,
			celType:   cel.BoolType,
			input:     true,
			output:    true,
			repr:      "bool",
		},
		{
			name:      "valid_int",
			paramType: IntParamType,
			celType:   cel.IntType,
			input:     int64(10),
			output:    int64(10),
			repr:      "int",
		},
		{
			name:      "valid_uint",
			paramType: UIntParamType,
			celType:   cel.UintType,
			input:     uint64(10),
			output:    uint64(10),
			repr:      "uint",
		},
		{
			name:      "valid_double",
			paramType: DoubleParamType,
			celType:   cel.DoubleType,
			input:     10.5,
			output:    float64(10.5),
			repr:      "double",
		},
		{
			name:      "valid_string",
			paramType: StringParamType,
			celType:   cel.StringType,
			input:     "hello",
			output:    "hello",
			repr:      "string",
		},
		{
			name:      "valid_duration",
			paramType: DurationParamType,
			celType:   cel.DurationType,
			input:     "1h",
			output:    time.Duration(1 * time.Hour),
			repr:      "duration",
		},
		{
			name:      "valid_timestamp",
			paramType: TimestampParamType,
			celType:   cel.TimestampType,
			input:     "1972-01-01T10:00:20.021Z",
			output:    time.Time(time.Date(1972, time.January, 1, 10, 0, 20, 21000000, time.UTC)),
			repr:      "timestamp",
		},
		{
			name:      "valid_ipaddress",
			paramType: IPAddressType,
			celType:   cel.ObjectType("IPAddress"),
			input:     "127.0.0.1",
			output:    mustParseIPAddress("127.0.0.1"),
			repr:      "unknown",
		},
		// {
		// 	name:      "valid_map_string",
		// 	paramType: mustMapParamType(StringParamType),
		// 	celType:   cel.MapType(cel.StringType, cel.StringType),
		// 	input:     map[string]string{"hello": "world"},
		// 	output:    map[string]string{"hello": "world"},
		// 	repr:      "TYPE_NAME_MAP<string>",
		// },
		// {
		// 	name:      "valid_list_string",
		// 	paramType: mustListParamType(StringParamType),
		// 	celType:   cel.ListType(cel.StringType),
		// 	input:     []string{"hello", "world"},
		// 	output:    []string{"hello", "world"},
		// 	repr:      "TYPE_NAME_LIST<string>",
		// },
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			converted, err := test.paramType.ConvertValue(test.input)
			require.Equal(t, test.output, converted)
			require.NoError(t, err)
		})
	}
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

func mustParseIPAddress(ip string) IPAddress {
	addr, err := ParseIPAddress(ip)
	if err != nil {
		panic(err)
	}

	return addr
}
