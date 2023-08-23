package types

import (
	"fmt"

	"github.com/google/cel-go/cel"
)

var (
	AnyParamType       = registerParamType("any", cel.AnyType, anyTypeConverterFunc)
	BoolParamType      = registerParamType("bool", cel.BoolType, primitiveTypeConverterFunc[bool])
	StringParamType    = registerParamType("string", cel.StringType, primitiveTypeConverterFunc[string])
	IntParamType       = registerParamType("int", cel.IntType, numericTypeConverterFunc[int64])
	UIntParamType      = registerParamType("uint", cel.UintType, numericTypeConverterFunc[uint64])
	DoubleParamType    = registerParamType("double", cel.DoubleType, numericTypeConverterFunc[float64])
	DurationParamType  = registerParamType("duration", cel.DurationType, durationTypeConverterFunc)
	TimestampParamType = registerParamType("timestamp", cel.TimestampType, timestampTypeConverterFunc)
	MapParamType       = registerParamTypeWithGenerics("map", 1, func(genericTypes []ParameterType) ParameterType {
		return ParameterType{
			name:         "map",
			celType:      cel.MapType(cel.StringType, genericTypes[0].celType),
			genericTypes: genericTypes,
			typedParamConverter: func(value any) (any, error) {
				v, ok := value.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("map requires a map, found: %T", value)
				}

				converted := make(map[string]any, len(v))
				for key, item := range v {
					convertedItem, err := genericTypes[0].ConvertValue(item)
					if err != nil {
						return nil, fmt.Errorf("found an invalid value for key `%s`: %w", key, err)
					}

					converted[key] = convertedItem
				}

				return converted, nil
			},
		}
	})

	ListParamType = registerParamTypeWithGenerics("list", 1, func(genericTypes []ParameterType) ParameterType {
		return ParameterType{
			name:         "list",
			celType:      cel.ListType(genericTypes[0].celType),
			genericTypes: genericTypes,
			typedParamConverter: func(value any) (any, error) {
				v, ok := value.([]any)
				if !ok {
					return nil, fmt.Errorf("list requires a list, found: %T", value)
				}

				converted := make([]any, len(v))
				for index, item := range v {
					convertedItem, err := genericTypes[0].ConvertValue(item)
					if err != nil {
						return nil, fmt.Errorf("found an invalid list item at index `%d`: %w", index, err)
					}

					converted[index] = convertedItem
				}

				return converted, nil
			},
		}
	})
)
