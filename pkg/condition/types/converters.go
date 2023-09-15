package types

import (
	"fmt"
	"math/big"
	"reflect"
	"time"
)

func primitiveTypeConverterFunc[T any](value any) (any, error) {
	v, ok := value.(T)
	if !ok {
		return nil, fmt.Errorf("unexpected generic type value '%T', expected '%T'", reflect.TypeOf(value), *new(T))
	}

	return v, nil
}

func numericTypeConverterFunc[T int64 | uint64 | float64](value any) (any, error) {
	v, ok := value.(T)
	if ok {
		return v, nil
	}

	floatValue, ok := value.(float64)
	bigFloat := big.NewFloat(floatValue)
	if !ok {
		stringValue, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected generic type value '%T', expected '%T'", reflect.TypeOf(value), *new(T))
		}

		f, _, err := big.ParseFloat(stringValue, 10, 64, 0)
		if err != nil {
			return nil, fmt.Errorf("a %T value is expected, but found invalid string value `%v`", *new(T), value)
		}

		bigFloat = f
	}

	n := *new(T)
	switch any(n).(type) {
	case int64:
		if !bigFloat.IsInt() {
			return nil, fmt.Errorf("a int value is expected, but found numeric value `%s`", bigFloat.String())
		}

		numericValue, _ := bigFloat.Int64()
		return numericValue, nil

	case uint64:
		if !bigFloat.IsInt() {
			return nil, fmt.Errorf("a uint value is expected, but found numeric value `%s`", bigFloat.String())
		}

		numericValue, _ := bigFloat.Int64()
		if numericValue < 0 {
			return nil, fmt.Errorf("a uint value is expected, but found int64 value `%s`", bigFloat.String())
		}
		return uint64(numericValue), nil

	case float64:
		numericValue, _ := bigFloat.Float64()
		return numericValue, nil

	default:
		return nil, fmt.Errorf("unsupported numeric type in numerical parameter type conversion: %T", n)
	}
}

func anyTypeConverterFunc(value any) (any, error) {
	return value, nil
}

func durationTypeConverterFunc(value any) (any, error) {
	v, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("duration require a duration string, found: %T", value)
	}

	d, err := time.ParseDuration(v)
	if err != nil {
		return nil, fmt.Errorf("failed to parse duration string '%s': %w", v, err)
	}

	return d, nil
}

func timestampTypeConverterFunc(value any) (any, error) {
	v, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("timestamps requires a RFC 3339 formatted timestamp string, found: %T `%v`", value, value)
	}

	d, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return nil, fmt.Errorf("could not parse RFC 3339 formatted timestamp string `%s`: %w", v, err)
	}

	return d, nil
}

func ipaddressTypeConverterFunc(value any) (any, error) {
	ipaddr, ok := value.(IPAddress)
	if ok {
		return ipaddr, nil
	}

	v, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("an ipaddress string is required for an ipaddress parameter type, got: %T `%v`", value, value)
	}

	d, err := ParseIPAddress(v)
	if err != nil {
		return nil, fmt.Errorf("could not parse string as an ipaddress `%s`: %w", v, err)
	}

	return d, nil
}
