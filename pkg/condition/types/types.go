package types

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
)

var CustomParamTypes = map[string][]cel.EnvOption{}

var paramTypeDefinitions = map[string]paramTypeDefinition{}

// typedParamValueConverter defines a signature that implementations can provide to enforce type enforcements
// over any values provided.
type typedParamValueConverter func(value any) (any, error)

// paramTypeDefinition represents a parameter type definition included in a relationship condition.
//
// For example, the following condition defines two parameter type definitions (user and color), and
// the 'user' parameter has a type that is a map<any> (a map with a generic type of any) and the 'color'
// parameter has a type that is a string.
//
//	condition favorite_color(user map<any>, color string) {
//	  user.favoriteColor == color
//	}
type paramTypeDefinition struct {

	// name is the name/keyword for the type (e.g. 'string', 'timestamp', 'duration', 'map', 'list', 'ipaddress')
	name string

	genericTypeCount uint

	toParameterType func(genericType []ParameterType) (*ParameterType, error)
}

func registerParamTypeWithGenerics(
	paramTypeKeyword string,
	genericTypeCount uint,
	toParameterType func(genericType []ParameterType) ParameterType,
) func(genericTypes ...ParameterType) (ParameterType, error) {

	paramTypeDefinitions[paramTypeKeyword] = paramTypeDefinition{
		name:             paramTypeKeyword,
		genericTypeCount: genericTypeCount,
		toParameterType: func(genericTypes []ParameterType) (*ParameterType, error) {
			if uint(len(genericTypes)) != genericTypeCount {
				return nil, fmt.Errorf("type `%s` requires %d generic types; found %d", paramTypeKeyword, genericTypeCount, len(genericTypes))
			}

			built := toParameterType(genericTypes)
			return &built, nil
		},
	}

	return func(genericTypes ...ParameterType) (ParameterType, error) {
		if uint(len(genericTypes)) != genericTypeCount {
			return ParameterType{}, fmt.Errorf("invalid number of parameters given to type constructor. expected: %d, found: %d", genericTypeCount, len(genericTypes))
		}

		return toParameterType(genericTypes), nil
	}
}

func registerParamType(
	paramTypeKeyword string,
	celType *cel.Type,
	typedParamConverter typedParamValueConverter,
) ParameterType {
	paramType := ParameterType{
		name:                paramTypeKeyword,
		celType:             celType,
		genericTypes:        nil,
		typedParamConverter: typedParamConverter,
	}

	paramTypeDefinitions[paramTypeKeyword] = paramTypeDefinition{
		name:             paramTypeKeyword,
		genericTypeCount: 0,
		toParameterType: func(genericTypes []ParameterType) (*ParameterType, error) {
			return &paramType, nil
		},
	}

	return paramType
}

func registerCustomParamType(
	paramTypeKeyword string,
	celType *cel.Type,
	typeConverter typedParamValueConverter,
	celOpts ...cel.EnvOption,
) ParameterType {
	CustomParamTypes[paramTypeKeyword] = celOpts
	return registerParamType(paramTypeKeyword, celType, typeConverter)
}

// ParameterType defines the canonical representation of parameter types supported in conditions.
type ParameterType struct {
	name                string
	celType             *cel.Type
	genericTypes        []ParameterType
	typedParamConverter typedParamValueConverter
}

func NewParameterType(
	name string,
	celType *cel.Type,
	generics []ParameterType,
	typedParamConverter typedParamValueConverter,
) ParameterType {
	return ParameterType{
		name,
		celType,
		generics,
		typedParamConverter,
	}
}

// CelType returns the underlying Google CEL type for the variable type.
func (pt ParameterType) CelType() *cel.Type {
	return pt.celType
}

func (pt ParameterType) String() string {

	if len(pt.genericTypes) > 0 {
		genericTypeStrings := make([]string, 0, len(pt.genericTypes))

		for _, genericType := range pt.genericTypes {
			genericTypeStrings = append(genericTypeStrings, genericType.String())
		}

		// e.g. map<int>
		return fmt.Sprintf("%s<%s>", pt.name, strings.Join(genericTypeStrings, ", "))
	}

	return pt.name
}

func (pt ParameterType) ConvertValue(value any) (any, error) {
	converted, err := pt.typedParamConverter(value)
	if err != nil {
		return nil, fmt.Errorf("for %s: %w", pt.String(), err)
	}

	return converted, nil
}
