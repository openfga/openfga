package types

import (
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

func DecodeParameterType(conditionParamType *openfgav1.ConditionParamTypeRef) (*ParameterType, error) {
	paramTypedef, ok := paramTypeDefinitions[conditionParamType.TypeName]
	if !ok {
		return nil, fmt.Errorf("unknown condition parameter type `%s`", conditionParamType.TypeName)
	}

	if len(conditionParamType.GenericTypes) != int(paramTypedef.genericTypeCount) {
		return nil, fmt.Errorf(
			"condition parameter type `%s` requires %d generic types; found %d",
			conditionParamType.TypeName,
			len(conditionParamType.GenericTypes),
			paramTypedef.genericTypeCount,
		)
	}

	genericTypes := make([]ParameterType, 0, paramTypedef.genericTypeCount)
	for _, encodedGenericType := range conditionParamType.GenericTypes {
		genericType, err := DecodeParameterType(encodedGenericType)
		if err != nil {
			return nil, err
		}

		genericTypes = append(genericTypes, *genericType)
	}

	return paramTypedef.toParameterType(genericTypes)
}
