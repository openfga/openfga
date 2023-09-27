package condition

import (
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/condition/types"
)

// castContextToTypedParameters takes a map of context and a map of condition parameter
// types and casts/converts the map into a map of typed values that can be evaluated
// with strong type guarantees.
func castContextToTypedParameters(
	contextMap map[string]any,
	conditionParameterTypes map[string]*openfgav1.ConditionParamTypeRef,
) (map[string]any, error) {
	if len(contextMap) == 0 {
		return nil, nil
	}

	if len(conditionParameterTypes) == 0 {
		return nil, fmt.Errorf("no parameters defined for the condition")
	}

	converted := make(map[string]any, len(contextMap))

	for key, value := range contextMap {
		paramTypeRef, ok := conditionParameterTypes[key]
		if !ok {
			continue
		}

		varType, err := types.DecodeParameterType(paramTypeRef)
		if err != nil {
			return nil, fmt.Errorf("failed to decode condition parameter type '%s': %v", paramTypeRef.TypeName, err)
		}

		convertedParam, err := varType.ConvertValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert context parameter '%s': %w", key, err)
		}

		converted[key] = convertedParam
	}

	return converted, nil
}
