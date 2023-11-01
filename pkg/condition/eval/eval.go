package eval

import (
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/typesystem"
)

// TupleConditionMet returns a bool indicating if the provided tupleKey's condition (if any) was met.
func TupleConditionMet(
	tupleKey *openfgav1.TupleKey,
	typesys *typesystem.TypeSystem,
	context map[string]interface{},
) (bool, error) {
	tupleCondition := tupleKey.GetCondition()
	conditionName := tupleCondition.GetName()
	if conditionName != "" {
		evaluableCondition, ok := typesys.GetCondition(conditionName)
		if !ok {
			return false, fmt.Errorf("failed to evaluate relationship condition: condition '%s' was not found", conditionName)
		}
		conditionResult, err := evaluableCondition.Evaluate(
			context,
			tupleCondition.GetContext().AsMap(),
		)
		if err != nil {
			return false, fmt.Errorf("failed to evaluate relationship condition: %v", err)
		}

		if !conditionResult.ConditionMet {
			if len(conditionResult.MissingParameters) > 0 {
				missingParam := conditionResult.MissingParameters[0]
				return false, fmt.Errorf("failed to evaluate relationship condition: missing value for parameter '%s' in condition '%s'", missingParam, conditionName)
			}
			return false, nil
		}
	}

	return true, nil
}
