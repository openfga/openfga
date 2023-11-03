package eval

import (
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/condition"
	"github.com/openfga/openfga/pkg/typesystem"
)

// TupleConditionMet returns a bool indicating if the provided tupleKey's condition (if any) was met.
func EvaluateTupleCondition(
	tupleKey *openfgav1.TupleKey,
	typesys *typesystem.TypeSystem,
	context map[string]interface{},
) (*condition.EvaluationResult, error) {
	tupleCondition := tupleKey.GetCondition()
	conditionName := tupleCondition.GetName()
	if conditionName != "" {
		evaluableCondition, ok := typesys.GetCondition(conditionName)
		if !ok {
			return nil, condition.NewEvaluationError(conditionName, fmt.Errorf("condition was not found"))
		}

		// merge both contexts
		contextSlice := []map[string]interface{}{context}
		tupleContext := tupleCondition.GetContext()
		if tupleContext != nil {
			contextSlice = append(contextSlice, tupleContext.AsMap())
		}

		conditionResult, err := evaluableCondition.Evaluate(contextSlice...)
		if err != nil {
			return nil, err
		}

		return &conditionResult, nil
	}

	return &condition.EvaluationResult{
		ConditionMet: true,
	}, nil
}
