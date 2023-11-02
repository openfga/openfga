package eval

import (
	"errors"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/condition"
	"github.com/openfga/openfga/pkg/typesystem"
)

var (
	ErrEvaluatingCondition = errors.New("condition evaluation failed")
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
		evaluableCondition := typesys.GetCondition(conditionName)

		contextSlice := []map[string]interface{}{
			context,
		}

		tupleContext := tupleCondition.GetContext()
		if tupleContext != nil {
			contextSlice = append(contextSlice, tupleContext.AsMap())
		}

		conditionResult, err := evaluableCondition.Evaluate(contextSlice...)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate relationship condition: %v", err)
		}

		return &conditionResult, nil
	}

	return &condition.EvaluationResult{
		ConditionMet: true,
	}, nil
}
