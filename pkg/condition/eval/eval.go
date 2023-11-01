package eval

import (
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
		evaluableCondition := typesys.GetCondition(conditionName)
		conditionResult, err := evaluableCondition.Evaluate(
			context,
			tupleCondition.GetContext().AsMap(),
		)
		if err != nil {
			return false, err
		}

		if !conditionResult.ConditionMet {
			return false, nil
		}
	}

	return true, nil
}
