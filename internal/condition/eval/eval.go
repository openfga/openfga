package eval

import (
	"fmt"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/metrics"
	"github.com/openfga/openfga/pkg/typesystem"
	"google.golang.org/protobuf/types/known/structpb"
)

// EvaluateTupleCondition returns a bool indicating if the provided tupleKey's condition (if any) was met.
func EvaluateTupleCondition(
	tupleKey *openfgav1.TupleKey,
	typesys *typesystem.TypeSystem,
	context *structpb.Struct,
) (*condition.EvaluationResult, error) {
	tupleCondition := tupleKey.GetCondition()
	conditionName := tupleCondition.GetName()
	if conditionName != "" {
		start := time.Now()

		evaluableCondition, ok := typesys.GetCondition(conditionName)
		if !ok {
			return nil, condition.NewEvaluationError(conditionName, fmt.Errorf("condition was not found"))
		}

		// merge both contexts
		contextFields := []map[string]*structpb.Value{
			{},
		}
		if context != nil {
			contextFields = []map[string]*structpb.Value{context.GetFields()}
		}

		tupleContext := tupleCondition.GetContext()
		if tupleContext != nil {
			contextFields = append(contextFields, tupleContext.GetFields())
		}

		conditionResult, err := evaluableCondition.Evaluate(contextFields...)
		if err != nil {
			return nil, err
		}

		metrics.Metrics.ObserveEvaluationDuration(time.Since(start))
		metrics.Metrics.ObserveEvaluationCost(conditionResult.Cost)

		return &conditionResult, nil
	}

	return &condition.EvaluationResult{
		ConditionMet: true,
	}, nil
}
