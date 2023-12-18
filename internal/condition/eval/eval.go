package eval

import (
	"context"
	"fmt"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/types/known/structpb"
)

var conditionEvaluationCostHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
	Namespace:                       build.ProjectName,
	Name:                            "condition_evaluation_cost",
	Help:                            "A histogram of the CEL evaluation cost of a Condition in a Relationship Tuple",
	Buckets:                         utils.LinearBuckets(0, config.DefaultMaxConditionEvaluationCost, 10),
	NativeHistogramBucketFactor:     1.1,
	NativeHistogramMaxBucketNumber:  config.DefaultMaxConditionEvaluationCost,
	NativeHistogramMinResetDuration: time.Hour,
})

// EvaluateTupleCondition returns a bool indicating if the provided tupleKey's condition (if any) was met.
func EvaluateTupleCondition(
	ctx context.Context,
	tupleKey *openfgav1.TupleKey,
	typesys *typesystem.TypeSystem,
	context *structpb.Struct,
) (*condition.EvaluationResult, error) {
	tupleCondition := tupleKey.GetCondition()
	conditionName := tupleCondition.GetName()
	if conditionName != "" {
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

		conditionResult, err := evaluableCondition.Evaluate(ctx, contextFields...)
		if err != nil {
			return nil, err
		}

		conditionEvaluationCostHistogram.Observe(float64(conditionResult.Cost))

		return &conditionResult, nil
	}

	return &condition.EvaluationResult{
		ConditionMet: true,
	}, nil
}
