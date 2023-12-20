package eval

import (
	"context"
	"fmt"
	"strconv"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/types/known/structpb"
)

var tracer = otel.Tracer("openfga/internal/condition/eval")

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
	ctx, span := tracer.Start(ctx, "EvaluateTupleCondition")
	defer span.End()

	span.SetAttributes(attribute.String("tuple_key", tupleKey.String()))

	tupleCondition := tupleKey.GetCondition()
	conditionName := tupleCondition.GetName()
	if conditionName != "" {
		span.SetAttributes(attribute.String("condition_name", conditionName))

		evaluableCondition, ok := typesys.GetCondition(conditionName)
		if !ok {
			err := condition.NewEvaluationError(conditionName, fmt.Errorf("condition was not found"))
			telemetry.TraceError(span, err)
			return nil, err
		}

		span.SetAttributes(attribute.String("condition_expression", evaluableCondition.GetExpression()))

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

		conditionResult, err := evaluableCondition.EvaluateWithContext(ctx, contextFields...)
		if err != nil {
			telemetry.TraceError(span, err)
			return nil, err
		}

		conditionEvaluationCostHistogram.Observe(float64(conditionResult.Cost))

		span.SetAttributes(attribute.Bool("condition_met", conditionResult.ConditionMet))
		span.SetAttributes(attribute.String("condition_cost", strconv.FormatUint(conditionResult.Cost, 10)))
		span.SetAttributes(attribute.StringSlice("condition_missing_params", conditionResult.MissingParameters))
		return &conditionResult, nil
	}

	return &condition.EvaluationResult{
		ConditionMet: true,
	}, nil
}
