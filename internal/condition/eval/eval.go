package eval

import (
	"context"
	"fmt"
	"strconv"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/metrics"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/typesystem"
)

var tracer = otel.Tracer("openfga/internal/condition/eval")

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
		start := time.Now()
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

		conditionResult, err := evaluableCondition.Evaluate(ctx, contextFields...)
		if err != nil {
			telemetry.TraceError(span, err)
			return nil, err
		}

		metrics.Metrics.ObserveEvaluationDuration(time.Since(start))
		metrics.Metrics.ObserveEvaluationCost(conditionResult.Cost)

		span.SetAttributes(attribute.Bool("condition_met", conditionResult.ConditionMet))
		span.SetAttributes(attribute.String("condition_cost", strconv.FormatUint(conditionResult.Cost, 10)))
		span.SetAttributes(attribute.StringSlice("condition_missing_params", conditionResult.MissingParameters))
		return &conditionResult, nil
	}

	return &condition.EvaluationResult{
		ConditionMet: true,
	}, nil
}
