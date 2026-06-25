package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetricsInitialized(t *testing.T) {
	require.NotNil(t, Metrics)
	require.NotNil(t, Metrics.compilationTime)
	require.NotNil(t, Metrics.evaluationTime)
	require.NotNil(t, Metrics.evaluationCost)
}

func TestObserveMethodsDoNotPanic(t *testing.T) {
	require.NotPanics(t, func() {
		Metrics.ObserveCompilationDuration(5 * time.Millisecond)
		Metrics.ObserveCompilationDuration(0)
		Metrics.ObserveEvaluationDuration(250 * time.Millisecond)
		Metrics.ObserveEvaluationDuration(0)
		Metrics.ObserveEvaluationCost(0)
		Metrics.ObserveEvaluationCost(100)
	})
}
