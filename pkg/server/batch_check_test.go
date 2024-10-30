package server

import (
	"context"
	"errors"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/graph"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"testing"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/stretchr/testify/require"
)

func TestTransformCheckCommandErrorToBatchCheckError(t *testing.T) {
	errMsg := "oh_no"
	scenarios := map[string]struct {
		inputError     error
		expectedOutput *openfgav1.CheckError
	}{
		`test_invalid_relation_error`: {
			inputError: &commands.InvalidRelationError{Cause: errors.New(errMsg)},
			expectedOutput: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error},
				Message: errMsg,
			},
		},
		`test_invalid_tuple_error`: {
			inputError: &commands.InvalidTupleError{Cause: errors.New(errMsg)},
			expectedOutput: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_invalid_tuple},
				Message: errMsg,
			},
		},
		`test_resolution_depth_error`: {
			inputError: graph.ErrResolutionDepthExceeded,
			expectedOutput: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_authorization_model_resolution_too_complex},
				Message: graph.ErrResolutionDepthExceeded.Error(),
			},
		},
		`test_condition_error`: {
			inputError: condition.ErrEvaluationFailed,
			expectedOutput: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error},
				Message: condition.ErrEvaluationFailed.Error(),
			},
		},
		`test_throttled_error`: {
			inputError: &commands.ThrottledError{Cause: errors.New(errMsg)},
			expectedOutput: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error},
				Message: errMsg,
			},
		},
		`test_deadline_exceeded`: {
			inputError: context.DeadlineExceeded,
			expectedOutput: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InternalError{InternalError: openfgav1.InternalErrorCode_deadline_exceeded},
				Message: context.DeadlineExceeded.Error(),
			},
		},
		`test_generic_error`: {
			inputError: errors.New(errMsg),
			expectedOutput: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InternalError{InternalError: openfgav1.InternalErrorCode_internal_error},
				Message: errMsg,
			},
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			result := transformCheckCommandErrorToBatchCheckError(scenario.inputError)
			require.Equal(t, scenario.expectedOutput, result)
		})
	}
}

func TestTransformCheckResultToProto(t *testing.T) {
	var expectedResult = map[string]*openfgav1.BatchCheckSingleResult{}
	outcomes := map[string]*commands.BatchCheckOutcome{}
	duration := time.Millisecond * 5

	// Create two fake outcomes, one happy path and one error
	// These two make 100% coverage
	id1 := "abc123"
	outcomes[id1] = &commands.BatchCheckOutcome{
		CheckResponse: &graph.ResolveCheckResponse{
			Allowed: true,
		},
		Duration: duration,
	}

	id2 := "def456"
	outcomes[id2] = &commands.BatchCheckOutcome{
		Duration: duration,
		Err:      graph.ErrResolutionDepthExceeded,
	}

	// format the expected final output of the transform function
	expectedResult[id1] = &openfgav1.BatchCheckSingleResult{
		QueryDurationMs: wrapperspb.Int32(int32(duration.Milliseconds())),
		CheckResult:     &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true},
	}

	expectedResult[id2] = &openfgav1.BatchCheckSingleResult{
		QueryDurationMs: wrapperspb.Int32(int32(duration.Milliseconds())),
		CheckResult: &openfgav1.BatchCheckSingleResult_Error{
			Error: &openfgav1.CheckError{
				Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_authorization_model_resolution_too_complex},
				Message: graph.ErrResolutionDepthExceeded.Error(),
			},
		},
	}

	t.Run("test_transform_check_result_to_proto", func(t *testing.T) {
		result := transformCheckResultToProto(outcomes)
		require.Equal(t, expectedResult, result)
	})
}

func BenchmarkTransformCheckResultToProto(b *testing.B) {
	duration := time.Millisecond * 5

	outcomes := map[string]*commands.BatchCheckOutcome{
		"abc123": {
			CheckResponse: &graph.ResolveCheckResponse{
				Allowed: true,
			},
			Duration: duration,
		},
		"def456": {
			Err:      graph.ErrResolutionDepthExceeded,
			Duration: duration,
		},
	}

	b.Run("benchmark_transform_check_result_to_proto", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			transformCheckResultToProto(outcomes)
		}
	})
}
