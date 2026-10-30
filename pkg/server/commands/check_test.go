package commands

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/check"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestIsV2CheckTerminalError(t *testing.T) {
	t.Parallel()

	// v2Check funnels most internal errors through CheckCommandErrorToServerError,
	// so the classifier sees gRPC status errors rather than the original sentinels/types.
	// These cases mirror what that converter actually produces.
	tests := []struct {
		name     string
		err      error
		terminal bool
	}{
		{"nil", nil, false},
		{"raw_context_canceled", context.Canceled, true},
		{"raw_context_deadline_exceeded", context.DeadlineExceeded, true},
		{"server_request_cancelled", serverErrors.ErrRequestCancelled, true},
		{"server_request_deadline_exceeded", serverErrors.ErrRequestDeadlineExceeded, true},
		{"server_throttled_timeout", serverErrors.ErrThrottledTimeout, true},
		{"server_transaction_throttled", serverErrors.ErrTransactionThrottled, true},
		{
			"validation_error_from_check_ErrValidation",
			CheckCommandErrorToServerError(check.ErrValidation),
			true,
		},
		{
			"validation_error_from_check_ErrInvalidUser",
			CheckCommandErrorToServerError(check.ErrInvalidUser),
			true,
		},
		{
			"invalid_tuple_from_contextual_tuple_validation",
			CheckCommandErrorToServerError(&tuple.InvalidTupleError{
				Cause:    check.ErrValidation,
				TupleKey: &openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"},
			}),
			true,
		},
		{
			"non_terminal_random_error",
			errors.New("transient datastore failure"),
			false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.terminal, IsV2CheckTerminalError(tc.err))
		})
	}
}
