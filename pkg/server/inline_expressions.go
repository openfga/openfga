package server

import (
	"context"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/condition"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
)

// enableInlineExpressions injects a per-request compilation cache into ctx and enforces
// the ExperimentalInlineExpressions gate on contextual tuples. Stored tuples with $expression
// are always evaluated (the flag guards writes, not reads). Contextual tuples, however, are
// request-supplied and must be rejected when the flag is off, because they were never persisted
// under flag control.
//
// When the flag is on, contextual $expression tuples are eagerly compiled into the cache so
// that (a) invalid expressions surface as a ValidationError at request entry rather than as
// a FatalError mid-evaluation, and (b) the cache is warm for all subsequent evaluations of
// those tuples within the request.
func (s *Server) enableInlineExpressions(ctx context.Context, storeID string, contextualTupleKeys []*openfgav1.TupleKey) (context.Context, error) {
	if !s.featureFlagClient.Boolean(serverconfig.ExperimentalInlineExpressions, storeID) {
		for _, tk := range contextualTupleKeys {
			if condition.IsInlineExpression(tk.GetCondition().GetName()) {
				return ctx, serverErrors.ValidationError(
					fmt.Errorf("$expression requires the %q experimental feature flag", serverconfig.ExperimentalInlineExpressions),
				)
			}
		}
	}
	ctx = condition.NewContextWithInlineConditionCache(ctx)
	for _, tk := range contextualTupleKeys {
		if condition.IsInlineExpression(tk.GetCondition().GetName()) {
			if _, err := condition.FromInlineExpression(ctx, tk); err != nil {
				return ctx, serverErrors.ValidationError(err)
			}
		}
	}
	return ctx, nil
}
