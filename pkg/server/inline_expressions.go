package server

import (
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/condition"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
)

// enableInlineExpressions enforces the ExperimentalInlineExpressions gate on
// contextual tuples. Stored tuples with $expression are always evaluated (the
// flag guards writes, not reads). Contextual tuples, however, are
// request-supplied and must be rejected when the flag is off, because they
// were never persisted under flag control.
func (s *Server) enableInlineExpressions(storeID string, contextualTupleKeys []*openfgav1.TupleKey) error {
	if !s.featureFlagClient.Boolean(serverconfig.ExperimentalInlineExpressions, storeID) {
		for _, tk := range contextualTupleKeys {
			if condition.IsInlineExpression(tk.GetCondition().GetName()) {
				return serverErrors.ValidationError(
					fmt.Errorf("$expression requires the %q experimental feature flag", serverconfig.ExperimentalInlineExpressions),
				)
			}
		}
	}
	return nil
}
