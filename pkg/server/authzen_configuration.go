package server

import (
	"context"
	"fmt"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"

	"github.com/openfga/openfga/internal/build"
)

// GetConfiguration returns PDP metadata and capabilities per AuthZEN spec section 13.
// This endpoint is NOT gated behind the experimental flag as it's needed for discovery.
// Following the AuthZEN spec's multi-tenant pattern, this endpoint is scoped to a specific
// store and returns absolute endpoint URLs for that store.
func (s *Server) GetConfiguration(ctx context.Context, req *authzenv1.GetConfigurationRequest) (*authzenv1.GetConfigurationResponse, error) {
	_, span := tracer.Start(ctx, "authzen.GetConfiguration")
	defer span.End()

	storeID := req.GetStoreId()

	return &authzenv1.GetConfigurationResponse{
		PolicyDecisionPoint: &authzenv1.PolicyDecisionPoint{
			Name:        "OpenFGA",
			Version:     build.Version,
			Description: "OpenFGA is a high-performance and flexible authorization system that supports Fine-Grained Authorization (FGA) and implements the AuthZEN specification.",
		},
		AccessEndpoints: &authzenv1.Endpoints{
			Evaluation:     fmt.Sprintf("/stores/%s/access/v1/evaluation", storeID),
			Evaluations:    fmt.Sprintf("/stores/%s/access/v1/evaluations", storeID),
			SubjectSearch:  fmt.Sprintf("/stores/%s/access/v1/search/subject", storeID),
			ResourceSearch: fmt.Sprintf("/stores/%s/access/v1/search/resource", storeID),
			ActionSearch:   fmt.Sprintf("/stores/%s/access/v1/search/action", storeID),
		},
		Capabilities: []string{
			"evaluation",
			"evaluations",
			"subject_search",
			"resource_search",
			"action_search",
		},
	}, nil
}
